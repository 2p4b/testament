defmodule Testament.Publisher do
    alias Testament.Repo
    alias Testament.Store
    alias Signal.Transaction
    alias Testament.Publisher
    alias Signal.Events.Stage

    use GenServer

    require Logger

    defstruct [ index: 0, streams: %{} ]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(_opts) do
        index = Store.index()
        {:ok, struct(__MODULE__, [index: index])}
    end

    @impl true
    def handle_call(:index, _from, publisher) do
        {:reply, publisher.index, publisher}
    end

    @impl true
    def handle_call({:publish, %Transaction{}=transaction}, _from, publisher) do
        # { streams, events }
        %Transaction{staged: staged, snapshots: snapshots} = transaction
        initial = {%{}, []}
        {streams, events} = 
            staged
            |> Enum.map(&(Publisher.prepare_stage(publisher, &1)))
            |> Enum.reduce(initial, fn 
                {stream, position, ev_attrs}, {streams, events} -> 

                    events = events ++ List.wrap(ev_attrs)

                    streams = Map.put(streams, stream, position)

                    {streams, events}
            end)

        {index, events} = Publisher.number_events(publisher, events)

        preped_events = Publisher.prepare_event_attrs(events)

        preped_snapshots = Enum.map(snapshots, &(Map.from_struct(&1)))

        Repo.transaction(fn -> 
            Publisher.insert_events(preped_events)
            Publisher.record_states(preped_snapshots)
        end)

        publisher =
            %Publisher{publisher | index: index}
            |> Publisher.update_streams(streams)

        {:reply, events, publisher}
    end

    def index() do
        GenServer.call(__MODULE__, :index)
    end

    def publish(%Transaction{}=transaction) do
        GenServer.call(__MODULE__, {:publish, transaction})
        |> Enum.map(fn attrs -> 
            event = struct(Signal.Stream.Event, attrs)
            info = """

            [PUBLISHER] Published #{event.type}
            stream: #{event.stream}
            number: #{event.number}
            position: #{event.position}
            """
            Logger.info(info)
            Testament.broadcast_event(event)
            event
        end)
        :ok
    end

    def publish(%Stage{}=staged) do
        Transaction.new(staged)
        |> publish()
    end

    def publish(event) when is_struct(event) do
        stage_event(event)
        |> publish()
    end

    def prepare_stage(publisher, %Stage{}=stage) do
        %{stream: stream} =  stage

        position = get_stream_position(publisher, stream)

        %{events: events, version: version} = stage

        version = 
            if is_nil(version) do 
                position + length(events)
            else 
                version
            end

        prepped = 
            Enum.reduce(events, {[], position}, fn 
                event, {events, position} -> 
                    position = position + 1
                    attrs = 
                        Map.from_struct(event)
                        |> Map.put(:stream, stream)
                        |> Map.put(:uuid, Ecto.UUID.generate())
                        |> Map.put(:position, position)
                        
                    {events ++ List.wrap(attrs), position}
            end)

        {events, ^version} = prepped 

        {stream, version, events}
    end

    def stage_event(event) when is_struct(event) do
        link = [
            causation_id: UUID.uuid4(),
            correlation_id: UUID.uuid4()
        ]

        {_, stream} = Signal.Stream.stream(event)

        events = 
            event
            |> Signal.Events.Event.new(link)
            |> List.wrap()

        %Stage{stream: stream, events: events}
    end


    def get_stream_position(%Publisher{streams: streams}, id) do
        position =
            case Map.get(streams, id) do 
                nil ->  Store.stream_position(id)

                position -> position
            end

        if is_nil(position) do
            0
        else
            position
        end
    end

    def number_events(%{index: index}, events) do
        Enum.reduce(events, {index, []}, fn attrs, {index, events} -> 
            number = index + 1
            event = Map.put(attrs, :number, number)
            {number, events ++ List.wrap(event)}
        end)
    end

    def prepare_event_attrs(event_attrs) when is_list(event_attrs) do
        base = %Store.Event{}
        Enum.map(event_attrs, fn attrs -> 
            Store.Event.changeset(base, attrs)
            |> Map.get(:changes)
        end)
    end

    def insert_events(event_attrs) do
        Repo.insert_all(Store.Event, event_attrs)
    end

    def record_states(snapshot_attrs) do
        Repo.insert_all(Store.Snapshot, snapshot_attrs)
    end

    def update_streams(%Publisher{streams: streams}=publisher, updated_streams) do
        %Publisher{publisher | 
            streams: Map.merge(streams, updated_streams)
        }
    end

end

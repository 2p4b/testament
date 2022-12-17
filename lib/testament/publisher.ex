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

        snapshot_uuids = Enum.map(snapshots, &(Map.get(&1, :uuid)))

        recorded =
            cond do
                Enum.empty?(snapshot_uuids) -> %{}

                true ->
                    Store.Snapshot.query(uuid: snapshot_uuids)
                    |> Repo.all()
                    |> Enum.reduce(%{}, fn snapshot, acc -> 
                        Map.put(acc, snapshot.uuid, snapshot) 
                    end)
            end

        preped_snapshots = Enum.map(snapshots, fn snapshot -> 
            changeset =
                case Map.get(recorded, snapshot.uuid) do
                    nil  -> %Store.Snapshot{uuid: snapshot.uuid}
                    snapshot ->  snapshot
                end
                |> Store.Snapshot.changeset(Map.from_struct(snapshot))
            {snapshot.uuid, changeset}
        end)

        transaction =
            Ecto.Multi.new()
            |> insert_events(preped_events)
            |> record_states(preped_snapshots)

        {:ok, _} = Repo.transaction(transaction)

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

            [PUBLISHER] 
            published #{event.topic}
            stream: #{event.stream_id}
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
        %{stream: {stream_id, _stream}} =  stage

        position = get_stream_position(publisher, stream_id)

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
                        |> Map.put(:stream_id, stream_id)
                        |> Map.put(:uuid, Ecto.UUID.generate())
                        |> Map.put(:position, position)
                        
                    {events ++ List.wrap(attrs), position}
            end)

        {events, ^version} = prepped 

        {stream_id, version, events}
    end

    def stage_event(event) when is_struct(event) do
        link = [
            causation_id: UUID.uuid4(),
            correlation_id: UUID.uuid4()
        ]

        stream = Signal.Stream.stream(event)

        events = 
            event
            |> Signal.Events.Event.new(link)
            |> List.wrap()

        %Stage{stream: stream, events: events}
    end


    def get_stream_position(%Publisher{streams: streams}, id) do
        position =
            case Map.get(streams, id) do 
                nil -> Store.stream_position(id)
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
        end)
    end

    def insert_events(transaction, []) do
        transaction
    end
    def insert_events(transaction, [event_attrs | rest]) do
        id = event_attrs.changes.number
        transaction
        |> Ecto.Multi.insert(id, event_attrs)
        |> insert_events(rest)
    end

    def record_states(transaction, []) do
        transaction
    end
    def record_states(transaction, [{uuid, changeset} | rest]) do
        transaction
        |> Ecto.Multi.insert_or_update(uuid, changeset)
        |> record_states(rest)
    end

    def update_streams(%Publisher{streams: streams}=publisher, updated_streams) do
        %Publisher{publisher | 
            streams: Map.merge(streams, updated_streams)
        }
    end

end

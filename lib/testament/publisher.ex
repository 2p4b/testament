defmodule Testament.Publisher do
    alias Testament.Repo
    alias Testament.Store
    alias Testament.Publisher
    alias Signal.Events.Stage

    use GenServer

    defstruct [ index: 0, streams: %{} ]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(_opts) do
        {:ok, struct(__MODULE__, [])}
    end

    @impl true
    def handle_call(:index, _from, publisher) do
        {:reply, publisher.index, publisher}
    end

    @impl true
    def handle_call({:publish, staged}, _from, publisher) do
        # { nstreams, ustreams, events }
        initial = {[], [], []}
        {nstreams, ustreams, events} = 
            staged
            |> Enum.map(&(Publisher.prepare_stage(publisher, &1)))
            |> Enum.reduce(initial, fn 
                {stream, ev_attrs}, {nstreams, ustreams, events} -> 
                    nstreams =
                        if is_struct(stream) do
                            nstreams
                        else
                            nstreams ++ List.wrap(stream)   
                        end

                    ustreams =
                        if is_struct(stream) do
                            ustreams ++ List.wrap(stream)
                        else
                            ustreams
                        end

                    events = events ++ List.wrap(ev_attrs)

                    {nstreams, ustreams, events}
            end)

        {index, events} = Publisher.number_events(publisher, events)

        preped_events = Publisher.prepare_event_attrs(events)

        Repo.transaction(fn -> 
            Publisher.create_streams(nstreams)
            Publisher.advance_streams(ustreams)
            Publisher.insert_events(preped_events)
        end)

        publisher =
            %Publisher{publisher | index: index}
            |> Publisher.add_streams(nstreams)
            |> Publisher.update_streams(ustreams)

        {:reply, events, publisher}
    end

    def index() do
        GenServer.call(__MODULE__, :index)
    end

    def publish(%Stage{}=stage) do
        List.wrap(stage)
        |> publish()
    end

    def publish(event) when is_struct(event) do
        stage_event(event)
        |> publish()
    end

    def publish(staged) when is_list(staged) do
        GenServer.call(__MODULE__, {:publish, staged})
        |> Enum.map(fn attrs -> 
            event = struct(Signal.Stream.Event, attrs)
            Testament.broadcast("event", event)
            event
        end)
    end

    def prepare_stage(publisher, %Stage{}=stage) do
        %{stream: stage_stream} =  stage
        stream = get_stream(publisher, stage_stream)

        %{uuid: stream_id, position: position} = stream

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
                        |> Map.put(:stream, stage_stream)
                        |> Map.put(:uuid, Ecto.UUID.generate())
                        |> Map.put(:position, position)
                        |> Map.put(:stream_id, stream_id)
                        
                    {events ++ List.wrap(attrs), position}
            end)

        {events, ^version} = prepped 

        {Map.put(stream, :position, version), events}
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


    def get_stream(%Publisher{streams: streams}, {type, id}) do
        stream =
            case Map.get(streams, {type, id}) do 
                stream when is_struct(stream) ->
                    stream

                nil -> 
                    Store.Stream.query([id: id, type: type])
                    |> Repo.one()
            end

        if is_nil(stream) do
            %{
                id: id,
                type: type,
                uuid: Ecto.UUID.generate(),
                position: 0,
            }
        else
            stream
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

    def create_streams(stream_attrs) do
        Repo.insert_all(Store.Stream, stream_attrs)
    end

    def advance_streams(streams) do
        Enum.map(streams, fn stream -> 
            %{id: id, position: position} = stream
            Store.Stream.query(id: id)
            |> Repo.update_all(set: [position: position])
        end)
    end

    def add_streams(%Publisher{streams: streams}=publisher, new_streams) do
        streams = Enum.reduce(new_streams, streams, fn stream, streams -> 
            stream = struct(Store.Stream, stream)
            Map.put(streams, {stream.type, stream.id}, stream) 
        end)
        %Publisher{publisher | streams: streams}
    end

    def update_streams(%Publisher{streams: streams}=publisher, updated_streams) do
        streams = Enum.reduce(updated_streams, streams, fn stream, streams -> 
            Map.put(streams, {stream.type, stream.id}, stream) 
        end)
        %Publisher{publisher | streams: streams}
    end

end

defmodule Testament.Publisher do
    alias Testament.Repo
    alias Testament.Store
    alias Signal.Events.Stage

    use GenServer

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(opts) do
        {:ok, opts}
    end

    @impl true
    def handle_call({:publish, staged}, _from, recorder) do
        events = Store.record_events(staged)
        {:reply, events, recorder}
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
        Enum.map(staged, &Publisher.prepare_stage/1)
        #GenServer.call(__MODULE__, {:publish, staged})
    end

    def prepare_stage(%Stage{}=stage) do
        %{events: events, version: version} = staged

        %{uuid: stream_id, position: position} = get_or_create_stream(stage.stream)

        version = 
            if is_nil(version) do 
                position + length(events)
            else 
                version
            end

        initial = {[], position}

        prepped = 
            Enum.reduce(events, initial, fn event, {events, position} -> 
                position = position + 1
                attrs = 
                    Event.map_from_staged_event(event)
                    |> Map.put(:position, position)
                    |> Map.put(:stream_id, stream_id)
                    
                {events ++ List.wrap(attrs), position}
            end)

        {events, ^version} = prepped 

        {:ok, store_stream} = update_stream_position(store_stream, version)

        {store_stream, events}
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


    def get_or_create_steam({type, id}) do
        stream =
            Store.Stream.query([id: id, type: type])
            |> Repo.one()

        if is_nil(stream) do
            {:ok, stream} = Store.create_stream({type, id})
            stream
        else
            stream
        end
    end
end

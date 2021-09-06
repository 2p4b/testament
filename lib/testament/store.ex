defmodule Testament.Store do

    import Ecto.Query, warn: false
    alias Testament.Repo
    alias Testament.Store.Event
    alias Testament.Store.Handle
    alias Testament.Store.Snapshot

    def events_count() do
        query = from event in Event, select: count() 
        Repo.one(query)
    end

    def index() do
        index =
            from(e in Event, select: max(e.number))
            |> Repo.one()
        if is_nil(index) do
            0
        else
            index
        end
    end

    def get_event(number) do
        Event.query(number: number)
        |> Repo.one()
    end

    def update_handle(id, position) do
        res =
            %Handle{id: id, position: position}
            |> Handle.changeset(%{id: id, position: position})
            |> Repo.update()

        case res do
            {:ok, %Handle{position: position}} ->
                {:ok, position}

            error ->
                error
        end
    end

    def get_handle(id) when is_binary(id) do
        Handle.query([id: id])
        |> Repo.one()
    end

    def pull_events(topics, position, amount) 
    when is_list(topics) and is_integer(position) and is_integer(amount) do
        query = 
            from event in Event, 
            where: event.topic in ^topics,
            where: event.number > ^position,
            order_by: [asc: event.reduction],
            select: event,
            limit: ^amount

        query
        |> Repo.all() 
        |> Enum.map(&Event.to_stream_event/1)
    end


    def get_or_create_handle(id) do
        handle =
            Handle.query([id: id])
            |> Repo.one()

        if is_nil(handle) do
            {:ok, handle} = create_handle(id)
            handle
        else
            handle
        end
    end

    def create_stream({type, id}) when is_atom(type) and is_binary(id) do
        create_stream(%{type: type, id: id})
    end

    def create_handle(id, position \\ 0) do
        %Handle{}
        |> Handle.changeset(%{id: id, position: position})
        |> Repo.insert()
    end

    def stream_position(stream) do
        query =
            from [event: event] in Event.query([stream: stream]),
            select: max(event.position)

        Repo.one(query)
    end

    def update_handle_position(%Handle{}=handle, position) 
    when is_integer(position) do
        handle
        |> Handle.changeset(%{position: position})
        |> Repo.update()
    end

    def create_snapshot(%Signal.Snapshot{}=snapshot) do
        %Snapshot{}
        |> Snapshot.changeset(Map.from_struct(snapshot))
        |> Repo.insert()
    end

    def create_event(attrs) do
        %Event{}
        |> Event.changeset(attrs)
        |> Repo.insert()
    end

    def query_event_topics([]) do
        Event.query()
    end

    def query_event_topics(topics) when is_list(topics) do
        query_event_topics(Event.query(), topics)
    end

    def query_event_topics(query, [topic | topics]) do
        query =
            from [event: event] in query,  
            where: event.topic == ^topic

        Enum.reduce(topics, query, fn topic, query -> 
            from [event: event] in query,  
            or_where: event.topic == ^topic
        end) 
    end

    def query_event_streams([]) do
        Event.query()
    end

    def query_event_streams(streams) when is_list(streams) do
        query_event_streams(Event.query(), streams)
    end

    def query_event_streams(query, [stream | streams]) do
        root =
            from [event: event] in query,  
            where: [event: ^stream]

        Enum.reduce(streams, root, fn stream, query -> 
            from [event: event] in query,  
            or_where: [stream: ^stream]
        end) 
    end

    def query_events_from(number) when is_integer(number) do
        query_events_from(Event.query(), number)
    end

    def query_events_from(query, number) when is_integer(number) do
        from [event: event] in query,  
        where: event.number > ^number
    end

    def query_events_sort(order) when is_atom(order) do
        query_events_sort(Event.query(), order)
    end

    def query_events_sort(query, :asc) do
        from [event: event] in query,  
        order_by: [asc: event.number]
    end

    def query_events_sort(query, :desc) do
        from [event: event] in query,  
        order_by: [desc: event.number]
    end

end

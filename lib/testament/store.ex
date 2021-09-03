defmodule Testament.Store do

    import Ecto.Query, warn: false
    alias Testament.Repo
    alias Testament.Store.Event
    alias Testament.Store.Handle
    alias Testament.Store.Stream
    alias Testament.Store.Snapshot

    def events_count() do
        query = from event in Event, select: count() 
        Repo.one(query)
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

    def get_stream({type, id}) do
        Stream.query([id: id, type: type])
        |> Repo.one()
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

    def create_stream(attrs) when is_map(attrs) do
        attrs = 
            if Map.has_key?(attrs, :uuid) do
                attrs
            else
                Map.put(attrs, :uuid, Ecto.UUID.generate())
            end
        %Stream{}
        |> Stream.changeset(attrs)
        |> Repo.insert()
    end

    def create_handle(id, position \\ 0) do
        %Handle{}
        |> Handle.changeset(%{id: id, position: position})
        |> Repo.insert()
    end

    def update_stream_position(%Stream{}=stream, position) 
    when is_integer(position) do
        stream
        |> Stream.changeset(%{position: position})
        |> Repo.update()
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

    def query_event_streams(query, [{type, id} | streams]) do
        sub_query =
            from [stream: stream] in Stream.query(),  
            where: [type: ^type, id: ^id]

        sub_query =
            Enum.reduce(streams, sub_query, fn {type, id}, query -> 
                from [stream: stream] in query,  
                or_where: [type: ^type, id: ^id]
            end) 

        sub_query =
            from [stream: stream] in sub_query,
            select: stream.uuid

        from [event: event] in query,  
        where: event.stream_id in subquery(sub_query)
    end

    def query_events_from(number) when is_integer(number) do
        query_events_from(Event.query(), number)
    end

    def query_events_from(query, number) when is_integer(number) do
        from [event: event] in query,  
        where: event.number > ^number
    end

end

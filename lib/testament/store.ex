defmodule Testament.Store do

    import Ecto.Query, warn: false
    alias Testament.Repo
    alias Testament.Store.Event
    alias Testament.Store.Handle
    alias Testament.Store.Stream
    alias Testament.Store.Snapshot
    alias Signal.Aggregates.Aggregate

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

    def get_stream({type, id}) when is_atom(type) and is_binary(id) do
        Stream.query([id: id, type: type])
        |> Repo.one()
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

    def get_or_create_stream({type, id}) do
        stream =
            Stream.query([id: id, type: type])
            |> Repo.one()

        if is_nil(stream) do
            {:ok, stream} = create_stream({type, id})
            stream
        else
            stream
        end
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

    def create_stream({type, id}, position \\ 0) do
        attrs = %{id: id, type: type, position: position}
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

    def record_events(staged) when is_list(staged) do
        commit =
            Repo.transaction(fn -> 
                staged
                |> Enum.map(fn %{stream: stream} = staged -> 
                    store_stream = get_or_create_stream(stream)

                    insert_staged_events(store_stream, staged)
                end)
            end)

        case commit do
            {:ok, history} ->
                {:ok, history}

            error ->
                error
        end

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

    defp insert_staged_events(store_stream, staged) do

        %{events: events, version: version} = staged

        %{uuid: stream_id, position: position} = store_stream

        version = 
            if is_nil(version) do 
                position + length(events)
            else 
                version
            end

        initial = {[], position}

        preped = 
            Enum.reduce(events, initial, fn event, {events, position} -> 
                position = position + 1
                attrs = 
                    Event.map_from_staged_event(event)
                    |> Map.put(:position, position)
                    |> Map.put(:stream_id, stream_id)
                    
                {:ok, event} = create_event(attrs)

                {events ++ List.wrap(event), position}
            end)

        {events, ^version} = preped 

        {:ok, store_stream} = update_stream_position(store_stream, version)

        {store_stream, events}
    end

end

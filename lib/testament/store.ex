defmodule Testament.Store do

    import Ecto.Query, warn: false
    alias Testament.Repo
    alias Signal.Transaction
    alias Signal.Stream.Stage
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

    def commit_transaction(%Transaction{}=transaction) do
        %Transaction{staged: staged, snapshots: snapshots}=transaction

        prepped_events = prepare_staged_events(staged)
        prepped_snapshots = prepare_snapshots(snapshots) 

        {:ok, _} =
            Ecto.Multi.new()
            |> insert_events(prepped_events)
            |> record_states(prepped_snapshots)
            |> Repo.transaction()

        :ok
    end

    def get_handle(id) when is_binary(id) do
        Handle.query([id: id])
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

    def handler_position(handler) when is_binary(handler) do
        case get_handle(handler) do
            %Handle{position: position} ->
                position
            _ ->
                nil
        end
    end

    def handler_acknowledge(id, number) 
    when is_binary(id) and is_integer(number) do
        handler = 
            case Repo.get(Handle, id) do
                nil  -> %Handle{id: id, position: 0}
                handle ->  handle
            end

        result = 
            if handler.position < number do
                handler
                |> Handle.changeset(%{position: number, id: id})
                |> Repo.insert_or_update()
            else
                {:ok, handler}
            end

        case result do
            {:ok, %{position: position}} ->
                {:ok, position}

            error ->
                error
        end
    end

    def get_event(number) do
        Event.query(number: number)
        |> Repo.one()
    end

    def pull_events(topics, position, amount) 
    when is_list(topics) and is_integer(position) and is_integer(amount) do
        query = 
            from event in Event, 
            where: event.topic in ^topics,
            where: event.number > ^position,
            order_by: [asc: event.number],
            select: event,
            limit: ^amount

        query
        |> Repo.all() 
        |> Enum.map(&Event.to_signal_event/1)
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
            from [event: event] in Event.query([stream_id: stream]),
            select: max(event.index)
        Repo.one(query)
    end

    def update_handle_position(%Handle{}=handle, position) 
    when is_integer(position) do
        handle
        |> Handle.changeset(%{position: position})
        |> Repo.update()
    end

    def record_snapshot(%Signal.Snapshot{uuid: uuid}=snap) do
        case Repo.get(Snapshot, uuid) do
            nil  -> %Snapshot{uuid: uuid}
            snapshot ->  snapshot
        end
        |> Snapshot.changeset(Map.from_struct(snap))
        |> Repo.insert_or_update()
    end

    def delete_snapshot(id, opts \\ [])
    def delete_snapshot(id, _opts) when is_binary(id) do
        delete_snapshot({id, nil})
    end
    def delete_snapshot(iden, _opts) when is_tuple(iden) do
        query =
            case iden do
                {id, nil} ->
                    from shot in Snapshot.query([id: id]),
                    where: is_nil(shot.type)

                {id, type} when is_atom(type) ->
                    type = Signal.Helper.module_to_string(type)
                    Snapshot.query([id: id, type: type])

                {id, type} when is_binary(type) ->
                    Snapshot.query([id: id, type: type])
            end

        query =
            from [snapshot: shot] in query,
            order_by: [asc: shot.version]

        stream  = Repo.stream(query)

        Repo.transaction(fn ->
            stream
            |> Enum.each(fn snap ->
                Repo.delete(snap)
            end)
        end)
        :ok
    end

    def get_snapshot(iden, opts\\[])
    def get_snapshot(iden, opts) when is_binary(iden) do
        get_snapshot({iden, nil}, opts)
    end

    def get_snapshot(iden, opts) do
        version = Keyword.get(opts, :version, :max)
        query = 
            case iden do
                {id, nil} ->
                    from shot in Snapshot.query([id: id]),
                    where: is_nil(shot.type)

                {id, type} when is_atom(type) ->
                    type = Signal.Helper.module_to_string(type)
                    Snapshot.query([id: id, type: type])

                {id, type} when is_binary(type) ->
                    Snapshot.query([id: id, type: type])
            end

        query =
            case version do
                version when is_number(version) ->
                    from [snapshot: shot] in query,
                    where: shot.version == ^version

                :min ->
                    from [snapshot: shot] in query,
                    order_by: [asc: shot.version]

                _ ->
                    from [snapshot: shot] in query,
                    order_by: [desc: shot.version]
            end


        Repo.one(from [snapshot: shot] in query, limit: 1, select: shot)
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

    def query_event_topics(query, []) do
        query
    end

    def query_event_topics(query, topics) do
        from [event: event] in query,  
        where: event.topic in ^topics
    end

    def query_event_streams([]) do
        Event.query()
    end

    def query_event_streams(streams) when is_list(streams) do
        query_event_streams(Event.query(), streams)
    end

    def query_event_streams(query, []) do
        query
    end

    def query_event_streams(query, streams) do
        from [event: event] in query,  
        where: event.stream_id in ^streams
    end

    def query_events_upto(number) when is_integer(number) do
        query_events_upto(Event.query(), number)
    end

    def query_events_upto(query, number) when is_integer(number) do
        from [event: event] in query,  
        where: event.number <= ^number
    end

    def query_events_from(number) when is_integer(number) do
        query_events_from(Event.query(), number)
    end

    def query_events_from(query, number) when is_integer(number) do
        from [event: event] in query,  
        where: event.number >= ^number
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

    def list_events(opts \\ []) do
        opts
        |> build_store_query()
        |> Repo.all()
    end

    def read_events(func, opts \\ []) do
        query = build_store_query(opts)
        Repo.transaction(fn -> 
            query
            |> Repo.stream(max_rows: 10)
            |> Enum.find_value(nil, fn event -> 
                case Kernel.apply(func, [event]) do
                    :stop -> true
                    _ -> false
                end
            end)
        end, [timeout: :infinity])
        :ok
    end

    def build_store_query(opts \\ []) do
        streams = Keyword.get(opts, :streams, [])
        topics = Keyword.get(opts, :topics, [])
        range = Keyword.get(opts, :range, [1])

        query = 
            streams
            |> query_event_streams()
            |> query_event_topics(topics)

        [lower, upper, direction] = 
            Signal.Store.Helper.range(range)

        query = 
            if is_integer(lower) do
                query
                |> query_events_from(lower)
            else
                query
            end

        query = 
            if is_integer(upper) do
                query
                |> query_events_upto(upper)
            else
                query
            end

        query
        |> query_events_sort(direction)

    end

    def prepare_staged_events(staged) when is_list(staged) do
        staged
        |> Enum.reduce([], fn %Stage{events: events}, acc -> 
            acc ++ Enum.map(events, &Event.changeset/1)
        end)
    end

    def prepare_snapshots(snapshots) when is_list(snapshots) do
        snapshot_uuids = Enum.map(snapshots, &(Map.get(&1, :uuid)))

        recorded =
            cond do
                Enum.empty?(snapshot_uuids) -> %{}

                true ->
                    Snapshot.query(uuid: snapshot_uuids)
                    |> Repo.all()
                    |> Enum.reduce(%{}, fn snapshot, acc -> 
                        Map.put(acc, snapshot.uuid, snapshot) 
                    end)
            end

        Enum.map(snapshots, fn snapshot -> 
            changeset =
                case Map.get(recorded, snapshot.uuid) do
                    nil  -> %Snapshot{uuid: snapshot.uuid}
                    snapshot ->  snapshot
                end
                |> Snapshot.changeset(Map.from_struct(snapshot))
            {snapshot.uuid, changeset}
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
end

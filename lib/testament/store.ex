defmodule Testament.Store do

    import Ecto.Query, warn: false
    alias Signal.Transaction
    alias Signal.Stream.Stage
    alias Testament.Store.Event
    alias Testament.Store.Effect
    alias Testament.Store.Handle
    alias Testament.Store.Snapshot

    def events_count(repo) do
        query = from event in Event, select: count() 
        repo.one(query)
    end

    def index(repo) do
        index =
            from(e in Event, select: max(e.number))
            |> repo.one()
        if is_nil(index) do
            0
        else
            index
        end
    end

    def get_effect(repo, uuid) do
        repo.get(Effect, uuid)
    end

    def save_effect(repo, %Signal.Effect{uuid: uuid}=effect) do
        result =
            case get_effect(repo, uuid) do
                nil  -> %Effect{}
                effect -> effect
            end
            |> Effect.changeset(effect)
            |> repo.insert_or_update()

        case result do
            {:ok, _} ->
                :ok
            error ->
                error
        end
    end

    def list_effects(repo, namespace) do
        [namespace: namespace]
        |> Effect.query()
        |> repo.all()
    end

    def delete_effect(repo, uuid) do
        {_, _} = 
            [uuid: uuid]
            |> Effect.query()
            |> repo.delete_all()
        :ok
    end

    def commit_transaction(repo, %Transaction{}=transaction) do
        %Transaction{staged: staged}=transaction
        prepped_events = prepare_staged_events(staged)
        {:ok, _} =
            Ecto.Multi.new()
            |> insert_events(prepped_events)
            |> repo.transaction()

        :ok
    end

    def get_handle(repo, id) when is_binary(id) do
        Handle.query([id: id])
        |> repo.one()
    end

    def update_handle(repo, id, position) do
        res =
            %Handle{id: id, position: position}
            |> Handle.changeset(%{id: id, position: position})
            |> repo.update()

        case res do
            {:ok, %Handle{position: position}} ->
                {:ok, position}

            error ->
                error
        end
    end

    def handler_position(repo, handler) when is_binary(handler) do
        case get_handle(repo, handler) do
            %Handle{position: position} ->
                position
            _ ->
                nil
        end
    end

    def handler_acknowledge(repo, id, number) 
    when is_binary(id) and is_integer(number) do
        handler = 
            case repo.get(Handle, id) do
                nil  -> %Handle{id: id, position: 0}
                handle ->  handle
            end

        result = 
            if handler.position < number do
                handler
                |> Handle.changeset(%{position: number, id: id})
                |> repo.insert_or_update()
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

    def get_event(repo, number) do
        Event.query(number: number)
        |> repo.one()
    end

    def pull_events(repo, topics, position, amount) 
    when is_list(topics) and is_integer(position) and is_integer(amount) do
        query = 
            from event in Event, 
            where: event.topic in ^topics,
            where: event.number > ^position,
            order_by: [asc: event.number],
            select: event,
            limit: ^amount

        query
        |> repo.all() 
        |> Enum.map(&Event.to_signal_event/1)
    end


    def get_or_create_handle(repo, id) do
        handle =
            Handle.query([id: id])
            |> repo.one()

        if is_nil(handle) do
            {:ok, handle} = create_handle(repo, id)
            handle
        else
            handle
        end
    end

    def create_stream({type, id}) when is_atom(type) and is_binary(id) do
        create_stream(%{type: type, id: id})
    end

    def create_handle(repo, id, position \\ 0) do
        %Handle{}
        |> Handle.changeset(%{id: id, position: position})
        |> repo.insert()
    end

    def stream_position(repo, stream) do
        query =
            from [event: event] in Event.query([stream_id: stream]),
            select: max(event.position)
        repo.one(query)
    end

    def update_handle_position(repo, %Handle{}=handle, position) 
    when is_integer(position) do
        handle
        |> Handle.changeset(%{position: position})
        |> repo.update()
    end

    def record_snapshot(repo, %Signal.Snapshot{id: id}=snap) do
        case repo.get(Snapshot, id) do
            nil  -> %Snapshot{id: id}
            snapshot ->  snapshot
        end
        |> Snapshot.changeset(Map.from_struct(snap))
        |> repo.insert_or_update()
    end

    def delete_snapshot(repo, id, opts\\[])
    def delete_snapshot(repo, id, _opts) when is_binary(id) do
        delete_snapshot(repo, {id, nil})
    end
    def delete_snapshot(repo, iden, _opts) when is_tuple(iden) do
        query =
            case iden do
                {id, _type} ->
                    Snapshot.query([id: id])
            end

        query =
            from [snapshot: shot] in query,
            order_by: [asc: shot.version]

        stream  = repo.stream(query)

        repo.transaction(fn ->
            stream
            |> Enum.each(fn snap ->
                repo.delete(snap)
            end)
        end)
        :ok
    end

    def get_snapshot(repo, iden, opts\\[])
    def get_snapshot(repo, iden, opts) when is_binary(iden) do
        get_snapshot(repo, {iden, nil}, opts)
    end

    def get_snapshot(repo, iden, opts) do
        version = Keyword.get(opts, :version, :max)
        query = 
            case iden do
                {id, _type} ->
                    Snapshot.query([id: id])
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


        repo.one(from [snapshot: shot] in query, limit: 1, select: shot)
    end

    def create_event(repo, attrs) do
        %Event{}
        |> Event.changeset(attrs)
        |> repo.insert()
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
        query_events_upto(Event.query(), number, :number)
    end

    def query_events_upto(query, value, :number) when is_integer(value) do
        from [event: event] in query,  
        where: event.position <= ^value
    end

    def query_events_upto(query, value, :position) when is_integer(value) do
        from [event: event] in query,  
        where: event.position <= ^value
    end

    def query_events_from(number) when is_integer(number) do
        query_events_from(Event.query(), number, :number)
    end

    def query_events_from(query, value, :number) when is_integer(value) do
        from [event: event] in query,  
        where: event.number >= ^value
    end

    def query_events_from(query, value, :position) when is_integer(value) do
        from [event: event] in query,  
        where: event.position >= ^value
    end

    def query_events_sort(order) when is_atom(order) do
        query_events_sort(Event.query(), order)
    end

    def query_events_sort(query, order, index\\:number) do
        order_by = List.wrap({order, index})
        from query, order_by: ^order_by
    end

    def list_events(repo, opts\\[]) do
        opts
        |> build_store_query()
        |> repo.all()
    end

    def list_stream_events(repo, sid, opts\\[]) do
        params = [streams: List.wrap(sid), index: :position]
        opts
        |> Keyword.merge(params)
        |> build_store_query()
        |> repo.all()
    end

    def read_events(repo, func, opts\\[]) do
        query = build_store_query(opts)
        repo.transaction(fn -> 
            query
            |> repo.stream(max_rows: 10)
            |> Enum.find_value(nil, fn event -> 
                case Kernel.apply(func, [event]) do
                    :stop -> true
                    _ -> false
                end
            end)
        end, [timeout: :infinity])
        :ok
    end

    def read_stream_events(repo, sid, func, opts\\[]) do
        params = [streams: List.wrap(sid), index: :position]
        query = 
            opts
            |> Keyword.merge(params)
            |> build_store_query()
        repo.transaction(fn -> 
            query
            |> repo.stream(max_rows: 10)
            |> Enum.find_value(nil, fn event -> 
                case Kernel.apply(func, [event]) do
                    :stop -> true
                    _ -> false
                end
            end)
        end, [timeout: :infinity])
        :ok
    end

    def build_store_query(opts\\[]) do
        streams = Keyword.get(opts, :streams, [])
        topics = Keyword.get(opts, :topics, [])
        range = Keyword.get(opts, :range, [1])
        type = Keyword.get(opts, :index, :number)

        query = 
            streams
            |> query_event_streams()
            |> query_event_topics(topics)

        [lower, upper, direction] = 
            Signal.Store.Helper.range(range)

        query = 
            if is_integer(lower) do
                query
                |> query_events_from(lower, type)
            else
                query
            end

        query = 
            if is_integer(upper) do
                query
                |> query_events_upto(upper, type)
            else
                query
            end

        query
        |> query_events_sort(direction, type)
    end

    def prepare_staged_events(staged) when is_list(staged) do
        staged
        |> Enum.reduce([], fn %Stage{events: events}, acc -> 
            acc ++ Enum.map(events, &Event.changeset/1)
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
end


defmodule Testament.Subscription.Broker do

    use GenServer
    alias Testament.Repo
    alias Testament.Store
    alias Signal.Stream.Event
    alias Testament.Store.Handle
    alias Testament.Subscription.Broker
    alias Testament.Subscription.Supervisor

    require Logger

    defstruct [
        id: nil,
        worker: nil,
        cursor: 0, 
        buffer: [],
        handle: nil,
        position: 0,
        anonymous: true,
        subscriptions: [],
        topics: [],
        streams: [],
    ]

    @doc """
    Starts in memory store.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name, __MODULE__)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        id = Keyword.get(opts, :id)
        anonymous = 
            case Keyword.get(opts, :name, __MODULE__) do
                {:via, _, {_, _id, anonymous}} ->
                    anonymous

                _ -> 
                    false
            end

        handle = 
            if anonymous do
                %Handle{id: id, position: 0}
            else
                case Store.get_handle(id) do
                    %Handle{}=handle -> 
                        handle
                    nil ->
                        %Handle{id: id, position: 0}
                end
            end
        {:ok, struct(__MODULE__, id: id, handle: handle, anonymous: anonymous)}
    end

    @impl true
    def handle_call({:state, prop}, _from, %Broker{}=store) do
        {:reply, Map.get(store, prop), store} 
    end

    @impl true
    def handle_call(:subscription, {pid, _ref}, %Broker{subscriptions: subs}=store) do
        subscription = Enum.find(subs, &(Map.get(&1, :pid) == pid))
        {:reply, subscription, store} 
    end

    @impl true
    def handle_call({:subscribe, opts}, {pid, _ref}=from, %Broker{}=store) do
        %Broker{subscriptions: subscriptions} = store
        subscription = Enum.find(subscriptions, &(Map.get(&1, :pid) == pid))

        if is_nil(subscription) do
            subscription = create_subscription(store, pid, opts)
            GenServer.reply(from, {:ok, subscription})

            subscriptions =
                subscription
                |> List.wrap()
                |> Enum.concat(subscriptions)

            {topics, streams} = collect_topics_and_streams(subscriptions)

            broker = %Broker{store | 
                topics: topics, 
                streams: streams,
                subscriptions: subscriptions, 
            }

            broker = start_worker_stream(broker)

            {:noreply, broker} 
        else
            {:noreply, subscription, store}
        end
    end

    @impl true
    def handle_call(:unsubscribe, {pid, _ref}, %Broker{}=store) do
        subscriptions = Enum.filter(store.subscriptions, fn %{pid: spid} -> 
            spid != pid 
        end)
        {:reply, :ok, %Broker{store | subscriptions: subscriptions}} 
    end

    @impl true
    def handle_info({:broadcast, %{number: number}=event}, %Broker{}=broker) do
        %Broker{subscriptions: subscriptions} = broker
        index = Enum.find_index(subscriptions, fn sub -> 
            handle?(sub, event)
        end)

        if is_nil(index) do
            {:noreply, broker}
        else
            info = """

            [BROKER] published #{inspect(event.type)}
            """
            Logger.info(info)
            subscription = 
                Enum.at(subscriptions, index)
                |> Map.put(:syn, number)
            send(subscription.pid, event)
            subs = List.update_at(subscriptions, index, subscription)
            {:noreply, %Broker{broker | subscriptions: subs}}
        end
    end

    @impl true
    def handle_info({worker_ref, :finished}, %Broker{}=broker) do
        fallen = 
            build_query(broker) 
            |> Repo.all()
            |> Enum.map(&Event.to_stream_event/1)
                
        Testament.listern("event")
        {:noreply, %Broker{broker | buffer: fallen}}
    end

    @impl true
    def handle_info({:DOWN, ref, :process, _pid, _status}, %Broker{}=broker) do
        IO.inspect(ref, label: "down")
        {:noreply, broker}
    end

    @impl true
    def handle_cast({:ack, pid, number}, %Broker{}=broker) do
        %Broker{handle: handle, anonymous: anonymous}=broker
        %{position: position} = handle

        store = handle_ack(broker, pid, number)

        if anonymous do
            {:noreply, broker}
        else
            %{subscriptions: subs} = broker

            %{ack: ack} = Enum.max_by(subs, &(Map.get(&1, :ack)), fn -> 
                %{ack: position} 
            end)

            if ack > position do
                {:ok, handle} =
                    Handle.changeset(handle, %{position: ack})
                    |> Repo.insert_or_update()

                {:noreply, %Broker{broker | handle: handle}}
            else
                {:noreply, store}
            end
        end
    end

    defp handle?(%{syn: syn, ack: ack}, _ev)
    when syn != ack do
        false
    end

    defp handle?(%{handle: handle, syn: syn, ack: ack}, _event)
    when (not is_nil(handle)) and (syn > ack) do
        false
    end

    defp handle?(%{from: position}, %{number: number})
    when is_integer(position) and position > number do
        false
    end

    defp handle?(%{stream: s_stream, topics: topics}, %{stream: e_stream}=event) do

        %{topic: topic} = event
        {e_stream_type, _stream_id} = e_stream

        valid_stream =
            cond do
                # All streams
                is_nil(s_stream) ->
                    true

                # Same stream type 
                is_atom(s_stream) ->
                    s_stream == e_stream_type

                # Same stream 
                is_tuple(s_stream) ->
                    e_stream == s_stream

                true ->
                    false
            end

        valid_topic =
            if length(topics) == 0 do
                true
            else
                if topic in topics do
                    true
                else
                    false
                end
            end

        if valid_stream and valid_topic do
            true
        else
            false
        end
    end


    defp create_subscription(%Broker{handle: handle}, pid, opts) do
        from = Keyword.get(opts, :from, handle.position)
        topics = Keyword.get(opts, :topics, [])
        stream = Keyword.get(opts, :stream, nil)
        %{
            pid: pid,
            ack: from,
            syn: from,
            from: from,
            stream: stream,
            topics: topics,
        }
    end

    defp handle_ack(%Broker{}=store, pid, number) do
        %Broker{subscriptions: subscriptions} = store
        index = Enum.find_index(subscriptions, &(Map.get(&1, :pid) == pid and Map.get(&1, :syn) == number))
        if is_nil(index) do
            store
        else
            subscriptions = List.update_at(subscriptions, index, fn subscription -> 
                Map.put(subscription, :ack, number)
            end)
            %Broker{store| subscriptions: subscriptions}
            |> sched_next()
        end
    end

    def build_query(%Broker{streams: streams, topics: topics, subscriptions: subs}) do
        %{syn: position} = 
            Enum.max_by(subs, fn %{syn: syn} -> syn end, fn -> %{syn: 0} end) 

        build_query(streams, topics)
        |> Store.query_events_from(position)
        |> Store.query_events_sort(:asc)
    end

    def build_query([], []) do
        Store.query_event_topics([])
    end

    def build_query([], topics) do
        Store.query_event_topics(topics)
    end

    def build_query(streams, []) do
        Store.query_event_streams(streams)
    end

    def build_query(streams, topics) do
        Store.query_event_streams(streams)
        |> Store.query_event_topics(topics)
    end

    def collect_topics_and_streams(subscriptions) do
        Enum.reduce(subscriptions, {[],[]}, fn 
            %{stream: stream, topics: topic}, {topics, streams} -> 
                topics = Enum.uniq(topics ++ topic)
                streams = Enum.uniq(streams ++ List.wrap(stream))
                {topics, streams}
        end)
    end

    def start_worker_stream(%Broker{worker: worker}=broker) do
        bpid = self()

        if not(is_nil(worker)) do
            send(worker.pid, :stop)
        end

        query = build_query(broker)
        stream = Repo.stream(query)

        worker =
            Task.async(fn -> 
                {:ok, resp} =
                    Repo.transaction(fn -> 
                        Enum.find_value(stream, :finished, fn event -> 
                            send(bpid, {:broadcast, event})

                            receive do
                                :continue ->
                                    false
                                    
                                :stop ->
                                    {:stoped, event.number}
                            end
                        end)
                    end)
                resp
            end)
        %Broker{broker| worker: worker}
    end

    def sched_next(%Broker{}=broker) do
        %Broker{worker: worker}=broker
        if not(is_nil(worker)) do
            send(worker.pid, :continue)
        end
        broker
    end

    def subscribe(nil, opts) when is_list(opts) do
        handle_from_pid()
        |> Supervisor.prepare_broker(true)
        |> GenServer.call({:subscribe, opts}, 5000)
    end

    def subscribe(handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(Atom.to_string(handle), opts)
    end

    def subscribe(handle, opts) when is_list(opts) and is_binary(handle) do
        handle
        |> Supervisor.prepare_broker(false)
        |> GenServer.call({:subscribe, opts}, 5000)
    end

    def unsubscribe() do
        handle_from_pid()
        |> unsubscribe()
    end

    def unsubscribe(handle) do
        broker = Supervisor.broker(handle)
        with {:via, _reg, _iden} <- broker do
            GenServer.call(broker, :unsubscribe, 5000)
        end
    end

    def subscription() do
        handle_from_pid()
        |> subscription()
    end

    def subscription(handle) when is_binary(handle) do
        broker = Supervisor.broker(handle)
        with {:via, _reg, _iden} <- broker do
            GenServer.call(broker, :subscription, 5000)
        end
    end

    def acknowledge(id, number) do
        with {:via, reg, iden} <- Supervisor.broker(id) do
            GenServer.cast({:via, reg, iden}, {:ack, self(), number})
        end
    end

    def handle_from_pid do
        :crypto.hash(:md5 , inspect(self())) 
        |> Base.encode16()
    end

end


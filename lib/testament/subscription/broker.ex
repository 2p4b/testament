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

        handle = 
            case Store.get_handle(id) do
                %Handle{}=handle -> 
                    handle
                nil ->
                    %Handle{id: id, position: 0}
            end
        {:ok, struct(__MODULE__, id: id, handle: handle)}
    end

    @impl true
    def handle_call({:state, prop}, _from, %Broker{}=store) do
        {:reply, Map.get(store, prop), store} 
    end

    @impl true
    def handle_call(:subscription, {pid, _ref}, %Broker{subscriptions: subs}=store) do
        subscription = Enum.find(subs, &(Map.get(&1, :id) == pid))
        {:reply, subscription, store} 
    end

    @impl true
    def handle_call({:subscribe, opts}, {pid, _ref}=from, %Broker{}=store) do
        %Broker{subscriptions: subscriptions} = store
        subscription = Enum.find(subscriptions, &(Map.get(&1, :id) == pid))

        if is_nil(subscription) do
            subscription = create_subscription(store, pid, opts)
            GenServer.reply(from, {:ok, subscription})

            subscriptions = subscriptions ++ List.wrap(subscription)

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
            subs = List.update_at(subscriptions, index, fn sub -> 
                send(sub.id, event)
                Map.put(sub, :syn, number)
            end)
            {:noreply, %Broker{broker | subscriptions: subs}}
        end
    end

    @impl true
    def handle_info(%Event{}=event, %Broker{buffer: buffer}=broker) do
        buffer = buffer ++ List.wrap(event)
        broker = %Broker{broker | buffer: buffer}
        {:noreply, sched_next(broker)}
    end

    @impl true
    def handle_info({_worker_ref, :finished}, %Broker{}=broker) do
        fallen = 
            build_query(broker) 
            |> Repo.all()
            |> Enum.map(&Store.Event.to_stream_event/1)
                
        broker = %Broker{broker | buffer: fallen, worker: nil}
        Testament.listern_event()
        {:noreply, broker |> sched_next() }
    end

    @impl true
    def handle_info({_worker_ref, {:stoped, number}}, %Broker{}=broker) do
        {:noreply, broker}
    end

    @impl true
    def handle_info({:DOWN, _ref, :process, _pid, _status}, %Broker{}=broker) do
        {:noreply, broker}
    end

    @impl true
    def handle_call({:ack, pid, number}, from, %Broker{}=broker) do

        %Broker{handle: handle}=broker
        %{position: position} = handle

        broker = handle_ack(broker, pid, number)

        subscription = 
            broker
            |> Map.get(:subscriptions)
            |> Enum.max_by(&(Map.get(&1, :ack)), fn -> 
                %{ack: position, tack: false, id: pid} 
            end)

        %{ack: ack, track: track, id: id} = subscription

        if track and (ack > position) and (id == pid) do
            {:ok, handle} =
                Handle.changeset(handle, %{position: ack})
                |> Repo.insert_or_update()

            {:reply, number, %Broker{broker | handle: handle}}
        else
            {:reply, number,  broker}
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
        track = Keyword.get(opts, :track, true)
        %{
            id: pid,
            ack: from,
            syn: from,
            from: from,
            track: track,
            stream: stream,
            topics: topics,
            handle: handle.id,
        }
    end

    defp handle_ack(%Broker{}=broker, pid, number) do
        %Broker{subscriptions: subscriptions} = broker
        ack_sub = &(Map.get(&1, :id) == pid and Map.get(&1, :syn) == number)
        index = Enum.find_index(subscriptions, ack_sub)

        if is_nil(index) do
            broker
        else
            subscriptions = 
                List.update_at(subscriptions, index, fn subscription -> 
                    Map.put(subscription, :ack, number)
                end)

            %Broker{broker | subscriptions: subscriptions}
            |> sched_next()
        end
    end

    def build_query(%Broker{streams: streams, topics: topics, subscriptions: subs}) do
        %{syn: position} = 
            Enum.max_by(subs, fn %{syn: syn} -> syn end, fn -> %{syn: 0} end) 

        Store.query_events_from(position)
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
        stream = Repo.stream(query, max_rows: 1)

        worker =
            Task.async(fn -> 
                {:ok, resp} =
                    Repo.transaction(fn -> 
                        Enum.find_value(stream, :finished, fn event -> 
                            stream_event = Store.Event.to_stream_event(event)
                            send(bpid, {:broadcast, stream_event})

                            receive do
                                :continue ->
                                    false
                                    
                                :stop ->
                                    {:stoped, event.number}
                            end
                        end)
                    end, [timeout: :infinity])
                resp
            end)
        %Broker{broker| worker: worker}
    end

    def sched_next(%Broker{buffer: []}=broker) do
        %Broker{worker: worker}=broker
        if not(is_nil(worker)) do
            send(worker.pid, :continue)
        end
        broker
    end

    def sched_next(%Broker{buffer: [event | buffer], subscriptions: subs}=broker) do

        nil_max = fn -> nil end
        max_sub = Enum.max_by(subs, &(Map.get(&1, :syn)), nil_max)

        if is_nil(max_sub) do
            broker
        else
            if max_sub.ack == max_sub.syn do
                send(self(), {:broadcast, event})
                %Broker{broker | buffer: buffer}
            else
                broker
            end
        end
    end

    def subscribe(handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(Atom.to_string(handle), opts)
    end

    def subscribe(handle, opts) when is_list(opts) and is_binary(handle) do
        track = Keyword.get(opts, :track, true)
        handle
        |> Supervisor.prepare_broker(track)
        |> GenServer.call({:subscribe, opts}, 5000)
    end

    def unsubscribe(handle) do
        broker = Supervisor.broker(handle)
        with {:via, _reg, _iden} <- broker do
            GenServer.call(broker, :unsubscribe, 5000)
        end
    end

    def subscription(handle) when is_binary(handle) do
        broker = Supervisor.broker(handle)
        with {:via, _reg, _iden} <- broker do
            GenServer.call(broker, :subscription, 5000)
        end
    end

    def acknowledge(handle, number) do
        broker = Supervisor.broker(handle)
        with {:via, _reg, _iden} <- broker do
            GenServer.call(broker, {:ack, self(), number})
        end
    end

end


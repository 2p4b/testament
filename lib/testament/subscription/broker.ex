defmodule Testament.Subscription.Broker do

    use GenServer
    alias Testament.Store
    alias Signal.Void.Repo
    alias Signal.Stream.Event
    alias Testament.Store.Handle
    alias Testament.Subscription.Broker

    require Logger

    defstruct [
        id: nil,
        cursor: 0, 
        query: nil,
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
        Testament.listern("event")
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
            position = subscription.from

            subscriptions =
                Repo.events()
                |> Enum.filter(&(Map.get(&1, :number) > position))
                |> Enum.find_value(subscription, fn event -> 
                    sub = push_event(subscription, event) 
                    if sub.syn == subscription.syn do
                        false
                    else
                        sub
                    end
                end)
                |> List.wrap()
                |> Enum.concat(subscriptions)

            {topics, streams} = collect_topics_and_streams(subscriptions)

            broker = %Broker{store | 
                topics: topics, 
                streams: streams,
                subscriptions: subscriptions, 
            }

            query = build_query(broker)

            {:noreply, %Broker{broker| query: query}} 
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
    def handle_call({:next, position, opts}, _from, %Broker{}=store) 
    when is_integer(position) do
        {:ok, stream} = Keyword.fetch(opts, :stream)
        event = 
            Repo.events()
            |> Enum.find(fn %Event{stream: estream, number: number} -> 
                stream == estream and number > position
            end)
        {:reply, event, store} 
    end

    @impl true
    def handle_info({:next, pid}, %Broker{}=store) do
        %Broker{subscriptions: subs} = store
        index = Enum.find_index(subs, &(Map.get(&1, :pid) == pid))
        if is_nil(index) do
            {:noreply, store}
        else
            subs =
                store
                |> Map.get(:subscriptions)
                |> List.update_at(index, fn sub -> 
                    push_next(store, sub)
                end)
            {:noreply, %Broker{store | subscriptions: subs}}
        end
    end

    @impl true
    def handle_cast({:broadcast, %{number: number}=event}, %Broker{}=store) do
        info = """

        [BROKER] published #{inspect(event.type)}
        """
        Logger.info(info)
        subs = Enum.map(store.subscriptions, fn sub -> 
            push_event(sub, event)
        end)
        {:noreply, %Broker{store | subscriptions: subs, cursor: number}}
    end

    @impl true
    def handle_cast({:ack, pid, number}, %Broker{}=store) do
        store = handle_ack(store, pid, number)
        {:noreply, store}
    end

    defp push_event(%{syn: syn, ack: ack}=sub, _ev)
    when syn != ack do
        sub
    end

    defp push_event(%{handle: handle, syn: syn, ack: ack}=sub, _event)
    when (not is_nil(handle)) and (syn > ack) do
        sub
    end

    defp push_event(%{from: position}=sub, %{number: number})
    when is_integer(position) and position > number do
        sub
    end

    defp push_event(%{stream: s_stream}=sub, %{stream: e_stream}=event) do
        %{topics: topics} = sub
        %{topic: topic, number: number} = event
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
            Process.send(sub.pid, event, []) 
            Map.put(sub, :syn, number)
        else
            sub
        end
    end


    defp create_subscription(%Broker{cursor: cursor}, pid, opts) do
        from = Keyword.get(opts, :from, cursor)
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
        %Broker{subscriptions: subscriptions, cursor: cursor} = store
        index = Enum.find_index(subscriptions, &(Map.get(&1, :pid) == pid))
        if is_nil(index) do
            store
        else
            subscriptions = List.update_at(subscriptions, index, fn subscription -> 
                if cursor > subscription.ack do
                    Process.send(self(), {:next, subscription.pid}, [])
                end
                Map.put(subscription, :ack, number)
            end)
            %Broker{store| subscriptions: subscriptions}
        end
    end

    defp push_next(%Broker{}, %{ack: ack}=sub) do
        event = 
            Repo.events()
            |> Enum.find(&(Map.get(&1, :number) > ack))

        if event do
            push_event(sub, event)
        else
            sub
        end
    end

    def subscribe(nil, opts) when is_list(opts) do
        GenServer.call(__MODULE__, {:subscribe, opts}, 5000)
    end

    def subscribe(handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(Atom.to_string(handle), opts)
    end

    def subscribe(handle, opts) when is_list(opts) and is_binary(handle) do
        GenServer.call(__MODULE__, {:subscribe, opts}, 5000)
    end

    def unsubscribe() do
        GenServer.call(__MODULE__, :unsubscribe, 5000)
    end

    def subscription(_opts \\ []) do
        GenServer.call(__MODULE__, :subscription, 5000)
    end

    def acknowledge(number) do
        GenServer.cast(__MODULE__, {:ack, self(), number})
    end

    def build_query(%Broker{streams: [], topics: []}) do
        Store.query_event_topics([])
    end

    def build_query(%Broker{streams: [], topics: topics}) do
        Store.query_event_topics(topics)
    end

    def build_query(%Broker{streams: streams, topics: []}) do
        Store.query_event_streams(streams)
    end

    def build_query(%Broker{streams: streams, topics: topics}) do
        Store.query_event_streams(streams)
        |> Store.query_event_topics(topics)
    end

    def collect_topics_and_streams(subscriptions) do
        Enum.reduce(subscriptions, {[],[]}, fn 
            %{stream: stream, topics: topic}, {topics, streams} -> 
                topics = Enum.uniq(topics ++ topic)
                streams = Enum.uniq(streams ++ stream)
                {topics, streams}
        end)
    end
end


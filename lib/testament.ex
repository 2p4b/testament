defmodule Testament do

    alias Phoenix.PubSub
    alias Testament.Store
    alias Signal.Transaction
    alias Testament.Publisher
    alias Testament.Subscription.Broker

    @behaviour Signal.Store

    @moduledoc """
    Testament keeps the contexts that define your domain
    and business logic.

    Contexts are also responsible for managing your data, regardless
    if it comes from the database, an external API or others.
    """
    def record(snapshot, _opts \\ []) do
        Store.record(snapshot)
    end

    def publish(%Transaction{}=transaction, _opts \\ []) do
        Publisher.publish(transaction)
    end

    def acknowledge(handle, number, _opts \\ []) do
        Broker.acknowledge(handle, number)
    end

    def index(_opts) do
        Publisher.index()
    end

    def event(number, _opts \\ []) do
        case Store.get_event(number) do
            %{}=event ->
                Store.Event.to_stream_event(event)

            nil -> 
                nil
        end
    end

    def snapshot(id, opts \\ []) do
        Store.snapshot(id, opts)
    end

    def listern(topic) do
        PubSub.subscribe(:testament, topic)
    end

    def broadcast(topic, event) do
        PubSub.broadcast(:testament, topic, event)
    end

    def listern_event() do
        PubSub.subscribe(:testament, "events")
    end

    def broadcast_event(event) do
        PubSub.broadcast(:testament, "events", event)
    end

    def subscribe(handle, opts) when is_binary(handle) and is_list(opts) do
        Broker.subscribe(handle, opts)
    end

    def subscription(handle, _opts \\ []) do
        Broker.subscription(handle)
    end

    def unsubscribe(handle, _opts \\ []) do
        Broker.unsubscribe(handle)
    end

    def stream_position(stream, _opts\\[]) do
        Store.stream_position(stream)
    end

end

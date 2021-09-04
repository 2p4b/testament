defmodule Testament do

    alias Phoenix.PubSub
    alias Testament.Store
    alias Testament.Recorder
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
        snapshot
    end

    def publish(staged, _opts \\ []) do
        GenServer.call(Recorder, {:publish, staged})
    end

    def acknowledge(number, _opts \\ []) do
        number
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

    def snapshot(_id, _opts \\ []) do
        nil
    end

    def listern(topic) do
        PubSub.subscribe(:testament, topic)
    end

    def broadcast(topic, event) do
        PubSub.broadcast(:testament, topic, event)
    end

    def subscribe(opts) when is_list(opts) do
        Broker.subscribe(nil, opts)
    end

    def subscribe(handle) when is_binary(handle) do
        subscribe(handle, [])
    end

    def subscribe(handle, opts) when is_binary(handle) and is_list(opts) do
        Broker.subscribe(handle, opts)
    end

    def subscription(opts \\ []) do
        Broker.subscription(opts)
    end

    def unsubscribe(opts \\ []) do
        Broker.unsubscribe(opts)
    end

    def stream_position(stream, _opts\\[]) do
        case Store.get_stream(stream) do
            %{position: position} ->
                position

            nil ->
                nil
        end
    end

end

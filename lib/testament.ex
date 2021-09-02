defmodule Testament do

    alias Phoenix.PubSub
    alias Testament.Recorder

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
        0
    end

    def event(_number, _opts \\ []) do
        nil
    end

    def snapshot(_id, _opts \\ []) do
        nil
    end

    def listern(topic) do
        PubSub.subscribe(:testament, topic)
    end

    def boardcast(topic, event) do
        PubSub.broadcast(:testament, topic, event)
    end

    def subscribe(opts) when is_list(opts) do
        subscribe(nil, opts)
    end

    def subscribe(handle) when is_binary(handle) do
        subscribe(handle, [])
    end

    def subscribe(handle, opts) when is_binary(handle) and is_list(opts) do
        {handle, opts}
    end

    def subscription(_opts \\ []) do
    end

    def unsubscribe(_opts \\ []) do
    end

    def stream_position(_stream, _opts\\[]) do
        0
    end

    def get_state(id, _opts \\ []) when is_binary(id) do
    end

    def set_state(id, version, _state) 
    when is_binary(id) and is_integer(version) do
    end

end

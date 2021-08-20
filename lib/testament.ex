defmodule Testament do

    alias Testament.Recorder

    @moduledoc """
    Testament keeps the contexts that define your domain
    and business logic.

    Contexts are also responsible for managing your data, regardless
    if it comes from the database, an external API or others.
    """
    def record(stages) when is_list(stages) do
        GenServer.call(Recorder, {:record, stages}, 5000)
    end

    def publish(event) do
        PubSub.broadcast(:testament, "events", event)
    end

    def listern(topic) do
        PubSub.subscribe(:testament, topic)
    end

    def record(_log) do
    end

    def stream_position(_stream) do
        0
    end

    def get_state(id, _opts \\ []) when is_binary(id) do
    end

    def set_state(id, version, _state) 
    when is_binary(id) and is_integer(version) do
    end

end

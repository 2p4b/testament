defmodule Testament.Broker do
    use GenServer
    alias Testament.Subscription

    @impl true
    def init(opts) do
        with :ok <- Testament.subscribe("events") do
            {:ok, [struct(Subscription, opts)]}
        end
    end

    def handle_info(_event, sub) do
        {:noreply, sub}
    end

end

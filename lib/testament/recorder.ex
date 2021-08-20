defmodule Testament.Recorder do

    use GenServer

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(opts) do
        {:ok, opts}
    end

    @impl true
    def handle_call({:record, staged}, _from, state) 
    when is_list(staged) do
        IO.inspect(staged)
        {:reply, staged, state}
    end

end

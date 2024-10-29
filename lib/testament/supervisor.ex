defmodule Testament.Supervisor do
    ## Supervisor callbacks
    use Supervisor
    
    def start_link(opts) do
        store_name = Keyword.fetch!(opts, :name)
        supervisor_name = Module.concat([store_name, __MODULE__])
        init_opts = Keyword.put(opts, :supervisor, supervisor_name)
        Supervisor.start_link(__MODULE__, init_opts, name: supervisor_name)
    end

    @doc false
    def init(opts) do
        repo = Keyword.fetch!(opts, :ecto_repo)
        children = [repo]
        Supervisor.init(children, strategy: :one_for_one)
    end


end

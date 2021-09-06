defmodule Testament.Subscription.Supervisor do

    @registry Testament.Broker.Registry

    use DynamicSupervisor

    def start_link(args) do
        DynamicSupervisor.start_link(__MODULE__, args, name: __MODULE__ )
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(__MODULE__, {Testament.Subscription.Broker, args})
    end

    def prepare_broker(id, track \\ false) when is_binary(id) do
        case Registry.lookup(@registry, id) do
            [{_pid, _type}] ->
                via_tuple(id, track)            

            [] ->
                child_args(id, via_tuple(id, track)) 
                |> start_child()
                prepare_broker(id, track)
        end
    end

    defp child_args(id, via_name) do
        [
            name: via_name,
            id: id,
        ] 
    end

    defp via_tuple(id, track) when is_binary(id) do
        {:via, Registry, {@registry, id, track}}
    end

    def stop_child(id) when is_binary(id) do
        case Registry.lookup(@registry, id) do
            [{pid, _name}] ->
                Registry.unregister(@registry, id)
                DynamicSupervisor.terminate_child(__MODULE__, pid)

            [] -> {:error, :not_found}
        end
    end

    def broker(id) do
        case Registry.lookup(@registry, id) do
            [{_pid, type}] ->
                via_tuple(id, type)

            [] ->
                false
        end
    end

end

defmodule Testament do
    defmacro __using__(opts) do
        quote do
            @ecto_opts unquote(opts)
            @otp_app  Keyword.get(@ecto_opts, :otp_app)
            @ecto_repo Module.concat(__MODULE__, Repo)
            @ecto_adapter Keyword.get(@ecto_opts, :adapter)
            @store_opts [
                name: __MODULE__,
                otp_app: @otp_app,
                ecto_repo: @ecto_repo,
                ecto_repo_adapter: @ecto_adapter
            ]

            @behaviour Signal.Store
            @before_compile unquote(__MODULE__)

            def start_link(opts) do
                Testament.Supervisor.start_link(opts)
            end

            def child_spec(opts) do
                opts = Keyword.merge(opts, @store_opts) 
                %{
                    id: __MODULE__,
                    type: :supervisor,
                    start: {__MODULE__, :start_link, [opts]},
                }
            end

            def repo, do: @ecto_repo

            def get_cursor(opts), 
                do: Testament.Repo.get_cursor(@ecto_repo, opts)

            def get_effect(uuid, opts\\[]), 
                do: Testament.Repo.get_effect(@ecto_repo, uuid, opts)

            def save_effect(effect, opts\\[]), 
                do: Testament.Repo.save_effect(@ecto_repo, effect, opts)

            def list_effects(namespace, opts\\[]),
                do: Testament.Repo.list_effects(@ecto_repo, namespace, opts)

            def delete_effect(uuid, opts\\[]),
                do: Testament.Repo.delete_effect(@ecto_repo, uuid, opts)

            def get_snapshot(id, opts\\[]),
                do: Testament.Repo.get_snapshot(@ecto_repo, id, opts)

            def record_snapshot(snapshot, opts\\[]),
                do: Testament.Repo.record_snapshot(@ecto_repo, snapshot, opts)

            def delete_snapshot(uuid, opts\\[]),
                do: Testament.Repo.delete_snapshot(@ecto_repo, uuid, opts)

            def commit_transaction(trnx, opts\\[]),
                do: Testament.Repo.commit_transaction(@ecto_repo, trnx, opts)

            def handler_position(handler, opts\\[]),
                do: Testament.Repo.handler_position(@ecto_repo, handler, opts)

            def handler_acknowledge(handler, num, opts\\[]),
                do: Testament.Repo.handler_acknowledge(@ecto_repo, handler, num, opts)

            def read_events(reader, opts\\[]),
                do: Testament.Repo.read_events(@ecto_repo, reader, opts)

            def read_stream_events(sid, reader, opts\\[]), 
                do: Testament.Repo.read_stream_events(@ecto_repo, sid, reader, opts)

            def list_stream_events(sid, opts), 
                do: Testament.Repo.list_stream_events(@ecto_repo, sid, opts)

            def list_events(opts), 
                do: Testament.Repo.list_events(@ecto_repo, opts)

            def stream_position(stream, opts\\[]), 
                do: Testament.Repo.stream_position(@ecto_repo, stream, opts)

            def ensure_ready(opts\\[]) do
                unless __MODULE__.__setup__?(opts) do
                    ensure_ready(opts)
                else
                    :ok
                end
            end

            def __setup__(_opts\\[]), do: Testament.Repo.init(@ecto_repo)

            def __setup__?(_opts\\[]), do: Testament.Repo.initialized?(@ecto_repo)

            def __reset__(opts\\[]) do 
                Testament.Repo.delete_storage(@ecto_repo)
                __MODULE__.__setup__(opts)
            end

        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true, location: :keep do
            with opts <- Module.get_attribute(__MODULE__, :store_opts),
                 otp_app when is_atom(otp_app) <- Keyword.get(opts, :otp_app),
                 adapter when is_atom(adapter) <- Keyword.get(opts, :ecto_repo_adapter) do

                storename = __MODULE__
                 
                defmodule Repo do
                    @otp_app otp_app
                    @repo_storename storename
                    @repo_opts [otp_app: otp_app, adapter: adapter]
                    use Ecto.Repo, @repo_opts

                    def init(:supervisor, config) do
                        env_config = Application.get_env(@otp_app, @repo_storename, [])
                        {:ok, Keyword.merge(config, env_config)}
                    end

                    def init(:runtime, config) do
                        init(:supervisor, config)
                    end
                end
            else
                reason -> raise reason
            end
        end
    end
end

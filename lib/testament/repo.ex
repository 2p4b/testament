defmodule Testament.Repo do
    import Ecto.Query, warn: false
    alias Testament.Store
    alias Signal.Transaction

    @migrations_path "/priv/repo/migrations"

    @migrations_table "schema_migrations"

    def save_effect(repo, effect, _opts\\[]) do
        repo
        |> Store.save_effect(effect)
    end

    def delete_effect(repo, uuid, _opts\\[]) do
        repo
        |> Store.delete_effect(uuid)
    end

    def commit_transaction(repo, %Transaction{}=transaction, _opts\\[]) do
        repo
        |> Store.commit_transaction(transaction)
    end

    def record_snapshot(repo, snapshot, _opts\\[]) do
        repo
        |> Store.record_snapshot(snapshot)
    end

    def delete_snapshot(repo, uuid, _opts\\[]) do
        repo
        |> Store.delete_snapshot(uuid)
    end


    def get_cursor(repo, _opts\\[]) do
        Store.index(repo)
    end

    def get_effect(repo, uuid, _opts\\[]) do
        Store.get_effect(repo, uuid)
        case Store.get_effect(repo, uuid) do
            nil ->
                nil
            effect ->
                effect
                |> Store.Effect.to_signal_effect()
        end
    end

    def list_effects(repo, namespace, _opts\\[]) do
        repo
        |> Store.list_effects(namespace)
        |> Enum.map(&Store.Effect.to_signal_effect/1)
    end

    def get_snapshot(repo, id, _opts\\[]) do
        case Store.get_snapshot(repo, id) do
            nil ->
                nil
            snapshot ->
                snapshot
                |> Store.Snapshot.to_signal_snapshot()
        end
    end

    def handler_position(repo, handle, _opts\\[]) do
        repo
        |> Store.handler_position(handle)
    end

    def handler_acknowledge(repo, handle, number, _opts\\[]) do
        repo
        |> Store.handler_acknowledge(handle, number)
    end

    def read_events(repo, reader, opts\\[]) do
        repo
        |> Store.read_events(fn event -> 
            event
            |> Store.Event.to_signal_event()
            |> reader.()
        end, opts)
    end

    def read_stream_events(repo, sid, reader, opts\\[]) do
        repo
        |> Store.read_stream_events(sid, fn event -> 
            event
            |> Store.Event.to_signal_event()
            |> reader.()
        end, opts)
    end

    def list_stream_events(repo, sid,opts) do
        repo
        |> Store.list_stream_events(sid, opts)
        |> Enum.map(&Testament.Store.Event.to_signal_event/1)
    end

    def list_events(repo, opts) do
        repo
        |> Store.list_events(opts)
        |> Enum.map(&Testament.Store.Event.to_signal_event/1)
    end

    def stream_position(repo, stream, _opts\\[]) do
        repo
        |> Store.stream_position(stream)
    end

    def init(repo) do
        ensure_prereq_started()
        unless storage_up?(repo) do
            create_storage(repo)
        end
        if list_migrated(repo) == []  do
            migrate(repo)
        end
        case list_migrated(repo) do
            [] ->
                {:error, {:nomigrations, repo}}
            _  ->
                {:ok, repo}
        end
    end

    def delete_storage(repo) when is_atom(repo) do
        repo.__adapter__().storage_down(repo.config())
    end

    def storage_up?(repo) do
        storage_status(repo) === :up
    end

    def storage_down?(repo) do
        storage_status(repo) === :down
    end

    def list_migrated(repo) do
        try do
            from(@migrations_table, select: [:version])
            |> repo.all()
        rescue 
            _ -> [] 
        catch
            _ -> []
        end
    end

    def initialized?(repo) do
        storage_up?(repo) and (list_migrated(repo) |> Enum.empty?() |> Kernel.not())
    end

    defp ensure_installed(repo) do
        unless initialized?(repo)  do
            init(repo)
        end
    end

    defp ensure_prereq_started do
        {:ok, _} = Application.ensure_all_started(:ecto_sql)
    end

    defp storage_status(repo) when is_atom(repo)  do
        repo.__adapter__().storage_status(repo.config())
    end

    defp create_storage(repo) when is_atom(repo) do
        repo.__adapter__().storage_up(repo.config())
    end

    defp migrations_path do
        Application.app_dir(:testament) <> @migrations_path
    end

    defp migrate(repo) do
        path = migrations_path()
        Ecto.Migrator.run(repo, path, :up, [all: true])
        {:ok, repo}
    end

end

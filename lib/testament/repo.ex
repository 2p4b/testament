defmodule Testament.Repo do
    alias Testament.Store
    alias Signal.Transaction

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

    def save_effect(repo, effect, _opts\\[]) do
        repo
        |> Store.save_effect(effect)
    end

    def list_effects(repo, namespace, _opts\\[]) do
        repo
        |> Store.list_effects(namespace)
        |> Enum.map(&Store.Effect.to_signal_effect/1)
    end

    def delete_effect(repo, uuid, _opts\\[]) do
        repo
        |> Store.delete_effect(uuid)
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

    def record_snapshot(repo, snapshot, _opts\\[]) do
        repo
        |> Store.record_snapshot(snapshot)
    end

    def delete_snapshot(repo, uuid, _opts\\[]) do
        repo
        |> Store.delete_snapshot(uuid)
    end

    def commit_transaction(repo, %Transaction{}=transaction, _opts\\[]) do
        repo
        |> Store.commit_transaction(transaction)
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

    def install(repo) do
        migrate(repo)
    end

    def migrate(repo) do
        {:ok, _, _} = 
            repo
            |> Ecto.Migrator.with_repo(fn repo -> 
                Ecto.Migrator.run(repo, :up, all: true)
            end)
        {:ok, repo}
    end

end

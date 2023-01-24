defmodule Testament do

    alias Testament.Store
    alias Signal.Transaction

    @app :testament
    @behaviour Signal.Store

    @moduledoc """
    Testament keeps the contexts that define your domain
    and business logic.

    Contexts are also responsible for managing your data, regardless
    if it comes from the database, an external API or others.
    """

    def get_cursor(_opts) do
        Store.index()
    end

    def get_snapshot(id, _opts \\ []) do
        case Store.get_snapshot(id) do
            nil ->
                nil
            snapshot ->
              snapshot
              |> Store.Snapshot.to_signal_snapshot()
        end
    end

    def record_snapshot(snapshot, _opts \\ []) do
        Store.record_snapshot(snapshot)
    end

    def delete_snapshot(uuid, _opts \\ []) do
        Store.delete_snapshot(uuid)
    end

    def commit_transaction(%Transaction{}=transaction, _opts \\ []) do
        Store.commit_transaction(transaction)
    end

    def handler_position(handle, _opts \\ []) do
        Store.handler_position(handle)
    end

    def handler_acknowledge(handle, number, _opts \\ []) do
        Store.handler_acknowledge(handle, number)
    end

    def read_events(reader, opts) do
        Store.read_events(fn event -> 
            event
            |> Store.Event.to_signal_event()
            |> reader.()
        end, opts)
    end

    def list_events(opts) do
        Store.list_events(opts)
        |> Enum.map(&Testament.Store.Event.to_signal_event/1)
    end

    def stream_position(stream, _opts\\[]) do
        Store.stream_position(stream)
    end

    def install do
        migrate()
    end

    def migrate do
        for repo <- repos() do
            {:ok, _, _} = 
                repo
                |> Ecto.Migrator.with_repo(fn repo -> 
                    Ecto.Migrator.run(repo, :up, all: true)
                end)
        end
        {:ok, __MODULE__}
    end

    defp repos do
        Application.load(@app)
        Application.fetch_env!(@app, :ecto_repos)
    end

end

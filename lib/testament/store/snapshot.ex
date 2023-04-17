defmodule Testament.Store.Snapshot do
    use Testament.Schema, row: :snapshot
    import Ecto.Changeset

    alias Testament.Repo
    alias Testament.Store.Snapshot

    @fields [:id, :data, :version]

    @required [:id, :data, :version]

    @primary_key {:id, :string, autogenerate: false}

    schema "snapshots" do
        field :data,        Repo.JSON
        field :version,     :integer
    end

    @doc false
    def changeset(snapshot, attrs) do
        snapshot
        |> cast(attrs, @fields)
        |> validate_required(@required)
    end

    def to_signal_snapshot(%Snapshot{}=snapshot) do
        attrs = 
            snapshot
            |> Map.from_struct()
            |> Map.to_list()
        struct(Signal.Snapshot, attrs)
    end
end

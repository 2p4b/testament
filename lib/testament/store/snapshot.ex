defmodule Testament.Store.Snapshot do
    use Testament.Schema, row: :snapshot
    import Ecto.Changeset

    alias Testament.Repo
    alias Testament.Store.Snapshot

    @fields [:uuid, :id, :data, :version]

    @required [:uuid, :id, :data, :version]

    @primary_key {:uuid, :binary_id, autogenerate: true}

    schema "snapshots" do
        field :id,          :string
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

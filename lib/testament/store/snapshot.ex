defmodule Testament.Store.Snapshot do
    use Testament.Schema, row: :snapshot
    import Ecto.Changeset

    alias Testament.Repo
    alias Testament.Store.Snapshot

    @fields [:uuid, :id, :payload, :version, :type]

    @required [:uuid, :id, :payload, :version]

    @primary_key {:uuid, :binary_id, autogenerate: false}

    schema "snapshots" do
        field :id,          :string
        field :type,        :string
        field :payload,     Repo.JSON
        field :version,     :integer
    end

    @doc false
    def changeset(snapshot, attrs) do
        snapshot
        |> cast(attrs, @fields)
        |> validate_required(@required)
        |> unsafe_validate_unique([:type, :id, :version], Repo, message: "snapshot version must be unique")
    end

    def to_signal_snapshot(%Snapshot{}=snapshot) do
        attrs = 
            snapshot
            |> Map.from_struct()
            |> Map.to_list()
        struct(Signal.Snapshot, attrs)
    end
end

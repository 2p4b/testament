defmodule Testament.Store.Snapshot do
    use Testament.Schema, row: :snapshot
    import Ecto.Changeset

    alias Testament.Repo
    alias Testament.DataType

    @fields [:uuid, :id, :payload, :version, :type]

    @required [:uuid, :id, :payload, :version]

    @primary_key {:uuid, :binary_id, autogenerate: false}

    schema "snapshots" do
        field :id,          :string
        field :type,        :string
        field :payload,     Testament.Repo.JSON
        field :version,     :integer
    end

    @doc false
    def changeset(snapshot, attrs) do
        snapshot
        |> cast(attrs, @fields)
        |> validate_required(@required)
        |> unsafe_validate_unique([:type, :id, :version], Repo, message: "snapshot version must be unique")
    end
end

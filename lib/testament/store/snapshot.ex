defmodule Testament.Store.Snapshot do
    use Testament.Schema, row: :snapshot
    import Ecto.Changeset

    alias Testament.Repo
    alias Testament.DataType

    @fields [ :id, :data, :version, :type]

    @required [ :id, :data, :version]

    @primary_key {:uuid, :binary_id, autogenerate: true}

    schema "snapshots" do
        field :id,          :string
        field :type,        :string
        field :data,        DataType
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

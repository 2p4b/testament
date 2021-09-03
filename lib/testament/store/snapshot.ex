defmodule Testament.Store.Snapshot do
    use Ecto.Schema
    import Ecto.Changeset
    alias Testament.DataType

    @fields [ :id, :data, :version]

    @required [ :id, :data, :version]

    @primary_key {:uuid, :binary_id, autogenerate: true}

    schema "snapshots" do
        field :id,          :string
        field :data,        DataType
        field :version,     :integer
    end

    @doc false
    def changeset(snapshot, attrs) do
        snapshot
        |> cast(attrs, @fields)
        |> validate_required(@required)
    end
end

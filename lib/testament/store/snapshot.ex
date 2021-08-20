defmodule Testament.Store.Snapshot do
    use Ecto.Schema
    import Ecto.Changeset

    @fields [:version, :stream_type, :stream_id, :payload]

    schema "snapshots" do
        field :payload,     :string
        field :version,     :integer
        field :stream_id,   :string
        field :stream_type, :string
        timestamps()
    end

    @doc false
    def changeset(snapshot, attrs) do
        snapshot
        |> cast(attrs, @fields)
        |> validate_required(@fields)
    end
end

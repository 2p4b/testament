defmodule Testament.Store.Stream do
    use Testament.Schema, row: :stream

    import Ecto.Changeset
    alias Testament.AtomType

    @fields [:uuid, :id, :type, :position]

    @primary_key {:uuid, :binary_id, autogenerate: false}

    schema "streams" do
        field :id,              :string
        field :type,            AtomType
        field :position,        :integer,   default: 0
    end

    @doc false
    def changeset(event, attrs) do
        event
        |> cast(attrs, @fields)
        |> validate_required(@fields)
    end

end


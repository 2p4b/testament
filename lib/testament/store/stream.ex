defmodule Testament.Store.Stream do
    use Testament.Schema, row: :stream

    import Ecto.Changeset

    @fields [:id, :position]

    @primary_key {:id, :string, autogenerate: false}

    schema "streams" do
        field :position,        :integer,   default: 0
    end

    @doc false
    def changeset(event, attrs) do
        event
        |> cast(attrs, @fields)
        |> validate_required(@fields)
    end

end


defmodule Testament.Store.Handle do
    use Testament.Schema, row: :handle
    import Ecto.Changeset

    @fields [:position, :id]

    @primary_key {:id, :string, autogenerate: false}

    schema "handles" do
        field :position,    :integer,   default: 0
    end

    @doc false
    def changeset(handler, attrs) do
        handler
        |> cast(attrs, @fields)
        |> validate_required(@fields)
    end

end

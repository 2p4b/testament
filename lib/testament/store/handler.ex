defmodule Testament.Store.Handler do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key {:name, :string, autogenerate: false}

    schema "handlers" do
        field :position,    :integer,   default: 0
        timestamps()
    end

    @doc false
    def changeset(handler, attrs) do
        handler
        |> cast(attrs, [:position, :name])
        |> validate_required([:position, :name])
    end

end

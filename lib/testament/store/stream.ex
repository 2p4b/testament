defmodule Testament.Store.Stream do
    use Testament.Schema, row: :stream

    import Ecto.Changeset

    @fields [:id, :type, :position]

    @primary_key {:uuid, :binary_id, autogenerate: true}

    schema "streams" do
        field :id,              :string
        field :type,            :string
        field :position,        :string
        timestamps()
    end

    @doc false
    def changeset(event, attrs) do
        event
        |> cast(attrs, @fields)
        |> validate_required(@fields)
    end

end


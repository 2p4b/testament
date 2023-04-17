defmodule Testament.Store.Effect do
    use Testament.Schema, row: :effect
    import Ecto.Changeset

    alias Testament.Repo
    alias Testament.Store.Effect

    @fields [:uuid, :data, :namespace]

    @required [:uuid, :data, :namespace]

    @primary_key {:uuid, :binary_id, autogenerate: false}

    schema "effects" do
        field :namespace,   :string
        field :data,        Repo.JSON
    end

    @doc false
    def changeset(effect, %Signal.Effect{}=attrs)  do
        changeset(effect, Map.from_struct(attrs))
    end

    @doc false
    def changeset(effect, attrs) do
        effect
        |> cast(attrs, @fields)
        |> validate_required(@required)
    end

    def to_signal_effect(%Effect{}=effect) do
        attr = 
            effect
            |> Map.from_struct()
            |> Map.to_list()
        struct(Signal.Effect, attr)
    end
end


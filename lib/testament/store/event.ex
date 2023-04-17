defmodule Testament.Store.Event do

    use Testament.Schema, row: :event
    import Ecto.Changeset

    alias Testament.Repo
    alias Testament.Store.Event

    @fields [
        :uuid, :topic, :position, :number, :stream_id,
        :data, :causation_id, :correlation_id, :timestamp
    ]

    @required [
        :topic, :position, :number, :stream_id,
        :data, :causation_id, :correlation_id, :timestamp
    ]

    @foreign_key_type :binary_id

    @primary_key {:number, :id, autogenerate: false}

    schema "events" do
        field :uuid,            Ecto.UUID
        field :topic,           :string
        field :data,            Repo.JSON
        field :position,        :integer
        field :stream_id,       :string
        field :causation_id,    :string
        field :correlation_id,  :string
        field :timestamp,       :utc_datetime_usec
    end

    @doc false
    def changeset(%Signal.Event{}=event)  do
        attrs = 
            event
            |> Map.from_struct() 

        %Event{}
        |> cast(attrs, @fields)
        |> validate_required(@required)
    end

    @doc false
    def changeset(event, attrs) do
        event
        |> cast(attrs, @fields)
        |> validate_required(@required)
    end

    def to_signal_event(%Event{}=event) do
        attr = 
            event
            |> Map.from_struct()
            |> Map.to_list()
        struct(Signal.Event, attr)
    end

    def map_from_staged_event(event) when is_struct(event) do
        Map.from_struct(event)
    end

end

defmodule Testament.Store.Event do

    use Testament.Schema, row: :event
    import Ecto.Changeset

    alias Testament.Store.Event

    @fields [
        :uuid, :topic, :position, :number, :stream_id,
        :payload, :causation_id, :correlation_id, :timestamp
    ]

    @required [
        :topic, :position, :number, :stream_id,
        :payload, :causation_id, :correlation_id, :timestamp
    ]

    @foreign_key_type :binary_id

    @primary_key {:number, :id, autogenerate: false}

    schema "events" do
        field :uuid,            Ecto.UUID,  autogenerate: true
        field :topic,           :string
        field :payload,         Testament.Repo.JSON
        field :position,        :integer
        field :stream_id,       :string
        field :causation_id,    :string
        field :correlation_id,  :string
        field :timestamp,       :utc_datetime_usec
    end

    @doc false
    def changeset(event, attrs) do
        event
        |> cast(attrs, @fields)
        |> validate_required(@required)
    end

    def to_stream_event(%Event{}=event) do
        attr = 
            event
            |> Map.from_struct()
        struct(Signal.Stream.Event, attr)
    end

    def map_from_staged_event(event) when is_struct(event) do
        Map.from_struct(event)
    end

end

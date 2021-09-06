defmodule Testament.Store.Event do

    use Testament.Schema, row: :event
    import Ecto.Changeset

    alias Testament.DataType
    alias Testament.Store.Event
    alias Testament.Store.Stream

    @fields [
        :uuid, :topic, :position, :number, :type, :stream,
        :data, :causation_id, :correlation_id, :timestamp
    ]

    @required [
        :topic, :position, :type, :number, :stream,
        :data, :causation_id, :correlation_id, :timestamp
    ]

    @foreign_key_type :binary_id

    @primary_key {:number, :id, autogenerate: false}

    schema "events" do
        field :uuid,            Ecto.UUID,  autogenerate: true
        field :topic,           :string
        field :type,            :string
        field :data,            DataType
        field :position,        :integer
        field :causation_id,    :string
        field :correlation_id,  :string
        field :stream,          :string
        field :timestamp,       :utc_datetime_usec
    end

    @doc false
    def changeset(event, attrs) do
        event
        |> cast(attrs, @fields)
        |> validate_required(@required)
    end

    def to_stream_event(%Event{}=event) do
        attr = Map.from_struct(event) 
        struct(Signal.Stream.Event, attr)
    end

    def map_from_staged_event(event) when is_struct(event) do
        Map.from_struct(event)
    end

end

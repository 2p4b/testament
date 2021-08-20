defmodule Testament.Store.Event do

    use Testament.Schema, row: :event
    import Ecto.Changeset

    alias Signal.Helper
    alias Testament.Serializer
    alias Testament.Store.Event
    alias Signal.Events.Event, as: SigEvent

    @fields [
        :uuid, :topic, :reduction, :stream_type, :number, :type,
        :stream_id, :payload, :causation_id, :correlation_id, :timestamp
    ]

    @primary_key {:uuid, :binary_id, autogenerate: false}

    schema "events" do
        field :number,          :integer
        field :topic,           :string
        field :type,            :string
        field :payload,         :string
        field :reduction,       :integer
        field :stream_id,       :string
        field :stream_type,     :string
        field :causation_id,    :string
        field :correlation_id,  :string
        field :timestamp,       :utc_datetime_usec
        timestamps(created_at: false, updated_at: false)
    end

    @doc false
    def changeset(event, attrs) do
        event
        |> cast(attrs, @fields)
        |> validate_required(@fields)
    end

    def to_sig_event(%Event{}=event) do

        attr = %{
            uuid: event.uuid,
            topic: event.topic,
            number: event.number,
            reduction: event.reduction,
            timestamp: event.timestamp,
            payload: Event.payload(event),
            causation_id: event.causation_id,
            correlation_id: event.correlation_id,
            stream: Event.stream(event)
        }

        struct(SigEvent, attr)
    end

    def payload(%Event{type: type, payload: payload}) do
        Serializer.deserialize(payload, %{type: type})
    end

    def stream(%Event{stream_id: stream_id, stream_type: stream_type}) do
        {Helper.string_to_module(stream_type), stream_id}
    end

end

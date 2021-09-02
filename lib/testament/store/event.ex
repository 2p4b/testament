defmodule Testament.Store.Event do

    use Testament.Schema, row: :event
    import Ecto.Changeset

    alias Testament.DataType
    alias Testament.AtomType
    alias Testament.Store.Event
    alias Testament.Store.Stream

    @fields [
        :uuid, :topic, :position, :number, :type,
        :stream_id, :data, :causation_id, :correlation_id, :timestamp
    ]

    @required [
        :topic, :position, :type,
        :stream_id, :data, :causation_id, :correlation_id, :timestamp
    ]

    @foreign_key_type :binary_id

    @primary_key {:number, :id, autogenerate: true}

    schema "events" do
        field :uuid,            Ecto.UUID,  autogenerate: true
        field :topic,           :string
        field :type,            AtomType
        field :data,            DataType
        field :position,        :integer
        field :causation_id,    :string
        field :correlation_id,  :string
        field :timestamp,       :utc_datetime_usec

        belongs_to  :stream,    Stream,     foreign_key: :stream_id

        timestamps(created_at: false, updated_at: false)
    end

    @doc false
    def changeset(event, attrs) do
        event
        |> cast(attrs, @fields)
        |> validate_required(@required)
    end

    def to_stream_event(%Event{}=event) do

        stream = Event.stream(event)

        attr = 
            event
            |> Map.from_struct() 
            |> Map.put(:stream, stream)

        struct(Signal.Stream.Event, attr)
    end

    def map_from_staged_event(event) when is_struct(event) do
        Map.from_struct(event)
    end

    def stream(%Event{}=event) do
        %{type: type, id: id} =
            event
            |> Testament.Repo.preload(:stream)
            |> Map.get(:stream)

        {type, id}
    end

end

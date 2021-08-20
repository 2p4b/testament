defmodule Testament.Store do

    import Ecto.Query, warn: false
    alias Signal.Helper
    alias Testament.Repo
    alias Testament.Serializer
    alias Testament.Store.Event
    alias Testament.Store.Handler
    alias Signal.Aggregates.Aggregate
    alias Signal.Events.Event, as: SigEvent

    def snapshot(%Aggregate{}) do
    end

    def events_count() do
        query = from event in Event, select: count() 
        Repo.one(query)
    end

    def update_handler(name, position) do
        res =
            %Handler{name: name, position: position}
            |> Handler.changeset(%{name: name, position: position})
            |> Repo.update()

        case res do
            {:ok, %Handler{position: position}} ->
                {:ok, position}

            error ->
                error
        end
    end

    def get_handler_position(name) do
        query = 
            from handler in Handler, 
            where: handler.name == ^name,
            select: handler.position

        case Repo.one(query) do
            %Handler{position: position} ->
                 position

            _ ->
                position = 0
                attrs = %{name: name, position: position} 
                {:ok, _handler} =
                    %Handler{}
                    |> Handler.changeset(attrs)
                    |> Repo.insert()
                position
        end
    end

    def get_stream({type, id}) when is_atom(type) and is_binary(id) do
        type = Atom.to_string(type)
        Stream.query([id: id, type: type])
        |> Repo.one()
    end

    def record_events(number, events) 
    when is_integer(number) and is_list(events) do
        Repo.transaction(fn -> 
            events =
                events
                |> Enum.map(fn %SigEvent{}=sig_event -> 

                    %{type: type} = sig_event

                    stream = get_or_create_stream(sig_event.stream)

                    payload = Serializer.serialize(sig_event.payload)

                    attrs = 
                        sig_event
                        |> Map.from_struct()
                        |> Map.delete(:stream)
                        |> Map.put(:type, sig_event.type)
                        |> Map.put(:payload, payload)
                        |> Map.put(:stream_id, stream.id)
                        |> Map.put(:stream_type, stream.type)

                    {:ok, _event} =
                        %Event{}
                        |> Event.changeset(attrs)
                        |> Repo.insert()

                    sig_event
                end)
            {events, number}
        end)
    end

    def pull_events(topics, position, amount) 
    when is_list(topics) and is_integer(position) and is_integer(amount) do
        query = 
            from event in Event, 
            where: event.topic in ^topics,
            where: event.number > ^position,
            order_by: [asc: event.reduction],
            select: event,
            limit: ^amount

        query
        |> Repo.all() 
        |> Enum.map(&Event.to_sig_event/1)
    end

    defp get_or_create_stream({type, id}) do
        type = Atom.to_string(type)
        stream =
            Stream.query([id: id, type: type])
            |> Repo.one()
        if is_nil(stream) do
            attrs = %{id: id, type: type}
            %Stream{}
            |> Stream.changeset(attrs)
            |> Repo.insert()
        else
            stream
        end
    end

end

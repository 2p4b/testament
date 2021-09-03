defmodule Testament.StoreTest do
    use ExUnit.Case
    alias Testament.Store
    alias Testament.Publisher

    import Testament.Factory

    setup do
        :ok = Ecto.Adapters.SQL.Sandbox.checkout(Testament.Repo)
        Ecto.Adapters.SQL.Sandbox.mode(Testament.Repo, {:shared, self()})
        :ok
    end

    describe "Store" do

        @tag :store
        test "should have same type as published event" do
            event = build(:value_updated)

            staged = Publisher.stage_event(event)

            %{events: [event]} = staged

            %{data: data, topic: topic, type: type} = event

            event_attrs =
                Map.from_struct(event)
                |> Map.put(:number, 9)
                |> Map.put(:position, 0)
                |> Map.put(:uuid, Ecto.UUID.generate())
                |> Map.put(:stream_id, Ecto.UUID.generate())

            {:ok, store_event} = Store.create_event(event_attrs)

            assert store_event.type == type
            assert store_event.data == data
            assert store_event.topic == topic
        end

        @tag :store
        test "create stream" do
            {:ok, stream} = Store.create_stream({Factory, "sample"})
            assert stream.id == "sample"
            assert stream.type == Factory
        end

        @tag :store
        test "update stream position" do
            {:ok, stream} = Store.create_stream({Stream, "sample"})
            assert stream.position == 0
            {:ok, stream} = Store.update_stream_position(stream, 4)
            assert stream.position == 4
        end

        @tag :store
        test "create snapshot" do
            id = "snapid"
            data = %{"hello" => "world"}
            version = 1
            snapshot = %Signal.Snapshot{
                id: id, 
                data: data, 
                version: version
            }
            {:ok, snapshot} = Store.create_snapshot(snapshot)

            assert snapshot.id == id
            assert snapshot.data == data
            assert snapshot.version == version
        end

        @tag :store
        test "get or create handle" do
            id = "named handler"
            position = 0
            {:ok, handle} = Store.create_handle(id, position)
            assert handle.id == id
            assert handle.position == position
        end

        @tag :store
        test "update handle position" do
            id = "test.handler"
            {:ok, handle} = Store.create_handle(id)
            {:ok, handle} = Store.update_handle_position(handle, 10)
            assert handle.position == 10
        end

    end

end

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
        test "create snapshot" do
            id = "snapid"
            payload = %{"hello" => "world"}
            version = 1
            snapshot = Signal.Snapshot.new(id, payload, version: version)
            {:ok, snapshot} = Store.record(snapshot)

            assert snapshot.id == id
            assert snapshot.payload == payload
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

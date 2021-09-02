defmodule Testament.PublisherTest do
    use ExUnit.Case, async: true
    alias Testament.Publisher
    import Testament.Factory

    setup do
        start_supervised(Publisher)
        :ok = Ecto.Adapters.SQL.Sandbox.checkout(Testament.Repo)
        Ecto.Adapters.SQL.Sandbox.mode(Testament.Repo, {:shared, self()})
        :ok
    end

    describe "Publisher" do

        @tag :publisher
        test "should publish event struct" do
            event = build(:value_updated)
            Publisher.publish(event)
        end

        @tag :publisher
        test "should publish stage struct" do
            stage = 
                build(:value_updated)
                |> Publisher.stage_event()
            Publisher.publish(stage)
        end

        @tag :publisher
        test "should publish list stage" do
            staged = 
                build(:value_updated)
                |> Publisher.stage_event()
                |> List.wrap()
            Publisher.publish(staged)
        end

    end

end

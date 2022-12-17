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

        @tag :publish
        test "event publish/1" do
            event = build(:value_updated)
            Publisher.publish(event)
        end

        @tag :publish
        test "staged publish/1" do
            stage = 
                build(:value_updated)
                |> Publisher.stage_event()
            Publisher.publish(stage)
        end

    end

end

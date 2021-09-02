defmodule Testament.BrokerTest do
    use ExUnit.Case, async: true

    setup do
        :ok = Ecto.Adapters.SQL.Sandbox.checkout(Testament.Repo)
        Ecto.Adapters.SQL.Sandbox.mode(Testament.Repo, {:shared, self()})
        :ok
    end

    describe "Broker" do
        @tag :broker
        test "should start broker" do
        end
    end

end

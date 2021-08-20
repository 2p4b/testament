defmodule Testament.SerializerTest do
    use ExUnit.Case

    defmodule Accounts do

        use Signal.Type

        schema do
            field :number,      String.t,   default: "123"
            field :balance,     integer(),  default: 0
        end
    end

    defmodule Deposited do

        use Signal.Event,
            aggregate: {:account, Accounts}

        schema do
            field :account,     String.t
            field :amount,      integer()
        end

    end

    describe "Serializer" do

        @tag :serializer
        test "should serialize event" do

            {_type, serialized} =
                [account: "123", amount: 5000]
                |> Deposited.new()
                |> Testament.Serializer.serialize()

            assert serialized == "{\"account\":\"123\",\"amount\":5000}"
        end


        @tag :serializer
        test "should deserialize event" do

            serialized = "{\"account\":\"123\",\"amount\":5000}"

            params = %{type: Atom.to_string(Deposited)}

            event =
                serialized
                |> Testament.Serializer.deserialize(params)

            assert match?(%Deposited{account: "123", amount: 5000}, event)
        end

    end

end

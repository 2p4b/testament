defmodule Testament.BrokerTest do
    use ExUnit.Case, async: true

    alias Signal.Events.Stage
    alias Signal.Events.Event

    defmodule TestApp do
        use Signal.Application
    end

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
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
        end
    end


    defmodule Deposite do

        use Signal.Command,
            aggregate: {:account, Accounts}

        schema do
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
        end

        def handle(%Deposite{}=deposite, %Accounts{number: "123", balance: 0}, _param) do
            Deposited.from(deposite)
        end
    end

    setup do
        :ok = Ecto.Adapters.SQL.Sandbox.checkout(Testament.Repo)
        Ecto.Adapters.SQL.Sandbox.mode(Testament.Repo, {:shared, self()})
        :ok
    end

    describe "Broker" do

        @tag :broker
        test "should start broker" do
            name = "Test.Broker"
            topic = "Test.Topic"

            broker =
                name
                |> Testament.Stream.Supervisor.prepare_broker()

            subscription =
                broker
                |> GenServer.call({:subscribe, topic}, 5000)

            assert match?(%Testament.Subscription{
                broker: ^name,
                topics: [ ^topic ]
            }, subscription)

            Process.sleep(2000)
        end

        @tag :broker
        test "should recieve events from bus" do
            reduction = 1

            deposited = Deposited.new([amount: 5000])

            stream = Signal.Aggregate.aggregate(deposited)

            topic = Signal.Helper.module_to_string(Deposited)

            broker =
                "Test.Broker"
                |> Testament.Stream.Supervisor.prepare_broker()

            %Testament.Subscription{} =
                broker
                |> GenServer.call({:subscribe, topic}, 5000)

            event =
                [amount: 5000]
                |> Deposite.new()
                |> Signal.Execution.Task.new([application: TestApp])
                |> Signal.Command.Action.from()
                |> Signal.Events.Event.new(deposited, stream, reduction)

            stage =
                %Stage{events: [event], version: 1, stream: stream, task: self()}


            Testament.commit([stage])

            assert_receive(%Event{ payload: ^deposited })
        end

    end

end

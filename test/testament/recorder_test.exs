defmodule Testament.RecorderTest do
    use ExUnit.Case, async: true

    alias Signal.Events.Stage
    alias Signal.Events.Record
    alias Signal.Events.History

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

    describe "Recorder" do

        @tag :recorder
        test "should start recorder" do
            reduction = 1

            deposite = Deposited.new([amount: 5000])

            stream = Signal.Aggregate.aggregate(deposite)

            event =
                [amount: 5000]
                |> Deposite.new()
                |> Signal.Execution.Task.new([application: TestApp])
                |> Signal.Command.Action.from()
                |> Signal.Events.Event.new(deposite, stream, reduction)

            stage =
                %Stage{events: [event], version: 1, stream: stream, task: self()}


            %Record{histories: [ history ]} = Testament.commit([stage])

            assert match?(%History{}, history)
        end

    end

end

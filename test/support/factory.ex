defmodule Testament.Factory do
    use ExMachina

    defmodule Aggregate do

        use Signal.Type

        schema do
            field :id,      String.t,   default: "123"
            field :value,   integer(),  default: 0
        end

    end

    defmodule ValueUpdated do

        use Signal.Event,
            stream: {Aggregate, :id}

        schema do
            field :id,      String.t,   default: "123"
            field :value,   integer(),  default: 0
        end

    end

    def aggregate_factory do
        %Aggregate{}
    end

    def value_updated_factory do
        %ValueUpdated{}
    end

    def stream_stage_factory do
        %Signal.Events.Stage{
            stream: {Aggregate, "test.stream"},
            version: nil,
            events: [],
        }
    end

end

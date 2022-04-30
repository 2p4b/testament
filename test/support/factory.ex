defmodule Testament.Factory do
    use ExMachina

    defmodule Aggregate do
        defstruct [id: "123", value: 0]
    end

    defmodule ValueUpdated do

        use Signal.Event,
            stream: {Aggregate, :id}

        blueprint do
            field :id,      :string,   default: "123"
            field :value,   :number,  default: 0
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

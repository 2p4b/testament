defmodule Testament.Subscription do
    defstruct [:handle, :ack, :syn, :pid, :stream, :topics]
end

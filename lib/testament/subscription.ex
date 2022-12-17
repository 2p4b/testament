defmodule Testament.Subscription do
    defstruct [:handle, :ack, :syn, :pid, :stream_id, :topics]
end

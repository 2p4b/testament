defmodule Testament.Serializer do

    alias Signal.Codec
    alias Signal.Helper

    def serialize(%{__struct__: type}=event) when is_struct(event) do
        payload =
            event
            |> Codec.encode()
            |> Jason.encode!()
        {Helper.module_to_string(type), payload}
    end

    def deserialize(binary, %{type: type}) when is_binary(binary) and is_binary(type) do
        type
        |> Helper.string_to_module()
        |> Kernel.struct([])
        |> Codec.load(Jason.decode!(binary))
    end

end

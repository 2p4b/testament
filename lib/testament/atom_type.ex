defmodule Testament.AtomType do

    use Ecto.Type
    alias Testament.Serializer

    def type, do: :string

    def cast(data) when is_atom(data) do
        {:ok, data}
    end

    def cast(_), do: :error

    def load(data) when is_binary(data) do
        {:ok, Signal.Helper.string_to_module(data)}
    end

    def dump(data) when is_atom(data) do
        {:ok, Signal.Helper.module_to_string(data)}
    end

    def dump(_), do: :error

end

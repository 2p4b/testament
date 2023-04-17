defmodule Testament.Repo.JSON do
    use Ecto.Type

    def type, do: :string

    def cast(value) do
        {:ok, value}
    end

    def load(data) do
        {:ok, Jason.decode!(data)}
    end

    def dump(value) when is_list(value) or is_map(value) do
        {:ok, Jason.encode!(value)}
    end

end


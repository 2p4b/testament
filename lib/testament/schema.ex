defmodule Testament.Schema do

    defmacro __using__(opts) do

        t_row = Keyword.get(opts, :row)

        quote do

            use Ecto.Schema

            import Ecto.Query, warn: false

            @row_name unquote(t_row)

            def new(attrs \\ []) when is_map(attrs) or is_list(attrs) do
                struct!(__MODULE__, attrs)
            end

            def name, do: @row_name

            def query(filters \\ [], opts \\ []) when is_list(filters) do
                query = from row in __MODULE__, as: unquote(t_row)

                defined = query.from.as

                query =
                    Enum.reduce(filters, query, fn {column, value}, query -> 
                        if is_list(value) do
                            from [{^defined, row}] in query,
                            where: field(row, ^column) in ^value
                        else
                            from [{^defined, row}] in query,
                            where: field(row, ^column) == ^value
                        end
                    end)

                query =
                    case Keyword.fetch(opts, :preload) do
                        {:ok, preload } ->
                            from [{^defined, row}] in query,
                            preload: ^preload
                        _ -> query
                    end

                query =
                    case Keyword.fetch(opts, :select) do
                        {:ok, columns} ->
                            from [{^defined, row}] in query,
                            select: struct(row, ^columns)
                        _ -> query
                    end
            end

        end

    end

end


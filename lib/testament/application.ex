defmodule Testament.Application do
    # See https://hexdocs.pm/elixir/Application.html
    # for more information on OTP Applications
    @moduledoc false

    use Application

    def start(_type, _args) do
        name = Testament.Supervisor
        strategy = :one_for_one
        Supervisor.start_link([], strategy: strategy, name: name)
    end

end

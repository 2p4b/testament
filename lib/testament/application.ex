defmodule Testament.Application do
    # See https://hexdocs.pm/elixir/Application.html
    # for more information on OTP Applications
    @moduledoc false

    use Application

    def start(_type, _args) do
        children = [
            {Phoenix.PubSub, name: :testament},
            {Registry, keys: :unique, name: Testament.Broker.Registry},
            Testament.Subscription.Supervisor,
            Testament.Repo,
            Testament.Publisher,
        ]

        Supervisor.start_link(children, strategy: :one_for_one, name: Testament.Supervisor)
    end

end

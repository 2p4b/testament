defmodule Testament.Application do
    # See https://hexdocs.pm/elixir/Application.html
    # for more information on OTP Applications
    @moduledoc false

    use Application

    def start(_type, _args) do
        children = [
            Testament.Repo,
            Testament.Recorder,
            {Phoenix.PubSub, name: :testament}
        ]

        Supervisor.start_link(children, strategy: :one_for_one, name: Testament.Supervisor)
    end
end

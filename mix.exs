defmodule Testament.MixProject do
    use Mix.Project

    def project do
        [
            app: :testament,
            version: "0.1.0",
            elixir: "~> 1.14",
            elixirc_paths: elixirc_paths(Mix.env()),
            start_permanent: Mix.env() == :prod,
            consolidate_protocols: Mix.env() != :test,
            aliases: aliases(),
            deps: deps()
        ]
    end

    # Configuration for the OTP application.
    #
    # Type `mix help compile.app` for more information.
    def application do
        [
            mod: {Testament.Application, []},
            extra_applications: [:logger, :runtime_tools, :ex_machina]
        ]
    end

    # Specifies which paths to compile per environment.
    defp elixirc_paths(:test), do: ["lib", "test/support"]
    defp elixirc_paths(_), do: ["lib"]

    # Specifies your project dependencies.
    #
    # Type `mix help deps` for examples and options.
    defp deps do
        [
            {:jason, "~> 1.0"},
            {:myxql, ">= 0.0.0"},
            {:ecto_sql, "~> 3.1"},
            {:ex_machina, "~> 2.6.0"},
            {:phoenix_pubsub, "~> 2.0"},
            {:signal, git: "https://github.com/2p4b/signal.git", branch: "main"},
            #{:signal, path: "~/Projects/elixir/signal"},
        ]
    end

    # Aliases are shortcuts or tasks specific to the current project.
    # For example, to create, migrate and run the seeds file at once:
    #
    #     $ mix ecto.setup
    #
    # See the documentation for `Mix` for more info on aliases.
    defp aliases do
        [
            "testament.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
            "testament.reset": ["ecto.drop", "ecto.setup"],
            test: ["ecto.create --quiet", "ecto.migrate", "test"]
        ]
    end
end

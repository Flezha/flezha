defmodule Flezha.MixProject do
  use Mix.Project

  def project do
    [
      app: :flezha,
      version: "0.2.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      releases: releases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases()
    ]
  end

  # Mix only auto-sets MIX_ENV=test for the literal `mix test` task.
  # Aliases that wrap `test ...` need an explicit hint, otherwise the
  # inner call complains that it's running in :dev. Listed here is every
  # alias that ultimately invokes `test`.
  def cli do
    [preferred_envs: ["test.java_sdk": :test]]
  end

  # Aliases are invoked via `mix <alias>`. `test.java_sdk` runs the Java
  # integration matrix against the *committed* test compute-engine fat jar
  # at `test/apps/java_compute_engine/dist/java_compute_engine.jar`. The
  # jar — and the five native binaries it bundles — are refreshed locally
  # via `scripts/build-dist.sh`; CI never invokes Maven for either.
  defp aliases do
    [
      "test.java_sdk": ["test --only java_sdk"],
      # Opt-in dev-compose smoke test for the connection-healing change.
      # Brings up the agent + rust operator container, restarts each in turn,
      # asserts the operator <-> agent link re-establishes within
      # REJOIN_TIMEOUT_S seconds. Not part of `mix test` because it needs
      # Docker on the host. See test/scripts/connection_healing.sh.
      "test.connection_healing": ["cmd test/scripts/connection_healing.sh"],
      # Opt-in dev-compose smoke test for `ship-fresh-state-on-sdk-shutdown`.
      # Boots the agent + rust operator, starts a processor, sends SIGTERM
      # to the rust container, then asserts the agent's
      # `Overwatch.state.snapshot` was populated by the SDK's
      # `OperatorShutdownNotification`. Both periodic-enabled and
      # disabled-snapshot scenarios. Needs Docker on the host. See
      # test/scripts/state_freshness.sh.
      "test.state_freshness": ["cmd test/scripts/state_freshness.sh"]
    ]
  end

  def application do
    [extra_applications: extra_applications(Mix.env()), mod: {Flezha.Application, []}]
  end

  # :wx + :observer are dev-only — they pull wxWidgets shared libs the
  # headless prod release has no use for.
  defp extra_applications(:prod),
    do: [:logger, :runtime_tools, :inets, :crypto, :ssl, :telemetry]

  defp extra_applications(_),
    do: [:logger, :runtime_tools, :wx, :observer, :inets, :crypto, :ssl, :telemetry]

  defp releases() do
    [
      flezha: [
        include_executables_for: [:unix],
        include_erts: true,
        application: [:flezha],
        steps: [:assemble, &copy_cli_commands/1]
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      # {:process_hub, git: "https://github.com/alfetahe/process-hub", branch: "dev"},
      # {:process_hub, path: "../process-hub"},
      {:process_hub, "~> 0.5.0-beta"},
      {:blockade, "~> 0.2.2"},
      {:fresh, "~> 0.4.4"},
      {:kafka_ex, github: "alfetahe/kafka_ex", branch: "fix/setopts-invalid-socket-crash"},
      {:protox, "~> 2.0"},
      {:mimic, "~> 1.11"},
      {:logger_file_backend, "~> 0.0.13"},
      {:logger_backends, "~> 1.0"},
      {:rustler, "~> 0.35"}
    ]
  end

  # TODO: Also copy windows binaries
  defp copy_cli_commands(release) do
    env =
      case Mix.env() do
        :prod -> "release"
        _ -> "debug"
      end

    File.cp!("cli/target/#{env}/flz", Path.join(release.path, "bin/flz"))
    File.rename!(Path.join(release.path, "bin/flezha"), Path.join(release.path, "bin/flzvm"))

    release
  end

  defp elixirc_paths(:prod), do: ["lib"]
  defp elixirc_paths(:test), do: ["lib", "test", "test/support"]
  defp elixirc_paths(_), do: ["lib", "test"]
end

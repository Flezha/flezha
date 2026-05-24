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
  # integration matrix against the *committed* test executor fat jar
  # at `test/apps/java_executor/dist/java_executor.jar`. The
  # jar — and the five native binaries it bundles — are refreshed locally
  # via `scripts/build-dist.sh`; CI never invokes Maven for either.
  defp aliases do
    [
      "test.java_sdk": ["test --only java_sdk"],
      # Build the WebSocket testnet bundle that several `mix test` cases
      # reach for over WS (`flezha_test.exs` "websocket plugin connection
      # start", "beam msg forward plugin connection start", "relay flow
      # generation -> beam msg forward"). The bundle output at
      # `test/apps/testnet/dist/index.js` is not in git; without this
      # alias those cases skip with a clear message rather than crash
      # inside the WS plugin connect path.
      "test.setup": [
        "cmd npm --prefix test/apps/testnet install",
        "cmd npm --prefix test/apps/testnet run build"
      ],
      # Opt-in dev-compose smoke test for the connection-healing change.
      # Brings up the controller + rust executor container, restarts each in turn,
      # asserts the executor <-> controller link re-establishes within
      # REJOIN_TIMEOUT_S seconds. Not part of `mix test` because it needs
      # Docker on the host. See test/scripts/connection_healing.sh.
      "test.connection_healing": [
        "cmd test/scripts/connection_healing.sh",
        "cmd test/scripts/plugin_persistence.sh"
      ],
      # Opt-in dev-compose smoke test for `ship-fresh-state-on-sdk-shutdown`.
      # Boots the controller + rust executor, starts a relay, sends SIGTERM
      # to the rust container, then asserts the controller's
      # `Overwatch.state.snapshot` was populated by the SDK's
      # `ExecutorShutdownNotification`. Both periodic-enabled and
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
      # ProcessHub is pinned to the github master branch until durable-registry +
      # coordinator-bootstrap-recovery are stabilised and a new hex version is cut.
      #
      # Dev (self-healing-control-plane): consume the local ../process-hub checkout
      # (branch dev) so the additions (leader/0, purge_node/2) are usable before
      # they are pushed. Restore the git dep at the pushed dev SHA before the
      # Session-3 operator deploy.
      # {:process_hub, git: "https://github.com/alfetahe/process-hub", branch: "dev"},
      {:process_hub, path: "../process-hub"},
      # {:process_hub, "~> 0.5.0-beta"},
      {:blockade, "~> 0.2.2"},
      {:fresh, "~> 0.4.4"},
      {:kafka_ex, github: "alfetahe/kafka_ex", branch: "fix/setopts-invalid-socket-crash"},
      {:protox, "~> 2.0"},
      {:mimic, "~> 1.11"},
      {:logger_file_backend, "~> 0.0.13"},
      {:logger_backends, "~> 1.0"},
      {:rustler, "~> 0.35"},
      {:recon, "~> 2.5"},
      {:phoenix_pubsub, "~> 2.1"},
      # Hoisted to a top-level dep (already transitively present via
      # :mint_web_socket) so the OTLP exporter calls Mint directly.
      {:mint, "~> 1.7"}
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

defmodule Flezha.MixProject do
  use Mix.Project

  def project do
    [
      app: :flezha,
      version: "0.1.0",
      elixir: "~> 1.17",
      description: "Distributed stateful operatives — durable stream and workflow compute",
      source_url: "https://github.com/alfetahe/flezha",
      package: [
        licenses: ["MPL-2.0", "Apache-2.0"],
        links: %{"GitHub" => "https://github.com/alfetahe/flezha"}
      ],
      start_permanent: Mix.env() == :prod,
      releases: releases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      # The nested test/apps/elixir_runner mix project has its own mix.exs and
      # lib/*.ex; keep `mix test`'s test-file scan from mistaking them for
      # mis-named test files.
      test_ignore_filters: [&String.starts_with?(&1, "test/apps/")],
      deps: deps(),
      aliases: aliases()
    ]
  end

  # Aliases are invoked via `mix <alias>`.
  defp aliases do
    [
      # Build the WebSocket testnet bundle that several `mix test` cases
      # reach for over WS (`flezha_test.exs` "websocket plugin connection
      # start", "beam msg forward plugin connection start", "operative flow
      # generation -> beam msg forward"). The bundle output at
      # `test/apps/testnet/dist/index.js` is not in git; without this
      # alias those cases skip with a clear message rather than crash
      # inside the WS plugin connect path.
      "test.setup": [
        "cmd npm --prefix test/apps/testnet install",
        "cmd npm --prefix test/apps/testnet run build"
      ],
      # Opt-in dev-compose smoke test for the connection-healing change.
      # Brings up the controller + rust runner container, restarts each in turn,
      # asserts the runner <-> controller link re-establishes within
      # REJOIN_TIMEOUT_S seconds. Not part of `mix test` because it needs
      # Docker on the host. See test/scripts/connection_healing.sh.
      "test.connection_healing": [
        "cmd test/scripts/connection_healing.sh",
        "cmd test/scripts/plugin_persistence.sh"
      ],
      # Opt-in dev-compose smoke test for `ship-fresh-state-on-sdk-shutdown`.
      # Boots the controller + rust runner, starts an operative, sends SIGTERM
      # to the rust container, then asserts the controller's
      # `Overwatch.state.snapshot` was populated by the SDK's
      # `RunnerShutdownNotification`. Both periodic-enabled and
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
        steps: [:assemble, &copy_release_extras/1]
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      # Pinned to the dev branch until 0.7.1 is published: 0.7.0 ignores
      # `cluster_settle_ms` without a word and restarts a replayed row with no
      # metadata. A path dep could not reach the release build, which runs in a
      # container that mounts no sibling checkout, so a release silently shipped
      # 0.7.0. Back to `~> 0.7.0` once the release is on hex.
      {:process_hub, github: "alfetahe/process-hub", branch: "dev"},
      # process_hub declares elector `runtime: false`, which keeps it out of
      # every release. Declared here so the namespace hubs' leader election
      # reaches the assembled release.
      {:elector, "~> 0.3.4"},
      {:blockade, "~> 0.2.2"},
      # fresh 0.4.4's released mix.exs declares charlist elixirc_paths,
      # which Mix >= 1.20 rejects; upstream is unresponsive. Pinned to the
      # PR branch carrying the fix. Pin the branch head, not a fork's default
      # branch commit: the previous pin was rewritten away upstream and no
      # ref reached it any more, so a fresh clone could not resolve deps.
      {:fresh, github: "josevalim/fresh", ref: "d8159bf148df51a15d91c432bf2120ff56d384a0"},
      {:kafka_ex, github: "alfetahe/kafka_ex", ref: "0764363353dc2ffc9996bdc8af11b9fe00561a57"},
      # Compiled in unconditionally; active only when the Postgres sink
      # plugin is configured (same posture as :kafka_ex).
      {:postgrex, "~> 0.22"},
      {:protox, "~> 2.0"},
      {:mimic, "~> 1.11"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:logger_file_backend, "~> 0.0.13"},
      {:logger_backends, "~> 1.0"},
      {:rustler, "~> 0.35"},
      {:recon, "~> 2.5"},
      {:phoenix_pubsub, "~> 2.1"},
      # Hoisted to a top-level dep (already transitively present via
      # :mint_web_socket) so the OTLP exporter calls Mint directly.
      {:mint, "~> 1.7"},
      # IANA time zone database for the schedule evaluator (pure Elixir,
      # zero runtime deps; passed explicitly as Tz.TimeZoneDatabase).
      {:tz, "~> 0.28"}
    ]
  end

  # TODO: Also copy windows binaries
  defp copy_release_extras(release) do
    env =
      case Mix.env() do
        :prod -> "release"
        _ -> "debug"
      end

    File.cp!("target/#{env}/flz", Path.join(release.path, "bin/flz"))
    File.rename!(Path.join(release.path, "bin/flezha"), Path.join(release.path, "bin/flzvm"))

    for file <- ~w(LICENSE NOTICE THIRD_PARTY_NOTICES.md),
        do: File.cp!(file, Path.join(release.path, file))

    File.cp_r!("LICENSES", Path.join(release.path, "LICENSES"))

    announce_binaries(release)
  end

  # Mix's own closing message names `bin/<release name>`, which the rename
  # above has just moved to `bin/flzvm`, so it hands the operator a path that
  # does not exist. `:quiet` suppresses it (announcing is all Mix has left to
  # do once the steps return) and this prints the two real binaries instead.
  defp announce_binaries(release) do
    path = Path.relative_to_cwd(release.path)

    if !release.options[:quiet] do
      Mix.shell().info("""

      Release created at #{path}

          # To start the controller
          #{path}/bin/flzvm start

          # The operator CLI
          #{path}/bin/flz --help

      Once the controller is running:

          # To connect to it remotely
          #{path}/bin/flzvm remote

          # To stop it gracefully (you may also send SIGINT/SIGTERM)
          #{path}/bin/flzvm stop

      To list all controller commands:

          #{path}/bin/flzvm
      """)
    end

    %{release | options: Keyword.put(release.options, :quiet, true)}
  end

  # Explicit test dirs (not a recursive "test" glob) so a nested mix app under
  # test/apps/ — e.g. test/apps/elixir_runner — is never compiled into the
  # controller and cannot collide with the app's own compile.
  defp elixirc_paths(:prod), do: ["lib"]
  defp elixirc_paths(:test), do: ["lib", "test/fixtures", "test/helpers"]
  defp elixirc_paths(_), do: ["lib", "test/fixtures", "test/helpers"]
end

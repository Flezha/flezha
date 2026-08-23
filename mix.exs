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

  # Mix only auto-sets MIX_ENV=test for the literal `mix test` task.
  # Aliases that wrap `test ...` need an explicit hint, otherwise the
  # inner call complains that it's running in :dev. Listed here is every
  # alias that ultimately invokes `test`.
  def cli do
    [preferred_envs: ["test.java_sdk": :test]]
  end

  # Aliases are invoked via `mix <alias>`. `test.java_sdk` runs the Java
  # integration matrix against the *committed* test runner fat jar
  # at `test/apps/java_runner/dist/java_runner.jar`. The
  # jar — and the five native binaries it bundles — are refreshed locally
  # via `scripts/build-dist.sh`; CI never invokes Maven for either.
  defp aliases do
    [
      "test.java_sdk": ["test --only java_sdk"],
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
      # github `dev` branch by default (temporary — see process_hub_dep/0);
      # the deploy sets FLEZHA_PROCESS_HUB_PATH to build from a local checkout.
      {:process_hub, process_hub_dep()},
      {:blockade, "~> 0.2.2"},
      # fresh 0.4.4's released mix.exs declares charlist elixirc_paths,
      # which Mix >= 1.20 rejects; upstream is unresponsive. Pinned to
      # José Valim's fix (bunopnu/fresh PR #5) until it is merged+released.
      {:fresh, github: "josevalim/fresh", ref: "26291bc9b3d869b555785fa8fc790e8670711309"},
      {:kafka_ex, github: "alfetahe/kafka_ex", ref: "0764363353dc2ffc9996bdc8af11b9fe00561a57"},
      # Compiled in unconditionally; active only when the Postgres sink
      # plugin is configured (same posture as :kafka_ex).
      {:postgrex, "~> 0.22"},
      {:protox, "~> 2.0"},
      {:mimic, "~> 1.11"},
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

  # TEMPORARY: tracks process_hub's `dev` branch until the next hex release,
  # then reverts to a `"~> 0.6.x"` requirement. mix.lock pins the commit, so
  # `mix deps.get` alone does NOT follow the branch — `make refresh-process-hub`
  # (and the controller image build) unlock it to take the tip.
  defp process_hub_dep do
    case System.get_env("FLEZHA_PROCESS_HUB_PATH") do
      path when is_binary(path) and path != "" -> [path: path]
      _ -> [git: "https://github.com/alfetahe/process-hub", branch: "dev"]
    end
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

    release
  end

  # Explicit test dirs (not a recursive "test" glob) so a nested mix app under
  # test/apps/ — e.g. test/apps/elixir_runner — is never compiled into the
  # controller and cannot collide with the app's own compile.
  defp elixirc_paths(:prod), do: ["lib"]
  defp elixirc_paths(:test), do: ["lib", "test/fixtures", "test/helpers", "test/support"]
  defp elixirc_paths(_), do: ["lib", "test/fixtures", "test/helpers", "test/support"]
end

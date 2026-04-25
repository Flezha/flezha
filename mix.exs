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

  # Aliases are invoked via `mix <alias>`. `test.java_sdk` runs the Java
  # integration matrix against the *committed* test compute-engine fat jar
  # at `test/apps/java_compute_engine/dist/java_compute_engine.jar`. The
  # jar — and the five native binaries it bundles — are refreshed locally
  # via `scripts/build-dist.sh`; CI never invokes Maven for either.
  defp aliases do
    [
      "test.java_sdk": ["test --only java_sdk"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [
        :logger,
        :runtime_tools,
        :wx,
        :observer,
        :inets,
        :crypto,
        :ssl,
        :telemetry
      ],
      mod: {Flezha.Application, []}
    ]
  end

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
  defp elixirc_paths(_), do: ["lib", "test"]
end

defmodule Flezha.MixProject do
  use Mix.Project

  def project do
    [
      app: :flezha,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      releases: releases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :runtime_tools, :wx, :observer, :inets, :crypto, :ssl],
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
      {:process_hub, git: "https://github.com/alfetahe/process-hub", branch: "dev"},
      # {:process_hub, path: "./process-hub"},
      # {:process_hub, "~> 0.3.2-alpha"},
      {:blockade, "~> 0.2.1"},
      {:fresh, "~> 0.4.4"},
      {:kafka_ex, "~> 0.14.0"},
      {:protox, "~> 2.0"},
      {:mimic, "~> 1.11"}
    ]
  end

  # TODO: Also copy windows binaries
  defp copy_cli_commands(release) do
    env =
      case Mix.env() do
        :prod -> "release"
        _ -> "debug"
      end

    File.cp!("cli/target/#{env}/agent", Path.join(release.path, "bin/agent"))
    File.cp!("cli/target/#{env}/computation", Path.join(release.path, "bin/computation"))
    File.cp!("cli/target/#{env}/plugin", Path.join(release.path, "bin/plugin"))
    File.cp!("cli/target/#{env}/group", Path.join(release.path, "bin/group"))
    File.cp!("cli/target/#{env}/operator", Path.join(release.path, "bin/operator"))

    release
  end

  defp elixirc_paths(:prod), do: ["lib"]
  defp elixirc_paths(_), do: ["lib", "test"]
end

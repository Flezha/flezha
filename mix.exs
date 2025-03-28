defmodule Aflecha.MixProject do
  use Mix.Project

  def project do
    [
      app: :aflecha,
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
      mod: {Aflecha.Application, []}
    ]
  end

  defp releases() do
    [
      aflecha: [
        include_executables_for: [:unix],
        include_erts: true,
        application: [:aflecha],
        steps: [:assemble, &copy_cli_commands/1]
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:process_hub, path: "../process-hub"},
      # {:process_hub, "~> 0.3.1-alpha"},
      {:blockade, "~> 0.2.1"},
      {:fresh, "~> 0.4.4"},
      {:kafka_ex, "~> 0.14.0"},
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
    File.cp!("cli/target/#{env}/worker", Path.join(release.path, "bin/worker"))
    File.cp!("cli/target/#{env}/io", Path.join(release.path, "bin/io"))
    release
  end

  defp elixirc_paths(:test), do: ["lib", "test"]
  defp elixirc_paths(_), do: ["lib"]
end

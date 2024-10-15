defmodule FlechaCore.MixProject do
  use Mix.Project

  def project do
    [
      app: :flecha_core,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      releases: releases(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :runtime_tools, :wx, :observer],
      mod: {Flecha.Application, []}
    ]
  end

  defp releases() do
    [
      flecha_core: [
        include_executables_for: [:unix, :windows],
        include_erts: true,
        application: [:flecha_core]
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:process_hub, "~> 0.2.9-alpha"},
      {:blockade, "~> 0.2.1"},
      {:fresh, "~> 0.4.4"}
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end

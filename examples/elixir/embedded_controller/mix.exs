defmodule EmbeddedController.MixProject do
  use Mix.Project

  # An ordinary Elixir application that IS a Flezha controller: depending on
  # flezha and leaving `controller_enabled` at its default boots the whole
  # controller tree inside this app's BEAM node. The counterpart is
  # `test/apps/elixir_runner`, which sets `controller_enabled: false` and is a
  # runner instead.
  #
  # The guide shows the dependency as a git reference, which is what a reader
  # outside this repository writes. Here it is a path dependency sharing the
  # repository's resolved dep set and lockfile, so the example builds from the
  # checkout it lives in rather than fetching a second copy of it.
  def project do
    [
      app: :embedded_controller,
      version: "0.1.0",
      elixir: "~> 1.17",
      deps_path: Path.expand("../../../deps", __DIR__),
      lockfile: Path.expand("../../../mix.lock", __DIR__),
      build_path: Path.expand("_build", __DIR__),
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [{:flezha, path: Path.expand("../../..", __DIR__)}]
  end
end

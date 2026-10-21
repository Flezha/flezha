defmodule FlezhaExamplePlugin.MixProject do
  use Mix.Project

  # A plugin is an ordinary Mix project: it depends on flezha and on the
  # libraries it needs, and names its providers in its application environment.
  # `mix flezha.plugin.package` turns it into an archive a controller loads at
  # boot.
  #
  # Here flezha is a path dependency sharing the repository's resolved deps and
  # lockfile, so the example builds from the checkout it lives in. A project
  # outside this repository depends on flezha by git or Hex instead.
  def project do
    [
      app: :flezha_example_plugin,
      version: "0.1.0",
      elixir: "~> 1.17",
      deps_path: Path.expand("../../../deps", __DIR__),
      lockfile: Path.expand("../../../mix.lock", __DIR__),
      build_path: Path.expand("_build", __DIR__),
      deps: deps()
    ]
  end

  # No `mod`: a plugin's processes are its providers' declared workers, which
  # the controller supervises.
  def application do
    [env: [flezha_providers: %{"example_stamp" => FlezhaExamplePlugin.Stamp}]]
  end

  defp deps do
    [
      {:flezha, path: Path.expand("../../..", __DIR__)},
      {:stamp_lib, path: "stamp_lib"}
    ]
  end
end

defmodule Flezha.Examples.HelloWorld do
  @moduledoc """
  Minimal installable-plugin example. Demonstrates the shape of a package:

      examples/plugins/hello_world/
      ├── manifest.exs
      ├── src/
      │   └── hello_world.ex
      └── CHECKSUMS.sha256

  Build the archive:

      cd examples/plugins/hello_world
      sha256sum manifest.exs src/*.ex > CHECKSUMS.sha256
      tar czf ../hello_world-1.0.0.fpz manifest.exs src/ CHECKSUMS.sha256

  Install:

      flz plugin install /path/to/hello_world-1.0.0.fpz
      flz plugin enable hello_world
  """

  @doc """
  Required by the installer when the plugin is enabled against an
  `:on_input` extension: called for every input the host operative
  observes. See `Flezha.Plugin.Extension` for the full callback table.
  """
  def on_input(ctx, opts) do
    _ = {ctx, opts}
    :ok
  end
end

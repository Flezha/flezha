defmodule StampLib.MixProject do
  use Mix.Project

  # A library the example plugin bundles: an application that starts a process,
  # a file under priv/, and an implementation of a protocol Elixir defines.
  def project do
    [app: :stamp_lib, version: "0.1.0", elixir: "~> 1.17"]
  end

  def application do
    [mod: {StampLib.Application, []}]
  end
end

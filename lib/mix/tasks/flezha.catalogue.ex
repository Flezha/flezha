defmodule Mix.Tasks.Flezha.Catalogue do
  @shortdoc "Generate cli/catalogue.json from the published operations"

  @moduledoc """
  Writes `cli/catalogue.json`, the catalogue of every published operation
  (`Flezha.Service.catalogue/0`), which `flz` embeds: `flz help --json` prints
  it, and the CLI's tests hold every door call `flz` sends to it. The file is
  generated and never edited by hand: `test/flezha/api/catalogue_test.exs`
  fails when the committed file differs from a fresh generation.

      mix flezha.catalogue
  """

  use Mix.Task

  @path "cli/catalogue.json"

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("compile")
    File.write!(@path, render())
    Mix.shell().info("wrote #{@path}")
  end

  @doc "The file's path, relative to the repository root."
  @spec path() :: String.t()
  def path, do: @path

  @doc "The file, as the compiled declarations describe the catalogue."
  @spec render() :: String.t()
  def render do
    operations = Enum.map(Flezha.Service.catalogue(), &Flezha.Service.describe/1)
    Jason.encode!(Jason.OrderedObject.new(operations: operations), pretty: true) <> "\n"
  end
end

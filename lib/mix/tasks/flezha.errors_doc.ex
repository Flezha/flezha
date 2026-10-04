defmodule Mix.Tasks.Flezha.ErrorsDoc do
  @shortdoc "Generate docs/errors.md from the error catalogue"

  @moduledoc """
  Writes `docs/errors.md`, the published list of every error code, from
  `flz_transport/errors.json`. The page is generated and never edited by hand:
  `test/flezha/error_catalogue_test.exs` fails when the committed page differs
  from a fresh generation.

      mix flezha.errors_doc
  """

  use Mix.Task

  @path "docs/errors.md"

  @impl Mix.Task
  def run(_args) do
    File.write!(@path, render())
    Mix.shell().info("wrote #{@path}")
  end

  @doc "The page, as the catalogue compiled into `Flezha.Error` describes it."
  @spec render() :: String.t()
  def render do
    catalogue = Flezha.Error.catalogue()
    exits = catalogue["classes"]

    families =
      catalogue["codes"]
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.group_by(fn {code, _entry} -> code |> String.split(".") |> hd() end)
      |> Enum.sort_by(&elem(&1, 0))

    IO.iodata_to_binary([
      header(exits, families),
      Enum.map(families, fn {family, codes} -> family_section(family, codes, exits) end)
    ])
  end

  defp header(exits, families) do
    classes =
      exits
      |> Enum.sort_by(&elem(&1, 1))
      |> Enum.map(fn {class, exit} -> "| `#{class}` | #{exit} | #{class_meaning(class)} |\n" end)

    [
      "# Errors\n\n",
      "<!-- Generated from flz_transport/errors.json by `mix flezha.errors_doc`; do not edit. -->\n\n",
      "Every failure Flezha reports carries one shape, on every surface: the `flz` CLI, the\n",
      "published consumer API, the SDK administration door, spawn, a runner connect and every\n",
      "SDK. A program acts on the `code` and the `class`.\n\n",
      "| Field | Meaning |\n|---|---|\n",
      "| `code` | Stable, spelled `family.snake_case`. |\n",
      "| `class` | What kind of failure it is, and so what a caller can do about it. |\n",
      "| `message` | One plain sentence naming what it is about. |\n",
      "| `details` | An object carrying the facts the message names. |\n",
      "| `hint` | The next command to run, or `null`. |\n\n",
      contents(families),
      "## Classes and exit codes\n\n",
      "A `flz` verb exits with the code of its failure's class. Success is 0.\n\n",
      "| Class | Exit code | Meaning |\n|---|---|---|\n",
      classes,
      "\n## Forms\n\n",
      "In text, a failure is written on stderr as:\n\n",
      "```\nerror: <message> (<code>)\nhint: <hint>\n```\n\n",
      "Under `--json` or `--output json`, it is one line on stderr:\n\n",
      "```json\n",
      ~s({"error": {"code": "namespace.not_found", "class": "not_found", ),
      ~s("message": "namespace 'ghost' does not exist", "details": {"namespace": "ghost"}, ),
      ~s("hint": "flz namespace list"}}\n),
      "```\n\n",
      "The consumer API answers `{:error, %Flezha.Error{}}` with the same five fields, and each\n",
      "SDK raises or returns its one error type carrying them.\n\n",
      "## Codes\n"
    ]
  end

  defp contents(families) do
    [
      "**Contents**\n\n",
      "1. [Classes and exit codes](#classes-and-exit-codes)\n",
      "2. [Forms](#forms)\n",
      "3. [Codes](#codes)\n",
      Enum.map(families, fn {family, _codes} -> "   1. [#{family}](##{family})\n" end),
      "\n"
    ]
  end

  defp class_meaning("internal"), do: "A defect or an unexpected failure inside Flezha."
  defp class_meaning("invalid"), do: "The request itself is wrong; fix it and ask again."
  defp class_meaning("unreachable"), do: "The controller could not be reached."
  defp class_meaning("denied"), do: "The caller's identity or role does not allow it."
  defp class_meaning("not_found"), do: "What the request names does not exist."
  defp class_meaning("conflict"), do: "The current state refuses it; change the state first."
  defp class_meaning("unavailable"), do: "Not possible right now; it may succeed if retried."

  defp family_section(family, codes, exits) do
    rows =
      Enum.map(codes, fn {code, entry} ->
        hint = if entry["hint"], do: "`#{String.replace(entry["hint"], "|", "\\|")}`", else: ""

        "| `#{code}` | #{entry["class"]} | #{exits[entry["class"]]} | " <>
          "#{escape(entry["message"])} | #{hint} |\n"
      end)

    [
      "\n### #{family}\n\n",
      "| Code | Class | Exit | Message | Hint |\n|---|---|---|---|---|\n",
      rows
    ]
  end

  # A table cell: a pipe would end it, and `<key>` would read as HTML.
  defp escape(text) do
    text
    |> String.replace("|", "\\|")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
  end
end

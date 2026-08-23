defmodule Mix.Tasks.Bench.Floor do
  use Mix.Task

  @shortdoc "Print the recorded BENCH_FLOOR_MSG_PER_SEC from docs/operations/benchmarking.md"
  @moduledoc """
  Prints the `BENCH_FLOOR_MSG_PER_SEC` value recorded in
  `docs/operations/benchmarking.md` to stdout. Used by the `bench-regression` CI job
  per `operative-bench-harness/design.md` Decision 6 to fetch the floor at
  runtime instead of hard-coding it in workflow YAML.

  Output format: a single integer line with no surrounding whitespace.
  Exits non-zero if the doc is missing or the value cannot be parsed.

      mix bench.floor
      300000
  """

  @doc_path "docs/operations/benchmarking.md"
  # Matches `BENCH_FLOOR_MSG_PER_SEC = 300_000` or `… = 300000` in the doc,
  # with optional spacing and underscores in the numeric literal.
  @floor_re ~r/BENCH_FLOOR_MSG_PER_SEC\s*=\s*([0-9_]+)/

  @impl Mix.Task
  def run(_args) do
    case File.read(@doc_path) do
      {:ok, content} ->
        case Regex.run(@floor_re, content) do
          [_, raw] ->
            n =
              raw
              |> String.replace("_", "")
              |> String.to_integer()

            IO.puts(Integer.to_string(n))

          _ ->
            Mix.shell().error(
              "Could not find BENCH_FLOOR_MSG_PER_SEC in #{@doc_path}. " <>
                "Expected a line like `BENCH_FLOOR_MSG_PER_SEC = 300_000`."
            )

            exit({:shutdown, 1})
        end

      {:error, reason} ->
        Mix.shell().error(
          "Could not read #{@doc_path}: #{:file.format_error(reason)}. " <>
            "The bench floor is recorded by `mix bench --shape regression` and " <>
            "embedded by operative-bench-harness §4.6."
        )

        exit({:shutdown, 1})
    end
  end
end

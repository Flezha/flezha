defmodule FlezhaTest.Helper.ReferenceTopologies do
  @moduledoc """
  `test/fixtures/reference_topologies.txt` read as data.

  Two modules are measured against that file — the seam test in the default
  tier and the drill under `:interop` — and a fixture parsed two ways is a
  fixture two readers can disagree about. This is the one reader.

  The file carries two tables. The seven-column one states what a well-formed
  topology DERIVES to; the four-column one names one refusal per row, with the
  authoring steps that produce it and the surface it needs.
  """

  @fixture "test/fixtures/reference_topologies.txt"

  @doc "Path of the fixture, for a failure message that names it."
  def path, do: @fixture

  @doc """
  The refusal table: `%{refusal, topology, steps, needs}` per row. `refusal` is
  a `TopologyIssue` variant name or `"ok"`; `needs` is `"-"` or the authoring
  surface the row cannot be written without.
  """
  def refusal_rows do
    for cols <- rows(), length(cols) == 4 do
      [refusal, topology, steps, needs] = cols
      %{refusal: refusal, topology: topology, steps: steps, needs: needs}
    end
  end

  @doc """
  The derived lines of one topology id, without the id column: the six columns
  `index | name | kind | producer | consumers | sink`, in declaration order.
  """
  def derived_lines(topology) do
    for [id | cols] <- rows(), length(cols) == 6, id == topology, do: cols
  end

  @doc """
  The surfaces the header declares a row may need. A skip is only countable
  against one of these, which is what stops "we do not support it" from
  becoming a silent hole.
  """
  def known_surfaces do
    header =
      read()
      |> String.split("\n")
      |> Enum.filter(&String.starts_with?(String.trim(&1), "#"))
      |> Enum.join("\n")

    ["-" | Regex.scan(~r/^#   ([a-z_]+) +\S/m, header) |> Enum.map(fn [_, name] -> name end)]
  end

  defp rows do
    read()
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(String.trim(&1), "#"))
    |> Enum.map(fn line -> line |> String.split("|") |> Enum.map(&String.trim/1) end)
  end

  defp read, do: File.read!(@fixture)
end

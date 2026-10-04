defmodule Flezha.Service.Cli.Listing do
  @moduledoc """
  `flz namespace list`, `flz runner list`, `flz operator list` and `flz cluster
  status`: the text forms of the listings.
  """

  alias Flezha.Service.Cli.Context

  @doc "`flz namespace list`: each namespace in the catalog, and whether it runs here."
  def namespaces(rows, _args, _opts) do
    table(rows, %{
      empty: "No namespaces found.",
      header: "Namespaces:",
      columns: [
        {"NAMESPACE", 35, & &1.namespace},
        {"RUNNING", 0, &if(&1.running, do: "yes", else: "no")}
      ]
    })
  end

  @doc "`flz runner list`: one bullet per runner, with its process id when known."
  def runners([], [namespace | _], _opts),
    do: IO.puts("No runners found for namespace '#{namespace}'.")

  def runners(rows, [namespace | _], _opts) do
    IO.puts("Runners in namespace '#{namespace}':")
    Enum.each(rows, fn row -> IO.puts("  - #{row.id}#{pid_suffix(row.pid)}") end)
  end

  @doc "`flz operator list`: one row per operator, a partition group as one row."
  def operators(rows, [namespace | _], _opts) do
    table(rows, %{
      empty: "No operators found for namespace '#{namespace}'.",
      header: "Operators in namespace '#{namespace}':",
      columns: [
        {"OPERATOR", 34, &to_string(&1.id)},
        {"HEALTH", 12, &to_string(&1.health)},
        {"PARTS", 8, &Context.label_or(&1[:partitions], "-")},
        {"NODE", 22, &Context.label_or(&1.node, "(unplaced)")},
        {"RUNNER", 0, &Context.label_or(&1.runner, "(none)")}
      ]
    })
  end

  @doc "`flz cluster status`."
  def cluster(s, _args, _opts) do
    IO.puts("Cluster status for namespace '#{s.namespace}':")
    IO.puts("")
    IO.puts("  Leader:     #{Context.label_or(s.leader, "(none)")}")
    IO.puts("  Local node: #{s.local_node}#{if s.is_leader, do: " (leader)", else: ""}")
    IO.puts("")
    IO.puts("  Nodes:")

    if s.nodes == [] do
      IO.puts("    (none)")
    else
      Enum.each(s.nodes, fn n ->
        marker = if n.leader, do: " [leader]", else: ""

        IO.puts(
          "    #{n.node}: #{n.operator_count} operator(s) recovery_state=#{n.recovery_state}#{marker}"
        )
      end)
    end

    if s.unplaced_operators > 0,
      do: IO.puts("    (unplaced): #{s.unplaced_operators} operator(s)")

    IO.puts("")
    IO.puts("  Operator health: #{health_summary(s.operator_health)}")
  end

  # Every listing answers with one line saying there is nothing, or a padded
  # table. Only the two texts and the columns differ, so the shape is written
  # once and each listing declares its own.
  defp table([], %{empty: empty}), do: IO.puts(empty)

  defp table(rows, %{header: header, columns: columns}) do
    IO.puts(header)
    IO.puts("")
    IO.puts(line(columns, fn {title, _width, _render} -> title end))

    Enum.each(rows, fn row ->
      IO.puts(line(columns, fn {_title, _width, render} -> render.(row) end))
    end)
  end

  # A column pads to its declared width. The last one declares 0, which
  # `pad_trailing/2` leaves alone, so no line ends in trailing whitespace.
  defp line(columns, text) do
    [
      "  "
      | Enum.map(columns, fn {_title, width, _render} = column ->
          String.pad_trailing(text.(column), width)
        end)
    ]
  end

  defp pid_suffix(nil), do: ""
  defp pid_suffix(pid), do: "  pid #{pid}"

  defp health_summary(health) when map_size(health) == 0, do: "no operators"

  defp health_summary(health),
    do: Enum.map_join(health, ", ", fn {state, n} -> "#{state}=#{n}" end)
end

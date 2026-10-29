defmodule Flezha.Service.Cli.Listing do
  @moduledoc """
  `flz namespace list`, `flz runner list`, `flz runner dropped`, `flz relay
  list`, `flz controller list` and `flz cluster status`: the text forms of the
  listings. A cluster read's listing ends naming the controllers that did not
  answer.
  """

  alias Flezha.Service.Cli.Context

  @doc "`flz namespace list`: each namespace in the catalog, and the controllers it runs on."
  def namespaces(answer, _args, _opts) do
    table(answer, %{
      empty: "No namespaces found.",
      header: "Namespaces:",
      columns: [
        {"NAMESPACE", 35, & &1.namespace},
        {"RUNNING", 8, &if(&1.running, do: "yes", else: "no")},
        {"ON", 0, &Enum.join(&1.nodes, ", ")}
      ]
    })
  end

  @doc "`flz runner list`: one row per runner: its process id when known, node, connect time and lateness."
  def runners(rows, [namespace | _], _opts) do
    table(rows, %{
      empty: "No runners found for namespace '#{namespace}'.",
      header: "Runners in namespace '#{namespace}':",
      columns: [
        {"RUNNER", 34, & &1.id},
        {"PID", 9, &Context.label_or(&1[:pid], "-")},
        {"NODE", 22, &to_string(&1.node)},
        {"CONNECTED AT", 22, &unix_iso(&1.connected_at)},
        {"LATE", 0, &if(&1.late?, do: "yes", else: "no")}
      ]
    })
  end

  @doc "`flz runner dropped`: one row per drop mark standing in the namespace."
  def dropped_runners(rows, [namespace | _], _opts) do
    table(rows, %{
      empty: "No dropped runners in namespace '#{namespace}'.",
      header: "Dropped runners in namespace '#{namespace}':",
      columns: [
        {"STABLE ID", 38, & &1.stable_id},
        {"PID", 9, &Context.label_or(&1.pid, "-")},
        {"NODE", 22, &to_string(&1.node)},
        {"DROPPED AT", 22, &unix_iso(&1.dropped_at)},
        {"BY", 0, &Context.label_or(&1.dropped_by, "-")}
      ]
    })
  end

  @doc "`flz relay list`: one row per relay, a partition group as one row."
  def relays(answer, [namespace | _], _opts) do
    table(answer, %{
      empty: "No relays found for namespace '#{namespace}'.",
      header: "Relays in namespace '#{namespace}':",
      columns: [
        {"RELAY", 34, &to_string(&1.id)},
        {"HEALTH", 12, &to_string(&1.health)},
        {"PARTS", 8, &Context.label_or(&1[:partitions], "-")},
        {"NODE", 22, &Context.label_or(&1.node, "(unplaced)")},
        {"RUNNER", 0, &Context.label_or(&1.runner, "(none)")}
      ]
    })
  end

  @doc "`flz controller list`: one row per controller of the cluster."
  def controllers(answer, _args, _opts) do
    table(answer, %{
      empty: "No controllers found.",
      header: "Controllers:",
      columns: [
        {"NODE", 40, &"#{&1.node}#{if &1.local?, do: " (this)", else: ""}"},
        {"HOST", 24, &controller_cell(&1, :host)},
        {"MACHINE", 18, &controller_cell(&1, :host_node)},
        {"VERSION", 10, &controller_cell(&1, :version)},
        {"UP", 14, &controller_cell(&1, :uptime_ms)},
        {"RUNNERS", 0, &controller_cell(&1, :runners)}
      ]
    })
  end

  defp controller_cell(%{reachable?: false}, :host), do: "unreachable"
  defp controller_cell(%{reachable?: false}, _field), do: "-"
  defp controller_cell(%{uptime_ms: ms}, :uptime_ms), do: uptime(div(ms, 1000))
  defp controller_cell(row, field), do: Context.label_or(row[field], "-")

  defp uptime(s) do
    clock =
      :io_lib.format("~2..0B:~2..0B:~2..0B", [
        div(rem(s, 86_400), 3600),
        rem(div(s, 60), 60),
        rem(s, 60)
      ])

    if s < 86_400, do: to_string(clock), else: "#{div(s, 86_400)}d #{clock}"
  end

  @doc "`flz cluster status`."
  def cluster(s, _args, _opts) do
    IO.puts("Cluster status for namespace '#{s.namespace}':")
    IO.puts("")
    IO.puts("  Answered by: #{s.answered_by}")
    IO.puts("")
    IO.puts("  Nodes:")

    if s.nodes == [] do
      IO.puts("    (none)")
    else
      Enum.each(s.nodes, fn n ->
        IO.puts("    #{n.node}: #{n.relay_count} relay(s) recovery_state=#{n.recovery_state}")
      end)
    end

    if s.unplaced_relays > 0,
      do: IO.puts("    (unplaced): #{s.unplaced_relays} relay(s)")

    IO.puts("")
    IO.puts("  Relay health: #{health_summary(s.relay_health)}")
    Context.unreachable(s.unreachable)
  end

  # Every listing answers with one line saying there is nothing, or a padded
  # table. Only the two texts and the columns differ, so the shape is written
  # once and each listing declares its own. A cluster read closes with the
  # controllers that did not answer.
  defp table(%{rows: rows, unreachable: unreachable}, spec) do
    table(rows, spec)
    Context.unreachable(unreachable)
  end

  defp table([], %{empty: empty}), do: IO.puts(empty)

  defp table(rows, %{header: header, columns: columns}) do
    IO.puts(header)
    IO.puts("")
    IO.puts(line(columns, fn {title, _width, _render} -> title end))

    Enum.each(rows, fn row ->
      IO.puts(line(columns, fn {_title, _width, render} -> render.(row) end))
    end)
  end

  # A column pads to its declared width, and no line ends in whitespace: the
  # last column declares 0, and an empty last cell leaves its padding behind.
  defp line(columns, text) do
    ["  " | Enum.map(columns, fn {_t, width, _r} = c -> String.pad_trailing(text.(c), width) end)]
    |> IO.iodata_to_binary()
    |> String.trim_trailing()
  end

  defp unix_iso(nil), do: "-"
  defp unix_iso(seconds), do: seconds |> DateTime.from_unix!() |> DateTime.to_iso8601()

  defp health_summary(health) when map_size(health) == 0, do: "no relays"

  defp health_summary(health),
    do: Enum.map_join(health, ", ", fn {state, n} -> "#{state}=#{n}" end)
end

defmodule Flezha.Service.Cli.Info do
  @moduledoc """
  `flz relay info` — the text form of the relay report: the partition
  groups and their coverage, then every live and finished relay, or with a
  relay id its API; `flz relay graph`, one relay's tasklets and channels;
  `flz relay activity`, what each of its tasklets is doing; and
  `flz relay diagnose`, one relay's findings.
  """

  alias Flezha.Service.Cli.Context

  @activity_columns [
    cells: "CELLS",
    queued: "QUEUED",
    queued_max: "MAX",
    admitted_total: "ADMITTED",
    blocked_total: "BLOCKED",
    dropped_newest_total: "DROP_NEW",
    dropped_oldest_total: "DROP_OLD",
    dropped_capped_total: "CAPPED",
    cells_created_total: "CREATED",
    cells_evicted_total: "EVICTED"
  ]

  @cell_columns [
    queued: "QUEUED",
    admitted_total: "ADMITTED",
    blocked_total: "BLOCKED",
    dropped_newest_total: "DROP_NEW",
    dropped_oldest_total: "DROP_OLD",
    age_ms: "AGE_MS"
  ]

  @doc "`flz relay info`."
  def info(%{relays: [], groups: []} = report, _args, _opts),
    do: IO.puts("No relays found for namespace '#{report.namespace}'.")

  def info(report, _args, _opts) do
    IO.puts("Relay information for namespace '#{report.namespace}':")
    IO.puts("")

    Enum.each(report.groups, &print_group(&1, report.key_groups))
    Enum.each(report.relays, &print_relay/1)
  end

  @doc """
  `flz relay info <relay_id>`: the relay's API, one block per entry channel
  with its format, whether it answers, its description, its schema on one
  line, its MCP tool and an example call.
  """
  def api(factories, _args, _opts) do
    names =
      Flezha.Mcp.relay_tool_names(
        for f <- factories, e <- f.channels, e.description, do: {f.factory, e.channel}
      )

    Enum.each(factories, &print_api(&1, names))
  end

  defp print_api(factory, names) do
    IO.puts(api_heading(factory))

    if factory.disagree,
      do: IO.puts("  Connected runners announce different APIs; this is the latest.")

    case factory.channels do
      [] ->
        IO.puts("  No API announced: connect a runner of factory '#{factory.factory}'.")

      channels ->
        Enum.each(channels, fn entry ->
          tool = names[{factory.factory, entry.channel}]
          print_channel(entry, tool, Map.get(factory, :relay))
        end)
    end
  end

  defp api_heading(%{relay: relay} = factory),
    do: "API of relay '#{relay}' (factory '#{factory.factory}'):"

  defp api_heading(factory), do: "API of factory '#{factory.factory}':"

  defp print_channel(entry, tool, relay) do
    answers = if entry.answer_codec, do: " → answer #{entry.answer_codec}", else: ", no answer"
    IO.puts("  #{entry.channel}  #{entry.codec}#{answers}")
    if entry.description, do: IO.puts("    #{entry.description}")
    if entry.schema, do: IO.puts("    schema: #{Jason.encode!(entry.schema)}")
    print_tool(tool)

    IO.puts(
      "    flz relay call #{relay || "<relay_id>"} --channel #{entry.channel} #{example(entry)}"
    )
  end

  defp print_tool({:ok, name}), do: IO.puts("    MCP tool: #{name}")
  defp print_tool({:taken, name}), do: IO.puts("    MCP tool: tool name taken (#{name})")
  defp print_tool(nil), do: :ok

  # `--arg` per required property of an object schema (every property when
  # none is required), `--text` otherwise.
  defp example(%{codec: "json", schema: %{"properties" => %{} = properties} = schema}) do
    names = Map.get(schema, "required") || properties |> Map.keys() |> Enum.sort()
    Enum.map_join(names, " ", &"--arg #{&1}=...")
  end

  defp example(_entry), do: "--text <payload>"

  @doc """
  `flz relay graph <relay_id>`: the relay's entry channels, then each tasklet
  with its cell mode and the channels it consumes and produces, a channel that
  ends at a plugin output or a call marked so.
  """
  def graph(%{graph: nil} = read, _args, _opts) do
    IO.puts(graph_heading(read))

    IO.puts(
      "  No graph announced: connect a runner of factory '#{read.factory}' that announces one."
    )
  end

  def graph(%{graph: graph} = read, _args, _opts) do
    IO.puts(graph_heading(read))

    if read.disagree,
      do: IO.puts("  Connected runners announce different graphs; this is the latest.")

    entries = for %{kind: "entry"} = channel <- graph.channels, do: channel_label(channel)
    IO.puts("  entry: #{Enum.join(entries, ", ")}")
    Enum.each(graph.tasklets, &print_tasklet(&1, graph.channels))
  end

  defp graph_heading(read), do: "Graph of relay '#{read.relay}' (factory '#{read.factory}'):"

  defp print_tasklet(tasklet, channels) do
    keys = if tasklet.cell_keys == [], do: "", else: " #{Enum.join(tasklet.cell_keys, ", ")}"
    IO.puts("  #{tasklet.name}  #{tasklet.mode}#{keys}, mailbox #{tasklet.mailbox_capacity}")
    inputs = for channel <- channels, tasklet.name in channel.consumers, do: channel.name
    outputs = for %{producer: producer} = c <- channels, producer == tasklet.name, do: c
    IO.puts("    in:  #{Enum.join(inputs, ", ")}")

    IO.puts(
      "    out: #{if outputs == [], do: "-", else: Enum.map_join(outputs, ", ", &channel_label/1)}"
    )
  end

  defp channel_label(channel), do: channel.name <> plugin_end(channel) <> overflow(channel)

  defp plugin_end(%{plugin_end: "output"}), do: " → output"
  defp plugin_end(%{plugin_end: "call", call_target: target}), do: " → call #{target}"
  defp plugin_end(_none), do: ""

  defp overflow(%{overflow: policy}) when policy in [nil, "drop_nothing"], do: ""
  defp overflow(%{overflow: policy}), do: " (#{policy})"

  @doc """
  `flz relay activity`: the runner and instance that answered, then one line
  per tasklet; `-` marks a figure the runner does not keep. With `--cells`,
  one line per Cell of that tasklet after them, `-` for an unkeyed Cell, and
  how long ago each last admitted an item.
  """
  def activity(read, args, _opts) do
    IO.puts(
      "Activity of relay '#{read.relay}' on runner '#{read.runner}', instance #{read.instance_seq}:"
    )

    table("TASKLET", @activity_columns, for(t <- read.tasklets, do: {t.tasklet, t}))
    activity_cells(read, args)
  end

  defp activity_cells(%{cells: %{rows: rows} = cells, read_at_ms: read_at}, _args) do
    IO.puts(
      "Cells of tasklet '#{cells.tasklet}', #{length(rows)} of #{cells.cells_total}, most recent first:"
    )

    ages = for cell <- rows, do: Map.put(cell, :age_ms, max(read_at - cell.last_admitted_ms, 0))
    table("KEY", @cell_columns, for(cell <- ages, do: {cell.key || "-", cell}))
  end

  defp activity_cells(%{runner: runner}, [_namespace, _relay_id, %{"cells_of" => _}]),
    do: IO.puts("Runner '#{runner}' does not report Cells.")

  defp activity_cells(_read, _args), do: :ok

  # A heading, then one line per named row, the name column as wide as the
  # widest name or the heading.
  defp table(heading, columns, rows) do
    width = Enum.reduce(rows, String.length(heading), &max(String.length(elem(&1, 0)), &2))

    line = fn name, figures ->
      "  " <>
        String.pad_trailing(name, width) <> Enum.map_join(figures, &String.pad_leading(&1, 10))
    end

    IO.puts(line.(heading, Keyword.values(columns)))

    for {name, row} <- rows,
        do: IO.puts(line.(name, for({key, _} <- columns, do: Context.label_or(row[key], "-"))))
  end

  @doc """
  `flz relay diagnose`: one block per finding, its level, code and message,
  then its hint indented; a healthy relay is the one line that says so.
  """
  def diagnose(%{findings: findings}, _args, _opts), do: Enum.each(findings, &print_finding/1)

  defp print_finding(%{code: "diagnose.healthy", message: message}),
    do: IO.puts(String.pad_trailing("OK", 9) <> message)

  defp print_finding(finding) do
    level = finding.level |> to_string() |> String.upcase()
    IO.puts(String.pad_trailing(level, 9) <> "#{finding.code}  #{finding.message}")
    if finding.hint, do: IO.puts(String.duplicate(" ", 9) <> "hint: #{finding.hint}")
  end

  defp print_group(group, key_groups) do
    IO.puts("Partition group: #{group.group} (#{length(group.members)}/#{group.partitions})")

    Enum.each(group.members, fn m ->
      [from, to] = m.key_groups

      IO.puts(
        "  p#{m.partition}  #{String.pad_trailing(m.id, 30)} " <>
          "key groups #{from}-#{to}  #{Context.label_or(m.runner, "(none)")}  #{m.health}"
      )
    end)

    case group.coverage do
      %{complete: true} ->
        IO.puts("  coverage: all #{key_groups} key groups owned exactly once")

      %{gaps: gaps, overlaps: overlaps} ->
        IO.puts("  WARNING: key-group coverage is broken — traffic for the named groups is lost")

        Enum.each(gaps, fn [from, to] ->
          IO.puts("    gap: key groups #{from}-#{to} are owned by no member")
        end)

        Enum.each(overlaps, fn %{key_group: key_group, owners: owners} ->
          IO.puts("    overlap: key group #{key_group} is claimed by #{Enum.join(owners, ", ")}")
        end)
    end

    IO.puts("")
  end

  defp print_relay(%{phase: "finished"} = relay) do
    IO.puts("Relay: #{relay.id}")
    IO.puts("  Run: #{relay.run}")
    IO.puts("  Phase: finished")
    IO.puts("  Status: #{relay.status}")
    if relay.error, do: IO.puts("  Error: #{relay.error}")
    IO.puts("  Finished At (unix s): #{relay.finished_at}")
    IO.puts("")
  end

  defp print_relay(%{error: error} = relay) do
    IO.puts("Relay: #{relay.id}")
    IO.puts("  Error getting overwatch state: #{error.message}")
    IO.puts("")
  end

  defp print_relay(relay) do
    IO.puts("Relay: #{relay.id}")
    IO.puts("  Run: #{run_line(relay)}")
    IO.puts("  Durable: #{durable_line(relay)}")
    IO.puts("  Phase: #{relay.phase}")
    IO.puts("  Health: #{relay.health}")

    with %{count: count, last_iteration: last} <- relay.failed_turns,
         do: IO.puts("  Failed turns: #{count} (last: iteration #{last})")

    IO.puts("  Runner ID: #{relay.runner_id}")
    IO.puts("  Group ID: #{relay.namespace}")
    IO.puts("  Uptime (ms): #{relay.uptime_ms}")
    IO.puts("  Default Input Type: #{relay.default_input_type}")
    IO.puts("  Waiting callers: #{relay.waiting_callers}")
    IO.puts("  Plugins:")

    case relay.plugins do
      [] ->
        IO.puts("    (none)")

      plugins ->
        Enum.each(plugins, fn plugin ->
          IO.puts("    - #{plugin.id}: #{plugin.provider}")

          Enum.each(plugin.hooks, fn hook ->
            IO.puts("        hook: #{hook.name} #{inspect(Map.to_list(hook.opts))}")
          end)

          print_plugin_state(plugin.state)
        end)
    end

    IO.puts("")
  end

  # `steps (each: per_input, on_failure: keep_progress)`, or `flow` with its
  # duplicate rule when it is not the default.
  defp run_line(%{run: "steps", each: each, on_failure: on_failure}) when not is_nil(each),
    do: "steps (each: #{each}, on_failure: #{on_failure || :undo_all})"

  defp run_line(%{effectively_once: true}), do: "flow (exactly: effectively_once)"
  defp run_line(relay), do: relay.run

  # `local (sleep_after: 6h)`; a class kept in memory has nothing to sleep on.
  defp durable_line(%{durable: "memory"}), do: "false"

  defp durable_line(relay),
    do: "#{relay.durable} (sleep_after: #{interval(relay.sleep_after_ms)})"

  defp interval(nil), do: "false"
  defp interval(ms) when rem(ms, 3_600_000) == 0, do: "#{div(ms, 3_600_000)}h"
  defp interval(ms) when rem(ms, 60_000) == 0, do: "#{div(ms, 60_000)}m"
  defp interval(ms) when rem(ms, 1_000) == 0, do: "#{div(ms, 1_000)}s"
  defp interval(ms), do: "#{ms}ms"

  defp print_plugin_state(state) when is_map(state) do
    Enum.each(state, fn {key, value} ->
      IO.puts("        #{key}: #{plugin_state_value(value)}")
    end)
  end

  defp print_plugin_state(_none), do: :ok

  defp plugin_state_value([]), do: "(none)"
  defp plugin_state_value(value) when is_list(value), do: Enum.join(value, ", ")
  defp plugin_state_value(nil), do: "(none)"
  defp plugin_state_value(value) when is_binary(value), do: value
  defp plugin_state_value(value), do: inspect(value)
end

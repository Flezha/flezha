defmodule Flezha.Service.Cli.Info do
  @moduledoc """
  `flz relay info` — the text form of the relay report: the partition
  groups and their coverage, then every live and finished relay.
  """

  alias Flezha.Service.Cli.Context

  @doc "`flz relay info`."
  def info(%{relays: [], groups: []} = report, _args, _opts),
    do: IO.puts("No relays found for namespace '#{report.namespace}'.")

  def info(report, _args, _opts) do
    IO.puts("Relay information for namespace '#{report.namespace}':")
    IO.puts("")

    Enum.each(report.groups, &print_group(&1, report.key_groups))
    Enum.each(report.relays, &print_relay/1)
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

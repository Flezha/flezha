defmodule Flezha.Service.Cli.Info do
  @moduledoc """
  `flz operator info` and its renderers, including the partition-coverage
  report `Cli.Listing` also prints.
  """

  alias Flezha.AuthZ.Namespace
  alias Flezha.Convergence.Snapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Operator.Calls
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Partition
  alias Flezha.Plugin.Extension.Table.Health, as: TableHealth
  alias Flezha.SDK.KeyGroup
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.TaskJournal

  @doc "Shows detailed information about operators and their enabled plugins."
  def operator_info(namespace, %{opts: opts}) do
    output_format = Context.format(opts)
    namespace_display = Namespace.or_default(namespace)

    live =
      case Flezha.list_operators(namespace_display) do
        {:ok, operators} -> operators
        _ -> []
      end

    # A finished operator is gone from the live registry but its
    # completion record is retained within the window — surface it too.
    finished = finished_operators(namespace_display, live)

    if live == [] and finished == [] do
      handle_empty_operators(namespace_display, output_format)
    else
      handle_operator_info_output(live, finished, namespace_display, output_format)
    end
  end

  defp handle_empty_operators(namespace_display, "json") do
    result = %{
      namespace: namespace_display,
      operators: [],
      message: "No operators found"
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_empty_operators(namespace_display, _) do
    IO.puts("No operators found for namespace '#{namespace_display}'.")
  end

  defp handle_operator_info_output(live, finished, namespace_display, "json") do
    live_data =
      Enum.map(live, fn operator_id ->
        case OperatorManager.get_overwatch_state(namespace_display, operator_id) do
          {:ok, state} ->
            %{
              id: operator_id,
              runner_id: state.runner_id,
              namespace: state.namespace,
              run: operator_run(state),
              durable: durable_level(operator_id, state),
              phase: operator_phase(namespace_display, operator_id, state),
              health: Snapshot.operator_health(namespace_display, operator_id),
              uptime_ms: operator_uptime_ms(state),
              default_input_type: state.default_input_type,
              waiting_callers: Calls.waiting(state.calls),
              failed_turns: failed_turns_json(failed_turns(operator_id, state)),
              plugins: format_plugins_json(state.extensions)
            }

          {:error, reason} ->
            %{id: operator_id, error: unreadable(reason)}
        end
      end)

    finished_data = Enum.map(finished, fn {id, rec} -> finished_operator_json(id, rec) end)

    result = %{
      namespace: namespace_display,
      operators: live_data ++ finished_data,
      groups: Enum.map(partition_groups(namespace_display), &group_report_json/1)
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_operator_info_output(live, finished, namespace_display, _) do
    IO.puts("Operator information for namespace '#{namespace_display}':")
    IO.puts("")

    Enum.each(partition_groups(namespace_display), &display_group_report/1)

    Enum.each(live, fn operator_id ->
      case OperatorManager.get_overwatch_state(namespace_display, operator_id) do
        {:ok, state} ->
          display_operator_info(operator_id, state, namespace_display)

        {:error, reason} ->
          IO.puts("Operator: #{operator_id}")
          IO.puts("  Error getting overwatch state: #{unreadable(reason).message}")
          IO.puts("")
      end
    end)

    Enum.each(finished, fn {id, rec} -> display_finished_operator(id, rec) end)
  end

  # One operator the listing could not read, as the error it answers: the row
  # carries it, and the listing is still an answer.
  defp unreadable(reason), do: Flezha.Error.from_reason(reason, :"operator.list")

  defp group_report_json(report) do
    %{
      group: report.group,
      partitions: report.declared,
      members:
        Enum.map(report.members, fn m ->
          %{
            id: m.id,
            partition: m.partition.index,
            key_groups: Tuple.to_list(m.partition.range),
            runner: m.runner,
            health: m.health
          }
        end),
      coverage: coverage_json(report.coverage)
    }
  end

  defp coverage_json(:ok), do: %{complete: true}

  defp coverage_json({:error, %{gaps: gaps, overlaps: overlaps}}) do
    %{
      complete: false,
      gaps: Enum.map(gaps, &Tuple.to_list/1),
      overlaps: Enum.map(overlaps, fn {group, ids} -> %{key_group: group, owners: ids} end)
    }
  end

  # Every partition group among `rows`, with its members in partition order and
  # the read-time coverage check. There is no stored group record, so the check
  # is what turns a missing member into a visible fault rather than a quietly
  # short list. The one grouping both group views read.
  def partition_groups(namespace) when is_binary(namespace),
    do: namespace |> HubManager.operator_rows() |> Partition.groups()

  defp display_group_report(report) do
    IO.puts("Partition group: #{report.group} (#{length(report.members)}/#{report.declared})")

    Enum.each(report.members, fn m ->
      {from, to} = m.partition.range

      IO.puts(
        "  p#{m.partition.index}  #{String.pad_trailing(m.id, 30)} " <>
          "key groups #{from}-#{to}  #{Context.label_or(m.runner, "(none)")}  #{m.health}"
      )
    end)

    case report.coverage do
      :ok ->
        IO.puts("  coverage: all #{KeyGroup.key_groups()} key groups owned exactly once")

      {:error, %{gaps: gaps, overlaps: overlaps}} ->
        IO.puts("  WARNING: key-group coverage is broken — traffic for the named groups is lost")

        Enum.each(gaps, fn {from, to} ->
          IO.puts("    gap: key groups #{from}-#{to} are owned by no member")
        end)

        Enum.each(overlaps, fn {group, owners} ->
          IO.puts("    overlap: key group #{group} is claimed by #{Enum.join(owners, ", ")}")
        end)
    end

    IO.puts("")
  end

  # Finished operators with a completion record still inside the
  # retention window, keyed by the operator_class dir (== operator_id).
  defp finished_operators(namespace, live_ids) do
    now = System.system_time(:second)

    Paths.operator_classes()
    |> Enum.reject(&(&1 in live_ids))
    |> Enum.flat_map(fn operator_id ->
      case Completion.read(operator_id) do
        {:ok, %{namespace: ns, finished_at: fa} = rec}
        when ns == namespace and is_integer(fa) ->
          window =
            PersistenceResolver.completed_retention_hours(operator_id)

          if fa + window * 3600 >= now, do: [{operator_id, rec}], else: []

        _ ->
          []
      end
    end)
  end

  defp finished_operator_json(operator_id, rec) do
    %{
      id: operator_id,
      namespace: Map.get(rec, :namespace),
      run: Map.get(rec, :run),
      phase: "finished",
      status: Map.get(rec, :status),
      error: nil_if_empty(Map.get(rec, :error)),
      result: encode_result(Map.get(rec, :result)),
      finished_at: Map.get(rec, :finished_at)
    }
  end

  defp display_finished_operator(operator_id, rec) do
    IO.puts("Operator: #{operator_id}")
    IO.puts("  Run: #{Map.get(rec, :run)}")
    IO.puts("  Phase: finished")
    IO.puts("  Status: #{Map.get(rec, :status)}")

    case nil_if_empty(Map.get(rec, :error)) do
      nil -> :ok
      err -> IO.puts("  Error: #{err}")
    end

    IO.puts("  Finished At (unix s): #{Map.get(rec, :finished_at)}")
    IO.puts("")
  end

  defp encode_result(r) when is_binary(r) and byte_size(r) > 0, do: Base.encode64(r)
  defp encode_result(_), do: nil

  defp nil_if_empty(s) when is_binary(s) and byte_size(s) > 0, do: s
  defp nil_if_empty(_), do: nil

  # The turns a steps session dead-lettered, from its journal; `nil` for any
  # other operator and for a session none of whose turns failed.
  defp failed_turns(operator_id, %{builder: %{run: :steps}}),
    do: TaskJournal.failed_turns(operator_id)

  defp failed_turns(_operator_id, _state), do: nil

  defp failed_turns_json(nil), do: nil
  defp failed_turns_json({count, last}), do: %{count: count, last_iteration: last}

  defp operator_run(%{builder: %{run: :steps}}), do: "steps"
  defp operator_run(_state), do: "flow"

  # `steps (each: per_input, on_failure: keep_progress)`, or `flow` with its
  # duplicate rule when it is not the default.
  defp run_line(%{builder: %{run: :steps}, settings: %{each: each, on_failure: fail}})
       when not is_nil(each),
       do: "steps (each: #{each}, on_failure: #{fail || :undo_all})"

  defp run_line(%{effectively_once: true}), do: "flow (exactly: effectively_once)"
  defp run_line(state), do: operator_run(state)

  # The class an instance's durable records live under.
  defp class_of(operator_id, state),
    do:
      state
      |> Map.get(:builder, %{})
      |> Map.get(:opts, [])
      |> Keyword.get(:operator_class, operator_id)

  # Where the class keeps its work, from what its runner announced.
  defp durable_level(operator_id, state),
    do: operator_id |> class_of(state) |> ClassRegistry.durable_level() |> Atom.to_string()

  # `local (sleep_after: 6h)`; a class kept in memory has nothing to sleep on.
  defp durable_line(operator_id, state) do
    case durable_level(operator_id, state) do
      "memory" ->
        "false"

      level ->
        sleep = ClassRegistry.sleep_after_ms(class_of(operator_id, state))
        "#{level} (sleep_after: #{format_interval(sleep)})"
    end
  end

  defp format_interval(nil), do: "false"
  defp format_interval(ms) when rem(ms, 3_600_000) == 0, do: "#{div(ms, 3_600_000)}h"
  defp format_interval(ms) when rem(ms, 60_000) == 0, do: "#{div(ms, 60_000)}m"
  defp format_interval(ms) when rem(ms, 1_000) == 0, do: "#{div(ms, 1_000)}s"
  defp format_interval(ms), do: "#{ms}ms"

  # The operator's own lifecycle state, read from the verdict record the
  # convergence loop published for it — the same field `flz operator list`,
  # `/healthz` and the console read, so no two surfaces describe one operator
  # in contradictory words. An operator the loop has not judged yet still gets
  # a phase, derived from its own state by the record module's one derivation.
  defp operator_phase(namespace, operator_id, state) do
    case Snapshot.verdict(namespace, operator_id) do
      %Verdict{phase: phase} when not is_nil(phase) -> to_string(phase)
      _ -> to_string(Verdict.phase_of(state))
    end
  end

  defp operator_uptime_ms(%{started_at: started}) when is_integer(started),
    do: max(0, System.system_time(:millisecond) - started)

  defp operator_uptime_ms(_), do: nil

  defp format_plugins_json(%{plugins: plugins} = extensions) do
    Enum.map(plugins, fn {plugin_id, plugin} ->
      %{
        id: plugin_id,
        provider: inspect(plugin.provider),
        hooks:
          Enum.map(plugin.hooks, fn {name, opts} ->
            %{name: Atom.to_string(name), opts: Map.new(opts)}
          end),
        state: plugin_state(extensions, plugin_id, plugin)
      }
    end)
  end

  defp format_plugins_json(_), do: []

  # A provider's own caller-facing detail. Same shape as the health contract:
  # the stored opts with the processes the table started injected, and a
  # provider that raises or has nothing to say reports nothing rather than
  # failing the whole listing.
  defp plugin_state(extensions, plugin_id, %{provider: provider, opts: opts}) do
    if Code.ensure_loaded?(provider) do
      entry = Map.get(Map.get(extensions, :entries, %{}), plugin_id, %{})
      provider.inspect_state(TableHealth.with_worker_pids(entry, opts))
    end
  catch
    _, _ -> nil
  end

  defp display_plugin_state(nil), do: :ok

  defp display_plugin_state(state) when is_map(state) do
    Enum.each(state, fn {key, value} ->
      IO.puts("        #{key}: #{format_plugin_state_value(value)}")
    end)
  end

  defp display_plugin_state(_), do: :ok

  defp format_plugin_state_value(value) when is_list(value) do
    if value == [], do: "(none)", else: Enum.join(value, ", ")
  end

  defp format_plugin_state_value(nil), do: "(none)"
  defp format_plugin_state_value(value) when is_binary(value), do: value
  defp format_plugin_state_value(value), do: inspect(value)

  defp display_operator_info(operator_id, state, namespace) do
    IO.puts("Operator: #{operator_id}")
    IO.puts("  Run: #{run_line(state)}")
    IO.puts("  Durable: #{durable_line(operator_id, state)}")
    IO.puts("  Phase: #{operator_phase(namespace, operator_id, state)}")
    IO.puts("  Health: #{Snapshot.operator_health(namespace, operator_id)}")

    with {count, last} <- failed_turns(operator_id, state),
         do: IO.puts("  Failed turns: #{count} (last: iteration #{last})")

    IO.puts("  Runner ID: #{state.runner_id}")
    IO.puts("  Group ID: #{state.namespace}")
    IO.puts("  Uptime (ms): #{operator_uptime_ms(state)}")
    IO.puts("  Default Input Type: #{state.default_input_type}")
    IO.puts("  Waiting callers: #{Calls.waiting(state.calls)}")
    IO.puts("  Plugins:")

    case state.extensions do
      %{plugins: plugins} = extensions when map_size(plugins) > 0 ->
        Enum.each(plugins, fn {plugin_id, plugin} ->
          IO.puts("    - #{plugin_id}: #{inspect(plugin.provider)}")

          Enum.each(plugin.hooks, fn {name, opts} ->
            IO.puts("        hook: #{name} #{inspect(opts)}")
          end)

          display_plugin_state(plugin_state(extensions, plugin_id, plugin))
        end)

      _ ->
        IO.puts("    (none)")
    end

    IO.puts("")
  end
end

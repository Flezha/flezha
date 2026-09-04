defmodule Flezha.Service.Cli.Info do
  @moduledoc """
  `flz operative info` and its renderers, including the partition-coverage
  report `Cli.Listing` also prints.
  """

  alias Flezha.Convergence.Snapshot
  alias Flezha.Partition
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver

  @doc "Shows detailed information about operatives and their enabled plugins."
  def operative_info(namespace, output_format \\ "text") do
    namespace_display = Context.normalize_namespace(namespace)

    live =
      case Flezha.list_operatives(namespace_display) do
        {:ok, operatives} -> operatives
        _ -> []
      end

    # A finished workflow operative is gone from the live registry but its
    # completion record is retained within the window — surface it too.
    finished = finished_operatives(namespace_display, live)

    if live == [] and finished == [] do
      handle_empty_operatives(namespace_display, output_format)
    else
      handle_operative_info_output(live, finished, namespace_display, output_format)
    end
  end

  defp handle_empty_operatives(namespace_display, "json") do
    result = %{
      namespace: namespace_display,
      operatives: [],
      message: "No operatives found"
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_empty_operatives(namespace_display, _) do
    IO.puts("No operatives found for namespace '#{namespace_display}'.")
  end

  defp handle_operative_info_output(live, finished, namespace_display, "json") do
    live_data =
      Enum.map(live, fn operative_id ->
        case OperativeManager.get_overwatch_state(namespace_display, operative_id) do
          {:ok, state} ->
            %{
              id: operative_id,
              runner_id: state.runner_id,
              namespace: state.namespace,
              kind: operative_kind(state),
              phase: operative_phase(state),
              health: Snapshot.operative_health(namespace_display, operative_id),
              uptime_ms: operative_uptime_ms(state),
              default_input_type: state.default_input_type,
              plugins: format_plugins_json(state.extensions)
            }

          {:error, reason} ->
            %{id: operative_id, error: inspect(reason)}
        end
      end)

    finished_data = Enum.map(finished, fn {id, rec} -> finished_operative_json(id, rec) end)

    result = %{
      namespace: namespace_display,
      operatives: live_data ++ finished_data,
      groups: Enum.map(partition_groups(namespace_display), &group_report_json/1)
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_operative_info_output(live, finished, namespace_display, _) do
    IO.puts("Operative information for namespace '#{namespace_display}':")
    IO.puts("")

    Enum.each(partition_groups(namespace_display), &display_group_report/1)

    Enum.each(live, fn operative_id ->
      case OperativeManager.get_overwatch_state(namespace_display, operative_id) do
        {:ok, state} ->
          display_operative_info(operative_id, state, namespace_display)

        {:error, reason} ->
          IO.puts("Operative: #{operative_id}")
          IO.puts("  Error getting overwatch state: #{inspect(reason)}")
          IO.puts("")
      end
    end)

    Enum.each(finished, fn {id, rec} -> display_finished_operative(id, rec) end)
  end

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
    do: namespace |> HubManager.operative_rows() |> Partition.groups()

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
        IO.puts("  coverage: all #{Partition.key_groups()} key groups owned exactly once")

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

  # Finished workflow operatives with a completion record still inside the
  # retention window, keyed by the operative_class dir (== operative_id).
  defp finished_operatives(namespace, live_ids) do
    now = System.system_time(:second)

    Paths.operative_classes()
    |> Enum.reject(&(&1 in live_ids))
    |> Enum.flat_map(fn operative_id ->
      case Completion.read(operative_id) do
        {:ok, %{namespace: ns, finished_at: fa} = rec}
        when ns == namespace and is_integer(fa) ->
          window =
            PersistenceResolver.completed_retention_hours(operative_id)

          if fa + window * 3600 >= now, do: [{operative_id, rec}], else: []

        _ ->
          []
      end
    end)
  end

  defp finished_operative_json(operative_id, rec) do
    %{
      id: operative_id,
      namespace: Map.get(rec, :namespace),
      kind: "workflow",
      phase: "finished",
      status: Map.get(rec, :status),
      error: nil_if_empty(Map.get(rec, :error)),
      result: encode_result(Map.get(rec, :result)),
      finished_at: Map.get(rec, :finished_at)
    }
  end

  defp display_finished_operative(operative_id, rec) do
    IO.puts("Operative: #{operative_id}")
    IO.puts("  Kind: workflow")
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

  defp operative_kind(%{builder: %{kind: kind}}) when is_atom(kind), do: Atom.to_string(kind)
  defp operative_kind(_), do: "stream"

  # The operative's own lifecycle state, in the order the convergence loop
  # classifies it. "suspended": the loop is parked at a committed iteration
  # boundary — still registered, still addressable, holding zero runner
  # resources until an input (or its wake deadline) rehydrates it
  # (`workflow-durable-suspension`). "parked": `Lifecycle.park/2` stored a
  # failure reason and the Overwatch stayed alive, so it is registered and
  # readable but processes nothing until the reconciler un-parks it.
  defp operative_phase(%{suspended: true}), do: "suspended"
  defp operative_phase(%{init_error: reason}) when not is_nil(reason), do: "parked"
  defp operative_phase(_state), do: "running"

  defp operative_uptime_ms(%{started_at: started}) when is_integer(started),
    do: max(0, System.system_time(:millisecond) - started)

  defp operative_uptime_ms(_), do: nil

  defp format_plugins_json(%{plugins: plugins} = extensions) do
    Enum.map(plugins, fn {plugin_id, plugin} ->
      %{
        id: plugin_id,
        provider: inspect(plugin.provider),
        subscriptions:
          Enum.map(plugin.subscriptions, fn {name, opts} ->
            %{extension: Atom.to_string(name), opts: Map.new(opts)}
          end),
        state: plugin_state(extensions, plugin_id, plugin)
      }
    end)
  end

  defp format_plugins_json(_), do: []

  # A provider's own operator-facing detail, when it exports one. Same shape as
  # the health contract: the stored opts with the tracked pid injected, and a
  # provider that raises or has nothing to say reports nothing rather than
  # failing the whole listing.
  defp plugin_state(extensions, plugin_id, %{provider: provider, opts: opts}) do
    if Code.ensure_loaded?(provider) and function_exported?(provider, :inspect_state, 1) do
      owned_pid =
        case Map.get(Map.get(extensions, :owned_pids, %{}), plugin_id) do
          {pid, _ref} -> pid
          _ -> nil
        end

      provider.inspect_state(Map.put(opts || %{}, :owned_pid, owned_pid))
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

  defp display_operative_info(operative_id, state, namespace) do
    IO.puts("Operative: #{operative_id}")
    IO.puts("  Kind: #{operative_kind(state)}")
    IO.puts("  Phase: #{operative_phase(state)}")
    IO.puts("  Health: #{Snapshot.operative_health(namespace, operative_id)}")
    IO.puts("  Runner ID: #{state.runner_id}")
    IO.puts("  Group ID: #{state.namespace}")
    IO.puts("  Uptime (ms): #{operative_uptime_ms(state)}")
    IO.puts("  Default Input Type: #{state.default_input_type}")
    IO.puts("  Plugins:")

    case state.extensions do
      %{plugins: plugins} = extensions when map_size(plugins) > 0 ->
        Enum.each(plugins, fn {plugin_id, plugin} ->
          IO.puts("    - #{plugin_id}: #{inspect(plugin.provider)}")

          Enum.each(plugin.subscriptions, fn {name, opts} ->
            IO.puts("        subscription: #{name} #{inspect(opts)}")
          end)

          display_plugin_state(plugin_state(extensions, plugin_id, plugin))
        end)

      _ ->
        IO.puts("    (none)")
    end

    IO.puts("")
  end
end

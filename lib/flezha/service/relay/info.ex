defmodule Flezha.Service.Relay.Info do
  @moduledoc """
  What `Flezha.Service.Relay.info/2` answers for a namespace: every live
  relay as its Overwatch reports it, every finished one whose completion
  record is still inside its retention window, and every partition group with
  its read-time key-group coverage.
  """

  alias Flezha.Convergence.Snapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Relay.Calls
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Partition
  alias Flezha.Plugin.Extension.Table.Health, as: TableHealth
  alias Flezha.SDK.KeyGroup
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.TaskJournal

  @doc "The report for `namespace`."
  @spec report(String.t()) :: map()
  def report(namespace) do
    {:ok, live} = RelayManager.get_relay_ids(namespace)

    %{
      namespace: namespace,
      key_groups: KeyGroup.key_groups(),
      groups: namespace |> HubManager.relay_rows() |> Partition.groups() |> Enum.map(&group/1),
      relays:
        Enum.map(live, &live(namespace, &1)) ++
          Enum.map(retained(namespace, live), fn {id, record} -> finished(id, record) end)
    }
  end

  @doc """
  One relay of `namespace`: `{:live, state}` with its Overwatch state,
  `{:finished, row}` as `report/1` lists a completion record still inside its
  retention window, `relay_not_found` when it is neither, or the error a live
  relay's Overwatch read answered.
  """
  @spec relay(String.t(), String.t()) :: {:live, map()} | {:finished, map()} | {:error, term()}
  def relay(namespace, relay_id) do
    case RelayManager.get_overwatch_state(namespace, relay_id) do
      {:ok, state} ->
        {:live, state}

      {:error, reason} when elem(reason, 0) in [:relay_not_found, :hub_down] ->
        case retained_record(namespace, relay_id, System.system_time(:second)) do
          nil -> {:error, {:relay_not_found, namespace, relay_id}}
          record -> {:finished, finished(relay_id, record)}
        end

      {:error, _reason} = refused ->
        refused
    end
  end

  defp live(namespace, relay_id) do
    case RelayManager.get_overwatch_state(namespace, relay_id) do
      {:ok, state} ->
        settings = Map.get(state, :settings) || %{}

        %{
          id: relay_id,
          runner_id: state.runner_id,
          namespace: state.namespace,
          run: run(state),
          each: Map.get(settings, :each),
          on_failure: Map.get(settings, :on_failure),
          effectively_once: Map.get(state, :effectively_once) == true,
          durable: durable_level(relay_id, state),
          sleep_after_ms: ClassRegistry.sleep_after_ms(class_of(relay_id, state)),
          phase: phase(namespace, relay_id, state),
          health: Snapshot.relay_health(namespace, relay_id),
          uptime_ms: uptime_ms(state),
          default_input_type: state.default_input_type,
          waiting_callers: Calls.waiting(state.calls),
          failed_turns: failed_turns(relay_id, state),
          plugins: plugins(state.extensions)
        }

      {:error, reason} ->
        %{id: relay_id, error: Flezha.Error.from_reason(reason, :"relay.list")}
    end
  end

  # Finished relays with a completion record still inside the retention
  # window, keyed by the relay_class dir (== relay_id).
  defp retained(namespace, live_ids) do
    now = System.system_time(:second)

    Paths.relay_classes()
    |> Enum.reject(&(&1 in live_ids))
    |> Enum.flat_map(fn relay_id ->
      case retained_record(namespace, relay_id, now) do
        nil -> []
        record -> [{relay_id, record}]
      end
    end)
  end

  defp retained_record(namespace, relay_id, now) do
    case Completion.read(relay_id) do
      {:ok, %{namespace: ^namespace, finished_at: finished_at} = record}
      when is_integer(finished_at) ->
        window = PersistenceResolver.completed_retention_hours(relay_id)
        if finished_at + window * 3600 >= now, do: record

      _ ->
        nil
    end
  end

  defp finished(relay_id, record) do
    %{
      id: relay_id,
      namespace: Map.get(record, :namespace),
      run: Map.get(record, :run),
      phase: "finished",
      status: Map.get(record, :status),
      error: nil_if_empty(Map.get(record, :error)),
      result: encode_result(Map.get(record, :result)),
      finished_at: Map.get(record, :finished_at)
    }
  end

  defp group(report) do
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
      coverage: coverage(report.coverage)
    }
  end

  defp coverage(:ok), do: %{complete: true}

  defp coverage({:error, %{gaps: gaps, overlaps: overlaps}}) do
    %{
      complete: false,
      gaps: Enum.map(gaps, &Tuple.to_list/1),
      overlaps: Enum.map(overlaps, fn {group, ids} -> %{key_group: group, owners: ids} end)
    }
  end

  defp encode_result(r) when is_binary(r) and byte_size(r) > 0, do: Base.encode64(r)
  defp encode_result(_), do: nil

  defp nil_if_empty(s) when is_binary(s) and byte_size(s) > 0, do: s
  defp nil_if_empty(_), do: nil

  # The turns a steps session dead-lettered, from its journal; `nil` for any
  # other relay and for a session none of whose turns failed.
  defp failed_turns(relay_id, %{builder: %{run: :steps}}) do
    case TaskJournal.failed_turns(relay_id) do
      {count, last} -> %{count: count, last_iteration: last}
      _none -> nil
    end
  end

  defp failed_turns(_relay_id, _state), do: nil

  defp run(%{builder: %{run: :steps}}), do: "steps"
  defp run(_state), do: "flow"

  @doc "The class an instance's durable records live under."
  @spec class_of(String.t(), map()) :: String.t()
  def class_of(relay_id, state),
    do:
      state
      |> Map.get(:builder, %{})
      |> Map.get(:opts, [])
      |> Keyword.get(:relay_class, relay_id)

  # Where the class keeps its work, from what its runner announced.
  defp durable_level(relay_id, state),
    do: relay_id |> class_of(state) |> ClassRegistry.durable_level() |> Atom.to_string()

  # The relay's own lifecycle state, read from the verdict record the
  # convergence loop published for it — the same field `flz relay list`,
  # `/healthz` and the console read, so no two surfaces describe one relay
  # in contradictory words. A relay the loop has not judged yet still gets
  # a phase, derived from its own state by the record module's one derivation.
  defp phase(namespace, relay_id, state) do
    case Snapshot.verdict(namespace, relay_id) do
      %Verdict{phase: phase} when not is_nil(phase) -> to_string(phase)
      _ -> to_string(Verdict.phase_of(state))
    end
  end

  defp uptime_ms(%{started_at: started}) when is_integer(started),
    do: max(0, System.system_time(:millisecond) - started)

  defp uptime_ms(_), do: nil

  defp plugins(%{plugins: plugins} = extensions) do
    Enum.map(plugins, fn {plugin_id, plugin} ->
      %{
        id: plugin_id,
        provider: inspect(plugin.provider),
        hooks:
          Enum.map(plugin.hooks, fn {name, opts} ->
            %{name: Atom.to_string(name), opts: Map.new(opts)}
          end),
        state: TableHealth.inspect_state(extensions, plugin_id)
      }
    end)
  end

  defp plugins(_), do: []
end

defmodule Flezha.Service.Relay.Info do
  @moduledoc """
  What `Flezha.Service.Relay.info/2` answers for a namespace: every live
  relay as its Overwatch reports it, or by its published phase and health,
  marked `live_read`, when that Overwatch does not answer; every finished one
  whose completion record is still inside its retention window, with
  `expires_at` (unix seconds) the end of that window; and every
  partition group with its read-time key-group coverage.
  """

  alias Flezha.Convergence.Snapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Relay.Calls
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Overwatch
  alias Flezha.Partition
  alias Flezha.Plugin.Extension.Table.Health, as: TableHealth
  alias Flezha.Relay.KeyGroup
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
        Enum.map(live, &row(namespace, &1)) ++
          Enum.map(retained(namespace, live), fn {id, record} -> finished(id, record) end)
    }
  end

  @doc """
  One relay of `namespace`: `{:live, state}` with its Overwatch state,
  `{:finished, row}` as `report/1` lists a completion record still inside its
  retention window, `{:unread, code, verdict}` for a relay its hub holds whose
  Overwatch did not answer (`code` names the read's error) but whose verdict
  the convergence loop published,
  `relay_not_found` when it is none of these, or the error the read answered.
  """
  @spec relay(String.t(), String.t()) ::
          {:live, map()}
          | {:finished, map()}
          | {:unread, String.t(), Verdict.t()}
          | {:error, term()}
  def relay(namespace, relay_id) do
    case RelayManager.get_overwatch_state(namespace, relay_id) do
      {:ok, state} ->
        {:live, state}

      {:error, reason} when elem(reason, 0) in [:relay_not_found, :hub_down] ->
        case retained_record(namespace, relay_id, System.system_time(:second)) do
          nil -> {:error, {:relay_not_found, namespace, relay_id}}
          record -> {:finished, finished(relay_id, record)}
        end

      # A bare reason is the bounded call's refusal of a pid the hub resolved.
      {:error, reason} = refused when reason in [:timeout, :noproc, :other] ->
        case Snapshot.verdict(namespace, relay_id) do
          %Verdict{} = verdict ->
            {:unread, Flezha.Error.from_reason(reason, :"relay.list").code, verdict}

          nil ->
            refused
        end

      {:error, _reason} = refused ->
        refused
    end
  end

  # The row of a relay the hub lists, read once.
  defp row(namespace, relay_id) do
    case relay(namespace, relay_id) do
      {:live, state} ->
        live(namespace, relay_id, state)

      {:finished, row} ->
        row

      {:unread, code, verdict} ->
        %{
          id: relay_id,
          phase: to_string(verdict.phase || :unknown),
          health: Snapshot.relay_health(namespace, relay_id),
          live_read: code
        }

      {:error, reason} ->
        error = Flezha.Error.from_reason(reason, :"relay.list")
        %{id: relay_id, error: Jason.decode!(Flezha.Error.object(error))}
    end
  end

  defp live(namespace, relay_id, state) do
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
      phase: to_string(Overwatch.phase(state) || :unknown),
      health: Snapshot.relay_health(namespace, relay_id),
      uptime_ms: uptime_ms(state),
      default_input_type: state.default_input_type,
      waiting_callers: Calls.waiting(state.calls),
      failed_turns: failed_turns(relay_id, state),
      plugins: plugins(state.extensions)
    }
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
        expires_at = finished_at + PersistenceResolver.completed_retention_hours(relay_id) * 3600
        if expires_at >= now, do: Map.put(record, :expires_at, expires_at)

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
      finished_at: Map.get(record, :finished_at),
      expires_at: Map.get(record, :expires_at)
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

defmodule Flezha.Convergence.Probe do
  @moduledoc """
  Pure observer of live, ephemeral reality — "what the system actually
  looks like right now". No decisions, no side effects.

  `snapshot/0` builds a `Reality` map the Loop diffs against
  `Intent.declared/0`. Recovery lifecycle state is served here too:
  `recovery_state/1` reads the cache published by
  `Flezha.Convergence.Snapshot`; `await_recovery_complete/2` blocks the
  caller until every requested hub reaches `:normal | :skipped`.
  """

  alias Flezha.Convergence.{SafeCall, Snapshot}
  alias Flezha.Executor.FleetManager
  alias Flezha.Service.HubManager

  @relay_call_timeout_ms 1_000

  @type reality :: %{
          relays: %{{binary(), binary()} => map()},
          active_executors: %{binary() => MapSet.t()},
          recovery: %{atom() => atom()},
          stalled_relays: MapSet.t(),
          tick_at: integer()
        }

  # ---------- reality ----------

  @spec snapshot() :: reality()
  def snapshot() do
    namespaces = running_namespaces()

    %{
      relays: relays(namespaces),
      active_executors: active_executors(namespaces),
      recovery: Snapshot.recovery_states(),
      stalled_relays: stalled_relays(),
      tick_at: System.monotonic_time(:millisecond)
    }
  end

  defp relays(namespaces) do
    Enum.reduce(namespaces, %{}, fn ns, acc ->
      Enum.reduce(safe_relays(ns), acc, fn {relay_id, pid}, inner ->
        Map.put(inner, {ns, relay_id}, observe_relay(pid))
      end)
    end)
  end

  defp observe_relay(pid) when is_pid(pid) do
    case SafeCall.bounded(pid, :get_overwatch_state, @relay_call_timeout_ms) do
      {:ok, ow} when is_map(ow) ->
        %{
          pid: pid,
          bound_executor: Map.get(ow, :executor_id),
          factory_id: Map.get(ow, :factory_id),
          parked?: Map.get(ow, :init_error) != nil,
          running_plugins: running_plugins(ow)
        }

      _ ->
        %{pid: pid, bound_executor: nil, factory_id: nil, parked?: false, running_plugins: []}
    end
  end

  defp observe_relay(_),
    do: %{pid: nil, bound_executor: nil, factory_id: nil, parked?: false, running_plugins: []}

  # The plugin ids currently subscribed in the relay's extension table.
  defp running_plugins(ow) do
    case Map.get(ow, :extensions) do
      %{plugins: plugins} when is_map(plugins) -> Map.keys(plugins)
      _ -> []
    end
  end

  defp active_executors(namespaces) do
    Enum.into(namespaces, %{}, fn ns ->
      executors =
        try do
          FleetManager.active_executors(ns) |> MapSet.new()
        catch
          :exit, _ -> MapSet.new()
        end

      {ns, executors}
    end)
  end

  # Output-stall set is published into the Snapshot by the Loop.
  defp stalled_relays() do
    Snapshot.current()
    |> Map.get(:stalled_relays, [])
    |> MapSet.new()
  end

  # ---------- recovery ----------

  @doc "Last known recovery state for `hub_id`; `:unknown` if never observed."
  @spec recovery_state(atom()) :: atom()
  defdelegate recovery_state(hub_id), to: Snapshot

  @doc "All observed `{hub_id => state}` recovery entries."
  @spec recovery_states() :: %{atom() => atom()}
  defdelegate recovery_states(), to: Snapshot

  @doc """
  Block until every hub in `hub_ids` (defaults to all running hubs plus
  any hub the Snapshot has observed) reaches `:normal | :skipped`, or
  `timeout` ms expires. Runs in the caller so multiple subsystems can
  await in parallel. Returns `:ok` or
  `{:error, {:recovery_timeout, [hub_id]}}`.
  """
  @spec await_recovery_complete([atom()] | nil, non_neg_integer()) ::
          :ok | {:error, {:recovery_timeout, [atom()]}}
  def await_recovery_complete(hub_ids \\ nil, timeout)
      when is_integer(timeout) and timeout >= 0 do
    deadline = System.monotonic_time(:millisecond) + timeout

    unfinished =
      hub_ids
      |> resolve_targets()
      |> Enum.reduce([], fn hub_id, acc ->
        remaining = max(deadline - System.monotonic_time(:millisecond), 0)
        if wait_for_normal(hub_id, remaining) == :ok, do: acc, else: [hub_id | acc]
      end)

    case Enum.reverse(unfinished) do
      [] -> :ok
      hubs -> {:error, {:recovery_timeout, hubs}}
    end
  end

  defp resolve_targets(nil) do
    Enum.uniq(running_hub_ids() ++ Map.keys(recovery_states()))
  end

  defp resolve_targets(hub_ids) when is_list(hub_ids), do: hub_ids

  defp wait_for_normal(hub_id, timeout) do
    if ProcessHub.is_alive?(hub_id) do
      case ProcessHub.await_normal(hub_id, timeout) do
        :ok -> :ok
        {:error, :timeout} -> :timeout
      end
    else
      poll_recovery(hub_id, System.monotonic_time(:millisecond) + timeout)
    end
  end

  defp poll_recovery(hub_id, deadline) do
    case recovery_state(hub_id) do
      s when s in [:normal, :skipped] ->
        :ok

      _ ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          :timeout
        else
          Process.sleep(min(remaining, 50))
          poll_recovery(hub_id, deadline)
        end
    end
  end

  # ---------- helpers ----------

  defp running_namespaces() do
    HubManager.get_running_namespaces()
  catch
    :exit, _ -> []
  end

  defp safe_relays(namespace) do
    HubManager.get_all_relays(namespace)
  catch
    :exit, _ -> []
  end

  defp running_hub_ids() do
    HubManager.list_running_hub_ids()
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end
end

defmodule Flezha.Convergence.RecoveryGate do
  @moduledoc """
  Single query API for hub recovery state. Reads the ETS table owned
  by `Flezha.Convergence.Snapshot` — no GenServer, no state of its own.

  Entry points:

    * `ready_namespaces/1` — given an `Intent`, returns the `MapSet` of
      namespace identifiers whose hubs are ready for healing
      (`:normal | :skipped | :timeout`). `:timeout` hubs are included
      with a warning — DETS replay expired but the live ring is healthy.
    * `await_recovery_complete/2` — blocks until every hub reaches
      `:normal | :skipped`, or the timeout elapses.
    * `observe_state/2` — thin delegation to `Snapshot.observe_recovery/2`
      for callers that import this module for recovery-gate queries.
    * `recovering?/1`, `any_recovering?/0,1` and `recovery_summary/0` — is a
      planned recovery still running? The member list that decides it is
      written here and nowhere else.
  """

  alias Flezha.Convergence.{Intent, Snapshot}
  alias Flezha.Identificator
  alias Flezha.Process.Hub
  alias Flezha.Service.HubManager
  alias ProcessHub.Service.Recovery, as: HubRecovery

  require Logger

  @doc """
  Returns a `MapSet` of namespace identifiers whose hubs are ready for
  healing. Includes `:normal`, `:skipped`, and `:timeout` hubs.
  A `Logger.warning` is emitted for each `:timeout` hub so users
  are alerted without blocking relays from recovering.
  """
  @spec ready_namespaces(Intent.t()) :: MapSet.t(String.t())
  def ready_namespaces(%Intent{relays: relays}) do
    states = Snapshot.recovery_states()

    relays
    |> Enum.map(& &1.namespace)
    |> Enum.uniq()
    |> Enum.filter(fn ns ->
      hub_id = Identificator.hub(ns)

      case Map.get(states, hub_id, :unknown) do
        s when s in [:normal, :skipped] ->
          true

        :timeout ->
          Logger.warning(
            "[convergence] hub #{inspect(hub_id)} in :timeout — healing against live cluster state"
          )

          true

        _ ->
          false
      end
    end)
    |> MapSet.new()
  end

  @doc """
  Block until every hub in `hub_ids` reaches `:normal | :skipped`, or
  `timeout` ms expires. Returns `:ok` or
  `{:error, {:recovery_timeout, [hub_id]}}`.

  Runs in the caller process so multiple subsystems can await in parallel.
  `hub_ids` defaults to all running hubs plus any hub the Snapshot has
  observed.
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

  @doc "Thin delegation to `Snapshot.observe_recovery/2`."
  @spec observe_state(atom(), atom()) :: :ok
  defdelegate observe_state(hub_id, state), to: Snapshot, as: :observe_recovery

  @doc """
  Whether a hub recovery state means a planned recovery is still running.

  Distinct from `ready_namespaces/1` on purpose: that answers whether the
  convergence loop may heal a namespace and admits a `:timeout` hub, while a
  `:timeout` hub has not recovered and is reported here as still recovering.
  """
  @spec recovering?(atom()) :: boolean()
  def recovering?(state), do: state in [:recovery_pending, :recovering, :timeout]

  @doc """
  Whether any hub is still recovering. With no argument, over every observed
  hub; with one, over states the caller already sampled — the convergence
  tick passes its own `reality` so the tick is never re-read.
  """
  @spec any_recovering?(%{atom() => atom()} | [atom()]) :: boolean()
  def any_recovering?(states \\ Snapshot.recovery_states())
  def any_recovering?(states) when is_map(states), do: any_recovering?(Map.values(states))
  def any_recovering?(states) when is_list(states), do: Enum.any?(states, &recovering?/1)

  @doc """
  The recovery facts a status surface reports: every observed state, and
  whether any of them is recovering. Sampled once, so no two arms of one
  answer can disagree about the same moment.
  """
  @spec recovery_summary() :: %{states: [atom()], recovering?: boolean()}
  def recovery_summary() do
    states = Map.values(Snapshot.recovery_states())

    %{states: states, recovering?: any_recovering?(states)}
  end

  # ---------- private ----------

  defp resolve_targets(nil) do
    Enum.uniq(running_hub_ids() ++ Map.keys(Snapshot.recovery_states()))
  end

  defp resolve_targets(hub_ids) when is_list(hub_ids), do: hub_ids

  defp wait_for_normal(hub_id, timeout) do
    if Hub.alive?(hub_id) do
      case HubRecovery.await_normal(hub_id, timeout) do
        :ok -> :ok
        {:error, :timeout} -> :timeout
      end
    else
      poll_recovery(hub_id, System.monotonic_time(:millisecond) + timeout)
    end
  end

  defp poll_recovery(hub_id, deadline) do
    case Snapshot.recovery_state(hub_id) do
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

  defp running_hub_ids() do
    HubManager.list_running_hub_ids()
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end
end

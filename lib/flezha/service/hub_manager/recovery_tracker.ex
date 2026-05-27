defmodule Flezha.Service.HubManager.RecoveryTracker do
  @moduledoc """
  Caches the recovery lifecycle state of every per-namespace ProcessHub.

  Attaches a `:telemetry` handler for the upstream
  `[:process_hub, :recovery, :started | :complete | :skipped | :timeout]`
  events emitted by `ProcessHub.Service.Recovery` and exposes:

    * `state/1` — fast non-blocking lookup of a hub's last known
      recovery state.

    * `await_complete/2` — blocks the caller until every requested hub
      has reached `:normal`, or the provided timeout elapses.

  Used by every Flezha subsystem that touches the registry on boot —
  `Flezha.Executor.FleetManager`, `Flezha.State.Recovery`, the planned
  `Flezha.Convergence.Health`, `Flezha.Healthz.Server` — so a registry
  read never races a half-finished replay.
  """

  use GenServer

  alias Flezha.Service.HubManager

  require Logger

  @type hub_state ::
          :unknown
          | :recovery_pending
          | :recovering
          | :normal
          | :skipped
          | :timeout

  @telemetry_events [
    [:process_hub, :recovery, :started],
    [:process_hub, :recovery, :complete],
    [:process_hub, :recovery, :skipped],
    [:process_hub, :recovery, :timeout]
  ]

  @handler_id {__MODULE__, :recovery_events}

  # ---------- public API ----------

  @spec start_link(any()) :: GenServer.on_start()
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Last known recovery state for `hub_id`. Returns `:unknown` for hubs
  the tracker has not observed (newly started, no event fired yet) —
  callers treating `:unknown` as terminal should consult
  `ProcessHub.recovery_state/1` directly.
  """
  @spec state(atom()) :: hub_state()
  def state(hub_id) when is_atom(hub_id) do
    GenServer.call(__MODULE__, {:state, hub_id})
  end

  @doc """
  Block until every hub in `hub_ids` (defaults to all currently-running
  hubs known to `HubManager`) reaches `:normal`, or `timeout` ms
  expires.

  Returns `:ok` on success or `{:error, {:recovery_timeout, [hub_id]}}`
  carrying the hub_ids that did not reach a terminal state in time.

  Does NOT route through the tracker GenServer — runs in the caller so
  multiple subsystems can await in parallel and a slow recovery on one
  hub does not block tracker-state lookups by others. Delegates the
  per-hub wait to `ProcessHub.await_normal/2`, which is synchronized
  with the upstream coordinator's marker-write step.

  When `hub_ids` is `nil` and the tracker has observed an explicit
  non-terminal state for a hub (e.g. a unit test seeded `:recovering`
  via `observe/2`), those hubs are also waited on. This keeps unit
  tests deterministic without spinning up a real ProcessHub.
  """
  @spec await_complete([atom()] | nil, non_neg_integer()) ::
          :ok | {:error, {:recovery_timeout, [atom()]}}
  def await_complete(hub_ids \\ nil, timeout) when is_integer(timeout) and timeout >= 0 do
    targets = resolve_targets(hub_ids)
    deadline = System.monotonic_time(:millisecond) + timeout

    result =
      Enum.reduce(targets, [], fn hub_id, acc ->
        remaining_ms = max(deadline - System.monotonic_time(:millisecond), 0)

        if wait_for_normal(hub_id, remaining_ms) == :ok do
          acc
        else
          [hub_id | acc]
        end
      end)

    case Enum.reverse(result) do
      [] -> :ok
      unfinished -> {:error, {:recovery_timeout, unfinished}}
    end
  end

  defp resolve_targets(nil) do
    seeded =
      try do
        GenServer.call(__MODULE__, :seeded_hub_ids)
      catch
        :exit, _ -> []
      end

    Enum.uniq(running_hub_ids() ++ seeded)
  end

  defp resolve_targets(hub_ids) when is_list(hub_ids), do: hub_ids

  # Per-hub blocking wait. Falls back to a cache poll for hubs the
  # upstream coordinator does not know about (test seeds via
  # `observe/2`).
  defp wait_for_normal(hub_id, timeout) do
    if ProcessHub.is_alive?(hub_id) do
      case ProcessHub.await_normal(hub_id, timeout) do
        :ok -> :ok
        {:error, :timeout} -> :timeout
      end
    else
      poll_tracker(hub_id, timeout)
    end
  end

  defp poll_tracker(hub_id, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_poll_tracker(hub_id, deadline)
  end

  defp do_poll_tracker(hub_id, deadline) do
    case state(hub_id) do
      s when s in [:normal, :skipped] ->
        :ok

      _ ->
        remaining = deadline - System.monotonic_time(:millisecond)

        cond do
          remaining <= 0 -> :timeout
          true ->
            Process.sleep(min(remaining, 50))
            do_poll_tracker(hub_id, deadline)
        end
    end
  end

  @doc """
  Record an observed transition from a source other than telemetry —
  used by `HubManager.start_hub/1` so a brand-new hub's tracker entry
  is bootstrapped to the current upstream state immediately, instead
  of waiting for the first telemetry event.
  """
  @spec observe(atom(), hub_state()) :: :ok
  def observe(hub_id, state) when is_atom(hub_id) do
    GenServer.cast(__MODULE__, {:observe, hub_id, state})
  end

  # Called from the attached `:telemetry` handler. Module-public so the
  # detach hook stays referenceable.
  @doc false
  def handle_event(event, _measurements, %{hub_id: hub_id}, _config) do
    case List.last(event) do
      :started -> observe(hub_id, :recovering)
      :complete -> observe(hub_id, :normal)
      :skipped -> observe(hub_id, :skipped)
      :timeout -> observe(hub_id, :timeout)
      _ -> :ok
    end
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    _ = :telemetry.attach_many(@handler_id, @telemetry_events, &__MODULE__.handle_event/4, nil)
    {:ok, %{states: %{}}}
  end

  @impl true
  def handle_call({:state, hub_id}, _from, %{states: states} = s) do
    {:reply, Map.get(states, hub_id, :unknown), s}
  end

  def handle_call(:seeded_hub_ids, _from, %{states: states} = s) do
    {:reply, Map.keys(states), s}
  end

  @impl true
  def handle_cast({:observe, hub_id, state}, %{states: states} = s) do
    {:noreply, %{s | states: Map.put(states, hub_id, state)}}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :telemetry.detach(@handler_id)
    :ok
  end

  # ---------- internals ----------

  defp running_hub_ids do
    try do
      HubManager.list_running_hub_ids()
    rescue
      _ -> []
    catch
      :exit, _ -> []
    end
  end
end

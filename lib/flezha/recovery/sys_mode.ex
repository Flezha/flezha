defmodule Flezha.Recovery.SysMode do
  @moduledoc """
  Three-state lifecycle for the recovery subsystem.

      :bootstrap  -- just booted, window armed, DETS empty
      :recover    -- just booted, window armed, DETS has ≥1 row
      :normal     -- authoritative peer; window elapsed or short-circuited

  Transitions (see `openspec/changes/add-failure-recovery-mode/design.md`):
    - `init/1` picks `:normal` (disabled), `:recover` (rows) or `:bootstrap` (empty).
    - `{:peer_sysmode, _node, :normal}` while in `:recover` cancels the timer
      and jumps to `:normal` without running recovery (abort flag set for any
      in-flight Coordinator loop).
    - `:window_elapsed` while in `:recover` asynchronously asks the
      `Coordinator` to run recovery; mode stays `:recover` until
      `:recovery_done` arrives, then → `:normal`.
    - `:window_elapsed` while in `:bootstrap` simply → `:normal`.

  Recovery execution is delegated to `Flezha.Recovery.Coordinator` so SysMode
  remains responsive to peer messages (mid-recovery abort) during the loop.
  """

  use GenServer

  require Logger

  alias Flezha.Recovery.Store

  @type mode() :: :bootstrap | :recover | :normal
  @type reason() :: :init | :disabled | :window_elapsed | :peer_normal

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Current SYSMODE of this node."
  @spec get() :: mode()
  def get do
    case Process.whereis(__MODULE__) do
      nil -> :bootstrap
      pid -> GenServer.call(pid, :get, 5_000)
    end
  end

  @doc """
  Called by `Flezha.Cluster.Orchestrator` after a successful `Node.connect/1`.

  Sends our current mode to the peer's SysMode process; receiving our peer's
  mode back happens asynchronously via the peer's cast back to us.
  """
  @spec peer_connected(node()) :: :ok
  def peer_connected(peer_node) when is_atom(peer_node) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:peer_connected, peer_node})
    end
  end

  @doc """
  Internal API used by `Flezha.Recovery.Coordinator` to report that its
  recovery loop has finished (or was aborted).
  """
  @spec notify_recovery_done(map()) :: :ok
  def notify_recovery_done(result) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      pid -> send(pid, {:recovery_done, result}) && :ok
    end
  end

  ## GenServer callbacks

  @impl true
  def init(_opts) do
    enabled = Application.get_env(:flezha, :recovery_enabled, true)
    window_ms = Application.get_env(:flezha, :recovery_window_ms, 10_000)
    store_count = safe_store_count()

    state = %{
      mode: :bootstrap,
      window_ms: window_ms,
      timer: nil
    }

    state =
      cond do
        not enabled ->
          transition(state, :normal, :disabled)

        store_count > 0 and window_ms > 0 ->
          state
          |> transition(:recover, :init)
          |> arm_timer()

        window_ms > 0 ->
          # :bootstrap path — stay at :bootstrap, just arm.
          arm_timer(state)

        store_count > 0 ->
          # window_ms == 0: immediate recovery + normal.
          state = transition(state, :recover, :init)
          request_recovery()
          state

        true ->
          transition(state, :normal, :window_elapsed)
      end

    Logger.info(
      "[recovery] data_dir=#{inspect(Application.get_env(:flezha, :recovery_data_dir))} " <>
        "rows=#{store_count} mode=#{state.mode} window_ms=#{window_ms} enabled=#{enabled}"
    )

    {:ok, state}
  end

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state.mode, state}

  @impl true
  def handle_cast({:peer_connected, peer_node}, state) do
    GenServer.cast({__MODULE__, peer_node}, {:peer_sysmode, node(), state.mode})
    {:noreply, state}
  end

  def handle_cast({:peer_sysmode, _peer_node, :normal}, %{mode: :recover} = state) do
    set_abort_flag(true)

    new_state =
      state
      |> cancel_timer()
      |> transition(:normal, :peer_normal)

    {:noreply, new_state}
  end

  def handle_cast({:peer_sysmode, _peer_node, _peer_mode}, state) do
    # :bootstrap / :recover / duplicate :normal after we transitioned: all
    # ignorable. Only a :normal peer during our :recover window short-circuits.
    {:noreply, state}
  end

  @impl true
  def handle_info(:window_elapsed, state) do
    state = %{state | timer: nil}

    new_state =
      case state.mode do
        :recover ->
          # Kick off async recovery; transition to :normal when :recovery_done arrives.
          request_recovery()
          state

        :bootstrap ->
          transition(state, :normal, :window_elapsed)

        :normal ->
          state
      end

    {:noreply, new_state}
  end

  def handle_info({:recovery_done, _result}, %{mode: :recover} = state) do
    {:noreply, transition(state, :normal, :window_elapsed)}
  end

  def handle_info({:recovery_done, _result}, state) do
    # Already :normal (aborted via peer_normal). Noop.
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Helpers

  defp arm_timer(%{window_ms: ms} = state) when ms > 0 do
    ref = Process.send_after(self(), :window_elapsed, ms)
    %{state | timer: ref}
  end

  defp arm_timer(state), do: state

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: ref} = state) do
    _ = Process.cancel_timer(ref)
    %{state | timer: nil}
  end

  defp transition(%{mode: from} = state, to, reason) do
    if from == to do
      state
    else
      :telemetry.execute(
        [:flezha, :recovery, :sysmode_changed],
        %{},
        %{from: from, to: to, reason: reason, node: node()}
      )

      %{state | mode: to}
    end
  end

  defp request_recovery do
    cond do
      not Code.ensure_loaded?(Flezha.Recovery.Coordinator) ->
        notify_recovery_done(%{recovered: 0, failed: 0, duration_ms: 0, aborted: true})

      not function_exported?(Flezha.Recovery.Coordinator, :start_recovery_async, 0) ->
        notify_recovery_done(%{recovered: 0, failed: 0, duration_ms: 0, aborted: true})

      is_nil(Process.whereis(Flezha.Recovery.Coordinator)) ->
        # Coordinator not running — skip recovery cleanly.
        notify_recovery_done(%{recovered: 0, failed: 0, duration_ms: 0, aborted: true})

      true ->
        try do
          Flezha.Recovery.Coordinator.start_recovery_async()
        catch
          kind, reason ->
            Logger.warning(
              "[recovery] start_recovery_async raised #{inspect(kind)}: #{inspect(reason)} — skipping to :normal"
            )

            notify_recovery_done(%{recovered: 0, failed: 0, duration_ms: 0, aborted: true})
        end
    end

    :ok
  end

  defp set_abort_flag(value) do
    if Code.ensure_loaded?(Flezha.Recovery.Coordinator) and
         function_exported?(Flezha.Recovery.Coordinator, :set_abort_flag, 1) do
      Flezha.Recovery.Coordinator.set_abort_flag(value)
    end

    :ok
  end

  defp safe_store_count do
    if Process.whereis(Store) do
      try do
        Store.count()
      catch
        _, _ -> 0
      end
    else
      0
    end
  end
end

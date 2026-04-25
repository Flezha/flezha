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

  alias Flezha.ComputeEngine.FleetManager
  alias Flezha.Recovery.Store

  @type mode() :: :bootstrap | :recover | :normal
  @type reason() :: :init | :disabled | :window_elapsed | :peer_normal | :factory_advertised

  # Factory-gate poll cadence. 500ms keeps the startup delay small while the
  # INFO throttle (see `@wait_log_interval_ms`) keeps the log quiet.
  @wait_poll_interval_ms 500
  @wait_log_interval_ms 5_000

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
    wait_factory = Application.get_env(:flezha, :recovery_wait_factory, "")
    wait_namespace = Application.get_env(:flezha, :recovery_wait_namespace, "default")

    wait_timeout_ms = Application.get_env(:flezha, :recovery_wait_timeout_ms, 60_000)
    store_count = safe_store_count()

    state = %{
      mode: :bootstrap,
      window_ms: window_ms,
      wait_factory: wait_factory,
      wait_namespace: wait_namespace,
      wait_timeout_ms: wait_timeout_ms,
      timer: nil,
      wait_poll_ref: nil,
      last_wait_log_ms: 0
    }

    state =
      cond do
        not enabled ->
          transition(state, :normal, :disabled)

        store_count > 0 and wait_factory != "" ->
          # Factory-gated recovery path (design §D7). Replay fires either on
          # first matching advertisement or on `wait_timeout_ms`.
          state
          |> transition(:recover, :init)
          |> arm_factory_gate()

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

    gate =
      if wait_factory == "",
        do: "disabled",
        else: "factory=#{wait_factory} namespace=#{wait_namespace} timeout_ms=#{wait_timeout_ms}"

    Logger.info(
      "[recovery] data_dir=#{inspect(Application.get_env(:flezha, :recovery_data_dir))} " <>
        "rows=#{store_count} mode=#{state.mode} window_ms=#{window_ms} " <>
        "enabled=#{enabled} wait_gate=#{gate}"
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
      |> cancel_wait_poll()
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

  def handle_info(:wait_factory_poll, %{mode: :recover} = state) do
    case factories_all_advertised?(state.wait_factory, state.wait_namespace) do
      true ->
        Logger.info(
          "[recovery] factories #{state.wait_factory} advertised — triggering replay"
        )

        new_state =
          state
          |> cancel_wait_poll()
          |> cancel_timer()

        request_recovery()
        {:noreply, new_state}

      false ->
        {:noreply, reschedule_wait_poll(state)}
    end
  end

  def handle_info(:wait_factory_poll, state) do
    # Already advanced past :recover (peer_normal, :recovery_done). Drop the
    # poll tick; any outstanding timeout is still valid and will be ignored
    # if mode is no longer :recover.
    {:noreply, %{state | wait_poll_ref: nil}}
  end

  def handle_info(:wait_factory_timeout, %{mode: :recover} = state) do
    Logger.warning(
      "[recovery] factory #{state.wait_factory} not advertised within " <>
        "#{state.wait_timeout_ms}ms — proceeding with replay; fail-open semantics may yield empty state"
    )

    new_state =
      state
      |> cancel_wait_poll()
      |> Map.put(:timer, nil)

    request_recovery()
    {:noreply, new_state}
  end

  def handle_info(:wait_factory_timeout, state) do
    {:noreply, %{state | timer: nil}}
  end

  def handle_info({:recovery_done, _result}, %{mode: :recover} = state) do
    {:noreply,
     state
     |> cancel_wait_poll()
     |> cancel_timer()
     |> transition(:normal, :window_elapsed)}
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

  defp arm_factory_gate(state) do
    Logger.info(
      "[recovery] waiting for factory #{state.wait_factory} in namespace " <>
        "#{state.wait_namespace} (timeout=#{state.wait_timeout_ms}ms)"
    )

    timer_ref = Process.send_after(self(), :wait_factory_timeout, state.wait_timeout_ms)
    # Kick off the first poll on the next message-loop iteration so a
    # factory already advertised at init/1 time is caught immediately.
    poll_ref = Process.send_after(self(), :wait_factory_poll, 0)

    %{
      state
      | timer: timer_ref,
        wait_poll_ref: poll_ref,
        last_wait_log_ms: monotonic_ms()
    }
  end

  defp reschedule_wait_poll(state) do
    state =
      if throttled_log_due?(state) do
        Logger.info(
          "[recovery] waiting for factory #{state.wait_factory} in namespace " <>
            "#{state.wait_namespace}"
        )

        %{state | last_wait_log_ms: monotonic_ms()}
      else
        state
      end

    ref = Process.send_after(self(), :wait_factory_poll, @wait_poll_interval_ms)
    %{state | wait_poll_ref: ref}
  end

  defp cancel_wait_poll(%{wait_poll_ref: nil} = state), do: state

  defp cancel_wait_poll(%{wait_poll_ref: ref} = state) do
    _ = Process.cancel_timer(ref)
    %{state | wait_poll_ref: nil}
  end

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: ref} = state) do
    _ = Process.cancel_timer(ref)
    %{state | timer: nil}
  end

  defp factory_advertised?(factory_id, namespace) do
    try do
      case FleetManager.list_factories(namespace) do
        {:ok, factories} when is_list(factories) ->
          Enum.any?(factories, fn entry -> Map.get(entry, :id) == factory_id end)

        _ ->
          false
      end
    catch
      _kind, _reason -> false
    end
  end

  # Multi-factory gate — `wait_factory` may be a single id or a
  # comma-separated list ("crypto_trades,market-init-state"). All listed
  # factories must be advertised before replay starts, so processors whose
  # factory lives on a slower-to-connect compute engine (e.g. market-app,
  # which Spring Boots up ~10 s after the agent) aren't replayed before
  # their target node registers.
  defp factories_all_advertised?(factory_list, namespace) do
    ids = split_factory_list(factory_list)
    ids != [] and Enum.all?(ids, &factory_advertised?(&1, namespace))
  end

  defp split_factory_list(s) when is_binary(s) do
    s
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp split_factory_list(_), do: []

  defp throttled_log_due?(%{last_wait_log_ms: last}) do
    monotonic_ms() - last >= @wait_log_interval_ms
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)

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

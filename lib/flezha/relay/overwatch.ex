defmodule Flezha.Relay.Overwatch do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Service.RelayManager
  alias Flezha.Executor.ControlChannel
  alias Flezha.Executor.ControlUnit
  alias Flezha.Executor.FleetManager
  alias Flezha.Executor.StreamSup
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Reconciler.Activity
  alias Flezha.State.{Restore, Wire}

  # Defaults for the bounded-retry governor introduced by
  # `fix-overwatch-restart-storm`. Tuned to ~19 s aggregate so a transient
  # control-channel rebind (raspberrypi5 cold-start hits ~5–10 s) recovers
  # within one burst without spinning the supervisor.
  @default_retry_backoff_ms [200, 500, 1_000, 2_000, 5_000, 10_000]

  require Logger

  use GenServer
  use ProcessHub.Strategy.Migration.HotSwap

  use Flezha.Plugin.Host,
    extensions: [
      {:on_relay_init, :fold, Flezha.Plugin.Extension.Ctx.Init},
      {:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO},
      {:on_output, :transform, Flezha.Plugin.Extension.Ctx.IO},
      {:on_interval, :timer, Flezha.Plugin.Extension.Ctx.Tick},
      {:on_message, :request, Flezha.Plugin.Extension.Ctx.Msg},
      {:on_snapshot, :fanout, Flezha.Plugin.Extension.Ctx.Snapshot},
      {:on_terminate, :fanout, Flezha.Plugin.Extension.Ctx.Terminate}
    ]

  defstruct [
    :id,
    :namespace,
    :executor_id,
    :factory_id,
    :channel,
    :extensions,
    :builder,
    snapshot: nil,
    init_values: %{},
    default_input_type: "2",
    arrived_via_hotswap?: false,
    init_error: nil
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init({namespace, stale_executor_id, relay}) do
    Process.flag(:trap_exit, true)

    # IMPORTANT: init/1 must return quickly so the `dist_sup` supervisor
    # tree (which calls us synchronously from `DistributedSupervisor.init/1`
    # during the DETS replay) can unblock. Any GenServer.call to
    # FleetManager / RelayManager from inside init/1 deadlocks because
    # FleetManager is itself waiting on :default_hub, which is waiting
    # for dist_sup.init to complete.
    #
    # Defer all blocking work (executor wait, plugin subscribe, SDK
    # start) to handle_continue — by then init has returned, dist_sup
    # has unblocked, the hub has finished initialization, and
    # FleetManager can answer our calls.
    state = %__MODULE__{
      id: relay.id,
      namespace: namespace,
      factory_id: relay.factory_id,
      init_values: relay.init_values,
      executor_id: stale_executor_id,
      channel: nil,
      snapshot: nil,
      default_input_type: Keyword.get(relay.opts, :default_input_type, "2"),
      extensions: Table.new(__extensions_catalog__()),
      builder: relay
    }

    {:ok, state, {:continue, {:bind_executor, stale_executor_id, relay}}}
  end

  @impl true
  def handle_continue({:bind_executor, stale_executor_id, relay}, %__MODULE__{} = state) do
    # Wait for an active executor in this namespace advertising
    # `relay.factory_id`. The DETS-baked executor_id is usually stale
    # after a restart (Identificator.unique_id/1 generates a fresh id
    # per connect), so we re-resolve via RelayManager.assign_executor/2
    # — the same lookup HubManager.alter_child_data/2 performs.
    case await_active_executor(state.namespace, relay.factory_id, executor_wait_ms()) do
      {:ok, executor_id} ->
        if executor_id != stale_executor_id do
          Logger.info(
            "[Overwatch] relay #{relay.id} rebound from stale executor #{inspect(stale_executor_id)} to live #{inspect(executor_id)}"
          )
        end

        case complete_init(state, executor_id, relay) do
          {:ok, new_state} -> {:noreply, new_state}
          {:error, reason, parked} -> {:noreply, park(parked, reason)}
        end

      {:error, :timeout} ->
        Logger.warning(
          "[Overwatch] no active executor in namespace #{inspect(state.namespace)} for factory #{inspect(relay.factory_id)} within #{executor_wait_ms()} ms; deferring relay #{relay.id} startup"
        )

        {:stop, :executor_not_ready, state}
    end
  end

  defp complete_init(%__MODULE__{} = state, executor_id, relay) do
    state = bind_to_executor(state, relay.id, executor_id)
    plugins = Keyword.get(relay.opts, :plugins, [])

    with {:ok, state} <- prepare_initial_plugins(state, plugins),
         {:ok, state, relay} <- resolve_init_state(state, relay),
         {:ok, final_executor_id, state} <- start_relay_sdk_with_retry(state, relay) do
      state =
        state
        |> bind_to_executor(relay.id, final_executor_id)
        |> Map.put(:init_error, nil)
        |> activate_initial_plugins()

      _ = maybe_dispatch_restore_init_state(state, relay)
      {:ok, state}
    else
      {:error, reason, %__MODULE__{} = parked} -> {:error, reason, parked}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp bind_to_executor(%__MODULE__{} = state, relay_id, executor_id) do
    %__MODULE__{
      state
      | executor_id: executor_id,
        channel: attach_channel(relay_id, executor_id)
    }
  end

  # Returns `{:ok, executor_id, state}` on success or `{:error, reason,
  # state}` on exhaustion. On every attempt the executor is re-resolved
  # so a fresh executor that came online during a backoff window is
  # picked up.
  defp start_relay_sdk_with_retry(%__MODULE__{} = state, relay) do
    do_start_with_retry(state, relay, retry_backoff_ms(), 1)
  end

  defp do_start_with_retry(state, _relay, [], _attempt) do
    {:error, :exhausted, state}
  end

  defp do_start_with_retry(%__MODULE__{} = state, relay, [delay | rest], attempt) do
    executor_id = resolve_executor(state.namespace, relay.factory_id, state.executor_id)
    state = %__MODULE__{state | executor_id: executor_id}

    case attempt_ping_and_start(relay, executor_id) do
      {:ok, _} ->
        {:ok, executor_id, state}

      {:error, reason} when rest == [] ->
        {:error, reason, state}

      {:error, reason} ->
        Logger.warning(
          "[Overwatch] relay #{relay.id} startup attempt #{attempt} failed: " <>
            "#{inspect(reason)} — retrying within #{delay} ms"
        )

        wait_with_jitter(delay)
        do_start_with_retry(state, relay, rest, attempt + 1)
    end
  end

  # On ping failure, reset the underlying `ControlChannel`'s `stream_ref`
  # so the SDK's next data stream can claim it via the existing rebind
  # path (`executor-controller-connection-healing`). `:start_relay` is
  # only issued AFTER the ping confirms the round-trip works.
  defp attempt_ping_and_start(relay, executor_id) do
    case ping_executor(executor_id) do
      :ok ->
        case start_relay_sdk(executor_id, relay) do
          {:ok, _} = ok -> ok
          {:error, reason} -> {:error, {:start_relay_failed, reason}}
        end

      {:error, reason} ->
        _ = reset_control_channel(executor_id)
        {:error, {:ping_failed, reason}}
    end
  end

  # `:overwatch_ping_fn` / `:overwatch_reset_channel_fn` are test-only
  # function-injection hooks (Mimic cannot stub across the dist_sup-spawned
  # Overwatch process boundary from a test pid).
  defp ping_executor(executor_id) do
    cond do
      not Application.get_env(:flezha, :overwatch_ping_enabled, true) ->
        :ok

      fun = test_hook(:overwatch_ping_fn) ->
        normalize_ping(fun.(executor_id))

      true ->
        normalize_ping(ControlUnit.ping_executor(executor_id))
    end
  end

  defp normalize_ping({:ok, :pong}), do: :ok
  defp normalize_ping({:error, _} = err), do: err

  defp reset_control_channel(executor_id) do
    case test_hook(:overwatch_reset_channel_fn) do
      nil -> FleetManager.reset_control_channel(executor_id)
      fun -> fun.(executor_id)
    end
  end

  defp test_hook(key) do
    case Application.get_env(:flezha, key) do
      fun when is_function(fun, 1) -> fun
      _ -> nil
    end
  end

  defp resolve_executor(namespace, factory_id, fallback) do
    case RelayManager.assign_executor(namespace, factory_id) do
      id when is_binary(id) and byte_size(id) > 0 -> id
      _ -> fallback
    end
  end

  defp retry_backoff_ms,
    do: Application.get_env(:flezha, :overwatch_retry_backoff_ms, @default_retry_backoff_ms)

  # Idle the GenServer for `1..delay` ms. The empty receive consumes no
  # mailbox messages — it only fires the `after` branch on timeout, so
  # casts and infos arriving during the wait stay queued for the next
  # dispatch. Equivalent to `Process.sleep/1` minus the dedicated
  # sleep primitive.
  defp wait_with_jitter(0), do: :ok

  defp wait_with_jitter(delay) when is_integer(delay) and delay > 0 do
    receive do
    after
      :rand.uniform(delay) -> :ok
    end
  end

  # Transition the Overwatch into the parked-error state: store the failure
  # reason and stay alive (no `:stop`, no supervisor restart). Plugins remain
  # in `:prepared` so no upstream I/O fires while parked. The reconciler drives
  # the event-driven un-park (`executor_migrate`) once a healthy executor for
  # the relay's factory is available.
  defp park(%__MODULE__{} = state, reason) do
    Map.put(state, :init_error, reason)
  end

  # The minimal SDK relay for a running-relay migrate: the factory + the
  # current (snapshot-derived) init_values, no plugins (the controller-side
  # plugin table is untouched on a running migrate).
  defp migrate_relay(%__MODULE__{} = state) do
    init_values =
      if state.snapshot != nil,
        do: Payloader.json_to_map(state.snapshot),
        else: state.init_values

    %Flezha.Relay.Builder{
      id: state.id,
      factory_id: state.factory_id,
      init_values: init_values,
      opts: []
    }
  end

  defp executor_wait_ms,
    do: Application.get_env(:flezha, :overwatch_executor_wait_ms, 30_000)

  # Poll `RelayManager.assign_executor/2` until it returns a live
  # executor id whose CU is registered in Flezha.Service.Identificator,
  # or the deadline elapses. Returns `{:ok, executor_id}` or
  # `{:error, :timeout}`.
  defp await_active_executor(namespace, factory_id, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_active_executor(namespace, factory_id, deadline)
  end

  defp do_await_active_executor(namespace, factory_id, deadline) do
    case RelayManager.assign_executor(namespace, factory_id) do
      executor_id when is_binary(executor_id) ->
        if executor_ready?(executor_id) do
          {:ok, executor_id}
        else
          retry_or_timeout(namespace, factory_id, deadline)
        end

      _ ->
        retry_or_timeout(namespace, factory_id, deadline)
    end
  end

  defp retry_or_timeout(namespace, factory_id, deadline) do
    now = System.monotonic_time(:millisecond)

    cond do
      now >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(50)
        do_await_active_executor(namespace, factory_id, deadline)
    end
  end

  # The executor's control unit GenServer registers itself in
  # `Flezha.Service.Identificator` as the LAST step of its supervision
  # tree (`ExecutorSupervisor.init/1` → `ControlUnit.start_link/1`),
  # so its presence is a sufficient signal that the executor's CU,
  # StreamSup, and StreamAcceptor are all live. Note: `StreamSup.stream_ids/2`
  # alone is NOT a liveness check — it derives names from config.
  defp executor_ready?(executor_id) do
    case Registry.lookup(Identificator, "#{executor_id}_cu") do
      [{_pid, _}] -> true
      _ -> false
    end
  end

  # Walk the on-disk Partition state for this relay (using `relay.id` as
  # the relay_class) and dispatch `RestoreInitState` to the SDK for each
  # Partition that has a non-empty payload. Skipped when the relay
  # arrived via HotSwap — handover already delivers the state in-band.
  # Skipped when persistence is disabled for this relay class.
  defp maybe_dispatch_restore_init_state(%__MODULE__{arrived_via_hotswap?: true}, _relay), do: :ok

  defp maybe_dispatch_restore_init_state(%__MODULE__{} = state, relay) do
    relay_class = Keyword.get(relay.opts, :relay_class, relay.id)

    if Flezha.State.PersistenceResolver.persistence_enabled?(relay_class) do
      partitions = enumerate_persisted_partitions(relay_class)

      Enum.each(partitions, fn {rc, sh, pk} ->
        case Flezha.State.Restore.has_persisted_state?({rc, sh, pk}) do
          true ->
            _ = dispatch_restore_init_state(state.executor_id, {rc, sh, pk})

          false ->
            :ok
        end
      end)
    end

    :ok
  end

  defp enumerate_persisted_partitions(relay_class) do
    Flezha.State.Registry.all_partitions()
    |> Enum.filter(fn {rc, _, _} -> rc == relay_class end)
  end

  # Phase 1 of the two-phase plugin lifecycle: register each plugin's
  # GenServer + struct (`init_provider/1` runs), but do NOT activate
  # upstream resources. Plugins enter the `:prepared` phase. Idempotent on
  # repeat — Table.bulk_subscribe/2 rejects duplicate plugin_ids, so on a
  # retry-driven re-entry we skip without re-init.
  defp prepare_initial_plugins(%__MODULE__{} = state, plugins) do
    # `relay.opts[:plugins]` accepts two shapes: bare `%Plugin{}` (the
    # cold-start `flz relay start --plugins` path emits this) and
    # `{plugin_id, %Plugin{}}` tuples (the cspec-persistence path written
    # by `Flezha.Service.HubManager.update_relay_plugins/3` after a
    # successful `flz plugin enable`). Both normalise into the tuple
    # form `Table.bulk_subscribe/2` expects.
    already_prepared? =
      state.extensions.plugins != %{} and Enum.count(state.extensions.plugins) > 0

    if already_prepared? do
      {:ok, state}
    else
      entries =
        Enum.map(plugins, fn
          {id, %Plugin{} = plugin} -> {id, plugin}
          %Plugin{id: id} = plugin -> {id, plugin}
        end)

      case Table.bulk_subscribe(state.extensions, entries) do
        {:ok, table} -> {:ok, %{state | extensions: table}}
        {:error, {_plugin_id, reason}} -> {:error, reason}
      end
    end
  end

  # Phase 2 of the two-phase plugin lifecycle: transition every prepared
  # plugin to `:running` by invoking its provider's optional `activate/1`
  # callback (which is where upstream TLS connects, subscribe handshakes,
  # and other resource-acquisition I/O happens — see
  # `Plugin.Provider.Websocket`). Called exactly once per successful
  # startup, immediately after `start_relay_sdk/2` returns `{:ok, _}`.
  defp activate_initial_plugins(%__MODULE__{} = state) do
    %__MODULE__{state | extensions: Table.bulk_activate(state.extensions)}
  end

  # Per the `relay-init-state` capability spec:
  #   - cold start with empty init_values → chain runs
  #   - cold start with non-empty init_values → chain MUST NOT run; inline values used as-is
  #
  # `init/1` is also the path for in-place restart plugin restoration: the
  # cspec's `relay.opts[:plugins]` list is read above and re-subscribed
  # by `subscribe_initial_plugins/2`. That path is independent of the
  # per-Partition state-restore path (`dispatch_restore_init_state/2`),
  # which rehydrates SDK-side WAL/snapshot state through the data plane.
  #
  # HotSwap migration is handled separately in `alter_handover_state/2`
  # and never reaches this function.
  defp resolve_init_state(%__MODULE__{} = state, %{init_values: init_values} = relay)
       when init_values == %{} do
    ctx = %Ctx.Init{
      relay_id: relay.id,
      namespace: state.namespace,
      factory_id: relay.factory_id,
      accumulated: relay.init_values
    }

    case Extension.run(state.extensions, :on_relay_init, ctx) do
      {:ok, %Ctx.Init{accumulated: resolved}} ->
        {:ok, %{state | init_values: resolved}, %{relay | init_values: resolved}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_init_state(%__MODULE__{} = state, relay) do
    # Non-empty inline init_values bypass the chain.
    {:ok, %{state | init_values: relay.init_values}, relay}
  end

  @impl true
  def terminate(reason, state) do
    unless is_nil(state.executor_id) do
      GenServer.cast(
        Identificator.control_unit(state.executor_id),
        {:stop_relay_async, state.id}
      )

      fire_on_terminate(state, reason)
      state = %{state | extensions: Table.teardown_all(state.extensions)}
      _ = state
    end

    Logger.info(
      "Terminating relay #{state.id} in namespace #{state.namespace}: #{inspect(reason)}"
    )

    :ok
  end

  defp fire_on_terminate(%__MODULE__{} = state, reason) do
    ctx = %Ctx.Terminate{
      relay_id: state.id,
      namespace: state.namespace,
      executor_id: state.executor_id,
      reason: reason
    }

    Extension.run(state.extensions, :on_terminate, ctx)
    :ok
  end

  @impl true
  def handle_call(:registry_executor_set, _from, %__MODULE__{init_error: nil} = state) do
    HubManager.relay_set_executor(state.namespace, state.id, state.executor_id)

    {:reply, {:ok, state.executor_id}, state}
  end

  def handle_call(:registry_executor_set, _from, %__MODULE__{init_error: reason} = state) do
    {:reply, {:error, reason}, state}
  end

  @impl true
  def handle_call(:stop_relay, from, state) do
    GenServer.call(
      Identificator.control_unit(state.executor_id),
      {:stop_relay, state.id}
    )

    fire_on_terminate(state, :stopped)
    table = Table.teardown_all(state.extensions)

    GenServer.reply(from, :ok)

    {:stop, :normal, %{state | executor_id: nil, extensions: table}}
  end

  @impl true
  def handle_call(
        {:plugin_subscribe, plugin_id, %Plugin{} = plugin},
        _from,
        %__MODULE__{} = state
      ) do
    case Table.subscribe(state.extensions, plugin_id, plugin) do
      {:ok, table} ->
        # Runtime adds: SDK is already up, so activate immediately.
        # Boot path goes through activate_initial_plugins/1 instead.
        {:reply, :ok, %{state | extensions: Table.bulk_activate(table)}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:plugin_unsubscribe, plugin_id}, _from, %__MODULE__{} = state) do
    table = Table.unsubscribe(state.extensions, plugin_id)
    {:reply, :ok, %{state | extensions: table}}
  end

  @impl true
  def handle_call(
        :get_executor,
        _from,
        %__MODULE__{executor_id: executor} = state
      ) do
    {:reply, executor, state}
  end

  @impl true
  def handle_call(:get_overwatch_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:get_partition_state, _from, state) do
    command = %ExecutorCommands.PartitionState{relay_id: state.id}

    result = send_control(state.executor_id, {:partition_state, command})

    {:reply, result, state}
  end

  # Parked relay → event-driven un-park (the reconciler's `:unpark` action):
  # run the full recovery against the reconnected executor immediately —
  # re-prepare plugins, ping before start, clear `init_error` on success, and
  # activate plugins so the relay resumes producing.
  @impl true
  def handle_call(
        {:executor_migrate, new_executor_id},
        _from,
        %__MODULE__{init_error: reason, builder: relay} = state
      )
      when reason != nil do
    case complete_init(state, new_executor_id, relay) do
      {:ok, recovered} ->
        Logger.info("[Overwatch] relay #{state.id} un-parked onto executor #{new_executor_id}")
        {:reply, :ok, recovered}

      {:error, err, parked} ->
        {:reply, {:error, err}, park(parked, err)}
    end
  end

  # Running relay moved to a new executor (HotSwap reroute / orphan reissue):
  # rebind the channel, ping before re-issuing start, and surface the start
  # result so the caller (reissue retry, reconciler) can re-drive on failure.
  def handle_call({:executor_migrate, new_executor_id}, _from, %__MODULE__{} = state) do
    state = bind_to_executor(state, state.id, new_executor_id)

    case attempt_ping_and_start(migrate_relay(state), new_executor_id) do
      {:ok, _} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, :pong, state}
  end

  @impl true
  def handle_call({:io_in, _message, _metadata}, _from, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    {:reply, {:error, :startup_failed}, state}
  end

  @impl true
  def handle_call({:io_in, message, metadata}, _from, state) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = run_io(state, :on_input, message, metadata)

    case payload do
      nil ->
        :ok

      _ ->
        # Count an unbound-stream drop instead of discarding the channel's
        # `{:error, :no_stream}` reply, so the reconciler-detected stall is
        # observable (the probe rebinds the stream).
        case GenServer.call(state.channel, {:io_in, state.id, payload}) do
          {:error, :no_stream} -> Activity.record_drop(state.id)
          _ -> :ok
        end
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:create_snapshot, _from, %__MODULE__{} = state) do
    snapshot = create_snapshot(state.id, state.executor_id)

    {:reply, {:ok, snapshot}, %__MODULE__{state | snapshot: snapshot}}
  end

  @impl true
  def handle_call(:get_snapshot_data, _from, %__MODULE__{} = state) do
    data = %{
      namespace: state.namespace,
      builder: state.builder,
      plugins: Table.serializable_plugins(state.extensions),
      snapshot_binary: state.snapshot
    }

    {:reply, data, state}
  end

  @impl true
  def handle_cast({:io_in, _message, _metadata}, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    # Parked: silently drop ingress casts. Prepared plugins do not emit
    # `:io_in`, so the only sources are upstream relays in a chain.
    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_in, message, metadata}, state) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = run_io(state, :on_input, message, metadata)

    case payload do
      nil -> nil
      _ -> GenServer.cast(state.channel, {:io_in, state.id, payload})
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, message, metadata}, state) do
    run_io(state, :on_output, message, metadata)
    {:noreply, state}
  end

  # Replaces `state.snapshot` with a binary supplied out-of-band by the SDK
  # on graceful shutdown. The snapshot is consumed by HotSwap's
  # `prepare_handover_state` so a subsequent migration ships the
  # executor's final state. Cold-boot restart is covered by the
  # per-Partition WAL files instead.
  @impl true
  def handle_cast({:apply_external_snapshot, binary}, %__MODULE__{} = state)
      when is_binary(binary) do
    {:noreply, %__MODULE__{state | snapshot: binary}}
  end

  defp run_io(%__MODULE__{} = state, extension, message, metadata) do
    ctx = %Ctx.IO{
      relay_id: state.id,
      namespace: state.namespace,
      executor_id: state.executor_id,
      message: message,
      metadata: metadata
    }

    case Extension.run(state.extensions, extension, ctx) do
      {:ok, %Ctx.IO{message: msg, metadata: md}} -> {msg, md}
      {:skip, _reason} -> nil
      {:error, _reason} -> {message, metadata}
    end
  end

  @impl true
  def handle_info({:extension_timer, name, plugin_id}, %__MODULE__{} = state) do
    table = Table.fire_timer(state.extensions, name, plugin_id)
    {:noreply, %{state | extensions: table}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, %__MODULE__{} = state) do
    case Table.handle_down(state.extensions, ref, pid, reason) do
      {:ok, table} ->
        {:noreply, %{state | extensions: table}}

      :ignore ->
        Logger.error("Binding not found for pid #{inspect(pid)} with reason: #{inspect(reason)}")

        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    case reason do
      :normal -> Logger.info("Process #{inspect(pid)} exited normally.")
      _ -> Logger.error("Process #{inspect(pid)} exited with reason: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:retry_plugin_restart, plugin_id}, state) do
    table = Table.retry(state.extensions, plugin_id)
    {:noreply, %{state | extensions: table}}
  end

  @impl true
  def handle_info(
        {:flezha_request, handler_key, payload, reply_to, ref},
        %__MODULE__{} = state
      ) do
    ctx = %Ctx.Msg{
      key: handler_key,
      payload: payload,
      reply_to: reply_to,
      ref: ref,
      relay_id: state.id,
      namespace: state.namespace,
      executor_id: state.executor_id
    }

    Extension.run(state.extensions, :on_message, ctx)

    {:noreply, state}
  end

  defp create_snapshot(relay_id, executor_id) do
    command = %ExecutorCommands.PartitionState{relay_id: relay_id}

    case send_control(executor_id, {:partition_state, command}) do
      {:ok, snapshot} -> snapshot
      {:error, _reason} -> nil
    end
  end

  @impl true
  def prepare_handover_state(state) do
    serialize_data =
      case serialize_relay_sdk(state.executor_id, state.id) do
        nil -> state.snapshot
        data -> data
      end

    handover = %{
      id: state.id,
      namespace: state.namespace,
      factory_id: state.factory_id,
      init_values: state.init_values,
      plugins: Table.serializable_plugins(state.extensions)
    }

    {handover, serialize_data}
  end

  @impl true
  def alter_handover_state(%__MODULE__{} = state, {handover_state, pipe_serialized}) do
    command = %ExecutorCommands.MigrationDeserialization{
      relay_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: pipe_serialized
    }

    result = send_control(state.executor_id, {:migration_deserialization, command})

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize relay: #{inspect(reason)}")
      _else -> :ok
    end

    # Tear down existing, then rehydrate; activate so providers reconnect.
    table = Table.teardown_all(state.extensions)
    rehydrated = Table.rehydrate(table, Map.get(handover_state, :plugins, []))

    %__MODULE__{
      state
      | extensions: Table.bulk_activate(rehydrated),
        init_values: handover_state.init_values,
        arrived_via_hotswap?: true
    }
  end

  defp send_control(executor_id, command) do
    control_unit = Identificator.control_unit(executor_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {
          :error,
          "control unit not available for executor #{executor_id} with payload"
        }

      _ ->
        GenServer.call(
          Identificator.control_unit(executor_id),
          {:send_control, command}
        )
    end
  end

  defp start_relay_sdk(executor_id, relay) do
    GenServer.call(
      Identificator.control_unit(executor_id),
      {:start_relay, relay},
      # 8s wasn't enough when the SDK is on the Java side — Spring/JPA
      # warmup plus the AddRelay control-channel round-trip easily
      # exceeds that during a cold start. 30s gives Spring time to breathe
      # without masking genuine stalls.
      30_000
    )
  end

  defp attach_channel(relay_id, executor_id) do
    relay_int = String.to_charlist(relay_id) |> Enum.sum()
    count = ConfigManager.concurrent_channel_count()
    channel_index = rem(relay_int, count)

    StreamSup.stream_ids(executor_id, :primary) |> Enum.at(channel_index)
  end

  defp serialize_relay_sdk(executor_id, relay_id) do
    command = %ExecutorCommands.MigrationSerialization{relay_id: relay_id}

    result = send_control(executor_id, {:migration_serialization, command})

    case result do
      {:ok, serialized_data} ->
        serialized_data

      {:error, reason} ->
        if Application.get_env(:flezha, :env) !== :test do
          Logger.error("Failed to serialize relay: #{inspect(reason)}")
        end

        nil
    end
  end

  # ---------- Per-Partition persistence wiring ----------
  #
  # Helpers used by `Flezha.State.*` and tests to ship persistence
  # control commands to the SDK. Each takes the controller-side
  # `executor_id` and a structured `partition_id` triple
  # `{relay_class, shard_id, partition_key}`.

  @doc """
  Ship a `RestoreInitState` command to the SDK and return the controller's
  freshly assembled payload. The caller is expected to await the
  matching `RestoreInitStateResponse` over the control channel before
  unblocking input streaming for the Partition.
  """
  @spec dispatch_restore_init_state(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_restore_init_state(executor_id, {rc, sh, pk}) do
    case Restore.assemble_payload({rc, sh, pk}) do
      {:ok, payload} ->
        partition_id_bytes = Wire.encode_partition_id(rc, sh, pk)

        command =
          {:restore_init_state,
           %ExecutorCommands.RestoreInitState{
             partition_id: partition_id_bytes,
             snapshot_bytes: payload.snapshot_bytes,
             change_log_bytes: payload.change_log_bytes
           }}

        ControlChannel.send_control(executor_id, command)
    end
  end

  @doc """
  Ship a `CompactPartition` command to the SDK. The SDK responds with a
  `partition_state_snapshot` data frame (handled by
  `Flezha.Executor.StreamChannel`) followed by a
  `CompactPartitionResponse` control reply.
  """
  @spec dispatch_compact_partition(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_compact_partition(executor_id, {rc, sh, pk} = partition_id) do
    partition_id_bytes = Wire.encode_partition_id(rc, sh, pk)

    command =
      {:compact_partition, %ExecutorCommands.CompactPartition{partition_id: partition_id_bytes}}

    case ControlChannel.send_control(executor_id, command) do
      {:ok, _ref} = ok ->
        # Bound the handshake: a snapshot that never arrives is timed out
        # by the Watchdog rather than left to wedge persistence.
        Flezha.State.Watchdog.compaction_started(partition_id)
        ok

      other ->
        other
    end
  end

  @doc """
  Ship a `PartitionMailboxStatus` notification to the SDK so the
  producing Shard's `OverflowPolicy` machinery can park or unpark
  emits to this Partition. `status` is `:full` or `:open`.
  """
  @spec notify_partition_backpressure({binary(), binary(), binary(), binary()}, :full | :open) ::
          :ok | {:error, term()}
  def notify_partition_backpressure({executor_id, rc, sh, pk}, status)
      when status in [:full, :open] do
    partition_id_bytes = Wire.encode_partition_id(rc, sh, pk)

    command =
      {:partition_mailbox_status,
       %ExecutorCommands.PartitionMailboxStatus{
         partition_id: partition_id_bytes,
         status: Atom.to_string(status)
       }}

    ControlChannel.send_control_async(executor_id, command)
  end
end

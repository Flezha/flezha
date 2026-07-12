defmodule Flezha.Operative.Overwatch do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Service.OperativeManager
  alias Flezha.Executor.ControlChannel
  alias Flezha.Executor.ControlUnit
  alias Flezha.Executor.FleetManager
  alias Flezha.Executor.StreamSup
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{InputLog, OpInbox, OpJournal, PersistenceResolver, Restore, Wire}
  alias Flezha.Transport.LivenessCounters

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
      {:on_operative_init, :fold, Flezha.Plugin.Extension.Ctx.Init},
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
    init_error: nil,
    # Wall-clock ms at process start, for `flz operative info` uptime. Wall-clock
    # (not monotonic) so the value stays comparable when read from another node.
    started_at: nil,
    # Liveness counters ref (io_in_received / forwarded / filtered_nil) so a
    # stall can be localised to "WS plugin stopped feeding" vs "operative stopped
    # forwarding to the channel".
    liveness_ref: nil,
    # The `:executor_migrate` retry runs in a `Task.Supervisor.async_nolink/3`
    # task so it never blocks `:stop_operative`; this field carries the in-flight
    # task's reference + pid so `:stop_operative` can terminate it cleanly. `nil`
    # when no migrate is in flight.
    migrate_task: nil,
    # Effectively-once tier, declared by the SDK in the AddOperative response
    # metadata. Gates the input-log journal + `flz.offset` stamp on `:io_in`.
    effectively_once: false,
    # Durable bounded class (kind + per-class persistence). Gates the durable
    # inbox on `:io_in` and the suspension lifecycle
    # (`bounded-durable-suspension`).
    durable_bounded: false,
    # Suspended at a committed iteration boundary: zero live executor resources,
    # registry entry retained. A new inbox append — or the journaled wake
    # deadline — rehydrates through the normal bind path.
    suspended: false,
    # Timer ref for the journaled wake deadline while suspended.
    wake_timer: nil
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init({namespace, stale_executor_id, operative}) do
    Process.flag(:trap_exit, true)

    # IMPORTANT: init/1 must return quickly so the `dist_sup` supervisor
    # tree (which calls us synchronously from `DistributedSupervisor.init/1`
    # during the DETS replay) can unblock. Any GenServer.call to
    # FleetManager / OperativeManager from inside init/1 deadlocks because
    # FleetManager is itself waiting on :default_hub, which is waiting
    # for dist_sup.init to complete.
    #
    # Defer all blocking work (executor wait, plugin subscribe, SDK
    # start) to handle_continue — by then init has returned, dist_sup
    # has unblocked, the hub has finished initialization, and
    # FleetManager can answer our calls.
    state = %__MODULE__{
      id: operative.id,
      namespace: namespace,
      factory_id: operative.factory_id,
      init_values: operative.init_values,
      executor_id: stale_executor_id,
      channel: nil,
      snapshot: nil,
      default_input_type: Keyword.get(operative.opts, :default_input_type, "2"),
      extensions: Table.new(__extensions_catalog__()),
      builder: operative,
      started_at: System.system_time(:millisecond),
      liveness_ref: LivenessCounters.register_operative(operative.id)
    }

    {:ok, state, {:continue, {:bind_executor, stale_executor_id, operative}}}
  end

  @impl true
  def handle_continue({:bind_executor, stale_executor_id, operative}, %__MODULE__{} = state) do
    state = %__MODULE__{state | durable_bounded: durable_bounded?(state, operative)}

    # Suspended instances are rediscovered from durable state alone (journal +
    # inbox), so a controller redeploy neither binds them to an executor nor
    # loses their suspension (`bounded-durable-suspension`). An instance with
    # undelivered input is not suspended any more — it binds and consumes it.
    if state.durable_bounded and suspended_on_disk?(operative.id) do
      {:noreply, enter_suspended(state, OpJournal.restore_state(operative.id))}
    else
      do_bind_executor(state, stale_executor_id, operative)
    end
  end

  defp do_bind_executor(%__MODULE__{} = state, stale_executor_id, operative) do
    # Wait for an active executor in this namespace advertising
    # `operative.factory_id`. The DETS-baked executor_id is usually stale
    # after a restart (Identificator.unique_id/1 generates a fresh id
    # per connect), so we re-resolve via OperativeManager.assign_executor/3
    # — the same lookup HubManager.alter_child_data/2 performs.
    case with_op_span(:await_executor, operative.id, fn ->
           await_active_executor(
             state.namespace,
             operative.factory_id,
             operative.id,
             executor_wait_ms()
           )
         end) do
      {:ok, executor_id} ->
        if executor_id != stale_executor_id do
          Logger.info(
            "[Overwatch] operative #{operative.id} rebound from stale executor #{inspect(stale_executor_id)} to live #{inspect(executor_id)}"
          )
        end

        case complete_init(state, executor_id, operative) do
          {:ok, new_state} -> {:noreply, new_state}
          {:error, reason, parked} -> {:noreply, park(parked, reason)}
        end

      {:error, :timeout} ->
        Logger.warning(
          "[Overwatch] no active executor in namespace #{inspect(state.namespace)} for factory #{inspect(operative.factory_id)} within #{executor_wait_ms()} ms; deferring operative #{operative.id} startup"
        )

        {:stop, :executor_not_ready, state}
    end
  end

  # `owner` is the Overwatch process itself. It is passed explicitly because the
  # un-park path runs `complete_init` inside a task (so the heavy bounded-retry
  # never blocks `:stop_operative`), and `self()` there is the TASK — registering
  # it as the operative's Overwatch would point the journal's outbox flush at a
  # process that exits moments later, silently dropping the output.
  defp complete_init(%__MODULE__{} = state, executor_id, operative, owner \\ nil) do
    owner = owner || self()
    state = bind_to_executor(state, operative.id, executor_id)
    plugins = Keyword.get(operative.opts, :plugins, [])

    with {:ok, state} <- prepare_initial_plugins(state, plugins),
         {:ok, state, operative} <- resolve_init_state(state, operative),
         operative =
           with_resume_segment_count(operative, state.builder.kind, operative_class(state)),
         {:ok, final_executor_id, add_operative_raw, state} <-
           start_operative_sdk_with_retry(state, operative) do
      # The SDK declares the effectively-once tier in the AddOperative
      # response metadata; resolve it BEFORE plugin activation so source
      # adapters pick the tier ingest mode (blocking call) from the env.
      effectively_once = decode_effectively_once(add_operative_raw)

      state =
        %{state | effectively_once: effectively_once}
        |> bind_to_executor(operative.id, final_executor_id)
        |> Map.put(:init_error, nil)

      # Tier: restore every persisted Segment (awaited) and re-forward the
      # journaled input tail BEFORE sources activate, so replayed offsets
      # reach the executor ahead of fresh ones (fold order at the SDK).
      state = if effectively_once, do: restore_and_replay_journal(state), else: state

      state = activate_initial_plugins(state)

      # Record this class's kind so the State subsystem can gate unbounded-only
      # behaviours (retention/watchdog/dynamic-evict/time-travel) for a durable
      # bounded operative — plus the tier and this pid (the fsync-gated
      # outbox flush dispatches `:on_output` through it).
      _ = ClassRegistry.put(operative_class(state), state.builder.kind)
      _ = ClassRegistry.put_effectively_once(operative_class(state), effectively_once)
      _ = ClassRegistry.put_overwatch(operative_class(state), owner)
      _ = maybe_dispatch_restore_init_state(state, operative)

      # A bind is also a rehydrate: whatever the durable inbox still owes is
      # re-forwarded from the recorded consumed offset, so input that arrived
      # while the instance was suspended, crashed, or migrating is delivered
      # exactly here — one path for every way an instance comes back.
      state = deliver_inbox_tail(cancel_wake_timer(%{state | suspended: false}))
      {:ok, state}
    else
      {:error, reason, %__MODULE__{} = parked} -> {:error, reason, parked}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp bind_to_executor(%__MODULE__{} = state, operative_id, executor_id) do
    %__MODULE__{
      state
      | executor_id: executor_id,
        channel: attach_channel(operative_id, executor_id)
    }
  end

  # Returns `{:ok, executor_id, add_operative_raw_data, state}` on success or `{:error, reason,
  # state}` on exhaustion. On every attempt the executor is re-resolved
  # so a fresh executor that came online during a backoff window is
  # picked up.
  defp start_operative_sdk_with_retry(%__MODULE__{} = state, operative) do
    do_start_with_retry(state, operative, retry_backoff_ms(), 1)
  end

  defp do_start_with_retry(state, _operative, [], _attempt) do
    {:error, :exhausted, state}
  end

  defp do_start_with_retry(%__MODULE__{} = state, operative, [delay | rest], attempt) do
    executor_id =
      resolve_executor(state.namespace, operative.factory_id, operative.id, state.executor_id)

    state = %__MODULE__{state | executor_id: executor_id}

    case attempt_ping_and_start(operative, executor_id) do
      {:ok, raw_data} ->
        {:ok, executor_id, raw_data, state}

      {:error, reason} when rest == [] ->
        {:error, reason, state}

      {:error, reason} ->
        Logger.warning(
          "[Overwatch] operative #{operative.id} startup attempt #{attempt} failed: " <>
            "#{inspect(reason)} — retrying within #{delay} ms"
        )

        wait_with_jitter(delay)
        do_start_with_retry(state, operative, rest, attempt + 1)
    end
  end

  # On ping failure, reset the underlying `ControlChannel`'s `stream_ref`
  # so the SDK's next data stream can claim it via the existing rebind
  # path (`executor-controller-connection-healing`). `:start_operative` is
  # only issued AFTER the ping confirms the round-trip works.
  defp attempt_ping_and_start(operative, executor_id) do
    case ping_executor(executor_id) do
      :ok ->
        case start_operative_sdk(executor_id, operative) do
          {:ok, _} = ok -> ok
          {:error, reason} -> {:error, {:start_operative_failed, reason}}
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

  defp resolve_executor(namespace, factory_id, operative_id, fallback) do
    case OperativeManager.assign_executor(namespace, factory_id, operative_id) do
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
  # the operative's factory is available.
  defp park(%__MODULE__{} = state, reason) do
    Map.put(state, :init_error, reason)
  end

  # The minimal SDK operative for a running-operative migrate: the factory + the
  # current (snapshot-derived) init_values, no plugins (the controller-side
  # plugin table is untouched on a running migrate).
  defp migrate_operative(%__MODULE__{} = state) do
    init_values =
      if state.snapshot != nil,
        do: Payloader.json_to_map(state.snapshot),
        else: state.init_values

    %Flezha.Operative.Builder{
      id: state.id,
      factory_id: state.factory_id,
      init_values: init_values,
      opts: []
    }
  end

  defp executor_wait_ms,
    do: Application.get_env(:flezha, :overwatch_executor_wait_ms, 30_000)

  # Poll `OperativeManager.assign_executor/3` until it returns a live
  # executor id whose CU is registered in Flezha.Service.Identificator,
  # or the deadline elapses. Returns `{:ok, executor_id}` or
  # `{:error, :timeout}`.
  defp await_active_executor(namespace, factory_id, operative_id, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_active_executor(namespace, factory_id, operative_id, deadline)
  end

  defp do_await_active_executor(namespace, factory_id, operative_id, deadline) do
    case OperativeManager.assign_executor(namespace, factory_id, operative_id) do
      executor_id when is_binary(executor_id) ->
        if executor_ready?(executor_id) do
          {:ok, executor_id}
        else
          retry_or_timeout(namespace, factory_id, operative_id, deadline)
        end

      _ ->
        retry_or_timeout(namespace, factory_id, operative_id, deadline)
    end
  end

  defp retry_or_timeout(namespace, factory_id, operative_id, deadline) do
    now = System.monotonic_time(:millisecond)

    cond do
      now >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(50)
        do_await_active_executor(namespace, factory_id, operative_id, deadline)
    end
  end

  # The executor's control unit GenServer registers itself in
  # `Flezha.Service.Identificator` as the LAST step of its supervision
  # tree (`ExecutorSupervisor.init/1` → `ControlUnit.start_link/1`).
  # Registration alone is NOT sufficient: a SIGKILL'd executor can leave
  # the cu registered as a zombie (the `{:flz_closed}` disconnect never
  # arrived), so we additionally require a fresh end-to-end ping when
  # `:overwatch_ping_enabled` is on. Skipped in the unit suites that drive
  # synthetic executors with no real SDK behind the cu.
  defp executor_ready?(executor_id) do
    case Registry.lookup(Identificator, "#{executor_id}_cu") do
      [{_pid, _}] ->
        if Application.get_env(:flezha, :overwatch_ping_enabled, true),
          do: ping_executor(executor_id) == :ok,
          else: true

      _ ->
        false
    end
  end

  # Walk the on-disk Segment state for this operative (using `operative.id` as
  # the operative_class) and dispatch `RestoreInitState` to the SDK for each
  # Segment that has a non-empty payload. Skipped when the operative
  # arrived via HotSwap — handover already delivers the state in-band.
  # Skipped when persistence is disabled for this operative class.
  defp maybe_dispatch_restore_init_state(%__MODULE__{arrived_via_hotswap?: true}, _operative),
    do: :ok

  # Tier operatives restored synchronously in `restore_and_replay_journal/1`.
  defp maybe_dispatch_restore_init_state(%__MODULE__{effectively_once: true}, _operative),
    do: :ok

  defp maybe_dispatch_restore_init_state(%__MODULE__{} = state, operative) do
    do_dispatch_restore(state, operative.opts, state.executor_id)
    :ok
  end

  defp do_dispatch_restore(%__MODULE__{} = state, operative_opts, executor_id) do
    # The SDK keys per-Segment state by the operative INSTANCE id (== `state.id`),
    # so the WAL lives under `state/{operative_id}/...`. Enumerate by the same key.
    operative_class = Keyword.get(operative_opts, :operative_class, state.id)
    overwatch = self()

    # Off-process: the replay does blocking disk reads, never touches state.
    spawn(fn ->
      replay_persisted_segments(state.builder.kind, operative_class, executor_id, overwatch)
    end)

    :ok
  end

  # Bind-time restore is capability-gated on `persistent_state?` (kind+class):
  # `:unbounded` always; a durable `:bounded` class (this IS its crash-resume)
  # unless it opted out. A stateless kind has no WAL to replay.
  defp replay_persisted_segments(kind, operative_class, executor_id, overwatch) do
    # Transactional outbox re-drive (bounded-transactional-outbox): an operation
    # journaled `op_committed` (or `compensated`) whose outbox never reached the
    # plugins owes that output. Re-drive it — with the original keys — before the
    # resumed run advances, so a crash between commit and flush loses nothing.
    if kind == :bounded do
      _ = Flezha.State.OpJournal.flush_pending(operative_class, overwatch)
    end

    Enum.each(restorable_segments(kind, operative_class), fn segment_id ->
      _ = dispatch_restore_init_state(executor_id, segment_id)
    end)
  end

  # Persisted, restorable Segments of this class — empty when the kind/class
  # combination has no persistent state.
  defp restorable_segments(kind, operative_class) do
    if PersistenceResolver.persistent_state?(kind, operative_class) do
      operative_class
      |> enumerate_persisted_segments()
      |> Enum.filter(&Restore.has_persisted_state?/1)
    else
      []
    end
  end

  # Effectively-once bind path: restore each persisted Segment through the
  # awaited control round-trip (the SDK has applied state + consumed floor
  # when the reply lands), then re-forward the journaled input tail above
  # the min durable consumed-offset. Runs in the Overwatch process before
  # plugin activation; Segments ahead of the min watermark skip re-forwarded
  # offsets they already consumed (SDK `consumed_floor`).
  defp restore_and_replay_journal(%__MODULE__{} = state) do
    state.builder.kind
    |> restorable_segments(operative_class(state))
    |> Enum.each(fn segment_id ->
      # Starting the writer replays its WAL scan: arms the outbox re-drive
      # and repopulates the Segment's durable consumed-offset watermark.
      _ = Flezha.State.Supervisor.find_or_start(segment_id)
      _ = await_restore_init_state(state.executor_id, segment_id)
    end)

    replay_journaled_inputs(state)
    state
  end

  defp await_restore_init_state(executor_id, segment_id) do
    with {:ok, command} <- restore_command(segment_id) do
      GenServer.call(
        Identificator.control_unit(executor_id),
        {:send_control, command},
        restore_await_timeout_ms()
      )
    end
  catch
    :exit, reason ->
      Logger.warning(
        "[Overwatch] awaited restore failed segment=#{inspect(segment_id)}: #{inspect(reason)}"
      )

      {:error, reason}
  end

  defp restore_await_timeout_ms(),
    do: Application.get_env(:flezha, :restore_await_timeout_ms, 15_000)

  # Re-forward the retained journaled inputs above the min durable
  # consumed-offset, stamping each record's original `flz.offset` (never
  # re-journaling — the log already holds them, post-`:on_input`).
  @doc false
  def replay_journaled_inputs(%__MODULE__{} = state) do
    operative_class = operative_class(state)

    if File.exists?(Flezha.State.Registry.input_log_path(operative_class)) do
      floor = InputLog.watermark(operative_class) || -1
      last = InputLog.last_offset(operative_class)

      if last > floor do
        operative_class
        |> InputLog.read_range(floor + 1, last)
        |> Enum.map(fn {offset, message, metadata} ->
          {message, Map.put(metadata, "flz.offset", Integer.to_string(offset))}
        end)
        |> then(&reforward(state, &1))
      end
    end

    :ok
  end

  # Re-forward already-durable payloads, stamped with the offset they were
  # journaled at. The delivery half of both durability tiers' resume — the
  # effectively-once input log above, and the durable bounded inbox below — and
  # deliberately NOT an ingest: these payloads are already in their log and
  # already ran `:on_input`, so re-journaling them would duplicate them.
  defp reforward(%__MODULE__{} = state, payloads) do
    Enum.each(payloads, &GenServer.cast(state.channel, {:io_in, state.id, &1}))
  end

  @doc """
  External trigger for the bind-driven per-Segment replay. Called from
  `Flezha.State.Recovery.resync_for_overwatch/2` when a non-bind caller
  (the convergence Loop's rebind heal) needs to re-issue the
  `RestoreInitState` handshake for an already-bound Overwatch.

  Implementation routes the work back to the Overwatch process via a
  cast so the caller never blocks on enumeration / dispatch — the
  Overwatch is the single owner of "enumerate my segments, dispatch
  RestoreInitState for each".
  """
  @spec kick_restore_for_bound_executor(pid(), binary()) :: :ok
  def kick_restore_for_bound_executor(overwatch_pid, executor_id)
      when is_pid(overwatch_pid) and is_binary(executor_id) do
    GenServer.cast(overwatch_pid, {:kick_restore, executor_id})
  end

  defp enumerate_persisted_segments(operative_class) do
    Flezha.State.Registry.all_segments()
    |> Enum.filter(fn {rc, _, _} -> rc == operative_class end)
  end

  @doc """
  Number of persisted Segments the bind-time restore path will ship a
  `RestoreInitState` for on this (re)bind — the value threaded onto
  `AddOperative.resume_segment_count` so a durable bounded driver holds its
  first Operation until that many restores land (crash-resume,
  `bounded-operative-durability`).

  Non-zero only for a durable `:bounded` class with WAL state on disk; `0` for a
  fresh spawn, an unbounded operative, or an opted-out bounded one — those need
  no restore-before-run gate. Matches the frames `replay_persisted_segments/3`
  ships (same enumeration + `has_persisted_state?` filter).
  """
  @spec resume_segment_count(atom(), binary()) :: non_neg_integer()
  def resume_segment_count(:bounded, operative_class),
    do: length(restorable_segments(:bounded, operative_class))

  def resume_segment_count(_kind, _operative_class), do: 0

  # Stamp `resume_segment_count/2` onto the operative's opts so `control_unit`
  # can set the `AddOperative` field.
  defp with_resume_segment_count(operative, kind, operative_class) do
    count = resume_segment_count(kind, operative_class)
    %{operative | opts: Keyword.put(operative.opts, :resume_segment_count, count)}
  end

  # The operative-class identity used for per-Segment state (defaults to the
  # instance id, per the SDK convention that segment frames carry the operative_id).
  defp operative_class(%__MODULE__{} = state),
    do: Keyword.get(state.builder.opts, :operative_class, state.id)

  # Durable ingest point — the one place an inbound payload is made durable
  # BEFORE it is forwarded, for both durability tiers:
  #
  #   * effectively-once (`durable-input-log`): appended to the operative's
  #     input log and stamped `flz.offset`, which the SDK threads into the
  #     effect record;
  #   * durable bounded (`bounded-durable-suspension`): appended to the
  #     instance's `inbox.wal` and stamped `flz.inbox_offset`, which the SDK
  #     threads into the iteration commit as the consumed offset. A duplicate
  #     dedup id in the undelivered window is absorbed (`nil` — nothing is
  #     forwarded), and an append that fails is NOT forwarded either: the
  #     contract is append-before-forward, so a lost frame the sender can retry
  #     beats an undurable one it believes was accepted.
  #
  # Every other operative passes through untouched (cost-free default).
  @doc false
  def journal_input(_state, nil), do: nil

  def journal_input(%__MODULE__{effectively_once: true} = state, {message, metadata}) do
    {:ok, offset} = InputLog.append(operative_class(state), message, metadata)
    {message, Map.put(metadata, "flz.offset", Integer.to_string(offset))}
  end

  def journal_input(%__MODULE__{durable_bounded: true} = state, {message, metadata})
      when is_binary(message) do
    case OpInbox.append(state.id, message, metadata) do
      {:ok, offset} ->
        {message, Map.put(metadata, "flz.inbox_offset", Integer.to_string(offset))}

      {:duplicate, offset} ->
        :telemetry.execute(
          [:flezha, :state, :inbox_duplicate],
          %{count: 1},
          %{operative_id: state.id, offset: offset}
        )

        nil

      {:error, reason} ->
        Logger.error(
          "[Overwatch] durable-inbox append failed for #{state.id}: #{inspect(reason)} — " <>
            "input NOT forwarded (append-before-forward)"
        )

        nil
    end
  end

  def journal_input(_state, payload), do: payload

  # ---------------------------------------------------------------
  # Durable suspension (`bounded-durable-suspension`)
  # ---------------------------------------------------------------

  @doc """
  Suspend a bounded instance parked at the committed boundary of iteration
  `iter`: evict it from its executor (`RemOperative`), release the registry's
  executor binding, and arm the journaled wake deadline. The registry entry —
  and this process — stay, so a new input still has somewhere to land; it is the
  **executor** that holds zero resources for a suspended operative.

  Called by `ControlChannel` after the `parked` journal entry is durable.
  """
  @spec suspend(pid(), non_neg_integer(), non_neg_integer() | nil) :: :ok
  def suspend(overwatch_pid, iter, wake_deadline_ms) when is_pid(overwatch_pid),
    do: GenServer.cast(overwatch_pid, {:suspend, iter, wake_deadline_ms})

  # Bind state for a suspended instance: no executor, no stream channel, the
  # wake timer armed from the journaled deadline.
  defp enter_suspended(%__MODULE__{} = state, journal) do
    deadline = journal && journal.wake_deadline_ms

    :telemetry.execute(
      [:flezha, :bounded, :suspended],
      %{count: 1},
      %{
        namespace: state.namespace,
        operative_id: state.id,
        iter: (journal && journal.iter) || 0
      }
    )

    %__MODULE__{
      cancel_wake_timer(state)
      | suspended: true,
        executor_id: nil,
        channel: nil,
        wake_timer: arm_wake_timer(deadline)
    }
  end

  # The durable timer: a `parked` record's wake deadline, re-armed from the
  # journal on every boot, so it survives a controller redeploy.
  defp arm_wake_timer(nil), do: nil

  defp arm_wake_timer(deadline_ms) do
    Process.send_after(
      self(),
      :wake_deadline,
      max(deadline_ms - System.system_time(:millisecond), 0)
    )
  end

  defp cancel_wake_timer(%__MODULE__{wake_timer: nil} = state), do: state

  defp cancel_wake_timer(%__MODULE__{wake_timer: ref} = state) do
    _ = Process.cancel_timer(ref)
    %__MODULE__{state | wake_timer: nil}
  end

  # The invariant that keeps a turn from being stranded: an instance with
  # undelivered input is never left suspended. Applied wherever suspension is
  # entered — at the park, and at boot from durable state.
  defp rehydrate_if_pending(%__MODULE__{} = state) do
    if OpInbox.undelivered(state.id) == [], do: state, else: rehydrate(state)
  end

  # Rehydrate a suspended instance through the normal bind path (the same
  # `complete_init` an un-park drives), which restores journal + snapshots and
  # re-delivers the undelivered inbox tail. Idempotent: an instance already
  # running, or with a rehydrate already in flight, no-ops — so two wake sources
  # racing (an inbox append and the wake deadline) rehydrate exactly once.
  defp rehydrate(%__MODULE__{suspended: false} = state), do: state
  defp rehydrate(%__MODULE__{migrate_task: task} = state) when task != nil, do: state

  defp rehydrate(%__MODULE__{} = state) do
    case OperativeManager.assign_executor(state.namespace, state.factory_id, state.id) do
      nil ->
        # No executor to rehydrate onto: stay suspended. The next input (or the
        # convergence tick that brings an executor up) tries again.
        Logger.warning(
          "[Overwatch] no active executor to rehydrate suspended operative #{state.id}"
        )

        state

      executor_id ->
        :telemetry.execute(
          [:flezha, :bounded, :rehydrating],
          %{count: 1},
          %{namespace: state.namespace, operative_id: state.id, executor_id: executor_id}
        )

        {ref, pid} = spawn_migrate_task({:unpark, state, executor_id})
        %__MODULE__{state | migrate_task: %{ref: ref, pid: pid, mode: :unpark}}
    end
  end

  # Re-forward every inbox entry above the durable consumed offset, stamped with
  # its original offset (never re-journaling — the inbox already holds them).
  # Runs on every bind of a durable bounded instance, which is what makes crash
  # resume, migration, and rehydrate-from-suspension one path.
  defp deliver_inbox_tail(%__MODULE__{durable_bounded: false} = state), do: state

  defp deliver_inbox_tail(%__MODULE__{} = state) do
    state.id
    |> OpInbox.undelivered()
    |> Enum.map(fn entry ->
      {entry.message,
       Map.put(entry.metadata, "flz.inbox_offset", Integer.to_string(entry.offset))}
    end)
    |> then(&reforward(state, &1))

    state
  end

  # A durable bounded class: the kind that owns an op-journal and an inbox. An
  # opted-out class keeps the memory-only inbox and re-runs from the top.
  defp durable_bounded?(%__MODULE__{} = state, operative) do
    state.builder.kind == :bounded and
      PersistenceResolver.persistent_state?(
        :bounded,
        Keyword.get(operative.opts, :operative_class, operative.id)
      )
  end

  # Suspended per durable state alone: the journal's last entry is a `parked`
  # record AND no input has arrived since. Undelivered input outranks the park —
  # the instance has work waiting, so it binds instead.
  defp suspended_on_disk?(instance_id) do
    case OpJournal.restore_state(instance_id) do
      %{parked: true} -> OpInbox.undelivered(instance_id) == []
      _ -> false
    end
  end

  # Ingest one inbound message: type it, run the `:on_input` plugin chain, count
  # it, and make it durable. The single pre-delivery path — every `:io_in` entry
  # point (call, cast, suspended) runs exactly this, and the payload it answers
  # is the one to forward (`nil` ⇒ filtered by a plugin, absorbed as a duplicate,
  # or refused by the durable log, so there is nothing to send).
  defp ingest(%__MODULE__{} = state, message, metadata) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = run_io(state, :on_input, message, metadata)
    record_io(state.liveness_ref, payload)
    journal_input(state, payload)
  end

  # Suspended: same ingest, but the forward is deferred to the rehydrating bind,
  # which delivers the whole undelivered inbox tail.
  defp ingest_suspended(%__MODULE__{} = state, message, metadata) do
    _ = ingest(state, message, metadata)
    rehydrate(state)
  end

  # The SDK's AddOperative response raw_data is an `AddOperativeResponse`
  # metadata map when the class declares SDK-side capabilities, empty
  # otherwise (including every pre-tier or Java executor).
  @doc false
  def decode_effectively_once(raw_data) when is_binary(raw_data) and raw_data != "" do
    case ExecutorCommands.AddOperativeResponse.decode(raw_data) do
      {:ok, %ExecutorCommands.AddOperativeResponse{metadata: metadata}} ->
        Map.get(metadata, "effectively_once") == "true"

      {:error, _} ->
        false
    end
  end

  def decode_effectively_once(_raw_data), do: false

  # Phase 1 of the two-phase plugin lifecycle: register each plugin's
  # GenServer + struct (`init_provider/1` runs), but do NOT activate
  # upstream resources. Plugins enter the `:prepared` phase. Idempotent on
  # repeat — Table.bulk_subscribe/2 rejects duplicate plugin_ids, so on a
  # retry-driven re-entry we skip without re-init.
  defp prepare_initial_plugins(%__MODULE__{} = state, plugins) do
    # `operative.opts[:plugins]` accepts two shapes: bare `%Plugin{}` (the
    # cold-start `flz operative start --plugins` path emits this) and
    # `{plugin_id, %Plugin{}}` tuples (the cspec-persistence path written
    # by `Flezha.Service.PluginManager` through `ProcessRegistry.update/3`
    # after a successful `flz plugin enable`). Both normalise into the
    # tuple form `Table.bulk_subscribe/2` expects.
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
  # startup, immediately after `start_operative_sdk/2` returns `{:ok, _}`.
  defp activate_initial_plugins(%__MODULE__{} = state) do
    %__MODULE__{state | extensions: Table.bulk_activate(state.extensions, activation_env(state))}
  end

  # Operative-level facts providers need at activation (the tier drives the
  # source adapters' blocking-ingest mode).
  defp activation_env(%__MODULE__{} = state), do: %{effectively_once: state.effectively_once}

  # Per the `operative-init-state` capability spec:
  #   - cold start with empty init_values → chain runs
  #   - cold start with non-empty init_values → chain MUST NOT run; inline values used as-is
  #
  # `init/1` is also the path for in-place restart plugin restoration: the
  # cspec's `operative.opts[:plugins]` list is read above and re-subscribed
  # by `subscribe_initial_plugins/2`. That path is independent of the
  # per-Segment state-restore path (`dispatch_restore_init_state/2`),
  # which rehydrates SDK-side WAL/snapshot state through the data plane.
  #
  # HotSwap migration is handled separately in `alter_handover_state/2`
  # and never reaches this function.
  defp resolve_init_state(%__MODULE__{} = state, %{init_values: init_values} = operative)
       when init_values == %{} do
    ctx = %Ctx.Init{
      operative_id: operative.id,
      namespace: state.namespace,
      factory_id: operative.factory_id,
      accumulated: operative.init_values
    }

    case Extension.run(state.extensions, :on_operative_init, ctx) do
      {:ok, %Ctx.Init{accumulated: resolved}} ->
        {:ok, %{state | init_values: resolved}, %{operative | init_values: resolved}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_init_state(%__MODULE__{} = state, operative) do
    # Non-empty inline init_values bypass the chain.
    {:ok, %{state | init_values: operative.init_values}, operative}
  end

  @impl true
  def terminate(reason, state) do
    # Evict the in-memory kind entry; the durable sidecar stays for the
    # process-less retention sweep over any retained finished-job state.
    _ = ClassRegistry.delete(operative_class(state))

    unless is_nil(state.executor_id) do
      ControlUnit.stop_operative_async(state.executor_id, state.id)

      fire_on_terminate(state, reason)
      state = %{state | extensions: Table.teardown_all(state.extensions)}
      _ = state
    end

    Logger.info(
      "Terminating operative #{state.id} in namespace #{state.namespace}: #{inspect(reason)}"
    )

    :ok
  end

  defp fire_on_terminate(%__MODULE__{} = state, reason) do
    ctx = %Ctx.Terminate{
      operative_id: state.id,
      namespace: state.namespace,
      executor_id: state.executor_id,
      reason: reason
    }

    Extension.run(state.extensions, :on_terminate, ctx)
    :ok
  end

  @impl true
  def handle_call(:registry_executor_set, _from, %__MODULE__{init_error: nil} = state) do
    HubManager.operative_set_executor(state.namespace, state.id, state.executor_id)

    {:reply, {:ok, state.executor_id}, state}
  end

  def handle_call(:registry_executor_set, _from, %__MODULE__{init_error: reason} = state) do
    {:reply, {:error, reason}, state}
  end

  @impl true
  def handle_call(:stop_operative, from, state) do
    # Terminate any in-flight `:executor_migrate` task BEFORE the synchronous
    # stop sequence so `flz operative stop r1` always returns within its normal
    # handle_call window even mid-retry.
    state = terminate_migrate_task(state)

    case state.executor_id do
      nil ->
        :ok

      _ ->
        with_op_span(:stop_operative, state.id, fn ->
          GenServer.call(
            Identificator.control_unit(state.executor_id),
            {:stop_operative, state.id}
          )
        end)
    end

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
        {:reply, :ok, %{state | extensions: Table.bulk_activate(table, activation_env(state))}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:plugin_unsubscribe, plugin_id}, _from, %__MODULE__{} = state) do
    table = Table.unsubscribe(state.extensions, plugin_id)
    {:reply, :ok, %{state | extensions: table}}
  end

  # Convergence heal for a declared plugin whose provider self-reports
  # unhealthy: tear down (kills the owned pid) and re-subscribe from the
  # declared manifest so `activate/1` reopens upstream resources. On a
  # failed re-subscribe the plugin stays unsubscribed — the next tick diffs
  # it as missing and retries via `start_plugin` under backoff.
  @impl true
  def handle_call({:plugin_restart, plugin_id, %Plugin{} = plugin}, _from, %__MODULE__{} = state) do
    table = Table.unsubscribe(state.extensions, plugin_id)

    case Table.subscribe(table, plugin_id, plugin) do
      {:ok, table} ->
        {:reply, :ok, %{state | extensions: Table.bulk_activate(table, activation_env(state))}}

      {:error, reason} ->
        {:reply, {:error, reason}, %{state | extensions: table}}
    end
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
  def handle_call(:get_segment_state, from, %__MODULE__{} = state) do
    if PersistenceResolver.persistent_state?(state.builder.kind, operative_class(state)) do
      # Query the executor off the GenServer loop so a slow reply can't back up
      # the mailbox; `from` is answered when the round-trip completes.
      operative_id = state.id
      executor_id = state.executor_id

      spawn(fn ->
        result =
          with_op_span(:segment_state, operative_id, fn ->
            send_control(
              executor_id,
              {:segment_state, %ExecutorCommands.SegmentState{operative_id: operative_id}}
            )
          end)

        GenServer.reply(from, result)
      end)

      {:noreply, state}
    else
      # The kind has no per-segment state; fail cleanly without the round-trip.
      {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, state}
    end
  end

  # Async `:executor_migrate` for the PARKED un-park path only — that's
  # where the bounded retry loop (up to 19 s aggregate) actually lives,
  # and the spec's stop-during-migrate scenario quotes "~10 s into the
  # backoff schedule". The running-migrate path is a single ping +
  # start round-trip; keeping it synchronous preserves the existing
  # HotSwap handover invariants (`executor_disconnect_migrate_op` and
  # peers) that depend on state being applied before the call returns.
  @impl true
  def handle_call({:executor_migrate, _}, _from, %__MODULE__{migrate_task: t} = state)
      when t != nil do
    {:reply, {:error, :busy}, state}
  end

  # Parked (a failed init) and suspended (a durable loop park) both come back
  # through the same un-park task: `complete_init` re-binds, restores journal +
  # segments, and delivers the undelivered inbox tail. A running operative
  # instead migrates in place, below.
  def handle_call(
        {:executor_migrate, new_executor_id},
        _from,
        %__MODULE__{init_error: reason, suspended: suspended} = state
      )
      when reason != nil or suspended do
    {ref, pid} = spawn_migrate_task({:unpark, state, new_executor_id})
    {:reply, :ok, %{state | migrate_task: %{ref: ref, pid: pid, mode: :unpark}}}
  end

  def handle_call({:executor_migrate, new_executor_id}, _from, %__MODULE__{} = state) do
    state = bind_to_executor(state, state.id, new_executor_id)

    case attempt_ping_and_start(migrate_operative(state), new_executor_id) do
      {:ok, _} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, :pong, state}
  end

  # Source adapters ask once (memoized) whether their ingest must be a
  # blocking call: the effectively-once tier parks ingestion at zero source
  # credit, which only back-pressures the source if it awaits the reply.
  @impl true
  def handle_call(:effectively_once?, _from, state) do
    {:reply, state.effectively_once, state}
  end

  @impl true
  def handle_call({:io_in, message, metadata}, _from, %__MODULE__{suspended: true} = state) do
    {:reply, :ok, ingest_suspended(state, message, metadata)}
  end

  @impl true
  def handle_call({:io_in, _message, _metadata}, _from, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    {:reply, {:error, :startup_failed}, state}
  end

  @impl true
  def handle_call({:io_in, message, metadata}, _from, state) do
    case ingest(state, message, metadata) do
      nil ->
        :ok

      payload ->
        # Count an unbound-stream drop instead of discarding the channel's
        # `{:error, :no_stream}` reply, so the reconciler-detected stall is
        # observable (the probe rebinds the stream).
        case GenServer.call(state.channel, {:io_in, state.id, payload}) do
          {:error, :no_stream} ->
            :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{operative_id: state.id})

          _ ->
            :ok
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

  # Suspended: the input is durable in the inbox, so it is not lost by not being
  # forwarded now — the rehydrating bind delivers the whole undelivered tail.
  @impl true
  def handle_cast({:io_in, message, metadata}, %__MODULE__{suspended: true} = state) do
    {:noreply, ingest_suspended(state, message, metadata)}
  end

  @impl true
  def handle_cast({:io_in, _message, _metadata}, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    # Parked: silently drop ingress casts. Prepared plugins do not emit
    # `:io_in`, so the only sources are upstream operatives in a chain.
    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_in, message, metadata}, state) do
    case ingest(state, message, metadata) do
      nil -> :ok
      payload -> GenServer.cast(state.channel, {:io_in, state.id, payload})
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, message, metadata}, state) do
    run_io(state, :on_output, message, metadata)
    {:noreply, state}
  end

  # The SDK parked at a committed iteration boundary and the `parked` journal
  # entry is durable: evict the instance from its executor and keep it
  # `suspended` here (`bounded-durable-suspension`). Not a terminal outcome — no
  # completion record, no state deletion.
  @impl true
  def handle_cast({:suspend, iter, wake_deadline_ms}, %__MODULE__{} = state) do
    if state.executor_id do
      ControlUnit.stop_operative_async(state.executor_id, state.id)
    end

    _ = HubManager.operative_set_executor(state.namespace, state.id, nil)

    Logger.info(
      "[Overwatch] operative #{state.id} suspended at iteration #{iter}" <>
        if(wake_deadline_ms, do: " (wakes at #{wake_deadline_ms})", else: "")
    )

    suspended =
      enter_suspended(state, %{iter: iter, wake_deadline_ms: wake_deadline_ms, parked: true})

    # An input that landed while the loop was parking — durable, but never
    # drained by the exiting driver — is undelivered work: rehydrate straight
    # away rather than leaving the instance suspended until the NEXT message
    # arrives. The park/rehydrate cycle costs a bind; stranding the turn would
    # cost correctness. `RemOperative` is already enqueued on the control
    # stream, so the rehydrating `AddOperative` cannot overtake it.
    {:noreply, rehydrate_if_pending(suspended)}
  end

  # Replaces `state.snapshot` with a binary supplied out-of-band by the SDK
  # on graceful shutdown. The snapshot is consumed by HotSwap's
  # `prepare_handover_state` so a subsequent migration ships the
  # executor's final state. Cold-boot restart is covered by the
  # per-Segment WAL files instead.
  @impl true
  def handle_cast({:apply_external_snapshot, binary}, %__MODULE__{} = state)
      when is_binary(binary) do
    {:noreply, %__MODULE__{state | snapshot: binary}}
  end

  # A operative that arrived via HotSwap handover already received its state
  # in-band (`alter_handover_state` ships `MigrationDeserialization`); a WAL
  # replay on top would apply every change a second time. Skip — same guard
  # as the bind-time `maybe_dispatch_restore_init_state`.
  def handle_cast({:kick_restore, _executor_id}, %__MODULE__{arrived_via_hotswap?: true} = state) do
    {:noreply, state}
  end

  # External callers (the convergence Loop's rebind heal) trigger
  # a fresh per-Segment replay against the Overwatch's bound
  # executor without knowing the Overwatch's internals. The cast is
  # the indirection point used by
  # `Flezha.State.Recovery.resync_for_overwatch/2`.
  def handle_cast({:kick_restore, executor_id}, %__MODULE__{} = state) do
    do_dispatch_restore(state, state.builder.opts, executor_id)
    {:noreply, state}
  end

  # Liveness bookkeeping for one ingress message: always an io_in, then either
  # filtered (on_input returned nil) or forwarded to the channel.
  defp record_io(ref, payload) do
    LivenessCounters.bump_operative(ref, :io_in_received)
    LivenessCounters.bump_operative(ref, if(is_nil(payload), do: :filtered_nil, else: :forwarded))
  end

  defp run_io(%__MODULE__{} = state, extension, message, metadata) do
    ctx = %Ctx.IO{
      operative_id: state.id,
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

  # The journaled wake deadline elapsed (`bounded-durable-suspension`): append a
  # timer wake to the inbox and rehydrate. Routing the wake THROUGH the inbox is
  # what makes two racing wake sources — a message and the deadline — collapse
  # into one rehydration: the second is a no-op on an already-rehydrating
  # instance, and the loop simply reads whatever the inbox holds. The wake item
  # carries an empty payload, which the class's entry channel decodes as its
  # tick.
  @impl true
  def handle_info(:wake_deadline, %__MODULE__{} = state) do
    _ = OpInbox.append(state.id, "", %{"flz.wake" => "timer"})
    {:noreply, rehydrate(%__MODULE__{state | wake_timer: nil})}
  end

  # The in-flight `:executor_migrate` task posts its result via a plain
  # `send/2` before the task process exits, so this clause runs before the
  # `:DOWN` cleanup. The state update happens here; the `:DOWN` message is
  # then drained by the dedicated clause below.
  @impl true
  def handle_info(
        {:migrate_task_result, _task_pid, result},
        %__MODULE__{migrate_task: %{}} = state
      ) do
    {:noreply, apply_migrate_result(state, result)}
  end

  def handle_info({:migrate_task_result, _task_pid, _result}, state) do
    # Late delivery: the task was terminated by `:stop_operative`, or a
    # second migrate already replaced `state.migrate_task`. Drop.
    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %__MODULE__{migrate_task: %{ref: ref}} = state
      ) do
    # The task has exited normally (or crashed before posting a result).
    # Clear the migrate_task slot so the next migrate can spawn.
    {:noreply, %{state | migrate_task: nil}}
  end

  # Task.Supervisor.async_nolink also sends `{ref, return_value}` from
  # the task before it exits — we already consumed the result via the
  # explicit `{:migrate_task_result, ...}` send/2 from inside the task
  # body, so the auto-reply is redundant. Drop it; without this clause
  # it crashes the Overwatch with FunctionClauseError.
  @impl true
  def handle_info({ref, _value}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, state}
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
      operative_id: state.id,
      namespace: state.namespace,
      executor_id: state.executor_id
    }

    Extension.run(state.extensions, :on_message, ctx)

    {:noreply, state}
  end

  defp create_snapshot(operative_id, executor_id) do
    command = %ExecutorCommands.SegmentState{operative_id: operative_id}

    case send_control(executor_id, {:segment_state, command}) do
      {:ok, snapshot} -> snapshot
      {:error, _reason} -> nil
    end
  end

  @impl true
  def prepare_handover_state(state) do
    serialize_data =
      case serialize_operative_sdk(state.executor_id, state.id) do
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
    # `serialized_state` is a protobuf bytes field. When the SDK has no state
    # to hand over (no prior snapshot, or the source executor is already gone)
    # `pipe_serialized` is nil, and Protox.encode/1 raises on a non-binary —
    # which crashed send_control before the deserialization command was ever
    # sent, leaving the executor stuck "awaiting controller control-plane
    # traffic". An empty binary means "no prior state": the operative starts fresh.
    serialized_state = if is_binary(pipe_serialized), do: pipe_serialized, else: <<>>

    command = %ExecutorCommands.MigrationDeserialization{
      operative_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: serialized_state
    }

    result =
      with_op_span(:migration_deserialize, state.id, fn ->
        send_control(state.executor_id, {:migration_deserialization, command})
      end)

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize operative: #{inspect(reason)}")
      _else -> :ok
    end

    # Tear down existing, then rehydrate; activate so providers reconnect.
    table = Table.teardown_all(state.extensions)
    rehydrated = Table.rehydrate(table, Map.get(handover_state, :plugins, []))

    %__MODULE__{
      state
      | extensions: Table.bulk_activate(rehydrated, activation_env(state)),
        init_values: handover_state.init_values,
        arrived_via_hotswap?: true
    }
  end

  # Time a known-blocking operation and surface a `[Overwatch] op_slow` warning
  # when it runs long. The last logged `op_begin` with no matching `op_end`
  # pinpoints exactly which operation wedged the GenServer. `try/after` (never
  # `catch`) so the span is recorded even on exit/throw while the failure still
  # propagates unchanged (`send_control`'s own `:exit` catch must still fire).
  defp with_op_span(op, operative_id, fun) do
    started = System.monotonic_time(:millisecond)
    Logger.debug("[Overwatch] op_begin op=#{op} operative=#{operative_id}")

    try do
      fun.()
    after
      ms = System.monotonic_time(:millisecond) - started

      if ms >= op_warn_ms() do
        Logger.warning("[Overwatch] op_slow op=#{op} operative=#{operative_id} duration_ms=#{ms}")
      else
        Logger.debug("[Overwatch] op_end op=#{op} operative=#{operative_id} duration_ms=#{ms}")
      end
    end
  end

  defp op_warn_ms,
    do: Application.get_env(:flezha, :overwatch_op_warn_ms, 2_000)

  defp send_control(executor_id, command) do
    control_unit = Identificator.control_unit(executor_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {
          :error,
          "control unit not available for executor #{executor_id} with payload"
        }

      _ ->
        # A wedged control unit (waiting on a reply the dead transport will
        # never deliver) must surface as a clean error, never an exit that
        # crashes this operative. The controller-side idle watchdog reaps the
        # corpse; here we just heal/park instead of crash-looping.
        try do
          GenServer.call(
            Identificator.control_unit(executor_id),
            {:send_control, command}
          )
        catch
          :exit, reason -> {:error, {:control_unit_exit, reason}}
        end
    end
  end

  defp start_operative_sdk(executor_id, operative) do
    with_op_span(:start_operative, operative.id, fn ->
      GenServer.call(
        Identificator.control_unit(executor_id),
        {:start_operative, operative},
        # 8s wasn't enough when the SDK is on the Java side — Spring/JPA
        # warmup plus the AddOperative control-channel round-trip easily
        # exceeds that during a cold start. 30s gives Spring time to breathe
        # without masking genuine stalls.
        30_000
      )
    end)
  end

  defp attach_channel(operative_id, executor_id) do
    operative_int = String.to_charlist(operative_id) |> Enum.sum()
    count = ConfigManager.concurrent_channel_count()
    channel_index = rem(operative_int, count)

    StreamSup.stream_ids(executor_id, :primary) |> Enum.at(channel_index)
  end

  defp serialize_operative_sdk(executor_id, operative_id) do
    command = %ExecutorCommands.MigrationSerialization{operative_id: operative_id}

    result = send_control(executor_id, {:migration_serialization, command})

    case result do
      {:ok, serialized_data} ->
        serialized_data

      {:error, reason} ->
        if Application.get_env(:flezha, :env) !== :test do
          Logger.error("Failed to serialize operative: #{inspect(reason)}")
        end

        nil
    end
  end

  # ---------- Per-Segment persistence wiring ----------
  #
  # Helpers used by `Flezha.State.*` and tests to ship persistence
  # control commands to the SDK. Each takes the controller-side
  # `executor_id` and a structured `segment_id` triple
  # `{operative_class, operation_id, segment_key}`.

  @doc """
  Ship a `RestoreInitState` command to the SDK and return the controller's
  freshly assembled payload. The caller is expected to await the
  matching `RestoreInitStateResponse` over the control channel before
  unblocking input streaming for the Segment.
  """
  @spec dispatch_restore_init_state(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_restore_init_state(executor_id, segment_id) do
    case restore_command(segment_id) do
      {:ok, command} -> ControlChannel.send_control(executor_id, command)
    end
  end

  defp restore_command({rc, sh, pk}) do
    case Restore.assemble_payload({rc, sh, pk}) do
      {:ok, payload} ->
        segment_id_bytes = Wire.encode_segment_id(rc, sh, pk)
        markers = Flezha.State.Registry.compensation_markers({rc, sh, pk})

        {:ok,
         {:restore_init_state,
          %ExecutorCommands.RestoreInitState{
            segment_id: segment_id_bytes,
            snapshot_bytes: payload.snapshot_bytes,
            change_log_bytes: payload.change_log_bytes,
            has_compensation_intent: markers.intent,
            has_compensation_done: markers.done,
            op_journal: op_journal_state(rc)
          }}}
    end
  end

  # Bounded op-journal crash-resume: ship the instance's committed op state
  # alongside the compensation markers (same restore exchange, no second
  # round-trip). `nil` (field absent) when the instance has no journal —
  # unbounded operatives, opted-out classes, pre-upgrade instances.
  defp op_journal_state(instance_id) do
    case Flezha.State.OpJournal.restore_state(instance_id) do
      nil ->
        nil

      %{} = resume ->
        %ExecutorCommands.OpJournalState{
          committed_op_idxs: resume.committed_op_idxs,
          iter: resume.iter,
          handoff:
            Enum.map(resume.handoff, fn {cid, bytes} ->
              %ExecutorCommands.OpHandoffItem{channel_id: cid, payload: bytes}
            end),
          # Every forward op commit with its iteration: the driver folds these
          # back into the compensation planner's per-iteration `committed` input,
          # which is what makes a rollback span journaled iterations (and what
          # rebuilds the in-flight iteration's completed stages on resume).
          committed_ops:
            Enum.map(resume.committed_ops, fn op ->
              %ExecutorCommands.OpCommittedOp{
                op_idx: op.op_idx,
                iter: op.iter,
                segments_ran: op.segments_ran
              }
            end),
          consumed_offset: resume.consumed_offset,
          parked: resume.parked,
          wake_deadline_ms: resume.wake_deadline_ms || 0
        }
    end
  end

  @doc """
  Ship a `CompactSegment` command to the SDK. The SDK responds with a
  `segment_state_snapshot` data frame (handled by
  `Flezha.Executor.StreamChannel`) followed by a
  `CompactSegmentResponse` control reply.
  """
  @spec dispatch_compact_segment(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_compact_segment(executor_id, {rc, sh, pk} = segment_id) do
    segment_id_bytes = Wire.encode_segment_id(rc, sh, pk)

    command =
      {:compact_segment, %ExecutorCommands.CompactSegment{segment_id: segment_id_bytes}}

    case ControlChannel.send_control(executor_id, command) do
      {:ok, _ref} = ok ->
        # Bound the handshake: a snapshot that never arrives is timed out
        # by the Watchdog rather than left to wedge persistence.
        Flezha.State.Watchdog.compaction_started(segment_id)
        ok

      other ->
        other
    end
  end

  @doc """
  Ship a `SegmentMailboxStatus` notification to the SDK so the
  producing Operation's `OverflowPolicy` machinery can park or unpark
  emits to this Segment. `status` is `:full` or `:open`.
  """
  @spec notify_segment_backpressure({binary(), binary(), binary(), binary()}, :full | :open) ::
          :ok | {:error, term()}
  def notify_segment_backpressure({executor_id, rc, sh, pk}, status)
      when status in [:full, :open] do
    segment_id_bytes = Wire.encode_segment_id(rc, sh, pk)

    command =
      {:segment_mailbox_status,
       %ExecutorCommands.SegmentMailboxStatus{
         segment_id: segment_id_bytes,
         status: Atom.to_string(status)
       }}

    ControlChannel.send_control_async(executor_id, command)
  end

  # -----------------------------------------------------------------
  # Async migrate task plumbing. Kept together so the task-spawn /
  # terminate / result-application surface is reviewable as a single unit.
  # -----------------------------------------------------------------

  defp terminate_migrate_task(%__MODULE__{migrate_task: nil} = state), do: state

  defp terminate_migrate_task(%__MODULE__{migrate_task: %{ref: ref, pid: pid}} = state) do
    _ = Process.demonitor(ref, [:flush])
    _ = Task.Supervisor.terminate_child(Flezha.Operative.MessageHandlerTaskSup, pid)
    %{state | migrate_task: nil}
  end

  defp spawn_migrate_task({:unpark, state, new_executor_id}) do
    operative = state.builder
    parent = self()

    task =
      Task.Supervisor.async_nolink(Flezha.Operative.MessageHandlerTaskSup, fn ->
        # complete_init is the heavy bounded-retry path; running it in
        # a task ensures `:stop_operative` is never blocked by it.
        case complete_init(state, new_executor_id, operative, parent) do
          {:ok, recovered} ->
            send(parent, {:migrate_task_result, self(), {:unparked, recovered}})
            :ok

          {:error, err, parked} ->
            send(parent, {:migrate_task_result, self(), {:park, parked, err}})
            :ok
        end
      end)

    {task.ref, task.pid}
  end

  defp apply_migrate_result(state, {:unparked, recovered}) do
    Logger.info(
      "[Overwatch] operative #{state.id} un-parked onto executor #{recovered.executor_id}"
    )

    %{recovered | migrate_task: nil}
  end

  defp apply_migrate_result(_state, {:park, parked, err}) do
    %{park(parked, err) | migrate_task: nil}
  end
end

defmodule Flezha.Operative.Overwatch do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Service.OperativeManager
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.StreamSup
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Operative.ClassRegistry

  alias Flezha.State.{
    InputLog,
    ProcInbox,
    ProcJournal,
    PersistenceResolver,
    Restore,
    WalTransfer,
    Wire
  }

  alias Flezha.Transport.LivenessCounters

  # Defaults for the bounded-retry governor introduced by
  # `fix-overwatch-restart-storm`. Tuned to ~19 s aggregate so a transient
  # control-channel rebind (raspberrypi5 cold-start hits ~5–10 s) recovers
  # within one burst without spinning the supervisor.
  @default_retry_backoff_ms [200, 500, 1_000, 2_000, 5_000, 10_000]

  # Reply-frame metadata key carrying the `change_seq` a single selected Entity
  # was serialized at (stamped by both SDKs' `EntityState` handler).
  @change_seq_meta_key "flz.change_seq"

  require Logger

  use GenServer
  use ProcessHub.Strategy.Migration.HotSwap
  use ProcessHub.Strategy.Migration.MigrationConsent

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
    :runner_id,
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
    # The `:runner_migrate` retry runs in a `Task.Supervisor.async_nolink/3`
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
    # Suspended at a committed iteration boundary: zero live runner resources,
    # registry entry retained. A new inbox append — or the journaled wake
    # deadline — rehydrates through the normal bind path.
    suspended: false,
    # Timer ref for the journaled wake deadline while suspended.
    wake_timer: nil,
    # Late-input correction window (ms), declared by the SDK. `nil` = the class
    # never corrects. Gates lateness handling and the event-time-ordered replay.
    correction_window_ms: nil,
    # Entities with a correction in flight, and the oldest event time each was
    # asked to correct back to. Concurrent requests for one Entity coalesce
    # into the one replay that reaches furthest back.
    corrections: %{}
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init({namespace, stale_runner_id, operative}) do
    Process.flag(:trap_exit, true)

    # IMPORTANT: init/1 must return quickly so the `dist_sup` supervisor
    # tree (which calls us synchronously from `DistributedSupervisor.init/1`
    # during the DETS replay) can unblock. Any GenServer.call to
    # FleetManager / OperativeManager from inside init/1 deadlocks because
    # FleetManager is itself waiting on :default_hub, which is waiting
    # for dist_sup.init to complete.
    #
    # Defer all blocking work (runner wait, plugin subscribe, SDK
    # start) to handle_continue — by then init has returned, dist_sup
    # has unblocked, the hub has finished initialization, and
    # FleetManager can answer our calls.
    state = %__MODULE__{
      id: operative.id,
      namespace: namespace,
      factory_id: operative.factory_id,
      init_values: operative.init_values,
      runner_id: stale_runner_id,
      channel: nil,
      snapshot: nil,
      default_input_type: Keyword.get(operative.opts, :default_input_type, "2"),
      extensions: Table.new(__extensions_catalog__()),
      builder: operative,
      started_at: System.system_time(:millisecond),
      liveness_ref: LivenessCounters.register_operative(operative.id)
    }

    {:ok, state, {:continue, {:bind_runner, stale_runner_id, operative}}}
  end

  @impl true
  def handle_continue({:bind_runner, stale_runner_id, operative}, %__MODULE__{} = state) do
    state = %__MODULE__{state | durable_bounded: durable_bounded?(state, operative)}

    # Suspended instances are rediscovered from durable state alone (journal +
    # inbox), so a controller redeploy neither binds them to a runner nor
    # loses their suspension (`bounded-durable-suspension`). An instance with
    # undelivered input is not suspended any more — it binds and consumes it.
    if state.durable_bounded and suspended_on_disk?(operative.id) do
      {:noreply, enter_suspended(state, ProcJournal.restore_state(operative.id))}
    else
      do_bind_runner(state, stale_runner_id, operative)
    end
    |> seed_input(operative)
  end

  # A procedure-requested spawn's payload, ingested as the instance's first
  # input. It runs inside the bind continue, which is what makes it first: no
  # other message in the mailbox — including a concurrent requester's delivery —
  # can be ingested ahead of it. A durable instance whose inbox already holds
  # entries is a restart rather than a spawn; its payload is journaled, so
  # re-seeding it would duplicate.
  defp seed_input({:noreply, %__MODULE__{} = state} = result, operative) do
    payload = Keyword.get(operative.opts, :seed_input)

    if is_binary(payload) and is_nil(state.init_error) and fresh_inbox?(state) do
      {_verdict, state} = confirming_ingest(state, payload, %{})
      {:noreply, state}
    else
      result
    end
  end

  defp seed_input(result, _operative), do: result

  defp fresh_inbox?(%__MODULE__{durable_bounded: true, id: id}),
    do: ProcInbox.last_offset(id) == 0

  defp fresh_inbox?(%__MODULE__{}), do: true

  defp do_bind_runner(%__MODULE__{} = state, stale_runner_id, operative) do
    # Wait for an active runner in this namespace advertising
    # `operative.factory_id`. The DETS-baked runner_id is usually stale
    # after a restart (Identificator.unique_id/1 generates a fresh id
    # per connect), so we re-resolve via OperativeManager.await_runner/5
    # — the same bounded lookup HubManager.alter_child_data/2 performs.
    case with_proc_span(:await_runner, operative.id, fn ->
           OperativeManager.await_runner(
             state.namespace,
             operative.factory_id,
             operative.id,
             runner_wait_ms(),
             &runner_ready?/1
           )
         end) do
      {:ok, runner_id} ->
        case complete_init(state, runner_id, operative) do
          # `complete_init` re-resolves the runner on every start attempt, so the
          # binding to publish is the one it ended on, not the one awaited.
          {:ok, new_state} ->
            {:noreply, publish_rebind(new_state, stale_runner_id)}

          {:error, reason, parked} ->
            {:noreply, park(parked, reason)}
        end

      # `:unavailable` (FleetManager never answered) defers exactly like
      # `:timeout` — the operative has no runner either way — but must be
      # distinguishable in the log, since only one of the two blames the fleet.
      {:error, reason} ->
        Logger.warning(
          "[Overwatch] no active runner in namespace #{inspect(state.namespace)} for factory #{inspect(operative.factory_id)} within #{runner_wait_ms()} ms (#{reason}); deferring operative #{operative.id} startup"
        )

        {:stop, :runner_not_ready, state}
    end
  end

  defp publish_rebind(%__MODULE__{runner_id: runner_id} = state, runner_id), do: state

  # Binding to a runner other than the recorded one leaves the durable row naming
  # the old one and the live runner's StreamChannels with no route to this pid, so
  # every output frame is dropped while the operative still reads as healthy.
  defp publish_rebind(%__MODULE__{} = state, stale_runner_id) do
    Logger.info(
      "[Overwatch] operative #{state.id} rebound from stale runner " <>
        "#{inspect(stale_runner_id)} to live #{inspect(state.runner_id)}"
    )

    publish_binding(state)
  end

  # Registry tag + the live runner's `operative_mapping` route. Same pair
  # `Convergence.Heal.rebind_operative/3` publishes; must run in the Overwatch
  # process, because the route points at `self()`. Both halves read the bound
  # state, so neither can name a runner this process is not on.
  defp publish_binding(%__MODULE__{id: operative_id, runner_id: runner_id} = state) do
    case HubManager.operative_set_runner(state.namespace, operative_id, runner_id) do
      :ok ->
        :ok

      err ->
        Logger.warning(
          "[Overwatch] could not publish binding of #{operative_id} to #{inspect(runner_id)}: " <>
            "#{inspect(err)}"
        )
    end

    _ = HubManager.hook_insert_local_pid(runner_id, operative_id, self())

    state
  end

  # `owner` is the Overwatch process itself. It is passed explicitly because the
  # un-park path runs `complete_init` inside a task (so the heavy bounded-retry
  # never blocks `:stop_operative`), and `self()` there is the TASK — registering
  # it as the operative's Overwatch would point the journal's outbox flush at a
  # process that exits moments later, silently dropping the output.
  defp complete_init(%__MODULE__{} = state, runner_id, operative, owner \\ nil) do
    owner = owner || self()
    state = bind_to_runner(state, operative.id, runner_id)
    plugins = Keyword.get(operative.opts, :plugins, [])

    with {:ok, state} <- prepare_initial_plugins(state, plugins),
         {:ok, state, operative} <- resolve_init_state(state, operative),
         operative = with_resume_state(operative, state.builder.kind, operative_class(state)),
         {:ok, final_runner_id, add_operative_raw, state} <-
           start_operative_sdk_with_retry(state, operative) do
      # The class's SDK-side declarations ride the AddOperative response; resolve
      # them BEFORE plugin activation so source adapters pick the tier ingest mode
      # (blocking call) from the env.
      %{effectively_once: effectively_once, correction_window_ms: correction_window_ms} =
        declarations = decode_declarations(add_operative_raw)

      state =
        %{state | effectively_once: effectively_once, correction_window_ms: correction_window_ms}
        |> bind_to_runner(operative.id, final_runner_id)
        |> Map.put(:init_error, nil)

      # Tier: restore every persisted Entity (awaited) and re-forward the
      # journaled input tail BEFORE sources activate, so replayed offsets
      # reach the runner ahead of fresh ones (fold order at the SDK).
      state = if effectively_once, do: restore_and_replay_journal(state), else: state

      state = activate_initial_plugins(state)

      # Record this class's kind so the State subsystem can gate unbounded-only
      # behaviours (retention/watchdog/dynamic-evict/time-travel) for a durable
      # bounded operative — plus every SDK declaration and this pid (the
      # fsync-gated outbox flush dispatches `:on_output` through it).
      _ = ClassRegistry.put(operative_class(state), state.builder.kind)
      _ = ClassRegistry.put_declarations(operative_class(state), declarations)
      _ = ClassRegistry.put_overwatch(operative_class(state), owner)

      # The declaration is durable now, so the retention floors it needs resolve
      # from it. Announce any it had to raise — a one-line declaration must not
      # commit the controller to disk silently.
      if correction_window_ms do
        announce_derived_retention(operative_class(state), correction_window_ms)
      end

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

  defp bind_to_runner(%__MODULE__{} = state, operative_id, runner_id) do
    %__MODULE__{
      state
      | runner_id: runner_id,
        channel: attach_channel(operative_id, runner_id)
    }
  end

  # Returns `{:ok, runner_id, add_operative_raw_data, state}` on success or `{:error, reason,
  # state}` on exhaustion. On every attempt the runner is re-resolved
  # so a fresh runner that came online during a backoff window is
  # picked up.
  defp start_operative_sdk_with_retry(%__MODULE__{} = state, operative) do
    do_start_with_retry(state, operative, retry_backoff_ms(), 1)
  end

  defp do_start_with_retry(state, _operative, [], _attempt) do
    {:error, :exhausted, state}
  end

  defp do_start_with_retry(%__MODULE__{} = state, operative, [delay | rest], attempt) do
    runner_id =
      resolve_runner(state.namespace, operative.factory_id, operative.id, state.runner_id)

    state = %__MODULE__{state | runner_id: runner_id}

    case attempt_ping_and_start(operative, runner_id) do
      {:ok, raw_data} ->
        {:ok, runner_id, raw_data, state}

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
  # path (`runner-controller-connection-healing`). `:start_operative` is
  # only issued AFTER the ping confirms the round-trip works.
  defp attempt_ping_and_start(operative, runner_id) do
    case ping_runner(runner_id) do
      :ok ->
        case start_operative_sdk(runner_id, operative) do
          {:ok, _} = ok -> ok
          {:error, reason} -> {:error, {:start_operative_failed, reason}}
        end

      {:error, reason} ->
        _ = reset_control_channel(runner_id)
        {:error, {:ping_failed, reason}}
    end
  end

  # `:overwatch_ping_fn` / `:overwatch_reset_channel_fn` are test-only
  # function-injection hooks (Mimic cannot stub across the dist_sup-spawned
  # Overwatch process boundary from a test pid).
  defp ping_runner(runner_id) do
    cond do
      not Application.get_env(:flezha, :overwatch_ping_enabled, true) ->
        :ok

      fun = test_hook(:overwatch_ping_fn) ->
        normalize_ping(fun.(runner_id))

      true ->
        normalize_ping(ControlUnit.ping_runner(runner_id))
    end
  end

  defp normalize_ping({:ok, :pong}), do: :ok
  defp normalize_ping({:error, _} = err), do: err

  defp reset_control_channel(runner_id) do
    case test_hook(:overwatch_reset_channel_fn) do
      nil -> FleetManager.reset_control_channel(runner_id)
      fun -> fun.(runner_id)
    end
  end

  defp test_hook(key) do
    case Application.get_env(:flezha, key) do
      fun when is_function(fun, 1) -> fun
      _ -> nil
    end
  end

  defp resolve_runner(namespace, factory_id, operative_id, fallback) do
    case OperativeManager.assign_runner(namespace, factory_id, operative_id) do
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
  # the event-driven un-park (`runner_migrate`) once a healthy runner for
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

  defp runner_wait_ms,
    do: Application.get_env(:flezha, :overwatch_runner_wait_ms, 30_000)

  # The runner's control unit GenServer registers itself in
  # `Flezha.Service.Identificator` as the first child of its supervision
  # tree (`RunnerSupervisor.init/1` → `ControlUnit.start_link/1`).
  # Registration alone is NOT sufficient: a SIGKILL'd runner can leave
  # the cu registered as a zombie (the `{:flz_closed}` disconnect never
  # arrived), so we additionally require a fresh end-to-end ping when
  # `:overwatch_ping_enabled` is on. Skipped in the unit suites that drive
  # synthetic runners with no real SDK behind the cu.
  defp runner_ready?(runner_id) do
    if OperativeManager.runner_registered?(runner_id) do
      if Application.get_env(:flezha, :overwatch_ping_enabled, true),
        do: ping_runner(runner_id) == :ok,
        else: true
    else
      false
    end
  end

  # Walk the on-disk Entity state for this operative (using `operative.id` as
  # the operative_class) and dispatch `RestoreInitState` to the SDK for each
  # Entity that has a non-empty payload. Skipped when the operative
  # arrived via HotSwap — handover already delivers the state in-band.
  # Skipped when persistence is disabled for this operative class.
  defp maybe_dispatch_restore_init_state(%__MODULE__{arrived_via_hotswap?: true}, _operative),
    do: :ok

  # Tier operatives restored synchronously in `restore_and_replay_journal/1`.
  defp maybe_dispatch_restore_init_state(%__MODULE__{effectively_once: true}, _operative),
    do: :ok

  defp maybe_dispatch_restore_init_state(%__MODULE__{} = state, operative) do
    do_dispatch_restore(state, operative.opts, state.runner_id)
    :ok
  end

  defp do_dispatch_restore(%__MODULE__{} = state, operative_opts, runner_id) do
    # The SDK keys per-Entity state by the operative INSTANCE id (== `state.id`),
    # so the WAL lives under `state/{operative_id}/...`. Enumerate by the same key.
    operative_class = Keyword.get(operative_opts, :operative_class, state.id)
    overwatch = self()

    # Off-process: the replay does blocking disk reads, never touches state.
    spawn(fn ->
      replay_persisted_entities(state.builder.kind, operative_class, runner_id, overwatch)
    end)

    :ok
  end

  # Bind-time restore is capability-gated on `persistent_state?` (kind+class):
  # `:unbounded` always; a durable `:bounded` class (this IS its crash-resume)
  # unless it opted out. A stateless kind has no WAL to replay.
  defp replay_persisted_entities(kind, operative_class, runner_id, overwatch) do
    # Transactional outbox re-drive (bounded-transactional-outbox): a procedure
    # journaled `proc_committed` (or `compensated`) whose outbox never reached the
    # plugins owes that output. Re-drive it — with the original keys — before the
    # resumed run advances, so a crash between commit and flush loses nothing.
    if kind == :bounded do
      _ = Flezha.State.ProcJournal.flush_pending(operative_class, overwatch)
    end

    Enum.each(restorable_entities(kind, operative_class), fn entity_id ->
      _ = dispatch_restore_init_state(runner_id, entity_id)
    end)
  end

  # Persisted, restorable Entities of this class — empty when the kind/class
  # combination has no persistent state.
  defp restorable_entities(kind, operative_class) do
    if PersistenceResolver.persistent_state?(kind, operative_class) do
      operative_class
      |> Flezha.State.Registry.entities_of()
      |> Enum.filter(&Restore.has_persisted_state?/1)
    else
      []
    end
  end

  # Effectively-once bind path: restore each persisted Entity through the
  # awaited control round-trip (the SDK has applied state + consumed floor
  # when the reply lands), then re-forward the journaled input tail above
  # the min durable consumed-offset. Runs in the Overwatch process before
  # plugin activation; Entities ahead of the min watermark skip re-forwarded
  # offsets they already consumed (SDK `consumed_floor`).
  defp restore_and_replay_journal(%__MODULE__{} = state) do
    state.builder.kind
    |> restorable_entities(operative_class(state))
    |> Enum.each(fn entity_id ->
      # Starting the writer replays its WAL scan: arms the outbox re-drive
      # and repopulates the Entity's durable consumed-offset watermark.
      _ = Flezha.State.Supervisor.find_or_start(entity_id)
      _ = await_restore_init_state(state.runner_id, entity_id)
    end)

    replay_journaled_inputs(state)
    state
  end

  defp await_restore_init_state(runner_id, entity_id) do
    with {:ok, command} <- restore_command(entity_id) do
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:send_control, command},
        restore_await_timeout_ms()
      )
    end
  catch
    :exit, reason ->
      Logger.warning(
        "[Overwatch] awaited restore failed entity=#{inspect(entity_id)}: #{inspect(reason)}"
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
        replay_slice(state, operative_class, floor + 1, last)
      end
    end

    :ok
  end

  # The one re-forward path, shared by crash recovery and deliberate correction
  # — which is the whole point. A correction leaves the on-disk state at a
  # rewound baseline with the input-log watermark lowered to match, and from
  # there the ordinary resume replay *is* the correction. The two differ only in
  # how the baseline was chosen.
  #
  # For a correction-declaring class every replayed slice is marked, and the
  # SDK holds it to the end marker so it can order it by event time before
  # dispatching any of it. Re-forwarding in arrival order would put the late
  # input back after the inputs that overtook it and recompute the same wrong
  # answer.
  defp replay_slice(%__MODULE__{} = state, operative_class, from, to) do
    case InputLog.read_range(operative_class, from, to) do
      [] ->
        :ok

      payloads ->
        marks = if state.correction_window_ms, do: %{"flz.replay" => "1"}, else: %{}

        payloads
        |> Enum.map(fn {offset, message, metadata} ->
          {message,
           metadata
           |> Map.put("flz.offset", Integer.to_string(offset))
           |> Map.merge(marks)}
        end)
        |> then(&reforward(state, &1))

        # Close the slice. A broadcast rather than a flag on the last input:
        # an input reaches exactly one Entity (routed by key), so a marker
        # riding one would strand the slice every *other* Entity buffered.
        if state.correction_window_ms do
          reforward(state, [{"", %{"flz.replay_end" => "1"}}])
        end

        :ok
    end
  end

  # ---------- Late-input correction ----------

  @doc """
  An Entity reported an input behind its event-time high-water but inside the
  correction window. Coalesce it; the debounce fires the replay.
  """
  @spec request_correction(pid(), tuple(), non_neg_integer()) :: :ok
  def request_correction(overwatch, entity_id, target_event_time),
    do: GenServer.cast(overwatch, {:correction_request, entity_id, target_event_time})

  @doc """
  Operator replay (`flz operative replay`): rewind an Entity to `baseline_seq`
  and re-run its retained inputs from there through the current code.

  The same transaction a late input drives, with the baseline chosen rather than
  derived. Synchronous, so the CLI reports the outcome rather than a hopeful
  "queued".
  """
  @spec replay_from(pid(), tuple(), non_neg_integer()) :: :ok | {:error, term()}
  def replay_from(overwatch, entity_id, baseline_seq),
    do: GenServer.call(overwatch, {:replay_from, entity_id, baseline_seq}, 60_000)

  # Concurrent late inputs for one Entity collapse into the one replay that
  # reaches furthest back — a burst of five late trades is one restore-replay
  # cycle, not five. The debounce is what gives the burst time to arrive; the
  # `Map.has_key?` guard is what keeps it a single timer.
  defp note_correction(%__MODULE__{} = state, entity_id, target) do
    pending = state.corrections

    if Map.has_key?(pending, entity_id) do
      %{state | corrections: Map.update!(pending, entity_id, &min(&1, target))}
    else
      Process.send_after(self(), {:run_correction, entity_id}, correction_debounce_ms())
      %{state | corrections: Map.put(pending, entity_id, target)}
    end
  end

  # The correction transaction. Runs synchronously in the Overwatch process,
  # which is what makes it safe: the Overwatch is also the only thing that
  # journals and forwards input for this operative, so while it is in here no
  # new input can be journaled and the slice's upper bound cannot move.
  #
  # A crash anywhere in here needs no marker to recover. Before the rewind
  # commits, nothing durable has changed: the late input is still in the log,
  # still unconsumed, and the ordinary resume re-forwards it, the SDK
  # re-classifies it, and we end up back here. After the rewind commits, the
  # on-disk state *is* the baseline and the watermark was lowered to match — so
  # the ordinary resume replay IS this replay. That is why crash recovery and
  # deliberate correction share one path.
  defp run_correction(%__MODULE__{} = state, entity_id, target) do
    result =
      with {:ok, writer} <- Flezha.State.Supervisor.find_or_start(entity_id),
           {:ok, %{seq: baseline_seq, consumed_offset: consumed}} <-
             Flezha.State.WalWriter.correction_baseline(writer, target) do
        rewind_and_replay(state, entity_id, writer, baseline_seq, consumed)
      end

    log_correction(state, entity_id, target, result)
    state
  end

  # The operator's half: the baseline is given, so only the consumed offset has
  # to be looked up. Everything downstream is the same transaction.
  defp run_replay(%__MODULE__{} = state, entity_id, baseline_seq) do
    result =
      with {:ok, writer} <- Flezha.State.Supervisor.find_or_start(entity_id) do
        consumed = Flezha.State.WalWriter.consumed_offset_at(writer, baseline_seq)
        rewind_and_replay(state, entity_id, writer, baseline_seq, consumed)
      end

    log_correction(state, entity_id, baseline_seq, result)
    if match?({:ok, _}, result), do: :ok, else: result
  end

  defp rewind_and_replay(state, entity_id, writer, baseline_seq, consumed) do
    operative_class = operative_class(state)
    started = System.monotonic_time(:millisecond)

    # Explicit, not `current + 1` inside the writer: a re-run of an interrupted
    # correction must re-assert the epoch it already committed to, never bump a
    # second time — a second bump would leave the first attempt's rows behind at
    # an epoch nothing overwrites.
    epoch = Flezha.State.WalWriter.outbox_epoch(writer) + 1
    :ok = Flezha.State.WalWriter.rewind_to(writer, baseline_seq, epoch)

    # Blocking: the restore rides the control channel while the replay rides the
    # data channel, and without this fence they race — the runner could fold
    # slice inputs into the very state the rewind was about to replace.
    _ = await_restore_init_state(state.runner_id, entity_id)

    last = InputLog.last_offset(operative_class)
    if last > consumed, do: replay_slice(state, operative_class, consumed + 1, last)

    {:ok,
     %{
       baseline_seq: baseline_seq,
       epoch: epoch,
       replayed: max(last - consumed, 0),
       duration_ms: System.monotonic_time(:millisecond) - started
     }}
  end

  defp log_correction(state, entity_id, _target, {:ok, measurements}) do
    emit_correction(:completed, operative_class(state), entity_id, measurements)
    :ok
  end

  defp log_correction(state, entity_id, target, {:error, reason}) do
    Logger.warning(
      "[Overwatch] correction refused entity=#{inspect(entity_id)} " <>
        "target=#{target}: #{inspect(reason)}"
    )

    emit_correction(:refused, operative_class(state), entity_id, %{reason: reason})
  end

  defp emit_correction(event, operative_class, {_, procedure, key}, measurements) do
    {reason, measurements} = Map.pop(measurements, :reason)

    :telemetry.execute(
      [:flezha, :correction, event],
      Map.put_new(measurements, :count, 1),
      %{operative: operative_class, procedure: procedure, key: key, reason: reason}
    )
  end

  defp correction_debounce_ms(),
    do: Application.get_env(:flezha, :correction_debounce_ms, 200)

  # Re-forward already-durable payloads, stamped with the offset they were
  # journaled at. The delivery half of both durability tiers' resume — the
  # effectively-once input log above, and the durable bounded inbox below — and
  # deliberately NOT an ingest: these payloads are already in their log and
  # already ran `:on_input`, so re-journaling them would duplicate them.
  defp reforward(%__MODULE__{} = state, payloads) do
    Enum.each(payloads, &GenServer.cast(state.channel, {:io_in, state.id, &1}))
  end

  @doc """
  External trigger for the bind-driven per-Entity replay. Called from
  `Flezha.State.Recovery.resync_for_overwatch/2` when a non-bind caller
  (the convergence Loop's rebind heal) needs to re-issue the
  `RestoreInitState` handshake for an already-bound Overwatch.

  Implementation routes the work back to the Overwatch process via a
  cast so the caller never blocks on enumeration / dispatch — the
  Overwatch is the single owner of "enumerate my entities, dispatch
  RestoreInitState for each".
  """
  @spec kick_restore_for_bound_runner(pid(), binary()) :: :ok
  def kick_restore_for_bound_runner(overwatch_pid, runner_id)
      when is_pid(overwatch_pid) and is_binary(runner_id) do
    GenServer.cast(overwatch_pid, {:kick_restore, runner_id})
  end

  @doc """
  Number of persisted Entities the bind-time restore path will ship a
  `RestoreInitState` for on this (re)bind — the value threaded onto
  `AddOperative.resume_entity_count` so a durable bounded driver holds its
  first Procedure until that many restores land (crash-resume,
  `bounded-operative-durability`).

  Non-zero only for a durable `:bounded` class with WAL state on disk; `0` for a
  fresh spawn, an unbounded operative, or an opted-out bounded one — those need
  no restore-before-run gate. Matches the frames `replay_persisted_entities/3`
  ships (same enumeration + `has_persisted_state?` filter).
  """
  @spec resume_entity_count(atom(), binary()) :: non_neg_integer()
  def resume_entity_count(:bounded, operative_class),
    do: length(restorable_entities(:bounded, operative_class))

  def resume_entity_count(_kind, _operative_class), do: 0

  # Stamp everything a resuming bind carries onto the operative's opts so
  # `control_unit` can set the `AddOperative` fields: how many Entity restores
  # to wait for, and the proc-journal the driver resumes its iteration from. Only
  # a bounded instance has a journal, and the kind check is what keeps an
  # unbounded bind from reading for a file that cannot exist.
  defp with_resume_state(operative, kind, operative_class) do
    opts =
      operative.opts
      |> Keyword.put(:resume_entity_count, resume_entity_count(kind, operative_class))
      |> Keyword.put(:proc_journal, if(kind == :bounded, do: proc_journal_state(operative_class)))

    %{operative | opts: opts}
  end

  # The operative-class identity used for per-Entity state (defaults to the
  # instance id, per the SDK convention that entity frames carry the operative_id).
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
  #     forwarded), and an append that fails answers `:refused` and is NOT
  #     forwarded either: the contract is append-before-forward, so a lost frame
  #     the sender can retry beats an undurable one it believes was accepted.
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
    case ProcInbox.append(state.id, message, metadata) do
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

        :refused
    end
  end

  def journal_input(_state, payload), do: payload

  # ---------------------------------------------------------------
  # Durable suspension (`bounded-durable-suspension`)
  # ---------------------------------------------------------------

  @doc """
  Suspend a bounded instance parked at the committed boundary of iteration
  `iter`: evict it from its runner (`RemOperative`), release the registry's
  runner binding, and arm the journaled wake deadline. The registry entry —
  and this process — stay, so a new input still has somewhere to land; it is the
  **runner** that holds zero resources for a suspended operative.

  Called by `ControlChannel` after the `parked` journal entry is durable.
  """
  @spec suspend(pid(), non_neg_integer(), non_neg_integer() | nil) :: :ok
  def suspend(overwatch_pid, iter, wake_deadline_ms) when is_pid(overwatch_pid),
    do: GenServer.cast(overwatch_pid, {:suspend, iter, wake_deadline_ms})

  @doc """
  Rewind a bounded loop to the committed boundary of iteration `to_iter`: journal
  the rewind — which bumps the epoch and un-commits every iteration above it —
  then evict the instance from its runner and rehydrate it.

  The rehydrating bind ships the folded journal back, so the driver undoes the
  unwound procedures through the ordinary compensation planner and resumes the
  loop at `to_iter + 1`, with the inbox items those iterations consumed
  undelivered again. The marker is durable before the eviction, so a crash
  mid-rewind resumes as a rewind rather than as a partially unwound loop.

  The caller (`CliManager`) has already refused an uncommitted target and an
  iteration whose plugin output cannot be compensated.
  """
  @spec rewind(pid(), non_neg_integer()) :: :ok | {:error, term()}
  def rewind(overwatch_pid, to_iter) when is_pid(overwatch_pid),
    do: GenServer.call(overwatch_pid, {:rewind, to_iter}, 30_000)

  # Refuse a rewind before it changes anything: an uncommitted target, or an
  # unwound procedure whose plugin output cannot be undone.
  defp rewind_guard(%__MODULE__{} = state, to_iter) do
    committed = ProcJournal.committed_iterations(state.id)

    cond do
      to_iter not in committed ->
        {:error, {:uncommitted_iteration, committed}}

      (uncovered = uncovered_channels(state, to_iter)) != [] ->
        {:error, {:uncovered, uncovered}}

      true ->
        :ok
    end
  end

  # A bounded operative's staged plugin output carries no natural key, so a
  # re-run's output cannot be relied on to overwrite the original at every sink.
  # The only undo the engine can guarantee is the procedure's own `compensate`
  # hook — so an unwound procedure that emitted and declares none blocks the
  # rewind, naming the channels it wrote. A procedure that emitted nothing needs
  # no coverage.
  defp uncovered_channels(%__MODULE__{} = state, to_iter) do
    compensable = ClassRegistry.compensable_procs(operative_class(state)) || []

    for {proc_idx, channel} <- ProcJournal.staged_channels_above(state.id, to_iter),
        proc_idx not in compensable,
        uniq: true,
        do: channel
  end

  # Bind state for a suspended instance: no runner, no stream channel, the
  # wake timer armed from the journaled deadline.
  defp enter_suspended(%__MODULE__{} = state, journal) do
    :telemetry.execute(
      [:flezha, :bounded, :suspended],
      %{count: 1},
      %{
        namespace: state.namespace,
        operative_id: state.id,
        iter: (journal && journal.iter) || 0
      }
    )

    # A committed iteration boundary is the migration-safe barrier.
    signal_migration_ready(state)
    evict_runner(state, journal && journal.wake_deadline_ms)
  end

  # Release every runner-side resource but keep the process and the registry
  # entry, so a new input still has somewhere to land. Shared by suspension (a
  # park, with its wake timer) and rewind (a rebind, with none).
  defp evict_runner(%__MODULE__{} = state, deadline) do
    %__MODULE__{
      cancel_wake_timer(state)
      | suspended: true,
        runner_id: nil,
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
    if ProcInbox.undelivered(state.id) == [], do: state, else: rehydrate(state)
  end

  # Rehydrate a suspended instance through the normal bind path (the same
  # `complete_init` an un-park drives), which restores journal + snapshots and
  # re-delivers the undelivered inbox tail. Idempotent: an instance already
  # running, or with a rehydrate already in flight, no-ops — so two wake sources
  # racing (an inbox append and the wake deadline) rehydrate exactly once.
  defp rehydrate(%__MODULE__{suspended: false} = state), do: state
  defp rehydrate(%__MODULE__{migrate_task: task} = state) when task != nil, do: state

  defp rehydrate(%__MODULE__{} = state) do
    case OperativeManager.assign_runner(state.namespace, state.factory_id, state.id) do
      nil ->
        # No runner to rehydrate onto: stay suspended. The next input (or the
        # convergence tick that brings a runner up) tries again.
        Logger.warning(
          "[Overwatch] no active runner to rehydrate suspended operative #{state.id}"
        )

        state

      runner_id ->
        :telemetry.execute(
          [:flezha, :bounded, :rehydrating],
          %{count: 1},
          %{namespace: state.namespace, operative_id: state.id, runner_id: runner_id}
        )

        {ref, pid} = spawn_migrate_task({:unpark, state, runner_id})
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
    |> ProcInbox.undelivered()
    |> Enum.map(fn entry ->
      {entry.message,
       Map.put(entry.metadata, "flz.inbox_offset", Integer.to_string(entry.offset))}
    end)
    |> then(&reforward(state, &1))

    state
  end

  # A durable bounded class: the kind that owns a proc-journal and an inbox. An
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
    case ProcJournal.restore_state(instance_id) do
      %{parked: true} -> ProcInbox.undelivered(instance_id) == []
      _ -> false
    end
  end

  # Ingest one inbound message: type it, run the `:on_input` plugin chain, count
  # it, and make it durable. The single pre-delivery path — every `:io_in` entry
  # point (call, cast, confirming, suspended) runs exactly this, and the payload
  # it answers is the one to forward. `nil` ⇒ filtered by a plugin or absorbed as
  # a duplicate; `:refused` ⇒ the durable log rejected the append. Neither is
  # forwarded; only the confirming entry point tells them apart.
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

  # The one ingest-and-report path behind both blocking entry points: `{:io_in,
  # _, _}`, which reports coarsely, and `{:ingest, _, _}`, which reports the
  # verdict. A parked operative is refused by the callers before it reaches here
  # — each owes its own reply for that.
  defp confirming_ingest(%__MODULE__{suspended: true} = state, message, metadata) do
    {{:ok, :suspended}, ingest_suspended(state, message, metadata)}
  end

  defp confirming_ingest(%__MODULE__{} = state, message, metadata) do
    verdict =
      case ingest(state, message, metadata) do
        nil -> {:ok, :absorbed}
        :refused -> {:error, :refused}
        payload -> confirm_forward(state, payload)
      end

    {verdict, state}
  end

  # A durable target has already journaled the payload, so an unbound stream is
  # not a delivery failure — the bind delivers it. A target with no durable log
  # has nowhere to hold it, so the sender is told to retry.
  defp confirm_forward(%__MODULE__{} = state, payload) do
    case forward_input(state, payload) do
      :ok -> :ok
      {:error, :no_stream} -> if durable_input?(state), do: :ok, else: {:error, :not_ready}
    end
  end

  defp durable_input?(%__MODULE__{effectively_once: eo, durable_bounded: db}), do: eo or db

  # Forward an ingested payload to the bound stream channel. Counts an
  # unbound-stream drop instead of discarding the channel's `{:error, :no_stream}`
  # reply, so the reconciler-detected stall is observable (the probe rebinds the
  # stream).
  defp forward_input(%__MODULE__{channel: nil} = state, _payload) do
    :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{operative_id: state.id})
    {:error, :no_stream}
  end

  defp forward_input(%__MODULE__{} = state, payload) do
    case GenServer.call(state.channel, {:io_in, state.id, payload}) do
      {:error, :no_stream} ->
        :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{operative_id: state.id})
        {:error, :no_stream}

      _ ->
        :ok
    end
  end

  @typedoc """
  A class's SDK-side declarations, as resolved from the `AddOperative` response.

  `compensable_procs` is the chain positions whose procedure declares a `compensate`
  hook, or `nil` when the class declares nothing (run-sugar, unbounded, or a Java
  runner) — which a rewind reads as "no coverage information", not as "nothing
  is compensable".
  """
  @type declarations :: %{
          effectively_once: boolean(),
          checkpoint_ack: boolean(),
          compensable_procs: [non_neg_integer()] | nil,
          leaf_procs: [non_neg_integer()] | nil,
          correction_window_ms: pos_integer() | nil,
          compact_after_bytes: pos_integer() | nil
        }

  @doc """
  Resolve a class's SDK-side declarations from the `AddOperative` response
  raw_data — an `AddOperativeResponse` metadata map when the class declares
  anything, empty otherwise (including every pre-tier or Java runner). The one
  place the wire form is decoded; `ClassRegistry.put_declarations/2` is the one
  place it is persisted. A new declared capability is added in those two and
  nowhere else.
  """
  @spec decode_declarations(binary() | nil) :: declarations()
  def decode_declarations(raw_data) do
    metadata = decode_metadata(raw_data)

    %{
      effectively_once: Map.get(metadata, "effectively_once") == "true",
      checkpoint_ack: Map.get(metadata, "ack_granularity") == "checkpoint",
      compensable_procs: parse_ops(Map.get(metadata, "compensable_procs")),
      leaf_procs: parse_ops(Map.get(metadata, "leaf_procs")),
      correction_window_ms: parse_window(Map.get(metadata, "correction_window_ms")),
      compact_after_bytes: parse_window(Map.get(metadata, "compact_after_bytes"))
    }
  end

  # A decimal declaration string as an integer, or nil when absent/malformed.
  # Declarations and the reply-frame `flz.change_seq` both arrive this way.
  defp parse_int(raw) when is_binary(raw) do
    case Integer.parse(raw) do
      {n, ""} -> n
      _ -> nil
    end
  end

  defp parse_int(_), do: nil

  defp parse_window(raw) do
    case parse_int(raw) do
      ms when is_integer(ms) and ms > 0 -> ms
      _ -> nil
    end
  end

  @doc """
  Announce the retention a declared correction window derived for itself.

  Retention is not a prerequisite an operator negotiates — a correction over a
  60 s window MEANS holding 60 s of input log and 60 s of WAL history, so the
  declaration derives both floors (`InputLog.hold_window_ms/1`,
  `PersistenceResolver.compact_after_age_ms/1`). But an SDK author's one line can
  commit the controller to real disk, so a floor RAISED above its configured value
  is logged and emitted once, at bind. Nothing is announced when nothing was
  raised.
  """
  @spec announce_derived_retention(binary(), pos_integer()) :: :ok
  def announce_derived_retention(operative_class, window_ms) do
    configured_hold = Application.get_env(:flezha, :input_log_hold_window_ms, 0)
    derived_hold = InputLog.hold_window_ms(operative_class)

    if derived_hold > configured_hold do
      emit_retention_derived(operative_class, :input_log_hold_window_ms, configured_hold,
        derived: derived_hold,
        window_ms: window_ms
      )
    end

    # `0` means "no age-triggered compaction", which already retains everything the
    # window needs — the resolver satisfies it rather than raising it.
    configured_age = PersistenceResolver.configured_compact_after_age_ms(operative_class)
    derived_age = PersistenceResolver.compact_after_age_ms(operative_class)

    if derived_age > configured_age do
      emit_retention_derived(operative_class, :compact_after_age_ms, configured_age,
        derived: derived_age,
        window_ms: window_ms
      )
    end

    :ok
  end

  defp emit_retention_derived(operative_class, floor, configured, opts) do
    derived = Keyword.fetch!(opts, :derived)

    Logger.info(
      "[Overwatch] #{operative_class} declares a #{Keyword.fetch!(opts, :window_ms)}ms " <>
        "correction window: #{floor} raised #{configured} -> #{derived}"
    )

    :telemetry.execute(
      [:flezha, :correction, :retention_derived],
      %{count: 1, configured: configured, derived: derived},
      %{operative: operative_class, floor: floor}
    )
  end

  defp decode_metadata(raw_data) when is_binary(raw_data) and raw_data != "" do
    case RunnerCommands.AddOperativeResponse.decode(raw_data) do
      {:ok, %RunnerCommands.AddOperativeResponse{metadata: metadata}} -> metadata
      {:error, _} -> %{}
    end
  end

  defp decode_metadata(_raw_data), do: %{}

  defp parse_ops(nil), do: nil
  defp parse_ops(""), do: []
  defp parse_ops(csv), do: csv |> String.split(",") |> Enum.map(&String.to_integer/1)

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
  # per-Entity state-restore path (`dispatch_restore_init_state/2`),
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

    unless is_nil(state.runner_id) do
      ControlUnit.stop_operative_async(state.runner_id, state.id)

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
      runner_id: state.runner_id,
      reason: reason
    }

    Extension.run(state.extensions, :on_terminate, ctx)
    :ok
  end

  @impl true
  def handle_call(:registry_runner_set, _from, %__MODULE__{init_error: nil} = state) do
    HubManager.operative_set_runner(state.namespace, state.id, state.runner_id)

    {:reply, {:ok, state.runner_id}, state}
  end

  def handle_call(:registry_runner_set, _from, %__MODULE__{init_error: reason} = state) do
    {:reply, {:error, reason}, state}
  end

  @impl true
  def handle_call(:stop_operative, from, state) do
    # Terminate any in-flight `:runner_migrate` task BEFORE the synchronous
    # stop sequence so `flz operative stop r1` always returns within its normal
    # handle_call window even mid-retry.
    state = terminate_migrate_task(state)

    case state.runner_id do
      nil ->
        :ok

      _ ->
        with_proc_span(:stop_operative, state.id, fn ->
          GenServer.call(
            Identificator.control_unit(state.runner_id),
            {:stop_operative, state.id}
          )
        end)
    end

    fire_on_terminate(state, :stopped)
    table = Table.teardown_all(state.extensions)

    GenServer.reply(from, :ok)

    {:stop, :normal, %{state | runner_id: nil, extensions: table}}
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
        :get_runner,
        _from,
        %__MODULE__{runner_id: runner} = state
      ) do
    {:reply, runner, state}
  end

  @impl true
  def handle_call(:get_overwatch_state, _from, state) do
    {:reply, state, state}
  end

  # Live read of Entity state, narrowed by `selector` (`%{}` = every Procedure,
  # the migration/handover shape). Always answered as `{:ok, json, change_seq}`
  # — the seq rides the reply frame's metadata, so the JSON body stays the shape
  # the whole-Operative caller already decodes; `OperativeManager` drops the seq
  # for callers that predate it.
  @impl true
  def handle_call({:get_entity_state, selector}, from, %__MODULE__{} = state)
      when is_map(selector) do
    read_entity_state(state, from, selector)
  end

  # Async `:runner_migrate` for the PARKED un-park path only — that's
  # where the bounded retry loop (up to 19 s aggregate) actually lives,
  # and the spec's stop-during-migrate scenario quotes "~10 s into the
  # backoff schedule". The running-migrate path is a single ping +
  # start round-trip; keeping it synchronous preserves the existing
  # HotSwap handover invariants (`runner_disconnect_migrate_op` and
  # peers) that depend on state being applied before the call returns.
  @impl true
  def handle_call({:runner_migrate, _}, _from, %__MODULE__{migrate_task: t} = state)
      when t != nil do
    {:reply, {:error, :busy}, state}
  end

  # Parked (a failed init) and suspended (a durable loop park) both come back
  # through the same un-park task: `complete_init` re-binds, restores journal +
  # entities, and delivers the undelivered inbox tail. A running operative
  # instead migrates in place, below.
  def handle_call(
        {:runner_migrate, new_runner_id},
        _from,
        %__MODULE__{init_error: reason, suspended: suspended} = state
      )
      when reason != nil or suspended do
    {ref, pid} = spawn_migrate_task({:unpark, state, new_runner_id})
    {:reply, :ok, %{state | migrate_task: %{ref: ref, pid: pid, mode: :unpark}}}
  end

  # The binding is committed only once the runner has answered. Committing it
  # first leaves a failed migrate naming a runner this process cannot reach —
  # the operative then reads as bound to a LIVE runner, so `Convergence.Diff`
  # stops classifying it misbound and the divergence hides itself permanently.
  def handle_call({:runner_migrate, new_runner_id}, _from, %__MODULE__{} = state) do
    case attempt_ping_and_start(migrate_operative(state), new_runner_id) do
      {:ok, _} -> {:reply, :ok, bind_to_runner(state, state.id, new_runner_id)}
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

  # Blocking ingest for source adapters, which act on "accepted" vs "parked" and
  # nothing finer — the same path as the confirming entry point below, reported
  # coarsely.
  @impl true
  def handle_call({:io_in, _message, _metadata}, _from, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    {:reply, {:error, :startup_failed}, state}
  end

  @impl true
  def handle_call({:io_in, message, metadata}, _from, state) do
    {_verdict, state} = confirming_ingest(state, message, metadata)
    {:reply, :ok, state}
  end

  # Confirming ingest (`operative-to-operative-messaging`): the same single
  # pre-delivery path as the `{:io_in, _, _}` cast, answering what happened so a
  # forwarding sink can tell "done" from "retry". The cast keeps its
  # fire-and-forget shape, including its silent drop for a parked operative.
  @impl true
  def handle_call({:ingest, _message, _metadata}, _from, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    {:reply, {:error, :not_ready}, state}
  end

  @impl true
  def handle_call({:ingest, message, metadata}, _from, state) do
    {verdict, state} = confirming_ingest(state, message, metadata)
    {:reply, verdict, state}
  end

  @impl true
  def handle_call(:create_snapshot, _from, %__MODULE__{} = state) do
    snapshot = create_snapshot(state.id, state.runner_id)

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
  def handle_call({:replay_from, entity_id, baseline_seq}, _from, %__MODULE__{} = state) do
    {:reply, run_replay(state, entity_id, baseline_seq), state}
  end

  @impl true
  def handle_call({:rewind, to_iter}, _from, %__MODULE__{} = state) do
    with :ok <- rewind_guard(state, to_iter),
         :ok <- ProcJournal.append_rewound(state.id, to_iter) do
      if state.runner_id do
        ControlUnit.stop_operative_async(state.runner_id, state.id)
      end

      _ = HubManager.operative_set_runner(state.namespace, state.id, nil)

      :telemetry.execute(
        [:flezha, :bounded, :rewind],
        %{count: 1, epoch: ProcJournal.current_epoch(state.id)},
        %{namespace: state.namespace, operative_id: state.id, to_iter: to_iter}
      )

      Logger.info("[Overwatch] operative #{state.id} rewound to iteration #{to_iter}")

      # A rewind is a rebind, not a park: no `parked` entry, no wake timer, and
      # the rehydrate is unconditional — the loop must come back up to undo the
      # iterations the marker unwound even with no input waiting on it.
      {:reply, :ok, rehydrate(evict_runner(state, nil))}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
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
    # Parked: silently drop ingress casts. A sender that needs the payload held
    # rather than dropped uses the confirming entry point, which answers
    # `{:error, :not_ready}` here instead.
    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_in, message, metadata}, state) do
    case ingest(state, message, metadata) do
      {_message, _metadata} = payload ->
        GenServer.cast(state.channel, {:io_in, state.id, payload})

      _ ->
        :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, message, metadata}, state) do
    run_io(state, :on_output, message, metadata)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:correction_request, entity_id, target}, state) do
    {:noreply, note_correction(state, entity_id, target)}
  end

  # The SDK parked at a committed iteration boundary and the `parked` journal
  # entry is durable: evict the instance from its runner and keep it
  # `suspended` here (`bounded-durable-suspension`). Not a terminal outcome — no
  # completion record, no state deletion.
  @impl true
  def handle_cast({:suspend, iter, wake_deadline_ms}, %__MODULE__{} = state) do
    if state.runner_id do
      ControlUnit.stop_operative_async(state.runner_id, state.id)
    end

    _ = HubManager.operative_set_runner(state.namespace, state.id, nil)

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
  # runner's final state. Cold-boot restart is covered by the
  # per-Entity WAL files instead.
  @impl true
  def handle_cast({:apply_external_snapshot, binary}, %__MODULE__{} = state)
      when is_binary(binary) do
    {:noreply, %__MODULE__{state | snapshot: binary}}
  end

  # A operative that arrived via HotSwap handover already received its state
  # in-band (`alter_handover_state` ships `MigrationDeserialization`); a WAL
  # replay on top would apply every change a second time. Skip — same guard
  # as the bind-time `maybe_dispatch_restore_init_state`.
  def handle_cast({:kick_restore, _runner_id}, %__MODULE__{arrived_via_hotswap?: true} = state) do
    {:noreply, state}
  end

  # External callers (the convergence Loop's rebind heal) trigger
  # a fresh per-Entity replay against the Overwatch's bound
  # runner without knowing the Overwatch's internals. The cast is
  # the indirection point used by
  # `Flezha.State.Recovery.resync_for_overwatch/2`.
  def handle_cast({:kick_restore, runner_id}, %__MODULE__{} = state) do
    do_dispatch_restore(state, state.builder.opts, runner_id)
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
      runner_id: state.runner_id,
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
  def handle_info({:run_correction, entity_id}, %__MODULE__{} = state) do
    {target, corrections} = Map.pop(state.corrections, entity_id)
    state = %{state | corrections: corrections}

    # At most one correction per Entity is ever in flight, by construction: the
    # transaction runs to completion inside this handler, so a request arriving
    # during it simply waits in the mailbox and debounces into the next one.
    state = if target, do: run_correction(state, entity_id, target), else: state
    if state.corrections == %{}, do: signal_migration_ready(state)
    {:noreply, state}
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
    _ = ProcInbox.append(state.id, "", %{"flz.wake" => "timer"})
    {:noreply, rehydrate(%__MODULE__{state | wake_timer: nil})}
  end

  # The in-flight `:runner_migrate` task posts its result via a plain
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

  # Dispatched from inside the registry write, so this runs after it: whoever
  # wrote last, the row ends up naming the runner this process is bound to.
  @impl true
  def handle_info(
        {:reassert_binding, operative_id},
        %__MODULE__{id: operative_id, runner_id: runner_id} = state
      )
      when is_binary(runner_id) do
    if HubManager.operative_runner_tag(state.namespace, operative_id) != runner_id do
      _ = HubManager.operative_set_runner(state.namespace, operative_id, runner_id)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:reassert_binding, _operative_id}, state), do: {:noreply, state}

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
      runner_id: state.runner_id
    }

    Extension.run(state.extensions, :on_message, ctx)

    {:noreply, state}
  end

  defp create_snapshot(operative_id, runner_id) do
    command = %RunnerCommands.EntityState{operative_id: operative_id}

    case send_control(runner_id, {:entity_state, command}) do
      {:ok, snapshot} -> snapshot
      {:error, _reason} -> nil
    end
  end

  @impl true
  def prepare_handover_state(state) do
    serialize_data =
      case serialize_operative_sdk(state.runner_id, state.id) do
        nil -> state.snapshot
        data -> data
      end

    handover = %{
      id: state.id,
      namespace: state.namespace,
      factory_id: state.factory_id,
      init_values: state.init_values,
      plugins: Table.serializable_plugins(state.extensions),
      # Where this operative's durable files are. `serialize_data` is nil
      # whenever the source runner is already gone, and then the disk is the
      # only surviving state — the destination pulls it from here.
      source_node: node()
    }

    {handover, serialize_data}
  end

  @impl true
  def alter_handover_state(%__MODULE__{} = state, {handover_state, pipe_serialized}) do
    # `serialized_state` is a protobuf bytes field. When the SDK has no state
    # to hand over (no prior snapshot, or the source runner is already gone)
    # `pipe_serialized` is nil, and Protox.encode/1 raises on a non-binary —
    # which crashed send_control before the deserialization command was ever
    # sent, leaving the runner stuck "awaiting controller control-plane
    # traffic". An empty binary means "no prior state": the operative starts fresh.
    serialized_state = if is_binary(pipe_serialized), do: pipe_serialized, else: <<>>

    command = %RunnerCommands.MigrationDeserialization{
      operative_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: serialized_state
    }

    result =
      with_proc_span(:migration_deserialize, state.id, fn ->
        send_control(state.runner_id, {:migration_deserialization, command})
      end)

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize operative: #{inspect(reason)}")
      _else -> :ok
    end

    # Tear down existing, then rehydrate; activate so providers reconnect.
    table = Table.teardown_all(state.extensions)
    rehydrated = Table.rehydrate(table, Map.get(handover_state, :plugins, []))

    migrated =
      %__MODULE__{
        state
        | extensions: Table.bulk_activate(rehydrated, activation_env(state)),
          init_values: handover_state.init_values,
          # Only a handover that actually carried state suppresses the WAL
          # restore. An empty one carries nothing to double-apply, so the
          # operative must fall through to its on-disk state — otherwise a
          # migration away from a dead runner starts fresh AND locks the
          # replay out, including the explicit `kick_restore` repair.
          arrived_via_hotswap?: serialized_state != <<>>
      }

    if migrated.arrived_via_hotswap?,
      do: migrated,
      else: adopt_source_state(migrated, Map.get(handover_state, :source_node))
  end

  # An empty handover means the source runner was already gone, so the source
  # controller's DISK holds the only surviving state — and `state_root` is
  # node-local, never assumed shared. Pull it before replaying, off-process:
  # this runs inside ProcessHub's migration call and the copy is bounded by
  # file size, not by anything this process should block on.
  defp adopt_source_state(
         %__MODULE__{runner_id: runner_id} = state,
         source_node
       )
       when is_atom(source_node) and source_node != nil and is_binary(runner_id) do
    operative_class = operative_class(state)
    overwatch = self()

    spawn(fn ->
      if WalTransfer.pull_operative(source_node, operative_class) > 0 do
        # The sanctioned restore trigger — the same one the convergence heal
        # reaches through `State.Recovery.resync_for_overwatch/2`. Its
        # `arrived_via_hotswap?` gate is false here precisely because the
        # handover was empty.
        kick_restore_for_bound_runner(overwatch, runner_id)
      end
    end)

    state
  end

  # No source to pull from, or nothing bound yet to replay onto — the ordinary
  # bind-time restore covers the latter once a runner is resolved.
  defp adopt_source_state(%__MODULE__{} = state, _source_node), do: state

  # Busy classification for ProcessHub's migration consent: defer while a
  # correction is pending, while a durable bounded is off an iteration
  # boundary (suspension IS the safe barrier), or while the effectively-once
  # outbox still owes output that would strand on this node. The synchronous
  # correction transaction needs no clause — mid-run this process cannot
  # answer, and consent's no-reply timeout defers it.
  @impl ProcessHub.Strategy.Migration.MigrationConsent
  def migration_consent(%__MODULE__{} = state) do
    cond do
      state.corrections != %{} -> :defer
      state.durable_bounded and not state.suspended -> :defer
      state.effectively_once and outbox_pending?(state) -> :defer
      true -> :ready
    end
  end

  # Bounded-time and fail-open: a query failure yields `:ready` — worst case
  # is pre-consent behaviour, never a wedged migration.
  defp outbox_pending?(%__MODULE__{} = state) do
    operative_class(state)
    |> Flezha.State.Supervisor.live_writers()
    |> Enum.any?(fn pid ->
      match?(%{pending: p} when p > 0, Flezha.State.WalWriter.outbox_status(pid, 1_000))
    end)
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  # A deferred migration re-queries consent on its own retry tick; this only
  # makes the move prompt. Any failure — hub down mid-shutdown, nothing
  # deferred — is a no-op.
  defp signal_migration_ready(%__MODULE__{} = state) do
    _ =
      ProcessHub.Service.Migration.migration_ready(
        Identificator.hub(state.namespace),
        state.id
      )

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  # Time a known-blocking procedure and surface a `[Overwatch] proc_slow` warning
  # when it runs long. The last logged `proc_begin` with no matching `proc_end`
  # pinpoints exactly which procedure wedged the GenServer. `try/after` (never
  # `catch`) so the span is recorded even on exit/throw while the failure still
  # propagates unchanged (`send_control`'s own `:exit` catch must still fire).
  defp with_proc_span(proc, operative_id, fun) do
    started = System.monotonic_time(:millisecond)
    Logger.debug("[Overwatch] proc_begin proc=#{proc} operative=#{operative_id}")

    try do
      fun.()
    after
      ms = System.monotonic_time(:millisecond) - started

      if ms >= proc_warn_ms() do
        Logger.warning(
          "[Overwatch] proc_slow proc=#{proc} operative=#{operative_id} duration_ms=#{ms}"
        )
      else
        Logger.debug(
          "[Overwatch] proc_end proc=#{proc} operative=#{operative_id} duration_ms=#{ms}"
        )
      end
    end
  end

  defp proc_warn_ms,
    do: Application.get_env(:flezha, :overwatch_proc_warn_ms, 2_000)

  # A wedged control unit (waiting on a reply the dead transport will never
  # deliver) must surface as a clean error, never an exit that crashes this
  # operative — `ControlUnit.send_control_await/2` catches the exit; here we add
  # the operative-facing "not available yet" message for a control unit that has
  # not been started, which the callers log/park on.
  defp send_control(runner_id, command, opts \\ []) do
    case GenServer.whereis(Identificator.control_unit(runner_id)) do
      nil ->
        {:error, "control unit not available for runner #{runner_id} with payload"}

      _ ->
        ControlUnit.send_control_await(runner_id, command, opts)
    end
  end

  # One `EntityState` round-trip, off the GenServer loop so a slow reply cannot
  # back up the mailbox; `from` is answered when it completes.
  #
  # An Entity that is not held in memory is refused, never served from the
  # durable store: the controller cannot decode the SDK's WAL bytes, and a
  # fallback would mean binding a runner and restoring an Entity as a side
  # effect of a read. `flz state at` is the durable-read path.
  defp read_entity_state(%__MODULE__{} = state, from, selector) do
    cond do
      not PersistenceResolver.persistent_state?(state.builder.kind, operative_class(state)) ->
        # The kind has no per-entity state; fail cleanly without the round-trip.
        {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, state}

      state.suspended ->
        {:reply, {:error, {:not_resident, :suspended}}, state}

      is_nil(state.runner_id) ->
        {:reply, {:error, {:not_resident, :unbound}}, state}

      true ->
        operative_id = state.id
        runner_id = state.runner_id

        command = %RunnerCommands.EntityState{
          operative_id: operative_id,
          procedure: Map.get(selector, :procedure) || "",
          entity_key: Map.get(selector, :entity_key) || ""
        }

        spawn(fn ->
          result =
            with_proc_span(:entity_state, operative_id, fn ->
              send_control(runner_id, {:entity_state, command}, metadata: true)
            end)

          GenServer.reply(from, with_change_seq(result))
        end)

        {:noreply, state}
    end
  end

  # `{:ok, json, metadata}` (a metadata-carrying reply) becomes
  # `{:ok, json, change_seq}`; every other shape passes through.
  defp with_change_seq({:ok, json, metadata}) when is_map(metadata) do
    {:ok, json, parse_int(Map.get(metadata, @change_seq_meta_key))}
  end

  defp with_change_seq(result), do: result

  defp start_operative_sdk(runner_id, operative) do
    with_proc_span(:start_operative, operative.id, fn ->
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_operative, operative},
        # 8s wasn't enough when the SDK is on the Java side — Spring/JPA
        # warmup plus the AddOperative control-channel round-trip easily
        # exceeds that during a cold start. 30s gives Spring time to breathe
        # without masking genuine stalls.
        30_000
      )
    end)
  end

  defp attach_channel(operative_id, runner_id) do
    operative_int = String.to_charlist(operative_id) |> Enum.sum()
    count = ConfigManager.concurrent_channel_count()
    channel_index = rem(operative_int, count)

    StreamSup.stream_ids(runner_id, :primary) |> Enum.at(channel_index)
  end

  defp serialize_operative_sdk(runner_id, operative_id) do
    command = %RunnerCommands.MigrationSerialization{operative_id: operative_id}

    result = send_control(runner_id, {:migration_serialization, command})

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

  # ---------- Per-Entity persistence wiring ----------
  #
  # Helpers used by `Flezha.State.*` and tests to ship persistence
  # control commands to the SDK. Each takes the controller-side
  # `runner_id` and a structured `entity_id` triple
  # `{operative_class, procedure_id, entity_key}`.

  @doc """
  Ship a `RestoreInitState` command to the SDK and return the controller's
  freshly assembled payload. The caller is expected to await the
  matching `RestoreInitStateResponse` over the control channel before
  unblocking input streaming for the Entity.
  """
  @spec dispatch_restore_init_state(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_restore_init_state(runner_id, entity_id) do
    case restore_command(entity_id) do
      {:ok, command} -> ControlChannel.send_control(runner_id, command)
    end
  end

  defp restore_command({rc, sh, pk}) do
    case Restore.assemble_payload({rc, sh, pk}) do
      {:ok, payload} ->
        entity_id_bytes = Wire.encode_entity_id(rc, sh, pk)
        markers = Flezha.State.Registry.compensation_markers({rc, sh, pk})

        {:ok,
         {:restore_init_state,
          %RunnerCommands.RestoreInitState{
            entity_id: entity_id_bytes,
            snapshot_bytes: payload.snapshot_bytes,
            change_log_bytes: payload.change_log_bytes,
            has_compensation_intent: markers.intent,
            has_compensation_done: markers.done
          }}}
    end
  end

  @doc """
  Bounded proc-journal crash-resume: the instance's committed proc state, threaded
  onto `AddOperative.proc_journal` so the resumed driver skips the journaled
  procedures, seeds the next one from the journaled handoff, and enters the
  iteration after the last committed one.

  It rides the bind rather than a per-Entity `RestoreInitState` because it is
  instance-scoped: a durable bounded class with no per-Entity state ships no
  restore at all, and would otherwise resume at iteration 0 on every wake.

  `nil` when the instance has no journal — a fresh spawn, an unbounded
  operative, an opted-out class.
  """
  @spec proc_journal_state(binary()) :: RunnerCommands.ProcJournalState.t() | nil
  def proc_journal_state(instance_id) do
    case Flezha.State.ProcJournal.restore_state(instance_id) do
      nil ->
        nil

      %{} = resume ->
        %RunnerCommands.ProcJournalState{
          committed_proc_idxs: resume.committed_proc_idxs,
          iter: resume.iter,
          # The previous iteration's terminal batch — what re-seeds iteration
          # N+1 of a handoff-chained loop.
          handoff: handoff_items(resume.handoff),
          # What each procedure of the in-flight iteration released. A branching
          # tree re-seeds every uncommitted child from its own parent's edge, so
          # each in-flight commit's handoff travels, not just the latest one's.
          proc_handoffs:
            Enum.map(resume.proc_handoffs, fn %{proc_idx: proc_idx, handoff: handoff} ->
              %RunnerCommands.ProcEdgeHandoff{
                proc_idx: proc_idx,
                handoff: handoff_items(handoff)
              }
            end),
          # Every forward proc commit with its iteration: the driver folds these
          # back into the compensation planner's per-iteration `committed` input,
          # which is what makes a rollback span journaled iterations (and what
          # rebuilds the in-flight iteration's completed stages on resume).
          committed_procs: Enum.map(resume.committed_procs, &committed_proc/1),
          # The procedures a rewind unwound and nothing has compensated yet. The
          # driver plans these through the ordinary compensation planner before
          # resuming forward.
          rewind_undo: Enum.map(resume.rewind_undo, &committed_proc/1),
          consumed_offset: resume.consumed_offset,
          parked: resume.parked,
          wake_deadline_ms: resume.wake_deadline_ms || 0
        }
    end
  end

  defp committed_proc(proc) do
    %RunnerCommands.ProcCommittedProc{
      proc_idx: proc.proc_idx,
      iter: proc.iter,
      entities_ran: proc.entities_ran
    }
  end

  defp handoff_items(handoff) do
    Enum.map(handoff, fn {cid, bytes} ->
      %RunnerCommands.ProcHandoffItem{channel_id: cid, payload: bytes}
    end)
  end

  # Bound on the CompactEntityResponse round-trip. Generous because the SDK
  # serializes the whole Entity State under its lock before replying.
  @compact_response_timeout_ms 30_000

  @doc """
  Ship a `CompactEntity` command to the SDK. The SDK responds with a
  `entity_state_snapshot` data frame (handled by
  `Flezha.Runner.StreamChannel`) followed by a
  `CompactEntityResponse` control reply. The reply is inspected off-process:
  a `{snapshot_seq: 0, snapshot_bytes_size: 0}` answer against a non-empty WAL
  is recorded as a failed handshake rather than a completion.
  """
  @spec dispatch_compact_entity(binary(), {binary(), binary(), binary()}) ::
          {:ok, term()} | {:error, term()}
  def dispatch_compact_entity(runner_id, {rc, sh, pk} = entity_id) do
    entity_id_bytes = Wire.encode_entity_id(rc, sh, pk)

    command =
      {:compact_entity, %RunnerCommands.CompactEntity{entity_id: entity_id_bytes}}

    case GenServer.whereis(Identificator.control_unit(runner_id)) do
      nil ->
        # No request/reply layer available (isolated unit tests): fire the
        # command directly. Stub detection still rides the timeout → backoff →
        # give-up path, so the missing inspection is not a coverage gap.
        case ControlChannel.send_control(runner_id, command) do
          {:ok, _ref} = ok ->
            Flezha.State.Watchdog.compaction_started(entity_id, runner_id)
            ok

          other ->
            other
        end

      _ ->
        # Bound the handshake: a snapshot that never arrives is timed out by the
        # Watchdog rather than left to wedge persistence.
        Flezha.State.Watchdog.compaction_started(entity_id, runner_id)

        # Inspect the confirming CompactEntityResponse off the caller's process:
        # serializing a large State under the entity lock can be slow, and
        # blocking the Watchdog sweep / WalWriter baseline pull on the round-trip
        # would stall unrelated entities. The `ControlUnit` request/reply sends
        # ONE command (via `send_control` internally) and awaits the reply. The
        # snapshot itself rides the data stream (driving `compaction_completed`);
        # this only catches a stub reply — a snapshot the SDK never ships —
        # sooner than the timeout sweep would.
        spawn(fn -> inspect_compact_response(runner_id, entity_id, command) end)
        {:ok, :dispatched}
    end
  end

  # A `CompactEntityResponse{snapshot_seq: 0, snapshot_bytes_size: 0}` means the
  # SDK produced no snapshot. Against a non-empty WAL that is a FAILED handshake
  # (a stub build, or an Entity the runner cannot address), never a completion —
  # record it so the strike/backoff path runs instead of waiting out the timeout.
  # Against an empty WAL there was genuinely nothing to snapshot, so it is not an
  # error.
  defp inspect_compact_response(runner_id, {rc, sh, pk} = entity_id, command) do
    with {:ok, raw_data} <-
           ControlUnit.send_control_await(runner_id, command,
             timeout: @compact_response_timeout_ms
           ),
         {:ok, %RunnerCommands.CompactEntityResponse{snapshot_seq: 0, snapshot_bytes_size: 0}} <-
           RunnerCommands.CompactEntityResponse.decode(raw_data),
         true <- Restore.has_persisted_state?(entity_id) do
      :telemetry.execute(
        [:flezha, :state, :compaction_failed],
        %{},
        %{operative: rc, procedure: sh, key: pk, runner: runner_id, reason: :no_snapshot}
      )

      Flezha.State.Watchdog.compaction_failed(entity_id)
    else
      {:error, reason} ->
        Logger.warning(
          "[Overwatch] compact_entity response inspection failed " <>
            "entity=#{inspect(entity_id)}: #{inspect(reason)}"
        )

      _ ->
        # A real snapshot (non-zero seq/size) or an empty-WAL no-op: the
        # data-stream frame path handles completion.
        :ok
    end
  end

  @doc """
  Ship an `EntityMailboxStatus` notification to the SDK so the
  producing Procedure's `OverflowPolicy` machinery can park or unpark
  emits to this Entity. `status` is `:full` or `:open`.
  """
  @spec notify_entity_backpressure({binary(), binary(), binary(), binary()}, :full | :open) ::
          :ok | {:error, term()}
  def notify_entity_backpressure({runner_id, rc, sh, pk}, status)
      when status in [:full, :open] do
    entity_id_bytes = Wire.encode_entity_id(rc, sh, pk)

    command =
      {:entity_mailbox_status,
       %RunnerCommands.EntityMailboxStatus{
         entity_id: entity_id_bytes,
         status: Atom.to_string(status)
       }}

    ControlChannel.send_control_async(runner_id, command)
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

  defp spawn_migrate_task({:unpark, state, new_runner_id}) do
    operative = state.builder
    parent = self()

    task =
      Task.Supervisor.async_nolink(Flezha.Operative.MessageHandlerTaskSup, fn ->
        # complete_init is the heavy bounded-retry path; running it in
        # a task ensures `:stop_operative` is never blocked by it.
        case complete_init(state, new_runner_id, operative, parent) do
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

  # `complete_init` ran inside the un-park task, whose `self()` is not this
  # process, so the binding is published here — the one point every un-park
  # caller funnels through, `rehydrate/1` included.
  defp apply_migrate_result(state, {:unparked, recovered}) do
    Logger.info("[Overwatch] operative #{state.id} un-parked onto runner #{recovered.runner_id}")

    publish_binding(recovered)

    %{recovered | migrate_task: nil}
  end

  defp apply_migrate_result(_state, {:park, parked, err}) do
    %{park(parked, err) | migrate_task: nil}
  end
end

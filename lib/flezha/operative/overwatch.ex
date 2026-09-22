defmodule Flezha.Operative.Overwatch do
  @moduledoc """
  The controller-side process that owns one operative instance.

  It holds the operative's state, its plugin extension table and its route to a
  runner. Everything it does is routing: a callback decides which phase a
  message belongs to and hands the state to the module that owns that phase —
  `Lifecycle`, `Migration`, `Ingest`, `Replay`, `Handover`, `Plugins`,
  `ClassRegistry`, `State.Commands`.

  Invariant: this process is the single owner of its operative's input. Every
  `:io_in` — cast, call, confirming, replayed or seeded — runs `Ingest.ingest/4`
  and nothing else does. That is what makes the append-before-forward contract
  hold and what lets the correction transaction assume no input can be
  journaled while it runs.
  """

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Operative.Handover
  alias Flezha.Operative.Ingest
  alias Flezha.Operative.Lifecycle
  alias Flezha.Operative.Migration
  alias Flezha.Operative.Plugins
  alias Flezha.Operative.Replay
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.StreamChannel
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.State.Outbox
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.TaskJournal
  alias Flezha.Transport.LivenessCounters

  # Reply-frame metadata key carrying the `change_seq` a single selected Cell
  # was serialized at (stamped by both SDKs' `CellState` handler). Spelled
  # here rather than read from `Flezha.SDK.Wire`, which owns the wire's other
  # `flz.` keys: `operative` ranks below `state`, so naming it would be a new
  # upward reference `scripts/check-layer-direction.sh` refuses.
  @change_seq_meta_key "flz.change_seq"

  require Logger

  use GenServer
  use Flezha.Process.Timer, keys: [:wake, :correction]
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
    # The un-park `:runner_migrate` runs in a `Task.Supervisor.async_nolink/3`
    # task so it never blocks the Overwatch; this field carries the in-flight
    # task's reference + pid so `terminate/2` can kill it cleanly. `nil`
    # when no migrate is in flight.
    migrate_task: nil,
    # Effectively-once tier, declared by the SDK in the AddOperative response
    # metadata: one of the two ways an instance has durable input, which gates
    # the input-log journal and the `flz.offset` stamp on `:io_in`.
    effectively_once: false,
    # Input-log appends in flight (`Ingest.journal_reply/2`): each request's
    # label carries the reply it owes and the payload to forward.
    journal_reqids: :gen_server.reqids_new(),
    # Durable workflow class (kind + per-class persistence): the other way an
    # instance has durable input, and the gate on the suspension lifecycle
    # (`workflow-durable-suspension`).
    durable_workflow: false,
    # Suspended at a committed iteration boundary: zero live runner resources,
    # registry entry retained. A new input-log append — or the journaled wake
    # deadline — rehydrates through the normal bind path.
    suspended: false,
    # Timer ref for the journaled wake deadline while suspended.
    timers: %{},
    # A park that landed while an un-park task was binding: the task's bound
    # state is stale by the time it reports, and the journaled tail still
    # owes a wake.
    parked_in_flight: false,
    # Late-input correction window (ms), declared by the SDK. `nil` = the class
    # never corrects. Gates lateness handling and the event-time-ordered replay.
    correction_window_ms: nil,
    # Cells with a correction in flight, and the oldest event time each was
    # asked to correct back to. Concurrent requests for one Cell coalesce
    # into the one replay that reaches furthest back.
    corrections: %{}
  ]

  @type t :: %__MODULE__{}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @doc """
  The operative-class identity used for per-Cell state (defaults to the
  instance id, per the SDK convention that cell frames carry the operative_id).
  """
  @spec operative_class(t()) :: binary()
  def operative_class(%__MODULE__{} = state),
    do: Keyword.get(state.builder.opts, :operative_class, state.id)

  @doc """
  Push one payload at the bound stream channel. The single statement of the
  frame shape a delivery takes, shared by a fresh ingest and by both durability
  tiers' re-forward. An unbound or absent stream answers `{:error, :no_stream}`,
  counted and emitted by the channel itself so a stall stays observable; a send
  buffer that never drained under a held sender answers
  `{:error, :send_buffer_full}`, with the frame unsent and nothing discarded.
  """
  @spec deliver(t(), {binary(), map()}) :: :ok | {:error, :no_stream | :send_buffer_full}
  def deliver(%__MODULE__{} = state, payload),
    do: StreamChannel.io_in(state.channel, state.id, payload)

  # Every plugin verb is one bounded call into a running Overwatch. The bound
  # belongs here rather than to each caller: an Overwatch busy folding an entry
  # is the normal case, and a caller that inherits its exit takes down a
  # node-wide service (`PluginManager`) or a convergence tick.

  @doc """
  Subscribe `plugin` to a running Overwatch — the client side of
  `handle_call({:plugin_subscribe, …})`.
  """
  @spec plugin_subscribe(pid(), binary(), Plugin.t()) :: :ok | {:error, term()}
  def plugin_subscribe(pid, plugin_id, %Plugin{} = plugin),
    do: plugin_call(pid, {:plugin_subscribe, plugin_id, plugin})

  @doc "Drop `plugin_id`'s subscription from a running Overwatch."
  @spec plugin_unsubscribe(pid(), binary()) :: :ok | {:error, term()}
  def plugin_unsubscribe(pid, plugin_id),
    do: plugin_call(pid, {:plugin_unsubscribe, plugin_id})

  @doc "Re-subscribe `plugin_id` on a running Overwatch, replacing its instance."
  @spec plugin_restart(pid(), binary(), Plugin.t()) :: :ok | {:error, term()}
  def plugin_restart(pid, plugin_id, %Plugin{} = plugin),
    do: plugin_call(pid, {:plugin_restart, plugin_id, plugin})

  defp plugin_call(pid, request) do
    case SafeCall.bounded(pid, request, Durations.get(:plugin_call_timeout_ms)) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, _} = err -> err
    end
  end

  @doc """
  Time a known-blocking tasklet and surface a `[Overwatch] task_slow` warning
  when it runs long. The last logged `task_begin` with no matching `task_end`
  pinpoints exactly which tasklet wedged the GenServer.

  `try/after` (never `catch`) so the span is recorded even on exit/throw while
  the failure still propagates unchanged — `ControlUnit.send_control_await/3`'s
  own `:exit` catch must still fire.
  """
  @spec with_task_span(atom(), binary(), (-> result)) :: result when result: term()
  def with_task_span(task, operative_id, fun) do
    started = System.monotonic_time(:millisecond)
    Logger.debug("[Overwatch] task_begin task=#{task} operative=#{operative_id}")

    try do
      fun.()
    after
      ms = System.monotonic_time(:millisecond) - started

      if ms >= task_warn_ms() do
        Logger.warning(
          "[Overwatch] task_slow task=#{task} operative=#{operative_id} duration_ms=#{ms}"
        )
      else
        Logger.debug(
          "[Overwatch] task_end task=#{task} operative=#{operative_id} duration_ms=#{ms}"
        )
      end
    end
  end

  defp task_warn_ms,
    do: Durations.get(:overwatch_task_warn_ms)

  # ---------------------------------------------------------------
  # Start-up
  # ---------------------------------------------------------------

  @impl true
  def init({namespace, stale_runner_id, operative}) do
    Process.flag(:trap_exit, true)

    # IMPORTANT: init/1 must return quickly so the `dist_sup` supervisor
    # tree (which calls us synchronously from `DistributedSupervisor.init/1`
    # during the DETS replay) can unblock. Any GenServer.call to
    # FleetManager / OperativeManager from inside init/1 deadlocks because
    # FleetManager is itself waiting on :default_hub, which is waiting
    # for dist_sup.init to complete. All blocking work is deferred to
    # handle_continue — by then init has returned and the hub has finished
    # initialization.
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
    # Claim the class's durable-state namespace BEFORE any path below
    # (journal probe, restore, WAL open) resolves: writes must land under
    # this operative's namespace, and a class owned by another namespace
    # must never be silently read or overwritten.
    with :ok <- one_state_key(state),
         :ok <- Paths.claim_namespace(state.id, state.namespace) do
      continue_bind(stale_runner_id, operative, state)
    else
      {:error, {:namespace_conflict, _class, owner}} ->
        Logger.error(
          "[overwatch] refusing to bind #{state.id} in namespace #{state.namespace}: " <>
            "its durable state belongs to namespace #{owner}"
        )

        {:stop, {:shutdown, {:namespace_conflict, state.id, owner}}, state}

      {:error, {:split_state_key, class}} ->
        Logger.error(
          "[overwatch] refusing to bind workflow #{state.id}: its state key is its " <>
            "instance id, not #{inspect(class)}"
        )

        {:stop, {:shutdown, {:split_state_key, state.id, class}}, state}
    end
  end

  # A workflow's journal, Cells and input log are all keyed by its instance id,
  # so an `:operative_class` naming anything else would split them: it is
  # refused, not ignored.
  defp one_state_key(%__MODULE__{builder: %{kind: :workflow}} = state) do
    case operative_class(state) do
      id when id == state.id -> :ok
      class -> {:error, {:split_state_key, class}}
    end
  end

  defp one_state_key(%__MODULE__{}), do: :ok

  defp continue_bind(stale_runner_id, operative, %__MODULE__{} = state) do
    state = %__MODULE__{state | durable_workflow: Migration.durable_workflow?(state, operative)}

    # Suspended instances are rediscovered from durable state alone (journal +
    # input log), so a controller redeploy neither binds them to a runner nor
    # loses their suspension (`workflow-durable-suspension`). An instance with
    # undelivered input is not suspended any more — it binds and consumes it.
    # The journal's floor is seeded first, since the log answers that question
    # against it.
    if state.durable_workflow, do: TaskJournal.release_input(operative.id)

    if state.durable_workflow and Migration.suspended_on_disk?(operative.id) do
      {:noreply, Migration.enter_suspended(state, TaskJournal.restore_state(operative.id)),
       :hibernate}
    else
      Lifecycle.bind(state, stale_runner_id, operative)
    end
    |> seed_input(operative)
  end

  # The instance's first input (a tasklet-requested spawn's payload, or the
  # message `flz operative start --text` and a schedule carry), ingested with
  # its metadata (the entry channel it names) inside the bind continue, which
  # is what makes it first: no other message in the mailbox — including a
  # concurrent requester's delivery — can be ingested ahead of it. A durable
  # instance whose inbox already holds entries is a restart rather than a
  # spawn; its payload is journaled, so re-seeding it would duplicate.
  defp seed_input({:noreply, %__MODULE__{} = state} = result, operative) do
    case Keyword.get(operative.opts, :seed_input) do
      {message, metadata}
      when is_binary(message) and is_map(metadata) and is_nil(state.init_error) ->
        if Ingest.first_input?(state) do
          {_verdict, state} = Ingest.confirming_ingest(state, message, metadata, :cast)
          {:noreply, state}
        else
          result
        end

      _ ->
        result
    end
  end

  defp seed_input(result, _operative), do: result

  @impl true
  def terminate(reason, state), do: Migration.terminate(state, reason)

  # ---------------------------------------------------------------
  # ProcessHub migration callbacks
  # ---------------------------------------------------------------

  @impl true
  def prepare_handover_state(state), do: Handover.prepare(state)

  @impl true
  def alter_handover_state(%__MODULE__{} = state, handover), do: Handover.absorb(state, handover)

  @impl ProcessHub.Strategy.Migration.MigrationConsent
  def migration_consent(%__MODULE__{} = state), do: Handover.consent(state)

  # ---------------------------------------------------------------
  # handle_call
  # ---------------------------------------------------------------

  def handle_call(:registry_runner_set, _from, %__MODULE__{init_error: nil} = state) do
    HubManager.operative_set_runner(state.namespace, state.id, state.runner_id)
    {:reply, {:ok, state.runner_id}, state}
  end

  def handle_call(:registry_runner_set, _from, %__MODULE__{init_error: reason} = state) do
    {:reply, {:error, reason}, state}
  end

  @impl true
  def handle_call({:plugin_subscribe, plugin_id, %Plugin{} = plugin}, _from, state) do
    case Plugins.subscribe(state, plugin_id, plugin) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:plugin_unsubscribe, plugin_id}, _from, %__MODULE__{} = state) do
    table = Plugins.unregister(Table.unsubscribe(state.extensions, plugin_id))
    {:reply, :ok, %{state | extensions: table}}
  end

  @impl true
  def handle_call({:plugin_restart, plugin_id, %Plugin{} = plugin}, _from, state) do
    case Plugins.restart(state, plugin_id, plugin) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:get_runner, _from, %__MODULE__{runner_id: runner} = state) do
    {:reply, runner, state}
  end

  @impl true
  def handle_call(:get_overwatch_state, _from, state) do
    {:reply, state, state}
  end

  # Live read of Cell state, narrowed by `selector` (`%{}` = every Tasklet,
  # the migration/handover shape). Always answered as `{:ok, json, change_seq}`
  # — the seq rides the reply frame's metadata, so the JSON body stays the shape
  # the whole-Operative caller already decodes; `OperativeManager` drops the seq
  # for callers that predate it.
  @impl true
  def handle_call({:get_cell_state, selector}, from, %__MODULE__{} = state)
      when is_map(selector) do
    read_cell_state(state, from, selector)
  end

  @impl true
  def handle_call({:runner_migrate, new_runner_id}, _from, %__MODULE__{} = state),
    do: Migration.runner_migrate(state, new_runner_id)

  # The convergence loop's `publish_binding`: re-write the durable row and the
  # runner's route from the binding this process holds. Must run here — the
  # route points at `self()`.
  @impl true
  def handle_call(:publish_binding, _from, %__MODULE__{runner_id: runner_id} = state)
      when is_binary(runner_id) do
    {:reply, :ok, Lifecycle.publish_binding(state, nil)}
  end

  def handle_call(:publish_binding, _from, state), do: {:reply, {:error, :unbound}, state}

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}

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

  # Confirming ingest (`operative-to-operative-messaging`): the same single
  # pre-delivery path as the `{:io_in, _, _}` cast, answering what happened so a
  # forwarding sink can tell "done" from "retry". The cast keeps its
  # fire-and-forget shape, including its silent drop for a parked operative.
  @impl true
  def handle_call({:ingest, _message, _metadata}, _from, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    {:reply, {:error, :not_ready}, state}
  end

  # Both blocking entry points: the reply is owed until the journal answers, so
  # a source parked at zero credit is parked on its own call, never on this
  # process.
  @impl true
  def handle_call({entry_point, message, metadata}, from, state)
      when entry_point in [:io_in, :ingest] do
    case Ingest.confirming_ingest(state, message, metadata, {entry_point, from}) do
      {:pending, state} -> {:noreply, state}
      {reply, state} -> {:reply, reply, state}
    end
  end

  @impl true
  def handle_call(:create_snapshot, _from, %__MODULE__{} = state),
    do: {:reply, {:ok, Handover.create_snapshot(state.id, state.runner_id)}, state}

  @impl true
  def handle_call({:replay_from, cell_id, baseline_seq}, _from, %__MODULE__{} = state) do
    {:reply, Replay.run_replay(state, cell_id, baseline_seq), state}
  end

  @impl true
  def handle_call({:rewind, to_iter}, _from, %__MODULE__{} = state) do
    case Migration.rewind_to(state, to_iter) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # ---------------------------------------------------------------
  # handle_cast
  # ---------------------------------------------------------------

  # Parked: drop ingress casts. A sender that needs the payload held rather than
  # dropped uses the confirming entry point, which answers `{:error, :not_ready}`
  # instead. The drop is silent to the sender by contract but never unrecorded:
  # `parked_drops` is the only handle a cast-based source's loss has while the
  # operative stays parked.
  @impl true
  def handle_cast({:io_in, _message, _metadata}, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    LivenessCounters.bump_operative(state.liveness_ref, :parked_drops)
    {:noreply, state}
  end

  # The same path as the calls with nobody to answer. Suspended: the input is
  # durable in the input log, so it is not lost by not being forwarded now —
  # the rehydrating bind delivers the whole undelivered tail.
  @impl true
  def handle_cast({:io_in, message, metadata}, state) do
    {_verdict, state} = Ingest.confirming_ingest(state, message, metadata, :cast)
    {:noreply, state}
  end

  # A durable outbox flush: one dispatch pass's whole batch. Each output's chain
  # run settles the entry it was dispatched under, carrying the holds its sinks
  # took — so an output no delivery-guaranteed sink took is complete the moment
  # the chain has had it, filtered included.
  @impl true
  def handle_cast({:io_out_batch, outputs}, state) do
    Outbox.run(outputs, fn message, metadata ->
      Plugins.transform_io(state, :on_output, message, metadata)
    end)

    {:noreply, state}
  end

  def handle_cast({:io_out, message, metadata}, state) do
    Plugins.transform_io(state, :on_output, message, metadata)
    Outbox.settled(metadata)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:correction_request, cell_id, target}, state) do
    {:noreply, Replay.note_correction(state, cell_id, target)}
  end

  @impl true
  def handle_cast({:plugin_opt_persist, plugin_id, key, value}, %__MODULE__{} = state) do
    {:noreply, Plugins.persist_opt(state, plugin_id, key, value)}
  end

  @impl true
  # A sleeper hibernates: the bind's restore frames and plugin setup leave
  # megabytes of garbage on a heap that otherwise sits uncollected until the
  # next wake (2.5 MiB → 5 KiB per sleeper measured).
  def handle_cast({:suspend, iter, wake_deadline_ms}, %__MODULE__{} = state) do
    {:noreply, Migration.suspend_at(state, iter, wake_deadline_ms), :hibernate}
  end

  # Replaces `state.snapshot` with the SDK's final state on graceful shutdown.
  # A handover ships it and the next running migrate seeds from it, until a
  # runner is bound again: that runner's state supersedes it, so the bind drops
  # it. Cold-boot restart is covered by the per-Cell WAL files instead.
  @impl true
  def handle_cast({:apply_external_snapshot, binary}, %__MODULE__{} = state)
      when is_binary(binary) do
    {:noreply, %__MODULE__{state | snapshot: binary}}
  end

  # An operative that arrived via HotSwap handover already received its state
  # in-band (`alter_handover_state` ships `MigrationDeserialization`); a WAL
  # replay on top would apply every change a second time. Skip — same guard
  # as the bind-time restore dispatch.
  def handle_cast({:kick_restore, _runner_id}, %__MODULE__{arrived_via_hotswap?: true} = state) do
    {:noreply, state}
  end

  # External callers (the convergence Loop's rebind heal) trigger a fresh
  # per-Cell replay against the bound runner without knowing this process's
  # internals. Used by `Flezha.State.Recovery.resync_for_overwatch/2`.
  def handle_cast({:kick_restore, runner_id}, %__MODULE__{} = state) do
    Replay.dispatch_restore(state, state.builder.opts, runner_id)
    {:noreply, state}
  end

  # The bound runner reconnected under a new session with the same stable
  # identity (`Flezha.Runner.FleetManager.Session.swap_bindings/3`).
  def handle_cast({:session_replaced, old_session, new_session}, %__MODULE__{} = state),
    do: {:noreply, Migration.session_replaced(state, old_session, new_session)}

  # ---------------------------------------------------------------
  # handle_info
  # ---------------------------------------------------------------

  # Parked: not serving, and the un-park's own bind re-forwards the tail. A
  # re-forward here would push input at the runner the operative is parked away
  # from — the same reason the ingress cast is dropped while parked.
  @impl true
  def handle_info({:stream_bound, _channel}, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    {:noreply, state}
  end

  # The operative's data stream bound. Anything its durable logs still hold above
  # the Cells' consumed offsets was issued onto a stream that could not carry
  # it — re-forward it now, on the replay path a restore uses. Which channel
  # announced the bind does not matter: the tail is owed either way, and it goes
  # out on whichever channel this operative is bound to now.
  @impl true
  def handle_info({:stream_bound, _channel}, %__MODULE__{} = state) do
    {:noreply, Replay.reforward_undelivered_tail(state)}
  end

  # A bind-time restore the SDK refused, reported from the off-process replay.
  # The Cell's durable state never reached the runner, so the operative parks
  # rather than serving from empty state; the convergence loop rebinds it.
  @impl true
  def handle_info({:restore_refused, reason}, %__MODULE__{} = state) do
    {:noreply, Lifecycle.park(state, {:restore_refused, reason})}
  end

  @impl true
  def handle_info({:run_correction, cell_id}, %__MODULE__{} = state) do
    {target, corrections} = Map.pop(state.corrections, cell_id)
    state = %{state | corrections: corrections}

    # At most one correction per Cell is ever in flight, by construction: the
    # transaction runs to completion inside this handler, so a request arriving
    # during it simply waits in the mailbox and debounces into the next one.
    state = if target, do: Replay.run_correction(state, cell_id, target), else: state
    if state.corrections == %{}, do: Migration.signal_migration_ready(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:extension_timer, name, plugin_id}, %__MODULE__{} = state) do
    {:noreply, %{state | extensions: Table.fire_timer(state.extensions, name, plugin_id)}}
  end

  # The journaled wake deadline elapsed (`workflow-durable-suspension`): append a
  # timer wake to the input log and rehydrate. Routing the wake THROUGH the log
  # is what makes two racing wake sources — a message and the deadline —
  # collapse into one rehydration: the second is a no-op on an
  # already-rehydrating instance, and the loop simply reads whatever the log
  # holds. The wake item carries an empty payload, which the class's entry
  # channel decodes as its tick.
  @impl true
  def handle_info(:wake_deadline, %__MODULE__{} = state) do
    :ok = Ingest.journal_wake(state)
    {:noreply, Migration.rehydrate(timer_fired(state, :wake))}
  end

  # The in-flight `:runner_migrate` task posts its result via a plain `send/2`
  # before the task process exits, so this clause runs before the `:DOWN`
  # cleanup. The state update happens here; the `:DOWN` message is then drained
  # by the dedicated clause below.
  @impl true
  def handle_info(
        {:migrate_task_result, _task_pid, result},
        %__MODULE__{migrate_task: %{}} = state
      ) do
    {:noreply, Migration.apply_migrate_result(state, result)}
  end

  # Late delivery: the task was terminated by a teardown, or a second
  # migrate already replaced `state.migrate_task`. Drop.
  def handle_info({:migrate_task_result, _task_pid, _result}, state), do: {:noreply, state}

  # The task has exited normally (or crashed before posting a result). Clear the
  # migrate_task slot so the next migrate can spawn.
  @impl true
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %__MODULE__{migrate_task: %{ref: ref}} = state
      ) do
    {:noreply, Migration.rehydrate_if_pending(%{state | migrate_task: nil})}
  end

  # Task.Supervisor.async_nolink also sends `{ref, return_value}` from the task
  # before it exits — the result was already consumed via the explicit
  # `{:migrate_task_result, ...}` send/2 from inside the task body, so the
  # auto-reply is redundant. Drop it; without this clause it crashes the
  # Overwatch with FunctionClauseError.
  @impl true
  def handle_info({ref, _value}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, state}
  end

  # A monitored input log that exits with an append in flight reports it as a
  # `:DOWN` the request collection recognises; every other `:DOWN` is a plugin's.
  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason} = msg, %__MODULE__{} = state) do
    case Ingest.journal_reply(state, msg) do
      {:ok, state} -> {:noreply, state}
      :ignore -> {:noreply, plugin_down(state, ref, pid, reason)}
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
  def handle_info({:flezha_request, handler_key, payload, reply_to, ref}, state) do
    Plugins.handle_request(state, handler_key, payload, reply_to, ref)
    {:noreply, state}
  end

  # The input log's answer to an append, in the request's own (opaque) message
  # shape — the one message this process expects without naming.
  @impl true
  def handle_info(msg, %__MODULE__{} = state) do
    case Ingest.journal_reply(state, msg) do
      {:ok, state} ->
        {:noreply, state}

      :ignore ->
        Logger.warning("[Overwatch] #{state.id} dropped an unexpected message: #{inspect(msg)}")
        {:noreply, state}
    end
  end

  # A crashed subscriber is recorded by the table and declared here: the table
  # is a data structure and decides nothing, and the restart is the loop's.
  defp plugin_down(%__MODULE__{} = state, ref, pid, reason) do
    case Table.handle_down(state.extensions, ref, pid, reason) do
      {:ok, table} ->
        for plugin_id <-
              Map.keys(Table.down_ids(table)) -- Map.keys(Table.down_ids(state.extensions)) do
          Facts.declare({:plugin_down, {state.namespace, state.id, plugin_id}, %{reason: reason}})
        end

        %{state | extensions: table}

      :ignore ->
        Logger.error("Binding not found for pid #{inspect(pid)} with reason: #{inspect(reason)}")
        state
    end
  end

  # ---------------------------------------------------------------
  # Live Cell read
  # ---------------------------------------------------------------

  # One `CellState` round-trip, off the GenServer loop so a slow reply cannot
  # back up the mailbox; `from` is answered when it completes.
  #
  # A Cell that is not held in memory is refused, never served from the
  # durable store: the controller cannot decode the SDK's WAL bytes, and a
  # fallback would mean binding a runner and restoring a Cell as a side
  # effect of a read. `flz state at` is the durable-read path.
  defp read_cell_state(%__MODULE__{} = state, from, selector) do
    cond do
      not PersistenceResolver.persistent_state?(state.builder.kind, operative_class(state)) ->
        {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, state}

      state.suspended ->
        {:reply, {:error, {:not_resident, :suspended}}, state}

      is_nil(state.runner_id) ->
        {:reply, {:error, {:not_resident, :unbound}}, state}

      true ->
        operative_id = state.id
        runner_id = state.runner_id

        command = %RunnerCommands.CellState{
          operative_id: operative_id,
          tasklet: Map.get(selector, :tasklet) || "",
          cell_key: Map.get(selector, :cell_key) || ""
        }

        spawn(fn -> reply_cell_state(from, runner_id, operative_id, command) end)

        {:noreply, state}
    end
  end

  # Off-process: the read is a runner round-trip and the Overwatch must keep
  # serving its own input while it is in flight.
  defp reply_cell_state(from, runner_id, operative_id, command) do
    result =
      with_task_span(:cell_state, operative_id, fn ->
        ControlUnit.send_control_await(runner_id, {:cell_state, command}, metadata: true)
      end)

    GenServer.reply(from, with_change_seq(result))
  end

  # `{:ok, json, metadata}` (a metadata-carrying reply) becomes
  # `{:ok, json, change_seq}`; every other shape passes through.
  defp with_change_seq({:ok, json, metadata}) when is_map(metadata) do
    seq =
      case Integer.parse(Map.get(metadata, @change_seq_meta_key) || "") do
        {n, ""} -> n
        _ -> nil
      end

    {:ok, json, seq}
  end

  defp with_change_seq(result), do: result
end

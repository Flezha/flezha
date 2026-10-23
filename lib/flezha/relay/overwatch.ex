defmodule Flezha.Relay.Overwatch do
  @moduledoc """
  The controller-side process that owns one relay instance.

  It holds the relay's state, its plugin extension table and its route to a
  runner. Everything it does is routing: a callback decides which phase a
  message belongs to and hands the state to the module that owns that phase —
  `Lifecycle`, `Rebind`, `Suspend`, `Ingest`, `Replay`, `Handover`, `Plugins`,
  `ClassRegistry`.

  Invariant: this process is the single owner of its relay's input. Every
  `:io_in` — cast, call, confirming, replayed or seeded — runs `Ingest.ingest/4`
  and nothing else does. That is what makes the append-before-forward contract
  hold and what lets the correction transaction assume no input can be
  journaled while it runs.
  """

  alias Flezha.Config.Durations
  alias Flezha.Relay.Calls.{Inbound, Outbound, Token}
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Handover
  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Plugins
  alias Flezha.Relay.Rebind
  alias Flezha.Relay.Replay
  alias Flezha.Relay.Suspend
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.StreamChannel
  alias Flezha.SDK.SafeCall
  alias Flezha.State.Outbox
  alias Flezha.Transport.LivenessCounters

  # Reply-frame metadata key carrying the `change_seq` a single selected Cell
  # was serialized at (stamped by both SDKs' `CellState` handler). Spelled
  # here rather than read from `Flezha.State.Wire`, which owns the wire's other
  # `flz.` keys: `relay` ranks below `state`, so naming it would be a new
  # upward reference `scripts/check-layer-direction.sh` refuses.
  @change_seq_meta_key "flz.change_seq"
  @reply_to Token.reply_to_key()

  require Logger

  use GenServer
  use Flezha.Process.Timer, keys: [:wake, :correction, :call]
  use ProcessHub.Strategy.Migration.HotSwap
  use ProcessHub.Strategy.Migration.MigrationConsent

  use Flezha.Plugin.Host,
    extensions: [
      {:on_relay_init, :fold, Flezha.Plugin.Extension.Ctx.Init},
      {:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO},
      {:on_output, :transform, Flezha.Plugin.Extension.Ctx.IO},
      {:on_preview, :fanout, Flezha.Plugin.Extension.Ctx.IO},
      {:on_call, :fanout, Flezha.Plugin.Extension.Ctx.IO},
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
    # `:running | :unbound | :halted | :suspended`, written only by the
    # transitions.
    phase: :unbound,
    # The transition under way, `nil` when none is.
    cause: nil,
    # Why the relay halted, kept through a suspension or a lost binding.
    halt_reason: nil,
    # Wall-clock ms at process start, for `flz relay info` uptime. Wall-clock
    # (not monotonic) so the value stays comparable when read from another node.
    started_at: nil,
    # This node's monotonic ms at process start: a Cell writer's dispatch from
    # before it went to an earlier Overwatch (`Replay.redrive_calls/1`).
    started_mono: nil,
    # Liveness counters ref (io_in_received / forwarded / filtered_nil) so a
    # stall can be localised to "WS plugin stopped feeding" vs "relay stopped
    # forwarding to the channel".
    liveness_ref: nil,
    # A bind onto another runner (a rebind, a wake, a restarted runner) runs
    # in a task linked to this process so it never blocks the Overwatch and
    # never outlives it: `%{pid, then, from, journaled}`, the task, what the
    # bind carries, the phase it started from and the input log's last offset
    # then, plus `reforward` once its own re-forward is owed again. `nil` when
    # no bind is in flight.
    bind_task: nil,
    # Outputs that arrived while a bind task was binding, newest first, and
    # whether any went unsettled with no plugin up, for the next bind to redrive.
    held_outputs: [],
    owes_outputs: false,
    # Effectively-once tier, declared by the SDK in the AddRelay response
    # metadata: one of the two ways an instance has durable input, which gates
    # the input-log journal and the `flz.offset` stamp on `:io_in`.
    effectively_once: false,
    # Input-log appends in flight (`Ingest.journal_reply/2`): each request's
    # label carries the reply it owes and the payload to forward.
    journal_reqids: :gen_server.reqids_new(),
    # The settings the last bind announced, for `flz relay info`.
    settings: nil,
    # Durable steps class (run + per-class persistence): the other way an
    # instance has durable input, and the one that journals stage by stage.
    durable_steps: false,
    # The class sleeps when idle, declared by the SDK: its input is journaled
    # so a message that finds it asleep wakes it and waits in the log.
    sleeps: false,
    # Timer ref for the recorded wake deadline while suspended.
    timers: %{},
    # Late-input correction window (ms), declared by the SDK. `nil` = the class
    # never corrects. Gates lateness handling and the event-time-ordered replay.
    correction_window_ms: nil,
    # Cells with a correction in flight, and the oldest event time each was
    # asked to correct back to. Concurrent requests for one Cell coalesce
    # into the one replay that reaches furthest back.
    corrections: %{},
    # The outside callers waiting on this relay for an answer; each row's
    # deadline is armed under the timer family `:call`.
    calls: %Inbound{},
    # Whether this process handed its state to a HotSwap target: it ends as a
    # move, whether or not the relay has a declared entry.
    handed_over?: false,
    # Whether a `:once` or `:chained` steps instance has taken its one
    # input in this process, so a later call is refused; a durable instance
    # also asks its input log (`Ingest.first_input?/1`), which outlives it.
    fed: false,
    # A start's input held for the start's own `:registry_runner_set` call
    # (`Ingest.seed_or_hold/2`), because the input log already holds entries.
    seed: nil
  ]

  @type t :: %__MODULE__{}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @doc """
  The relay-class identity used for per-Cell state (defaults to the
  instance id, per the SDK convention that cell frames carry the relay_id).
  """
  @spec relay_class(t()) :: binary()
  def relay_class(%__MODULE__{} = state),
    do: Keyword.get(state.builder.opts, :relay_class, state.id)

  @doc "The relay's phase; a state from an older controller, without the field, reads `nil`."
  @spec phase(map()) :: :running | :unbound | :halted | :suspended | nil
  def phase(state), do: Map.get(state, :phase)

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

  @doc """
  Who this relay's call table answers for, and the `on_call` listeners its
  calls are announced to: `activity` is `nil` while there are none, so nothing
  is built for them.
  """
  @spec callee(t()) :: Inbound.callee()
  def callee(%__MODULE__{} = state),
    do: %{
      namespace: state.namespace,
      relay_id: state.id,
      activity: Plugins.previewer(state, :on_call)
    }

  # Every plugin verb is one bounded call into a running Overwatch. The bound
  # belongs here rather than to each caller: an Overwatch busy folding an entry
  # is the normal case, and a caller that inherits its exit takes down a
  # node-wide service (`PluginManager`) or a convergence tick.

  @doc """
  Subscribe `plugin` to a running Overwatch — the client side of
  `handle_call({:plugin_subscribe, …})`.
  """
  @spec plugin_subscribe(pid(), binary(), Plugin.t(), Plugin.t() | :none | :read) ::
          :ok | {:error, term()}
  def plugin_subscribe(pid, plugin_id, %Plugin{} = binding, entry \\ :read),
    do: plugin_call(pid, {:plugin_subscribe, plugin_id, binding, entry})

  @doc "Drop `plugin_id`'s subscription from a running Overwatch."
  @spec plugin_unsubscribe(pid(), binary()) :: :ok | {:error, term()}
  def plugin_unsubscribe(pid, plugin_id),
    do: plugin_call(pid, {:plugin_unsubscribe, plugin_id})

  @doc """
  The convergence loop's plugin heal on a running Overwatch: `:start_plugin`
  brings the binding up as `plugin_subscribe/4` does, `:restart_plugin`
  replaces its instance. Answers the phase the call left the binding in, read
  in the same call, which is what the heal is judged by.
  """
  @spec plugin_heal(pid(), :start_plugin | :restart_plugin, binary(), Plugin.t()) ::
          {:ok, Table.phase() | :unknown} | {:error, term()}
  def plugin_heal(pid, primitive, plugin_id, %Plugin{} = plugin)
      when primitive in [:start_plugin, :restart_plugin],
      do: plugin_call(pid, {:plugin_heal, primitive, plugin_id, plugin})

  @doc "Apply a changed plugin entry to `plugin_id`'s binding, `:live` or by a `:restart`."
  @spec plugin_update(pid(), binary(), Plugin.t(), :live | :restart, Plugin.t()) ::
          :ok | {:error, term()}
  def plugin_update(pid, plugin_id, %Plugin{} = binding, mode, %Plugin{} = entry),
    do: plugin_call(pid, {:plugin_update, plugin_id, binding, mode, entry})

  defp plugin_call(pid, request) do
    case SafeCall.bounded(pid, request, Durations.get(:plugin_call_timeout_ms)) do
      {:ok, :ok} -> :ok
      {:ok, {:ok, _phase} = healed} -> healed
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
  def with_task_span(task, relay_id, fun) do
    started = System.monotonic_time(:millisecond)
    Logger.debug("[Overwatch] task_begin task=#{task} relay=#{relay_id}")

    try do
      fun.()
    after
      ms = System.monotonic_time(:millisecond) - started

      if ms >= Durations.get(:overwatch_task_warn_ms) do
        Logger.warning("[Overwatch] task_slow task=#{task} relay=#{relay_id} duration_ms=#{ms}")
      else
        Logger.debug("[Overwatch] task_end task=#{task} relay=#{relay_id} duration_ms=#{ms}")
      end
    end
  end

  # ---------------------------------------------------------------
  # Start-up
  # ---------------------------------------------------------------

  @impl true
  def init({namespace, stale_runner_id, relay}) do
    Process.flag(:trap_exit, true)

    # IMPORTANT: init/1 must return quickly so the `dist_sup` supervisor
    # tree (which calls us synchronously from `DistributedSupervisor.init/1`
    # during the DETS replay) can unblock. Any GenServer.call to
    # FleetManager / RelayManager from inside init/1 deadlocks because
    # FleetManager is itself waiting on :default_hub, which is waiting
    # for dist_sup.init to complete. All blocking work is deferred to
    # handle_continue — by then init has returned and the hub has finished
    # initialization.
    state = %__MODULE__{
      id: relay.id,
      namespace: namespace,
      factory_id: relay.factory_id,
      init_values: relay.init_values,
      runner_id: stale_runner_id,
      default_input_type: Keyword.get(relay.opts, :default_input_type, "2"),
      extensions: Host.relay_table(__extensions_catalog__(), relay.opts),
      builder: relay,
      started_at: System.system_time(:millisecond),
      started_mono: System.monotonic_time(:millisecond),
      liveness_ref: LivenessCounters.register_relay(relay.id)
    }

    Plugins.warn_old_rows(relay)
    {:ok, state, {:continue, {:bind_runner, stale_runner_id, relay}}}
  end

  @impl true
  def handle_continue({:bind_runner, stale_runner_id, relay}, %__MODULE__{} = state),
    do: Lifecycle.bind_continue(state, stale_runner_id, relay)

  @impl true
  def terminate(reason, state), do: Lifecycle.terminate(state, reason)

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

  # A start's held input is taken here and the start answered once it is: a
  # refused call or input fails the start, so its caller is told.
  def handle_call({:registry_runner_set, announce?}, from, %__MODULE__{halt_reason: nil} = state) do
    call = state.seed && elem(state.seed, 2)
    owed = {self(), {:seed_taken, from, announce?, call}}

    case Ingest.seed(%__MODULE__{state | seed: nil, cause: :start}, state.seed, owed) do
      {:pending, state} -> {:noreply, state}
      {verdict, state} -> {:reply, Ingest.started(state, announce?, call, verdict), state}
    end
  end

  def handle_call({:registry_runner_set, _}, _from, %__MODULE__{halt_reason: reason} = state),
    do: {:reply, {:error, reason}, %__MODULE__{state | cause: :start}}

  # The plugin verbs: literal heads, so no other call pays a guard for them.
  @impl true
  def handle_call({:plugin_subscribe, _, _, _} = verb, _from, state),
    do: Plugins.answer(verb, state)

  def handle_call({:plugin_unsubscribe, _} = verb, _from, state), do: Plugins.answer(verb, state)

  def handle_call({:plugin_update, _, _, _, _} = verb, _from, state),
    do: Plugins.answer(verb, state)

  def handle_call({:plugin_heal, _, _, _} = verb, _from, state), do: Plugins.answer(verb, state)

  @impl true
  def handle_call(:get_overwatch_state, _from, state), do: {:reply, state, state}

  # The state without the relay's own data, which can run to any size, so a
  # reader of the relay's facts is never sent its init values, snapshot or
  # held payloads.
  def handle_call(:get_overwatch_facts, _from, state),
    do: {:reply, %{state | init_values: nil, snapshot: nil, held_outputs: nil, seed: nil}, state}

  # A stop's cause, named before the hub stops this process; `nil` undoes it.
  @impl true
  def handle_call({:cause, cause}, from, %__MODULE__{} = state),
    do: handle_call(:get_overwatch_state, from, %{state | cause: cause})

  # Live read of Cell state, narrowed by `selector` (`%{}` = every Tasklet,
  # the migration/handover shape). Always answered as `{:ok, json, change_seq}`
  # — the seq rides the reply frame's metadata, so the JSON body stays the shape
  # the whole-Relay caller already decodes; `RelayManager` drops the seq
  # for callers that predate it.
  @impl true
  def handle_call({:get_cell_state, selector}, from, %__MODULE__{} = state)
      when is_map(selector) do
    if ClassRegistry.persistent?(relay_class(state)) do
      command = %RunnerCommands.CellState{
        relay_id: state.id,
        tasklet: Map.get(selector, :tasklet) || "",
        cell_key: Map.get(selector, :cell_key) || ""
      }

      read_runner(state, from, {:cell_state, command}, [metadata: true], fn result, _runner ->
        with_change_seq(result)
      end)
    else
      {:reply, {:error, {:persistence_off, state.id}}, state}
    end
  end

  # Each tasklet's live Cells, queues and totals, as the hosting runner counts
  # them, and the Cells of the tasklet `read` names, answered with that runner's
  # id; a relay that keeps no state has activity all the same.
  @impl true
  def handle_call({:get_tasklet_activity, read}, from, %__MODULE__{} = state) do
    command = struct(%RunnerCommands.TaskletActivity{relay_id: state.id}, read)

    opts = [timeout: Durations.get(:tasklet_activity_read_ms)]
    read_runner(state, from, {:tasklet_activity, command}, opts, &{&1, &2})
  end

  @impl true
  def handle_call({:rebind, new_runner_id}, _from, %__MODULE__{} = state),
    do: Rebind.rebind(state, new_runner_id)

  @impl true
  def handle_call({:wake, then: :stop}, _from, state), do: Suspend.wake_to_stop(state)

  # The convergence loop's `publish_binding`: re-write the durable row and the
  # runner's route from the binding this process holds. Must run here — the
  # route points at `self()`.
  @impl true
  def handle_call(:publish_binding, _from, %__MODULE__{runner_id: runner_id} = state)
      when is_binary(runner_id),
      do: {:reply, :ok, Lifecycle.publish_binding(state, nil)}

  def handle_call(:publish_binding, _from, state), do: {:reply, {:error, :unbound}, state}

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}

  # Blocking ingest for source adapters, which act on "accepted" vs "halted" and
  # nothing finer — the same path as the confirming entry point below, reported
  # coarsely.
  @impl true
  def handle_call({:io_in, _message, _metadata}, _from, %__MODULE__{halt_reason: reason} = state)
      when reason != nil,
      do: {:reply, {:error, :startup_failed}, state}

  # Confirming ingest (`relay-to-relay-messaging`): the same single
  # pre-delivery path as the `{:io_in, _, _}` cast, answering what happened so a
  # forwarding sink can tell "done" from "retry". The cast keeps its
  # fire-and-forget shape, including its silent drop for a halted relay.
  @impl true
  def handle_call({:ingest, _message, _metadata}, _from, %__MODULE__{halt_reason: reason} = state)
      when reason != nil,
      do: {:reply, Ingest.named({:error, :not_ready}, state.id), state}

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

  # The `:call` entry point: an outside call (an integer deadline) or an
  # relay's call the controller delivers (a map); see `Inbound.take_call/5`.
  @impl true
  def handle_call({:call, message, metadata, deadline}, from, %__MODULE__{} = state)
      when (is_integer(deadline) and deadline > 0) or is_map(deadline),
      do: Inbound.take_call(state, message, metadata, deadline, from)

  # The outcome of one of this relay's own calls, from the delivery.
  def handle_call({:call_answer, id, outcome}, from, %__MODULE__{} = state),
    do: {:noreply, Outbound.respond(state, id, outcome, from)}

  @impl true
  def handle_call(:create_snapshot, _from, %__MODULE__{} = state),
    do: {:reply, {:ok, Handover.create_snapshot(state.id, state.runner_id)}, state}

  @impl true
  def handle_call({:replay_from, cell_id, baseline_seq}, _from, %__MODULE__{} = state) do
    {:reply, Replay.run_replay(state, cell_id, baseline_seq), state}
  end

  @impl true
  def handle_call({:rewind, to_iter}, _from, %__MODULE__{} = state),
    do: Suspend.rewind_to(state, to_iter)

  # ---------------------------------------------------------------
  # handle_cast
  # ---------------------------------------------------------------

  # Halted: drop ingress casts. A sender that needs the payload held rather than
  # dropped uses the confirming entry point, which answers
  # `{:error, {:not_accepting_input, id}}` instead. The drop is silent to the sender by contract but never unrecorded:
  # `halted_drops` is the only handle a cast-based source's loss has while the
  # relay stays halted.
  @impl true
  def handle_cast({:io_in, _message, _metadata}, %__MODULE__{halt_reason: reason} = state)
      when reason != nil do
    LivenessCounters.bump_relay(state.liveness_ref, :halted_drops)
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
  # A part goes to the listeners there are now and is never held: a preview
  # waits for nothing, and nothing but `:on_preview` sees it.
  @impl true
  def handle_cast({:io_part, message, metadata}, state) do
    Plugins.preview(state, :on_preview, message, metadata)
    {:noreply, state}
  end

  # Held while a bind task brings the plugins up on its own copy; a moved running
  # instance keeps its table up, as holding would tear a run ending first down.
  # With no bind in flight and no plugin subscribed (a failed bind, a boot sleeper),
  # calls and answers go out unsettled, owed to the next bind. A running relay fails the first test.
  def handle_cast(frame, %__MODULE__{phase: phase, halt_reason: halt} = state)
      when (phase == :suspended or halt != nil) and is_tuple(frame) and
             elem(frame, 0) in [:io_out, :io_out_batch] and
             (state.bind_task != nil or state.extensions.active_mask == 0),
      do: {:noreply, hold_or_owe(frame, state)}

  def handle_cast({:io_out_batch, outputs}, state),
    do: {:noreply, Outbox.run(outputs, state, &output(&3, &1, &2))}

  def handle_cast({:io_out, message, metadata}, state) do
    state = output(state, message, metadata)
    Outbox.settled(metadata)
    {:noreply, state}
  end

  # A dead-letter entry for an input is durable: its caller, if one waits, is
  # answered, and its listeners are told of a new entry
  # (`Flezha.Dlq.Store.announce/3`).
  @impl true
  def handle_cast({:dead_lettered, notice}, %__MODULE__{} = state) do
    if notice.new, do: Plugins.preview_dead_letter(state, notice)
    details = %{dlq_ref: notice.entry, error: notice.error}
    calls = Inbound.refuse(state.calls, callee(state), notice.token, "call.failed", details)
    {:noreply, %{state | calls: calls}}
  end

  def handle_cast({:call_accepted, id, callee, created}, %__MODULE__{} = state),
    do: {:noreply, %{state | calls: Outbound.accepted(state.calls, id, callee, created)}}

  @impl true
  def handle_cast({:correction_request, cell_id, target}, state),
    do: {:noreply, Replay.note_correction(state, cell_id, target)}

  @impl true
  def handle_cast({:plugin_opt_persist, plugin_id, key, value}, %__MODULE__{} = state),
    do: {:noreply, Plugins.persist_opt(state, plugin_id, key, value)}

  @impl true
  # A sleeper hibernates: the bind's restore frames and plugin setup leave
  # megabytes of garbage on its heap until the next wake (2.5 MiB → 5 KiB per
  # sleeper measured).
  def handle_cast({:suspend, iter, park}, %__MODULE__{} = state),
    do: {:noreply, Suspend.suspend_at(state, iter, park), :hibernate}

  # A runner of its factory connected: a sleeper owing work wakes.
  def handle_cast(:rehydrate_if_pending, state), do: Suspend.runner_available(state)

  # Replaces `state.snapshot` with the SDK's final state on graceful shutdown.
  # A handover ships it and the next running bind seeds from it, until a
  # runner is bound again: that runner's state supersedes it, so the bind drops
  # it. Cold-boot restart is covered by the per-Cell WAL files instead.
  @impl true
  def handle_cast({:apply_external_snapshot, binary}, %__MODULE__{} = state)
      when is_binary(binary) do
    {:noreply, %__MODULE__{state | snapshot: binary}}
  end

  # External callers (the convergence loop's `resync_runner` heal) trigger a fresh
  # per-Cell replay against the bound runner without knowing this process's
  # internals. Used by `Flezha.State.Recovery.resync_for_overwatch/2`. A relay a
  # handover carried has no durable restore to replay (`Replay.carrier/1`).
  def handle_cast({:kick_restore, runner_id}, %__MODULE__{} = state) do
    if :durable in Replay.carrier(state),
      do: Replay.dispatch_restore(state, state.builder.opts, runner_id)

    {:noreply, state}
  end

  # The bound runner connected again under a new session with the same stable
  # identity (`Flezha.Runner.FleetManager.Session.swap_bindings/4`).
  def handle_cast(
        {:session_replaced, old_session, new_session, continuity},
        %__MODULE__{} = state
      ),
      do: {:noreply, Rebind.session_replaced(state, old_session, new_session, continuity)}

  # ---------------------------------------------------------------
  # handle_info
  # ---------------------------------------------------------------

  # Halted: not serving, and the rebind's own bind re-forwards the tail. A
  # re-forward here would push input at the runner the relay is halted away
  # from — the same reason the ingress cast is dropped while halted. A bind in
  # flight is owed the notice, which the clause below records on it.
  @impl true
  def handle_info(
        {:stream_bound, _channel},
        %__MODULE__{halt_reason: reason, bind_task: nil} = state
      )
      when reason != nil,
      do: {:noreply, state}

  # The relay's data stream bound. Anything its durable logs still hold above
  # the Cells' consumed offsets was issued onto a stream that could not carry
  # it — re-forward it now, on the replay path a restore uses. Which channel
  # announced the bind does not matter: the tail is owed either way, and it goes
  # out on whichever channel this relay is bound to now.
  @impl true
  def handle_info({:stream_bound, _channel}, %__MODULE__{} = state) do
    {:noreply, Replay.reforward_undelivered_tail(state)}
  end

  # A bind-time restore the SDK refused, reported from the off-process replay.
  # The Cell's durable state never reached the runner, so the relay halts
  # rather than serving from empty state; the convergence loop rebinds it.
  @impl true
  def handle_info({:restore_refused, reason}, %__MODULE__{} = state) do
    {:noreply, Lifecycle.halt(state, {:restore_refused, reason})}
  end

  @impl true
  def handle_info({:run_correction, cell_id}, %__MODULE__{} = state) do
    {target, corrections} = Map.pop(state.corrections, cell_id)
    state = %{state | corrections: corrections}

    # At most one correction per Cell is ever in flight, by construction: the
    # transaction runs to completion inside this handler, so a request arriving
    # during it simply waits in the mailbox and debounces into the next one.
    state = if target, do: Replay.run_correction(state, cell_id, target), else: state
    if state.corrections == %{}, do: Rebind.signal_migration_ready(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:call_deadline, token}, %__MODULE__{} = state) do
    state = timer_fired(state, {:call, token})
    {:noreply, %{state | calls: Inbound.expire(state.calls, callee(state), token)}}
  end

  def handle_info({:call_timeout, id}, %__MODULE__{} = state),
    do: {:noreply, Outbound.timeout(timer_fired(state, {:call, id}), id)}

  # The move started: the rows went with the handover; the arriving process answers them.
  @impl true
  def handle_info(:calls_handed_over, %__MODULE__{} = state) do
    keys = Map.keys(state.calls.rows) ++ Map.keys(state.calls.out)
    state = Enum.reduce(keys, state, &cancel_timer(&2, {:call, &1}))

    {:noreply, %{state | calls: Inbound.depart(state.calls), handed_over?: true, cause: :move}}
  end

  @impl true
  def handle_info({:extension_timer, name, plugin_id}, %__MODULE__{} = state) do
    {:noreply, %{state | extensions: Table.fire_timer(state.extensions, name, plugin_id)}}
  end

  @impl true
  def handle_info(:wake_deadline, %__MODULE__{} = state),
    do: Suspend.wake_deadline(timer_fired(state, :wake))

  # The in-flight bind task posts its result via a plain `send/2` before it
  # exits, so this clause runs before its `:EXIT` and clears the slot.
  @impl true
  def handle_info({:bind_task_result, _task_pid, result}, %__MODULE__{bind_task: %{}} = state),
    do: {:noreply, Rebind.bind_task_reported(state, result)}

  # Late delivery: the task was terminated by a teardown, or a second
  # bind already replaced `state.bind_task`. Drop.
  def handle_info({:bind_task_result, _task_pid, _result}, state), do: {:noreply, state}

  # The bind task died before it reported: a failed bind.
  @impl true
  def handle_info({:EXIT, pid, reason}, %__MODULE__{bind_task: %{pid: pid}} = state),
    do: {:noreply, Rebind.bind_task_exited(state, reason)}

  # A monitored input log that exits with an append in flight reports it as a
  # `:DOWN` the request collection recognises; every other `:DOWN` is a plugin's.
  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason} = msg, %__MODULE__{} = state) do
    case Ingest.journal_reply(state, msg) do
      {:ok, state} -> {:noreply, state}
      :ignore -> {:noreply, Plugins.down(state, ref, pid, reason)}
    end
  end

  # A linked helper (a bind task that reported, a restore it dispatched) ends
  # normally as a matter of course; only a crash is worth a line.
  @impl true
  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}

  def handle_info({:EXIT, pid, reason}, state) do
    Logger.error("Process #{inspect(pid)} exited with reason: #{inspect(reason)}")
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
        unexpected(state, msg)
        {:noreply, state}
    end
  end

  # Past the log's answers, so no append pays for it: a start's held input's verdict.
  defp unexpected(state, {{:seed_taken, from, announce?, call}, verdict}),
    do: GenServer.reply(from, Ingest.started(state, announce?, call, verdict))

  defp unexpected(state, msg),
    do: Logger.warning("[Overwatch] #{state.id} dropped an unexpected message: #{inspect(msg)}")

  # One output on either route: a request goes to its callee and never reaches
  # the `:on_output` chain. An output carrying a call's token answers its
  # caller first and then goes on, without the token, like any other. A
  # durable output a call takes stays held until the caller has its outcome.
  defp output(%__MODULE__{} = state, message, metadata) do
    case Inbound.divert(state.calls, callee(state), message, metadata) do
      :output ->
        Plugins.transform_io(state, :on_output, message, metadata)
        state

      {:answered, calls, output} ->
        Plugins.transform_io(state, :on_output, message, output)
        %{state | calls: calls}

      {:held, calls, output} ->
        Outbox.hold_delivery(metadata)
        Plugins.transform_io(state, :on_output, message, output)
        Inbound.retire_at_deadline(%{state | calls: calls}, metadata[@reply_to])

      :call ->
        Outbox.hold_delivery(metadata)
        Outbound.request(state, message, metadata, Outbox.guaranteed?(metadata))
    end
  end

  defp hold_or_owe(frame, %__MODULE__{bind_task: %{}} = state),
    do: %__MODULE__{state | held_outputs: [frame | state.held_outputs]}

  defp hold_or_owe({:io_out, message, metadata}, state), do: owe({message, metadata, nil}, state)
  defp hold_or_owe({:io_out_batch, outputs}, state), do: Enum.reduce(outputs, state, &owe/2)
  defp owe({msg, md, _ack}, state), do: output(%{state | owes_outputs: true}, msg, md)

  @doc "Answer the outbox hold a call took on `metadata`; `nil` holds nothing."
  @spec confirm_hold(map() | nil) :: :ok
  def confirm_hold(nil), do: :ok
  def confirm_hold(metadata), do: Outbox.confirm_delivery(metadata)

  # ---------------------------------------------------------------
  # Live runner read
  # ---------------------------------------------------------------

  # One runner round-trip, off the GenServer loop so a slow reply cannot back
  # up the mailbox; `from` is answered with `shape.(result, runner_id)` when it
  # completes. A relay that is not resident is refused without asking any
  # runner.
  #
  # A Cell that is not held in memory is refused, never served from the
  # durable store: the controller cannot decode the SDK's WAL bytes, and a
  # fallback would mean binding a runner and restoring a Cell as a side
  # effect of a read. `flz state at` is the durable-read path.
  defp read_runner(%__MODULE__{} = state, from, {kind, _} = command, opts, shape) do
    cond do
      state.phase == :suspended ->
        {:reply, {:error, {:not_resident, :suspended, state.id}}, state}

      is_nil(state.runner_id) ->
        {:reply, {:error, {:not_resident, :unbound, state.id}}, state}

      true ->
        {relay_id, runner_id} = {state.id, state.runner_id}

        spawn(fn ->
          result =
            with_task_span(kind, relay_id, fn ->
              ControlUnit.send_control_await(runner_id, command, opts)
            end)

          GenServer.reply(from, shape.(result, runner_id))
        end)

        {:noreply, state}
    end
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

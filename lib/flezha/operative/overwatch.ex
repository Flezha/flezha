defmodule Flezha.Operative.Overwatch do
  @moduledoc """
  The controller-side process that owns one operative instance.

  It holds the operative's state, its plugin extension table and its route to a
  runner. Everything it does is routing: a callback here decides *which* phase a
  message belongs to and hands the state to the module that owns that phase. The
  phases live beside it, so this file stays the map rather than the territory:

    * `Flezha.Operative.Lifecycle` — bind, park, suspend, rehydrate, teardown.
      One module because they are one state machine.
    * `Flezha.Operative.Ingest` — the input path, from `:on_input` to the
      durable append to the forward.
    * `Flezha.Operative.Replay` — restore, re-forward, late-input correction.
    * `Flezha.Operative.Handover` — moving the operative between nodes.
    * `Flezha.Operative.Plugins` — the extension table's bring-up and runtime ops.
    * `Flezha.Operative.ClassRegistry` — the SDK declarations a bind resolves.
    * `Flezha.State.Commands` — durable-state control commands, which the State
      subsystem issues with no Overwatch involved at all.

  The one invariant worth stating here: this process is the single owner of its
  operative's input. Every `:io_in` — cast, call, confirming, replayed or seeded
  — runs `Ingest.ingest/3` and nothing else does. That is what makes the
  append-before-forward contract hold, and what lets the correction transaction
  assume no input can be journaled while it runs.
  """

  alias Flezha.Operative.Handover
  alias Flezha.Operative.Ingest
  alias Flezha.Operative.Lifecycle
  alias Flezha.Operative.Plugins
  alias Flezha.Operative.Replay
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.HubManager
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.ProcInbox
  alias Flezha.State.ProcJournal
  alias Flezha.Transport.LivenessCounters

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
    # task so it never blocks the Overwatch; this field carries the in-flight
    # task's reference + pid so `terminate/2` can kill it cleanly. `nil`
    # when no migrate is in flight.
    migrate_task: nil,
    # Effectively-once tier, declared by the SDK in the AddOperative response
    # metadata. Gates the input-log journal + `flz.offset` stamp on `:io_in`.
    effectively_once: false,
    # Durable workflow class (kind + per-class persistence). Gates the durable
    # inbox on `:io_in` and the suspension lifecycle
    # (`workflow-durable-suspension`).
    durable_workflow: false,
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

  @type t :: %__MODULE__{}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @doc """
  The operative-class identity used for per-Entity state (defaults to the
  instance id, per the SDK convention that entity frames carry the operative_id).
  """
  @spec operative_class(t()) :: binary()
  def operative_class(%__MODULE__{} = state),
    do: Keyword.get(state.builder.opts, :operative_class, state.id)

  @doc """
  Push one payload at the bound stream channel. The single statement of the
  frame shape a delivery takes, shared by a fresh ingest and by both durability
  tiers' re-forward.
  """
  @spec deliver(t(), {binary(), map()}) :: :ok
  def deliver(%__MODULE__{} = state, payload),
    do: GenServer.cast(state.channel, {:io_in, state.id, payload})

  @doc """
  Time a known-blocking procedure and surface a `[Overwatch] proc_slow` warning
  when it runs long. The last logged `proc_begin` with no matching `proc_end`
  pinpoints exactly which procedure wedged the GenServer.

  `try/after` (never `catch`) so the span is recorded even on exit/throw while
  the failure still propagates unchanged — `ControlUnit.send_control_await/3`'s
  own `:exit` catch must still fire.
  """
  @spec with_proc_span(atom(), binary(), (-> result)) :: result when result: term()
  def with_proc_span(proc, operative_id, fun) do
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
    case Flezha.State.Registry.claim_namespace(state.id, state.namespace) do
      :ok ->
        continue_bind(stale_runner_id, operative, state)

      {:error, {:namespace_conflict, _class, owner}} ->
        Logger.error(
          "[overwatch] refusing to bind #{state.id} in namespace #{state.namespace}: " <>
            "its durable state belongs to namespace #{owner}"
        )

        {:stop, {:shutdown, {:namespace_conflict, state.id, owner}}, state}
    end
  end

  defp continue_bind(stale_runner_id, operative, %__MODULE__{} = state) do
    state = %__MODULE__{state | durable_workflow: Lifecycle.durable_workflow?(state, operative)}

    # Suspended instances are rediscovered from durable state alone (journal +
    # inbox), so a controller redeploy neither binds them to a runner nor
    # loses their suspension (`workflow-durable-suspension`). An instance with
    # undelivered input is not suspended any more — it binds and consumes it.
    if state.durable_workflow and Lifecycle.suspended_on_disk?(operative.id) do
      {:noreply, Lifecycle.enter_suspended(state, ProcJournal.restore_state(operative.id))}
    else
      Lifecycle.bind(state, stale_runner_id, operative)
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
      {_verdict, state} = Ingest.confirming_ingest(state, payload, %{})
      {:noreply, state}
    else
      result
    end
  end

  defp seed_input(result, _operative), do: result

  defp fresh_inbox?(%__MODULE__{durable_workflow: true, id: id}),
    do: ProcInbox.last_offset(id) == 0

  defp fresh_inbox?(%__MODULE__{}), do: true

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
    {:reply, :ok, %{state | extensions: Table.unsubscribe(state.extensions, plugin_id)}}
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
  # where the bounded retry loop (up to 19 s aggregate) actually lives.
  # The running-migrate path is a single ping + start round-trip; keeping it
  # synchronous preserves the existing HotSwap handover invariants that depend
  # on state being applied before the call returns.
  @impl true
  def handle_call({:runner_migrate, _}, _from, %__MODULE__{migrate_task: t} = state)
      when t != nil do
    {:reply, {:error, :busy}, state}
  end

  # Parked (a failed init) and suspended (a durable loop park) both come back
  # through the same un-park task: it re-binds, restores journal + entities, and
  # delivers the undelivered inbox tail. A running operative migrates in place,
  # below.
  def handle_call(
        {:runner_migrate, new_runner_id},
        _from,
        %__MODULE__{init_error: reason, suspended: suspended} = state
      )
      when reason != nil or suspended do
    {ref, pid} = Lifecycle.spawn_migrate_task({:unpark, state, new_runner_id})
    {:reply, :ok, %{state | migrate_task: %{ref: ref, pid: pid, mode: :unpark}}}
  end

  # The binding is committed only once the runner has answered. Committing it
  # first leaves a failed migrate naming a runner this process cannot reach —
  # the operative then reads as bound to a LIVE runner, so `Convergence.Diff`
  # stops classifying it misbound and the divergence hides itself permanently.
  def handle_call({:runner_migrate, new_runner_id}, _from, %__MODULE__{} = state) do
    case Lifecycle.attempt_ping_and_start(Lifecycle.migrate_operative(state), new_runner_id) do
      {:ok, _} -> {:reply, :ok, Lifecycle.bind_to_runner(state, state.id, new_runner_id)}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

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

  @impl true
  def handle_call({:io_in, message, metadata}, _from, state) do
    {_verdict, state} = Ingest.confirming_ingest(state, message, metadata)
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
    {verdict, state} = Ingest.confirming_ingest(state, message, metadata)
    {:reply, verdict, state}
  end

  @impl true
  def handle_call(:create_snapshot, _from, %__MODULE__{} = state) do
    snapshot = Handover.create_snapshot(state.id, state.runner_id)
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
    {:reply, Replay.run_replay(state, entity_id, baseline_seq), state}
  end

  @impl true
  def handle_call({:rewind, to_iter}, _from, %__MODULE__{} = state) do
    case Lifecycle.rewind_to(state, to_iter) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # ---------------------------------------------------------------
  # handle_cast
  # ---------------------------------------------------------------

  # Suspended: the input is durable in the inbox, so it is not lost by not being
  # forwarded now — the rehydrating bind delivers the whole undelivered tail.
  @impl true
  def handle_cast({:io_in, message, metadata}, %__MODULE__{suspended: true} = state) do
    {:noreply, Ingest.ingest_suspended(state, message, metadata)}
  end

  # Parked: silently drop ingress casts. A sender that needs the payload held
  # rather than dropped uses the confirming entry point, which answers
  # `{:error, :not_ready}` instead.
  @impl true
  def handle_cast({:io_in, _message, _metadata}, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_in, message, metadata}, state) do
    Ingest.forward(state, Ingest.ingest(state, message, metadata))
    {:noreply, state}
  end

  # A durable outbox flush: the chain run settles the entry it was dispatched
  # under, carrying the holds its sinks took — so an output no
  # delivery-guaranteed sink took is complete the moment the chain has had it,
  # filtered included.
  @impl true
  def handle_cast({:io_out, message, metadata, ack}, state) do
    Flezha.State.Outbox.run(ack, metadata, fn ->
      Plugins.transform_io(state, :on_output, message, metadata)
    end)

    {:noreply, state}
  end

  def handle_cast({:io_out, message, metadata}, state) do
    Plugins.transform_io(state, :on_output, message, metadata)
    Flezha.State.Outbox.settled(metadata)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:correction_request, entity_id, target}, state) do
    {:noreply, Replay.note_correction(state, entity_id, target)}
  end

  @impl true
  def handle_cast({:plugin_opt_persist, plugin_id, key, value}, %__MODULE__{} = state) do
    {:noreply, Plugins.persist_opt(state, plugin_id, key, value)}
  end

  @impl true
  def handle_cast({:suspend, iter, wake_deadline_ms}, %__MODULE__{} = state) do
    {:noreply, Lifecycle.suspend_at(state, iter, wake_deadline_ms)}
  end

  # Replaces `state.snapshot` with a binary supplied out-of-band by the SDK
  # on graceful shutdown, consumed by HotSwap's `prepare_handover_state` so a
  # subsequent migration ships the runner's final state. Cold-boot restart is
  # covered by the per-Entity WAL files instead.
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
  # per-Entity replay against the bound runner without knowing this process's
  # internals. Used by `Flezha.State.Recovery.resync_for_overwatch/2`.
  def handle_cast({:kick_restore, runner_id}, %__MODULE__{} = state) do
    Replay.dispatch_restore(state, state.builder.opts, runner_id)
    {:noreply, state}
  end

  # ---------------------------------------------------------------
  # handle_info
  # ---------------------------------------------------------------

  @impl true
  def handle_info({:run_correction, entity_id}, %__MODULE__{} = state) do
    {target, corrections} = Map.pop(state.corrections, entity_id)
    state = %{state | corrections: corrections}

    # At most one correction per Entity is ever in flight, by construction: the
    # transaction runs to completion inside this handler, so a request arriving
    # during it simply waits in the mailbox and debounces into the next one.
    state = if target, do: Replay.run_correction(state, entity_id, target), else: state
    if state.corrections == %{}, do: Lifecycle.signal_migration_ready(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:extension_timer, name, plugin_id}, %__MODULE__{} = state) do
    {:noreply, %{state | extensions: Table.fire_timer(state.extensions, name, plugin_id)}}
  end

  # The journaled wake deadline elapsed (`workflow-durable-suspension`): append a
  # timer wake to the inbox and rehydrate. Routing the wake THROUGH the inbox is
  # what makes two racing wake sources — a message and the deadline — collapse
  # into one rehydration: the second is a no-op on an already-rehydrating
  # instance, and the loop simply reads whatever the inbox holds. The wake item
  # carries an empty payload, which the class's entry channel decodes as its
  # tick.
  @impl true
  def handle_info(:wake_deadline, %__MODULE__{} = state) do
    _ = ProcInbox.append(state.id, "", %{"flz.wake" => "timer"})
    {:noreply, Lifecycle.rehydrate(%__MODULE__{state | wake_timer: nil})}
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
    {:noreply, Lifecycle.apply_migrate_result(state, result)}
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
    {:noreply, %{state | migrate_task: nil}}
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
    {:noreply, %{state | extensions: Table.retry(state.extensions, plugin_id)}}
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
  def handle_info({:flezha_request, handler_key, payload, reply_to, ref}, state) do
    Plugins.handle_request(state, handler_key, payload, reply_to, ref)
    {:noreply, state}
  end

  # ---------------------------------------------------------------
  # Live Entity read
  # ---------------------------------------------------------------

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
              ControlUnit.send_control_await(runner_id, {:entity_state, command}, metadata: true)
            end)

          GenServer.reply(from, with_change_seq(result))
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

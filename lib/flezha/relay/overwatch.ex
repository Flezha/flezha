defmodule Flezha.Relay.Overwatch do
  @moduledoc """
  The controller-side process that owns one relay instance.

  It holds the relay's state, its plugin extension table and its route to a
  runner. Everything it does is routing: a callback decides which phase a
  message belongs to and hands the state to the module that owns that phase —
  `Lifecycle`, `Migration`, `Ingest`, `Replay`, `Handover`, `Plugins`,
  `ClassRegistry`, `State.Commands`.

  Invariant: this process is the single owner of its relay's input. Every
  `:io_in` — cast, call, confirming, replayed or seeded — runs `Ingest.ingest/4`
  and nothing else does. That is what makes the append-before-forward contract
  hold and what lets the correction transaction assume no input can be
  journaled while it runs.
  """

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Relay.Calls
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Handover
  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Migration
  alias Flezha.Relay.Plugins
  alias Flezha.Relay.Replay
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.StreamChannel
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.State.Outbox
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.Transport.LivenessCounters

  # Reply-frame metadata key carrying the `change_seq` a single selected Cell
  # was serialized at (stamped by both SDKs' `CellState` handler). Spelled
  # here rather than read from `Flezha.SDK.Wire`, which owns the wire's other
  # `flz.` keys: `relay` ranks below `state`, so naming it would be a new
  # upward reference `scripts/check-layer-direction.sh` refuses.
  @change_seq_meta_key "flz.change_seq"

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
    init_error: nil,
    # Wall-clock ms at process start, for `flz relay info` uptime. Wall-clock
    # (not monotonic) so the value stays comparable when read from another node.
    started_at: nil,
    # Wall-clock ms at which a bind task's binding was last adopted: how long a
    # rebound instance has stayed up, for the convergence loop's stable window.
    bound_at: nil,
    # Liveness counters ref (io_in_received / forwarded / filtered_nil) so a
    # stall can be localised to "WS plugin stopped feeding" vs "relay stopped
    # forwarding to the channel".
    liveness_ref: nil,
    # A bind onto another runner (an un-park, a wake, a `:runner_migrate`, a
    # restarted runner) runs in a task linked to this process so it never
    # blocks the Overwatch and never outlives it; this field carries the
    # in-flight task's pid and what the bind carries. `nil` when no migrate
    # is in flight.
    migrate_task: nil,
    # Outputs that arrived while a migrate task was binding, newest first,
    # delivered once its binding is adopted.
    held_outputs: [],
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
    # Asleep (`durable-suspension`): zero live runner resources,
    # registry entry retained. A new input-log append, or the recorded wake
    # deadline — rehydrates through the normal bind path.
    suspended: false,
    # Timer ref for the recorded wake deadline while suspended.
    timers: %{},
    # A park that landed while a bind task was binding: the task's bound
    # state is stale by the time it reports, and the journaled tail still
    # owes a wake.
    parked_in_flight: false,
    # An exit report that landed while a bind task was binding, as
    # `{runner_id, reason}`: judged once the bind's result is adopted.
    exited_in_flight: nil,
    # Late-input correction window (ms), declared by the SDK. `nil` = the class
    # never corrects. Gates lateness handling and the event-time-ordered replay.
    correction_window_ms: nil,
    # Cells with a correction in flight, and the oldest event time each was
    # asked to correct back to. Concurrent requests for one Cell coalesce
    # into the one replay that reaches furthest back.
    corrections: %{},
    # The outside callers waiting on this relay for an answer; each row's
    # deadline is armed under the timer family `:call`.
    calls: %Calls{},
    # Whether a `:once` or `:chained` steps instance has taken its one
    # input in this process, so a later call is refused; a durable instance
    # also asks its input log (`Ingest.first_input?/1`), which outlives it.
    fed: false
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
  @spec callee(t()) :: Calls.callee()
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

      if ms >= task_warn_ms() do
        Logger.warning("[Overwatch] task_slow task=#{task} relay=#{relay_id} duration_ms=#{ms}")
      else
        Logger.debug("[Overwatch] task_end task=#{task} relay=#{relay_id} duration_ms=#{ms}")
      end
    end
  end

  defp task_warn_ms,
    do: Durations.get(:overwatch_task_warn_ms)

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
      channel: nil,
      snapshot: nil,
      default_input_type: Keyword.get(relay.opts, :default_input_type, "2"),
      extensions: Host.relay_table(__extensions_catalog__(), relay.opts),
      builder: relay,
      started_at: System.system_time(:millisecond),
      liveness_ref: LivenessCounters.register_relay(relay.id)
    }

    Plugins.warn_old_rows(relay)
    {:ok, state, {:continue, {:bind_runner, stale_runner_id, relay}}}
  end

  @impl true
  def handle_continue({:bind_runner, stale_runner_id, relay}, %__MODULE__{} = state) do
    # Claim the class's durable-state namespace BEFORE any path below
    # (journal probe, restore, WAL open) resolves: writes must land under
    # this relay's namespace, and a class owned by another namespace
    # must never be silently read or overwritten.
    with :ok <- one_state_key(state),
         :ok <- Paths.claim_namespace(state.id, state.namespace) do
      continue_bind(stale_runner_id, relay, state)
    else
      {:error, {:namespace_conflict, _class, owner}} ->
        Logger.error(
          "[overwatch] refusing to bind #{state.id} in namespace #{state.namespace}: " <>
            "its durable state belongs to namespace #{owner}"
        )

        {:stop, {:shutdown, {:namespace_conflict, state.id, owner}}, state}

      {:error, {:split_state_key, class}} ->
        Logger.error(
          "[overwatch] refusing to bind steps #{state.id}: its state key is its " <>
            "instance id, not #{inspect(class)}"
        )

        {:stop, {:shutdown, {:split_state_key, state.id, class}}, state}
    end
  end

  # A steps relay's journal, Cells and input log are all keyed by its instance id,
  # so an `:relay_class` naming anything else would split them: it is
  # refused, not ignored.
  defp one_state_key(%__MODULE__{builder: %{run: :steps}} = state) do
    case relay_class(state) do
      id when id == state.id -> :ok
      class -> {:error, {:split_state_key, class}}
    end
  end

  defp one_state_key(%__MODULE__{}), do: :ok

  defp continue_bind(stale_runner_id, relay, %__MODULE__{} = state) do
    class = relay_class(state)
    # A move brings the source's journal, input log and Cells over before
    # anything below reads them.
    relay =
      Replay.adopt_moved_state(state, relay, HubManager.get_pid(state.namespace, state.id))

    state = %__MODULE__{state | builder: relay}

    state = %__MODULE__{
      state
      | durable_steps: Migration.durable_steps?(state, relay),
        sleeps: ClassRegistry.sleeps?(class)
    }

    # Sleeping instances are rediscovered from durable state alone (the park
    # record and the input log), so a controller redeploy neither binds them to
    # a runner nor loses their sleep (`durable-suspension`). An
    # instance with undelivered input is not asleep any more: it binds and
    # consumes it. The floor is seeded first, since the log answers that
    # question against it.
    :ok = Replay.seed_floor(state)
    park = ClassRegistry.parked(class)

    if Migration.suspended_on_disk?(class) do
      {:noreply, Migration.enter_suspended(state, park), :hibernate}
    else
      Lifecycle.bind(state, stale_runner_id, relay)
    end
    |> seed_input(relay)
  end

  # The instance's first input (a tasklet-requested spawn's payload, or the
  # message `flz relay start --text` and a schedule carry), ingested with
  # its metadata (the entry channel it names) inside the bind continue, which
  # is what makes it first: no other message in the mailbox — including a
  # concurrent requester's delivery — can be ingested ahead of it. A durable
  # instance whose inbox already holds entries is a restart rather than a
  # spawn; its payload is journaled, so re-seeding it would duplicate.
  defp seed_input({:noreply, %__MODULE__{} = state} = result, relay) do
    case Keyword.get(relay.opts, :seed_input) do
      {message, metadata}
      when is_binary(message) and is_map(metadata) and is_nil(state.init_error) ->
        if Ingest.first_input?(state),
          do: {:noreply, seed(state, message, metadata, Keyword.get(relay.opts, :seed_call))},
          else: result

      _ ->
        result
    end
  end

  defp seed_input(result, _relay), do: result

  # A call that started this instance: its request is the first input, taken
  # through the call entry point so its answer reaches the caller.
  defp seed(state, message, metadata, nil),
    do: state |> Ingest.confirming_ingest(message, metadata, :cast) |> elem(1)

  defp seed(state, message, metadata, call) do
    case Ingest.take_call(state, message, metadata, call, nil) do
      {:noreply, state} -> state
      {:reply, _verdict, state} -> state
    end
  end

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

  def handle_call({:registry_runner_set, announce?}, _from, %__MODULE__{init_error: nil} = state) do
    Lifecycle.started(state, announce?)
    {:reply, {:ok, state.runner_id}, state}
  end

  def handle_call({:registry_runner_set, _}, _from, %__MODULE__{init_error: reason} = state) do
    {:reply, {:error, reason}, state}
  end

  @impl true
  def handle_call({:plugin_subscribe, plugin_id, %Plugin{} = binding, entry}, _from, state) do
    case Plugins.subscribe(state, plugin_id, binding, entry) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:plugin_unsubscribe, plugin_id}, _from, %__MODULE__{} = state),
    do: {:reply, :ok, %{state | extensions: Table.unsubscribe(state.extensions, plugin_id)}}

  @impl true
  def handle_call({:plugin_update, plugin_id, %Plugin{} = binding, mode, entry}, _from, state) do
    case Plugins.update(state, plugin_id, binding, mode, entry) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:plugin_heal, primitive, plugin_id, %Plugin{} = plugin}, _from, state) do
    healed =
      if primitive == :start_plugin,
        do: Plugins.subscribe(state, plugin_id, plugin),
        else: Plugins.restart(state, plugin_id, plugin)

    case healed do
      {:ok, state} -> {:reply, {:ok, Table.phase(state.extensions, plugin_id)}, state}
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
  # the whole-Relay caller already decodes; `RelayManager` drops the seq
  # for callers that predate it.
  @impl true
  def handle_call({:get_cell_state, selector}, from, %__MODULE__{} = state)
      when is_map(selector) do
    if PersistenceResolver.keeps_state?(relay_class(state)) do
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
  def handle_call({:runner_migrate, new_runner_id}, _from, %__MODULE__{} = state),
    do: Migration.runner_migrate(state, new_runner_id)

  # A stop of a sleeping steps relay: woken onto a runner with the stop carried,
  # so it ends as it would stopped awake. `{:error, :awake}` when it is not
  # asleep, and the caller stops it as an awake one.
  @impl true
  def handle_call({:wake, then: :stop}, _from, %__MODULE__{suspended: true} = state) do
    {reply, state} = Migration.wake(state, :stop)
    {:reply, reply, state}
  end

  def handle_call({:wake, then: :stop}, _from, %__MODULE__{} = state),
    do: {:reply, {:error, :awake}, state}

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

  # Confirming ingest (`relay-to-relay-messaging`): the same single
  # pre-delivery path as the `{:io_in, _, _}` cast, answering what happened so a
  # forwarding sink can tell "done" from "retry". The cast keeps its
  # fire-and-forget shape, including its silent drop for a parked relay.
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

  # The `:call` entry point: an outside call (an integer deadline) or an
  # relay's call the controller delivers (a map); see `Ingest.take_call/5`.
  @impl true
  def handle_call({:call, message, metadata, deadline}, from, %__MODULE__{} = state)
      when (is_integer(deadline) and deadline > 0) or is_map(deadline),
      do: Ingest.take_call(state, message, metadata, deadline, from)

  # The outcome of one of this relay's own calls, from the delivery.
  def handle_call({:call_answer, id, outcome}, from, %__MODULE__{} = state),
    do: {:noreply, Ingest.respond(state, id, outcome, from)}

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
  # relay stays parked.
  @impl true
  def handle_cast({:io_in, _message, _metadata}, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    LivenessCounters.bump_relay(state.liveness_ref, :parked_drops)
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
  # A migrate task binds the plugins on its own copy of this state, so an
  # output of the instance it woke or un-parked waits for that binding to be
  # adopted rather than meet a plugin table the bind has not brought up (a
  # sleeper rediscovered at boot has none running). An instance moved while it
  # was running still has its table up, and its output is not held: a run that
  # completes before the bind reports would be torn down holding it.
  # A part goes to the listeners there are now and is never held: a preview
  # waits for nothing, and nothing but `:on_preview` sees it.
  @impl true
  def handle_cast({:io_part, message, metadata}, state) do
    Plugins.preview(state, :on_preview, message, metadata)
    {:noreply, state}
  end

  def handle_cast(frame, %__MODULE__{migrate_task: %{}} = state)
      when is_tuple(frame) and elem(frame, 0) in [:io_out, :io_out_batch] and
             (state.suspended or state.init_error != nil) do
    {:noreply, %__MODULE__{state | held_outputs: [frame | state.held_outputs]}}
  end

  def handle_cast({:io_out_batch, outputs}, state),
    do: {:noreply, Outbox.run(outputs, state, &output(&3, &1, &2))}

  def handle_cast({:io_out, message, metadata}, state) do
    state = output(state, message, metadata)
    Outbox.settled(metadata)
    {:noreply, state}
  end

  # A dead-letter entry for an input is durable: its caller, if one waits, is
  # answered, and its listeners are told (`Flezha.Dlq.Store.announce/2`).
  @impl true
  def handle_cast({:dead_lettered, notice}, %__MODULE__{} = state) do
    Plugins.preview_dead_letter(state, notice)
    details = %{dlq_ref: notice.entry, error: notice.error}
    calls = Calls.refuse(state.calls, callee(state), notice.token, "call.failed", details)
    {:noreply, %{state | calls: calls}}
  end

  def handle_cast({:call_accepted, id, callee, created}, %__MODULE__{} = state),
    do: {:noreply, %{state | calls: Calls.accepted(state.calls, id, callee, created)}}

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
  def handle_cast({:suspend, iter, park}, %__MODULE__{} = state) do
    {:noreply, Migration.suspend_at(state, iter, park), :hibernate}
  end

  # The runner reports this instance's process exited.
  def handle_cast({:relay_exited, runner_id, reason}, %__MODULE__{} = state),
    do: {:noreply, Migration.park_exited(state, runner_id, reason)}

  # Replaces `state.snapshot` with the SDK's final state on graceful shutdown.
  # A handover ships it and the next running migrate seeds from it, until a
  # runner is bound again: that runner's state supersedes it, so the bind drops
  # it. Cold-boot restart is covered by the per-Cell WAL files instead.
  @impl true
  def handle_cast({:apply_external_snapshot, binary}, %__MODULE__{} = state)
      when is_binary(binary) do
    {:noreply, %__MODULE__{state | snapshot: binary}}
  end

  # A relay that arrived via HotSwap handover already received its state
  # in-band (`alter_handover_state` ships `MigrationDeserialization`); a WAL
  # replay on top would apply every change a second time. Skip — same guard
  # as the bind-time restore dispatch.
  def handle_cast({:kick_restore, _runner_id}, %__MODULE__{arrived_via_hotswap?: true} = state) do
    {:noreply, state}
  end

  # External callers (the convergence loop's `resync_runner` heal) trigger a fresh
  # per-Cell replay against the bound runner without knowing this process's
  # internals. Used by `Flezha.State.Recovery.resync_for_overwatch/2`.
  def handle_cast({:kick_restore, runner_id}, %__MODULE__{} = state) do
    Replay.dispatch_restore(state, state.builder.opts, runner_id)
    {:noreply, state}
  end

  # The bound runner connected again under a new session with the same stable
  # identity (`Flezha.Runner.FleetManager.Session.swap_bindings/4`).
  def handle_cast(
        {:session_replaced, old_session, new_session, continuity},
        %__MODULE__{} = state
      ),
      do: {:noreply, Migration.session_replaced(state, old_session, new_session, continuity)}

  # ---------------------------------------------------------------
  # handle_info
  # ---------------------------------------------------------------

  # Parked: not serving, and the un-park's own bind re-forwards the tail. A
  # re-forward here would push input at the runner the relay is parked away
  # from — the same reason the ingress cast is dropped while parked.
  @impl true
  def handle_info({:stream_bound, _channel}, %__MODULE__{init_error: reason} = state)
      when reason != nil do
    {:noreply, state}
  end

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
  # The Cell's durable state never reached the runner, so the relay parks
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
  def handle_info({:call_deadline, token}, %__MODULE__{} = state) do
    state = timer_fired(state, {:call, token})
    {:noreply, %{state | calls: Calls.expire(state.calls, callee(state), token)}}
  end

  def handle_info({:call_timeout, id}, %__MODULE__{} = state),
    do: {:noreply, Ingest.timeout(timer_fired(state, {:call, id}), id)}

  # The rows went with a handover: the arriving process answers them now.
  @impl true
  def handle_info(:calls_handed_over, %__MODULE__{} = state) do
    keys = Map.keys(state.calls.rows) ++ Map.keys(state.calls.out)
    state = Enum.reduce(keys, state, &cancel_timer(&2, {:call, &1}))

    {:noreply, %{state | calls: Calls.depart(state.calls)}}
  end

  @impl true
  def handle_info({:extension_timer, name, plugin_id}, %__MODULE__{} = state) do
    {:noreply, %{state | extensions: Table.fire_timer(state.extensions, name, plugin_id)}}
  end

  # The recorded wake deadline elapsed (`durable-suspension`): it is
  # the earliest timer the instance's Cells held when it parked, so the
  # rehydrate alone wakes it: the restored Cells re-arm the timer and its
  # firing opens the pass. Nothing is appended. A racing message collapses
  # into the same rehydration, which is a no-op on a rehydrating instance.
  # With no runner to wake on, the deadline is retried a convergence tick
  # later, since no input will arrive to wake it.
  @impl true
  def handle_info(:wake_deadline, %__MODULE__{} = state) do
    case Migration.wake(timer_fired(state, :wake), nil) do
      {:ok, state} ->
        {:noreply, state}

      {{:error, :no_runner}, state} ->
        retry_at = System.system_time(:millisecond) + Durations.get(:convergence_tick_ms)
        {:noreply, Lifecycle.arm_wake_timer(state, retry_at)}
    end
  end

  # The in-flight bind task posts its result via a plain `send/2` before it
  # exits, so this clause runs before its `:EXIT` and clears the slot.
  @impl true
  def handle_info(
        {:migrate_task_result, _task_pid, result},
        %__MODULE__{migrate_task: %{}} = state
      ) do
    # The task reports a copy of this state taken before the outputs it held.
    %__MODULE__{} = adopted = Migration.apply_migrate_result(state, result)
    {:noreply, release_outputs(%__MODULE__{adopted | held_outputs: state.held_outputs})}
  end

  # Late delivery: the task was terminated by a teardown, or a second
  # migrate already replaced `state.migrate_task`. Drop.
  def handle_info({:migrate_task_result, _task_pid, _result}, state), do: {:noreply, state}

  # The bind task died before it reported: a failed bind.
  @impl true
  def handle_info({:EXIT, pid, reason}, %__MODULE__{migrate_task: %{pid: pid}} = state),
    do: {:noreply, state |> Migration.bind_task_exited(reason) |> release_outputs()}

  # A monitored input log that exits with an append in flight reports it as a
  # `:DOWN` the request collection recognises; every other `:DOWN` is a plugin's.
  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason} = msg, %__MODULE__{} = state) do
    case Ingest.journal_reply(state, msg) do
      {:ok, state} -> {:noreply, state}
      :ignore -> {:noreply, plugin_down(state, ref, pid, reason)}
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
        Logger.warning("[Overwatch] #{state.id} dropped an unexpected message: #{inspect(msg)}")
        {:noreply, state}
    end
  end

  # One output on either route: a request goes to its callee and never reaches
  # the `:on_output` chain. An output carrying a call's token answers its
  # caller first and then goes on, without the token, like any other. A
  # durable output a call takes stays held until the caller has its outcome.
  defp output(%__MODULE__{} = state, message, metadata) do
    case Calls.divert(state.calls, callee(state), message, metadata) do
      :output ->
        Plugins.transform_io(state, :on_output, message, metadata)
        state

      {:answered, calls, output} ->
        Plugins.transform_io(state, :on_output, message, output)
        %{state | calls: calls}

      {:held, calls, output} ->
        Outbox.hold(metadata)
        Plugins.transform_io(state, :on_output, message, output)
        Ingest.retire_at_deadline(%{state | calls: calls}, metadata["flz.reply_to"])

      :call ->
        Outbox.hold(metadata)
        Ingest.request(state, message, metadata, Outbox.guaranteed?(metadata))
    end
  end

  @doc "Answer the outbox hold a call took on `metadata`; `nil` holds nothing."
  @spec confirm_hold(map() | nil) :: :ok
  def confirm_hold(nil), do: :ok
  def confirm_hold(metadata), do: Outbox.confirm(metadata)

  # Deliver what arrived while a migrate task was binding, in arrival order;
  # a migrate spawned meanwhile holds them again.
  defp release_outputs(%__MODULE__{held_outputs: []} = state), do: state

  defp release_outputs(%__MODULE__{held_outputs: held} = state) do
    held
    |> Enum.reverse()
    |> Enum.reduce(%__MODULE__{state | held_outputs: []}, fn frame, acc ->
      {:noreply, acc} = handle_cast(frame, acc)
      acc
    end)
  end

  # A crashed subscriber is recorded by the table and declared here: the table
  # is a data structure and decides nothing, and the restart is the loop's.
  defp plugin_down(%__MODULE__{} = state, ref, pid, reason) do
    case Table.handle_down(state.extensions, ref, pid, reason) do
      {:ok, table} ->
        for plugin_id <-
              Map.keys(Table.phase_reasons(table, :down)) --
                Map.keys(Table.phase_reasons(state.extensions, :down)) do
          Facts.declare({:plugin_down, {state.namespace, state.id, plugin_id}, %{reason: reason}})
        end

        %{state | extensions: table}

      :ignore ->
        Logger.error("Binding not found for pid #{inspect(pid)} with reason: #{inspect(reason)}")
        state
    end
  end

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
      state.suspended ->
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

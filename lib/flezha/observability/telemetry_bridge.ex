defmodule Flezha.Observability.TelemetryBridge do
  @moduledoc """
  Single dispatching `:telemetry` handler that lifts existing
  `[:flezha, ...]` telemetry events onto the observability bus.

  No producer call-site is rewritten. The mapping has three halves: the
  hand-kept lifecycle half in `@mapping`; a half DERIVED from the convergence
  loop's detector table, so every event a detector entry declares is bridged by
  that declaration and a new detector needs no edit here; and a half derived at
  start from the providers packages installed (`provider_mapping/0`), so a
  package's events are exported under its provider's name with no edit here.
  Where halves name one event the hand-kept line wins, because its source, name
  and kind are a deliberate choice a derivation cannot know.

  Every `[:flezha, …]` event NAME the tree writes must be a key of the merged
  mapping or a member of `unexported/0`, which carries the reason it reaches no
  exporter. The suite judges the name where it is written rather than at the
  `:telemetry.execute` call, because three names are assembled into a tuple in one
  module and executed in another, and a provider's path is assembled from its
  own identifier, so an emitter cannot be added without an answer. It pins
  `unexported/0`'s length so the list can only shrink, and `unwritten/0` names
  the mapped events nothing writes any more.

  A broken bridge must never crash a producer: handler bodies are
  wrapped in `try`. On error the `:telemetry_bridge_failures` counter
  increments and the error is logged once per unique error class.
  """

  use GenServer, restart: :transient

  require Logger

  alias Flezha.Convergence.Probe.Table
  alias Flezha.Observability.{Bus, Counters, Registry, Signal}
  alias Flezha.Plugin.{ProviderBehaviour, Providers}

  @handler_id :flezha_observability_telemetry_bridge

  # event => {source_atom, name_atom, kind}
  @mapping %{
    [:flezha, :state, :wal, :backpressure] => {:flezha_state, :wal_backpressure, :metric},
    [:flezha, :state, :fsync, :tick] => {:flezha_state, :fsync_tick, :event},
    [:flezha, :boot, :recovery_complete] => {:flezha_boot, :recovery_complete, :event},
    [:flezha, :boot, :reconcile_duplicate] => {:flezha_boot, :reconcile_duplicate, :event},
    [:flezha, :shutdown, :requested] => {:flezha_shutdown, :requested, :event},
    [:flezha, :shutdown, :phase_started] => {:flezha_shutdown, :phase_started, :event},
    [:flezha, :shutdown, :phase_completed] => {:flezha_shutdown, :phase_completed, :event},
    [:flezha, :shutdown, :drain_progress] => {:flezha_shutdown, :drain_progress, :event},
    [:flezha, :shutdown, :timeout] => {:flezha_shutdown, :timeout, :event},
    [:flezha, :shutdown, :complete] => {:flezha_shutdown, :complete, :event},
    [:flezha, :plugin_package, :loaded] => {:flezha_plugin_packages, :loaded, :event},
    [:flezha, :plugin_package, :refused] => {:flezha_plugin_packages, :refused, :event},
    [:flezha, :plugin, :websocket, :idle_reconnect] => {:flezha_ws, :idle_reconnect, :event},
    [:flezha, :plugin, :websocket, :resubscribe] => {:flezha_ws, :resubscribe, :event},
    [:flezha, :plugin, :websocket, :reconcile] => {:flezha_ws, :reconcile, :event},
    [:flezha, :state, :seq_gap] => {:flezha_state, :seq_gap, :event},
    [:flezha, :state, :credit_anomaly] => {:flezha_state, :credit_anomaly, :event},
    [:flezha, :state, :output_stall] => {:flezha_state, :output_stall, :event},
    [:flezha, :state, :compaction_timeout] => {:flezha_state, :compaction_timeout, :event},
    [:flezha, :state, :compaction_trigger] => {:flezha_state, :compaction_trigger, :event},
    [:flezha, :state, :baseline_seed] => {:flezha_state, :baseline_seed, :event},
    [:flezha, :state, :wal_realign] => {:flezha_state, :wal_realign, :event},
    [:flezha, :state, :stale_writer_refused] => {:flezha_state, :stale_writer_refused, :event},
    # Remote durability tier. Every alarm it documents was a Logger line until
    # these entries existed — including `rpo_exceeded`, which the user guide
    # calls alarm-grade and reports through `/healthz`.
    [:flezha, :state, :rpo_exceeded] => {:flezha_durability, :rpo_exceeded, :event},
    [:flezha, :state, :ship_fenced] => {:flezha_durability, :ship_fenced, :event},
    [:flezha, :state, :ship_failed] => {:flezha_durability, :ship_failed, :event},
    [:flezha, :state, :artifact_shipped] => {:flezha_durability, :artifact_shipped, :metric},
    [:flezha, :state, :remote_commit] => {:flezha_durability, :remote_commit, :metric},
    [:flezha, :state, :hydrated] => {:flezha_durability, :hydrated, :event},
    [:flezha, :state, :hydration_failed] => {:flezha_durability, :hydration_failed, :event},
    [:flezha, :state, :artifact_missing] => {:flezha_durability, :artifact_missing, :event},
    [:flezha, :state, :durability_gc] => {:flezha_durability, :durability_gc, :metric},
    [:flezha, :leadership, :transition] => {:flezha_leadership, :transition, :event},
    # A reason that reached a caller with no catalogue code: a defect to read.
    [:flezha, :error, :uncatalogued] => {:flezha_error, :uncatalogued, :event},
    [:flezha, :reconcile, :action] => {:flezha_reconcile, :action, :event},
    [:flezha, :reconcile, :health] => {:flezha_reconcile, :health, :event},
    [:flezha, :stream, :drop] => {:flezha_stream, :drop, :metric},
    [:flezha, :convergence, :intent_mismatch] => {:flezha_convergence, :intent_mismatch, :event},
    [:flezha, :dlq, :parked] => {:flezha_dlq, :dlq_parked_total, :metric},
    [:flezha, :dlq, :retention_dropped] =>
      {:flezha_dlq, :dlq_dropped_by_retention_total, :metric},
    [:flezha, :dlq, :usage] => {:flezha_dlq, :dlq_usage, :metric},
    [:flezha, :plugin, :relay_forward, :shipped] => {:flezha_relay_forward, :delivered, :metric},
    [:flezha, :plugin, :relay_forward, :retried] => {:flezha_relay_forward, :retry, :metric},
    [:flezha, :plugin, :relay_forward, :queue_depth] =>
      {:flezha_relay_forward, :queue_depth, :metric},
    [:flezha, :plugin, :relay_forward, :overflowed] =>
      {:flezha_relay_forward, :saturated, :event},
    [:flezha, :plugin, :http_poll, :shipped] => {:flezha_http_poll, :injected, :metric},
    [:flezha, :plugin, :http_poll, :unchanged] => {:flezha_http_poll, :unchanged, :metric},
    [:flezha, :plugin, :http_poll, :failed] => {:flezha_http_poll, :failed, :event},
    # The file source and sink. A delivered line and a synced batch are counts;
    # a truncation, a failure and a record refused as a line are read one at a time.
    [:flezha, :plugin, :file_source, :shipped] => {:flezha_file_source, :injected, :metric},
    [:flezha, :plugin, :file_source, :truncated] => {:flezha_file_source, :truncated, :event},
    [:flezha, :plugin, :file_source, :failed] => {:flezha_file_source, :failed, :event},
    # The Postgres poll source: rows delivered are a count; a failure and the
    # moment it caught up are read one at a time.
    [:flezha, :plugin, :postgres_poll, :shipped] => {:flezha_postgres_poll, :injected, :metric},
    [:flezha, :plugin, :postgres_poll, :failed] => {:flezha_postgres_poll, :failed, :event},
    [:flezha, :plugin, :postgres_poll, :caught_up] => {:flezha_postgres_poll, :caught_up, :event},
    [:flezha, :plugin, :file_sink, :shipped] => {:flezha_file_sink, :flushed, :metric},
    [:flezha, :plugin, :file_sink, :refused] => {:flezha_file_sink, :refused, :event},
    [:flezha, :plugin, :file_sink, :failed] => {:flezha_file_sink, :failed, :event},
    # A deliberate start, and a stop or completion: read by every sink, the
    # hook sink among them, which names this source.
    [:flezha, :relay, :started] => {:flezha_relay, :relay_started, :event},
    [:flezha, :relay, :stopped] => {:flezha_relay, :relay_stopped, :event},
    # What controller plugins hear and nothing else does: a new dead letter
    # (`Flezha.Observability.HookSink`).
    [:flezha, :dlq, :dead_lettered] => {:flezha_hooks, :on_dead_letter, :event},
    # The two HTTP senders (`Flezha.Plugin.Provider.HttpSend`): a request sent,
    # retried or overflowed is a count; one given up is read one at a time. The
    # webhook's queue is its copy's, so it never overflows its own.
    [:flezha, :plugin, :hook_webhook, :shipped] => {:flezha_hook_webhook, :shipped, :metric},
    [:flezha, :plugin, :hook_webhook, :retried] => {:flezha_hook_webhook, :retried, :metric},
    [:flezha, :plugin, :hook_webhook, :failed] => {:flezha_hook_webhook, :failed, :event},
    [:flezha, :plugin, :hook_webhook, :overflowed] =>
      {:flezha_hook_webhook, :overflowed, :metric},
    [:flezha, :plugin, :http_push, :shipped] => {:flezha_http_push, :shipped, :metric},
    [:flezha, :plugin, :http_push, :retried] => {:flezha_http_push, :retried, :metric},
    [:flezha, :plugin, :http_push, :failed] => {:flezha_http_push, :failed, :event},
    [:flezha, :plugin, :http_push, :overflowed] => {:flezha_http_push, :overflowed, :metric},
    # The plugin lifecycle's one event: every phase a plugin enters or leaves,
    # from the table's one transition function.
    [:flezha, :plugin, :phase] => {:flezha_plugin, :phase, :event},
    [:flezha, :relay, :parked] => {:flezha_relay, :relay_parked_total, :metric},
    # A steps session's turn that failed and was parked, the session going on.
    [:flezha, :steps, :turn_dead_lettered] =>
      {:flezha_steps, :flezha_steps_turns_dead_lettered_total, :metric},
    # A call, an outside caller's or a relay's: how long each answer took,
    # and the calls that ended with none, counted by reason or code; a caller's
    # call recovered past its deadline and the retention margin settles with no
    # `Response` at all.
    [:flezha, :call, :answered] => {:flezha_call, :flezha_call_answer_duration, :metric},
    [:flezha, :call, :timeout] => {:flezha_call, :flezha_call_timeouts_total, :metric},
    [:flezha, :call, :answer_dropped] =>
      {:flezha_call, :flezha_call_answers_dropped_total, :metric},
    [:flezha, :call, :refused] => {:flezha_call, :flezha_call_refused_total, :metric},
    [:flezha, :call, :recovered_unanswered] =>
      {:flezha_call, :flezha_call_recovered_unanswered_total, :metric},
    # A runner's report that an instance's process died, which parks it.
    [:flezha, :relay, :exited] => {:flezha_relay, :relay_exited, :event},
    # The backoff table's two events: every wait a ladder takes and every reset
    # a success records, whichever site walks the row.
    [:flezha, :backoff, :wait] => {:flezha_convergence, :backoff_wait, :metric},
    [:flezha, :backoff, :reset] => {:flezha_convergence, :backoff_reset, :metric},
    # Every hub owner's start failure, from the one `attempt_start/2` that
    # reports it; `target` names the hub that would not come up.
    [:flezha, :hub, :start_failed] => {:flezha_boot, :hub_start_failed, :event},
    # The recovery replay's opening marker, beside the two hooks that report its
    # outcome; one per hub per boot.
    [:flezha, :boot, :pre_recovery_replay] => {:flezha_boot, :pre_recovery_replay, :event},
    # The websocket sink's four provider verbs, on the source its three
    # watchdog events already publish under. `shipped` counts, so it is the one
    # new exposed series; the other three are read one at a time.
    [:flezha, :plugin, :websocket, :shipped] => {:flezha_ws, :shipped, :metric},
    [:flezha, :plugin, :websocket, :retried] => {:flezha_ws, :retried, :event},
    [:flezha, :plugin, :websocket, :failed] => {:flezha_ws, :failed, :event},
    [:flezha, :plugin, :websocket, :overflowed] => {:flezha_ws, :overflowed, :event},
    # Detection verdicts outside the detector table: a refusal, a failure, a
    # loss, a breach or a death the controller observed. Every one was a
    # `Logger` line and an unexported event until these entries existed.
    [:flezha, :correction, :refused] => {:flezha_correction, :refused, :event},
    # A runner session opening and closing, for consumers on another node:
    # the event bus that carries the same two facts reaches local processes only.
    [:flezha, :runner, :connected] => {:flezha_runner, :runner_connected, :event},
    [:flezha, :runner, :disconnected] => {:flezha_runner, :runner_disconnected, :event},
    # A runner process an operator dropped, a drop lifted, and a dropped
    # process refused again, at the rate the runner's reconnect backoff sets.
    [:flezha, :runner, :dropped] => {:flezha_runner, :runner_dropped, :event},
    [:flezha, :runner, :readmitted] => {:flezha_runner, :runner_readmitted, :event},
    [:flezha, :runner, :drop_refused] => {:flezha_runner, :runner_drop_refused, :event},
    [:flezha, :runner, :factory_language_conflict] =>
      {:flezha_runner, :factory_language_conflict, :event},
    [:flezha, :runner, :factory_run_conflict] => {:flezha_runner, :factory_run_conflict, :event},
    [:flezha, :runner, :quota_refused] => {:flezha_runner, :quota_refused, :event},
    [:flezha, :runner, :admin_request] => {:flezha_runner, :admin_request, :event},
    [:flezha, :runner, :foreign_frame_refused] =>
      {:flezha_runner, :foreign_frame_refused, :event},
    [:flezha, :schedule, :spawn_failed] => {:flezha_schedule, :spawn_failed, :event},
    [:flezha, :stream, :fanout_warning] => {:flezha_stream, :fanout_warning, :event},
    [:flezha, :state, :claim_superseded] => {:flezha_durability, :claim_superseded, :event},
    [:flezha, :state, :compaction_failed] => {:flezha_state, :compaction_failed, :event},
    [:flezha, :state, :compaction_gave_up] => {:flezha_state, :compaction_gave_up, :event},
    [:flezha, :state, :disk_quota_breach] => {:flezha_state, :disk_quota_breach, :event},
    [:flezha, :state, :inbox_duplicate] => {:flezha_state, :inbox_duplicate, :event},
    [:flezha, :state, :input_log_frozen_watermark] =>
      {:flezha_state, :input_log_frozen_watermark, :event},
    [:flezha, :state, :input_log_syncer_died] => {:flezha_state, :input_log_syncer_died, :event},
    [:flezha, :state, :record_frame, :refused] => {:flezha_state, :record_frame_refused, :event},
    [:flezha, :state, :restore_refused] => {:flezha_state, :restore_refused, :event},
    [:flezha, :state, :wal_pull_failed] => {:flezha_state, :wal_pull_failed, :event},
    # A torn tail discarded from a durable log, and a WAL rewound from one seq
    # to another. Neither is a step completing: both report bytes the system
    # gave up on, and `record_frame :refused` above is the same event's other
    # outcome.
    [:flezha, :state, :record_frame, :repaired] =>
      {:flezha_state, :record_frame_repaired, :event},
    [:flezha, :state, :wal_rewind] => {:flezha_state, :wal_rewind, :event}
  }

  # Every `[:flezha, …]` event name the tree writes that reaches no exporter,
  # grouped under the reason it has none. A ratchet: the suite pins the total,
  # so an event may leave a group and none may join.
  @unexported %{
    # One signal per durable steps relay commit, at the data plane's rate, with
    # `task_idx` and `iter` unique to each. The commit's outcome rides the ack
    # the SDK observes and the durability counters, not a per-commit event.
    commit_path: [
      [:flezha, :state, :iter_commit],
      [:flezha, :state, :task_commit]
    ],
    # The convergence Loop attaches to this itself: it is a control input, and
    # the Loop must not read what an exporter publishes.
    control_input: [
      [:flezha, :convergence, :tick]
    ],
    # An action the controller took, or a step it completed, and never a fault
    # it detected, which is the line the mapping draws. Each is already
    # answered by an operations surface that reports the resulting state: the
    # convergence snapshot, `/healthz`, `flz boot await`, the schedule listing.
    # Where one of these has a failing sibling, the sibling IS mapped
    # (`wal_pull_failed`, `spawn_failed`, `compaction_failed`).
    lifecycle: [
      [:flezha, :boot, :reconcile_round],
      [:flezha, :cluster, :ready],
      [:flezha, :correction, :completed],
      [:flezha, :correction, :retention_derived],
      [:flezha, :hub, :pid_removed],
      [:flezha, :relay, :requested_spawn],
      [:flezha, :schedule, :dormant],
      [:flezha, :schedule, :fired],
      [:flezha, :state, :compaction_completed],
      [:flezha, :state, :schema_migration],
      [:flezha, :state, :wal_moved],
      [:flezha, :state, :wal_pulled],
      [:flezha, :steps, :completed],
      [:flezha, :steps, :rehydrating],
      [:flezha, :steps, :rewind]
    ]
  }

  # Mapped events nothing in `lib/flezha` writes any more, each with the reason
  # the line stays. A dead line costs nothing at run time and removing one is a
  # published-surface decision, so the suite pins the set rather than the count.
  @unwritten %{
    [:flezha, :leadership, :transition] =>
      "named in the observability-foundation at-minimum list; the reconciler that " <>
        "emitted it was replaced by the convergence loop",
    [:flezha, :reconcile, :action] =>
      "named in the observability-foundation at-minimum list; same reconciler",
    [:flezha, :reconcile, :health] =>
      "named in the observability-foundation at-minimum list; same reconciler",
    [:flezha, :state, :ship_fenced] =>
      "documented as an event of `:flezha_durability`; the shipper reports a lost " <>
        "claim as `claim_superseded` instead"
  }

  # Derived at COMPILE time from the detector table, so adding a detector
  # recompiles this module and its event is bridged with no edit here.
  @derived Map.new(Table.events(), fn event ->
             {event, {:flezha_health, List.last(event), :event}}
           end)

  @all_mapping Map.merge(@derived, @mapping)

  # The controller hooks' lines: plugin notices, not telemetry, which only the
  # sinks naming their source hear (`Bus.emit_named/1`).
  @hook_mapping for {_event, {:flezha_hooks, _name, _kind}} = row <- @all_mapping,
                    into: %{},
                    do: row

  # The shared provider verbs: a failure is read one at a time, the rest are counts.
  @verb_kinds for verb <- ProviderBehaviour.verbs(),
                  do: {verb, if(verb == :failed, do: :event, else: :metric)}

  # ---------- Public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  The bridged source atoms of `mapping`. Pre-registered with the Registry on
  bridge start so `Bus` can enforce cardinality budgets against them.
  """
  @spec bridged_sources(map()) :: [{atom(), [Signal.kind()]}]
  def bridged_sources(mapping) do
    mapping
    |> Map.values()
    |> Enum.group_by(fn {source, _name, _kind} -> source end, fn {_s, _n, k} -> k end)
    |> Enum.map(fn {source, kinds} -> {source, Enum.uniq(kinds)} end)
  end

  @doc """
  The half derived from the provider table as it stands, for the providers a
  package installed: each one's shared verbs and its own declared ones under
  `[:flezha, :plugin, <name>, verb]`, `name` being the one its declaration
  answers and `ProviderBehaviour.emit/4` writes, sourced `flezha_<name>`, less
  every event `mapping/0` already names. A first-party provider's events are
  hand-kept lines, which the suite judges against what the tree writes. Read
  once at bridge start, which a package's provider row precedes.
  """
  @spec provider_mapping() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def provider_mapping() do
    for {_id, module} <- Providers.installed(),
        Code.ensure_loaded?(module),
        decl = module.__provider__(),
        # A package built before `telemetry:` existed declares no own verbs.
        {verb, kind} <- @verb_kinds ++ Map.get(decl, :telemetry, []),
        # A name from code a verified package ships, never input.
        event = ProviderBehaviour.event(String.to_atom(decl.name), verb),
        not Map.has_key?(@all_mapping, event),
        into: %{},
        do: {event, {:"flezha_#{decl.name}", verb, kind}}
  end

  @doc false
  @spec handler_id() :: atom()
  def handler_id(), do: @handler_id

  @doc """
  The bridged events: the derived half under the hand-kept one. A detector's
  declared event maps to `{:flezha_health, <last segment>, :event}` unless the
  hand-kept half already names it.
  """
  @spec mapping() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def mapping(), do: @all_mapping

  @doc "The half derived from the convergence loop's detector declarations."
  @spec derived() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def derived(), do: @derived

  @doc """
  Event names the tree writes that reach no exporter. A ratchet: the suite pins
  the total, so an event can leave this list and none may join.
  """
  @spec unexported() :: [[atom()]]
  def unexported(), do: Enum.concat(Map.values(@unexported))

  @doc "Why `event` reaches no exporter, or `nil` when it does."
  @spec unexported_reason([atom()]) :: atom() | nil
  def unexported_reason(event) do
    Enum.find_value(@unexported, fn {reason, events} -> if event in events, do: reason end)
  end

  @doc "The reasons an event may carry no bridge line."
  @spec reasons() :: [atom()]
  def reasons(), do: Map.keys(@unexported)

  @doc "Mapped events nothing writes any more, each with the reason its line stays."
  @spec unwritten() :: %{[atom()] => binary()}
  def unwritten(), do: @unwritten

  # ---------- :telemetry handler entry point ----------

  @doc false
  def handle_event(event, measurements, metadata, _config) do
    try do
      do_handle_event(event, measurements, metadata)
    rescue
      e -> handle_error(event, e, __STACKTRACE__)
    catch
      kind, reason -> handle_error(event, {kind, reason}, __STACKTRACE__)
    end

    :ok
  end

  defp do_handle_event(event, measurements, metadata) do
    case Map.fetch(@all_mapping, event) do
      {:ok, {source, name, kind}} ->
        signal =
          Signal.new(%{
            kind: kind,
            source: source,
            name: name,
            measurements: measurements,
            metadata: metadata
          })

        Bus.emit(signal)

      :error ->
        :ok
    end
  end

  # A provider-derived event, whose handler carries its own line.
  @doc false
  def handle_line(event, measurements, metadata, line),
    do: emit_line(&Bus.emit/1, event, measurements, metadata, line)

  # A controller hook's event, which reaches only the sinks naming its source.
  @doc false
  def handle_hook(event, measurements, metadata, line),
    do: emit_line(&Bus.emit_named/1, event, measurements, metadata, line)

  defp emit_line(emit, event, measurements, metadata, {source, name, kind}) do
    emit.(
      Signal.new(%{
        kind: kind,
        source: source,
        name: name,
        measurements: measurements,
        metadata: metadata
      })
    )
  rescue
    e -> handle_error(event, e, __STACKTRACE__)
  catch
    kind, reason -> handle_error(event, {kind, reason}, __STACKTRACE__)
  end

  defp handle_error(event, error, stacktrace) do
    Counters.bump(:telemetry_bridge_failures)
    class = Counters.error_class(error)

    if Counters.first_seen?({:telemetry_bridge, event, class}) do
      Logger.error(
        "[observability.telemetry_bridge] error event=#{inspect(event)} " <>
          "class=#{inspect(class)} error=#{inspect(error)} " <>
          "stack=#{Exception.format_stacktrace(stacktrace)}"
      )
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    providers = provider_mapping()
    all = Map.merge(providers, @all_mapping)

    # Pre-register the bridged sources so the Bus + Registry track
    # them (cardinality budget, source-status, etc.).
    Enum.each(bridged_sources(all), fn {source, kinds} ->
      _ =
        Registry.register_source(%{
          name: source,
          kinds: kinds,
          flavour: :declarative
        })
    end)

    compiled = Map.keys(@all_mapping) -- Map.keys(@hook_mapping)
    :ok = :telemetry.attach_many(@handler_id, compiled, &__MODULE__.handle_event/4, nil)

    # A handler's config is copied out of telemetry's table on every execute of
    # its events, so each of these carries only its own line.
    for {handler, lines} <- [handle_line: providers, handle_hook: @hook_mapping],
        {event, line} <- lines,
        do:
          :ok =
            :telemetry.attach(
              {@handler_id, event},
              event,
              Function.capture(__MODULE__, handler, 4),
              line
            )

    {:ok, %{line_events: Map.keys(providers) ++ Map.keys(@hook_mapping)}}
  end

  @impl true
  def terminate(_reason, state) do
    _ = :telemetry.detach(@handler_id)
    Enum.each(state.line_events, &:telemetry.detach({@handler_id, &1}))
    :ok
  end
end

defmodule Flezha.Observability.TelemetryBridge do
  @moduledoc """
  Single dispatching `:telemetry` handler that lifts existing
  `[:flezha, ...]` telemetry events onto the observability bus.

  No producer call-site is rewritten. The mapping has three halves: the
  hand-kept lifecycle half in `@lines`; a half DERIVED from the convergence
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
  alias Flezha.Observability.{Bus, Counters, Registry, Signal, Source}
  alias Flezha.Plugin.{ProviderBehaviour, Providers}

  @handler_id :flezha_observability_telemetry_bridge

  # The hand-kept half: event => {source, name, kind, meaning}. The meaning is
  # what a reader is told the signal says; it never reaches the per-event path,
  # which reads `@all_mapping`, the same lines without it.
  @lines %{
    [:flezha, :state, :wal, :backpressure] =>
      {:flezha_state, :wal_backpressure, :metric,
       "A Cell's write-ahead log is pushing back on its writer."},
    [:flezha, :state, :fsync, :tick] =>
      {:flezha_state, :fsync_tick, :event, "A batched fsync flushed pending writes to disk."},
    [:flezha, :boot, :recovery_complete] =>
      {:flezha_boot, :recovery_complete, :event, "A hub finished replaying its relays at boot."},
    [:flezha, :boot, :reconcile_duplicate] =>
      {:flezha_boot, :reconcile_duplicate, :event,
       "Boot found a relay running on more than one node and stopped the extras."},
    [:flezha, :shutdown, :requested] =>
      {:flezha_shutdown, :requested, :event, "A graceful shutdown of the controller began."},
    [:flezha, :shutdown, :phase_started] =>
      {:flezha_shutdown, :phase_started, :event, "A shutdown phase started."},
    [:flezha, :shutdown, :phase_completed] =>
      {:flezha_shutdown, :phase_completed, :event, "A shutdown phase finished."},
    [:flezha, :shutdown, :drain_progress] =>
      {:flezha_shutdown, :drain_progress, :event, "A hub finished draining its relays."},
    [:flezha, :shutdown, :timeout] =>
      {:flezha_shutdown, :timeout, :event, "A shutdown phase ran out of time."},
    [:flezha, :shutdown, :complete] =>
      {:flezha_shutdown, :complete, :event, "The graceful shutdown finished."},
    [:flezha, :plugin_package, :loaded] =>
      {:flezha_plugin_packages, :loaded, :event, "A plugin package was loaded at boot."},
    [:flezha, :plugin_package, :refused] =>
      {:flezha_plugin_packages, :refused, :event, "A plugin package was refused at boot."},
    [:flezha, :plugin, :websocket, :idle_reconnect] =>
      {:flezha_ws, :idle_reconnect, :event,
       "A websocket fell silent for too long and was reconnected."},
    [:flezha, :plugin, :websocket, :resubscribe] =>
      {:flezha_ws, :resubscribe, :event, "A websocket sent its subscriptions again."},
    [:flezha, :plugin, :websocket, :reconcile] =>
      {:flezha_ws, :reconcile, :event, "A websocket compared its subscriptions with the feed's."},
    [:flezha, :state, :seq_gap] =>
      {:flezha_state, :seq_gap, :event, "A Cell's state arrived with a gap in its sequence."},
    [:flezha, :state, :credit_anomaly] =>
      {:flezha_state, :credit_anomaly, :event,
       "A runner wrote past the credit it was given; a protocol fault, not back-pressure."},
    [:flezha, :state, :output_stall] =>
      {:flezha_state, :output_stall, :event, "A Cell has produced no output for too long."},
    [:flezha, :state, :compaction_timeout] =>
      {:flezha_state, :compaction_timeout, :event, "A Cell's log compaction ran out of time."},
    [:flezha, :state, :compaction_trigger] =>
      {:flezha_state, :compaction_trigger, :event, "A Cell's log grew enough to be compacted."},
    [:flezha, :state, :baseline_seed] =>
      {:flezha_state, :baseline_seed, :event, "A Cell's log took its first sequence number."},
    [:flezha, :state, :wal_realign] =>
      {:flezha_state, :wal_realign, :event,
       "A Cell's log moved its sequence to match its runner."},
    [:flezha, :state, :stale_writer_refused] =>
      {:flezha_state, :stale_writer_refused, :event,
       "A write from a runner that no longer owns the Cell was refused."},
    [:flezha, :state, :rpo_exceeded] =>
      {:flezha_durability, :rpo_exceeded, :event,
       "Remote copies fell further behind than the recovery point objective allows."},
    [:flezha, :state, :ship_failed] =>
      {:flezha_durability, :ship_failed, :event,
       "Shipping a state artifact to remote storage failed."},
    [:flezha, :state, :artifact_shipped] =>
      {:flezha_durability, :artifact_shipped, :metric,
       "A state artifact was shipped to remote storage."},
    [:flezha, :state, :remote_commit] =>
      {:flezha_durability, :remote_commit, :metric, "Remote storage confirmed a commit."},
    [:flezha, :state, :hydrated] =>
      {:flezha_durability, :hydrated, :event, "State was restored from remote storage."},
    [:flezha, :state, :hydration_failed] =>
      {:flezha_durability, :hydration_failed, :event,
       "Restoring state from remote storage failed."},
    [:flezha, :state, :artifact_missing] =>
      {:flezha_durability, :artifact_missing, :event,
       "Remote storage lacks an artifact the state refers to."},
    [:flezha, :state, :durability_gc] =>
      {:flezha_durability, :durability_gc, :metric,
       "Old artifacts were removed from remote storage."},
    [:flezha, :error, :uncatalogued] =>
      {:flezha_error, :uncatalogued, :event,
       "A caller got an error with no catalogue code: a defect to report."},
    [:flezha, :stream, :drop] =>
      {:flezha_stream, :drop, :metric, "Messages on a runner's stream were dropped."},
    [:flezha, :convergence, :intent_mismatch] =>
      {:flezha_convergence, :intent_mismatch, :event,
       "Durable state exists for relays nothing intends to run."},
    [:flezha, :dlq, :parked] =>
      {:flezha_dlq, :dlq_parked_total, :metric, "Messages were put in the dead-letter queue."},
    [:flezha, :dlq, :retention_dropped] =>
      {:flezha_dlq, :dlq_dropped_by_retention_total, :metric,
       "Dead letters were deleted by the retention limit."},
    [:flezha, :dlq, :usage] =>
      {:flezha_dlq, :dlq_usage, :metric, "Entries and bytes a tasklet's dead-letter queue holds."},
    [:flezha, :plugin, :relay_forward, :shipped] =>
      {:flezha_relay_forward, :delivered, :metric, "Messages were forwarded to the next relay."},
    [:flezha, :plugin, :relay_forward, :retried] =>
      {:flezha_relay_forward, :retry, :metric, "A forward failed and will be tried again."},
    [:flezha, :plugin, :relay_forward, :queue_depth] =>
      {:flezha_relay_forward, :queue_depth, :metric, "Messages waiting to be forwarded."},
    [:flezha, :plugin, :relay_forward, :overflowed] =>
      {:flezha_relay_forward, :saturated, :event,
       "The forward queue was full and refused messages."},
    [:flezha, :plugin, :http_poll, :shipped] =>
      {:flezha_http_poll, :injected, :metric, "A poll's response was delivered to the relay."},
    [:flezha, :plugin, :http_poll, :unchanged] =>
      {:flezha_http_poll, :unchanged, :metric,
       "A poll's response matched the last one and was skipped."},
    [:flezha, :plugin, :http_poll, :failed] =>
      {:flezha_http_poll, :failed, :event, "A poll request failed."},
    [:flezha, :plugin, :file_source, :shipped] =>
      {:flezha_file_source, :injected, :metric, "Lines read from the file were delivered."},
    [:flezha, :plugin, :file_source, :truncated] =>
      {:flezha_file_source, :truncated, :event,
       "The file was truncated or replaced; it is read again from the start."},
    [:flezha, :plugin, :file_source, :failed] =>
      {:flezha_file_source, :failed, :event, "Reading the file failed."},
    [:flezha, :plugin, :postgres_poll, :shipped] =>
      {:flezha_postgres_poll, :injected, :metric, "Rows read from the table were delivered."},
    [:flezha, :plugin, :postgres_poll, :failed] =>
      {:flezha_postgres_poll, :failed, :event, "A Postgres poll failed."},
    [:flezha, :plugin, :postgres_poll, :caught_up] =>
      {:flezha_postgres_poll, :caught_up, :event, "The poll reached the newest row."},
    [:flezha, :plugin, :file_sink, :shipped] =>
      {:flezha_file_sink, :flushed, :metric, "Lines were written to the file; count is how many."},
    [:flezha, :plugin, :file_sink, :refused] =>
      {:flezha_file_sink, :refused, :event,
       "A record was refused because it cannot be written as one line."},
    [:flezha, :plugin, :file_sink, :failed] =>
      {:flezha_file_sink, :failed, :event, "Writing to the file failed."},
    [:flezha, :relay, :started] =>
      {:flezha_relay, :relay_started, :event, "A relay was started."},
    [:flezha, :relay, :stopped] =>
      {:flezha_relay, :relay_stopped, :event, "A relay was stopped or completed."},
    # Only the sinks naming this source hear it (`Flezha.Observability.HookSink`).
    [:flezha, :dlq, :dead_lettered] =>
      {:flezha_hooks, :on_dead_letter, :event,
       "A message became a dead letter; controller plugins are told."},
    [:flezha, :plugin, :hook_webhook, :shipped] =>
      {:flezha_hook_webhook, :shipped, :metric, "A webhook request was sent."},
    [:flezha, :plugin, :hook_webhook, :retried] =>
      {:flezha_hook_webhook, :retried, :metric, "A webhook request failed and will be retried."},
    [:flezha, :plugin, :hook_webhook, :failed] =>
      {:flezha_hook_webhook, :failed, :event, "A webhook request was given up."},
    [:flezha, :plugin, :hook_webhook, :overflowed] =>
      {:flezha_hook_webhook, :overflowed, :metric,
       "The webhook queue was full and requests were dropped."},
    [:flezha, :plugin, :http_push, :shipped] =>
      {:flezha_http_push, :shipped, :metric, "An HTTP request was sent."},
    [:flezha, :plugin, :http_push, :retried] =>
      {:flezha_http_push, :retried, :metric, "An HTTP request failed and will be retried."},
    [:flezha, :plugin, :http_push, :failed] =>
      {:flezha_http_push, :failed, :event, "An HTTP request was given up."},
    [:flezha, :plugin, :http_push, :overflowed] =>
      {:flezha_http_push, :overflowed, :metric,
       "The HTTP queue was full and requests were dropped."},
    [:flezha, :plugin, :phase] =>
      {:flezha_plugin, :phase, :event, "A plugin entered or left a lifecycle phase."},
    [:flezha, :relay, :parked] =>
      {:flezha_relay, :relay_parked_total, :metric, "A relay was parked after a failure."},
    [:flezha, :steps, :turn_dead_lettered] =>
      {:flezha_steps, :flezha_steps_turns_dead_lettered_total, :metric,
       "A steps session's turn failed and was parked; the session goes on."},
    [:flezha, :call, :answered] =>
      {:flezha_call, :flezha_call_answer_duration, :metric,
       "A call was answered; how long it took."},
    [:flezha, :call, :timeout] =>
      {:flezha_call, :flezha_call_timeouts_total, :metric, "A call got no answer in time."},
    [:flezha, :call, :answer_dropped] =>
      {:flezha_call, :flezha_call_answers_dropped_total, :metric,
       "An answer arrived that no caller was waiting for."},
    [:flezha, :call, :refused] =>
      {:flezha_call, :flezha_call_refused_total, :metric, "A call was refused."},
    [:flezha, :call, :recovered_unanswered] =>
      {:flezha_call, :flezha_call_recovered_unanswered_total, :metric,
       "A call recovered after a restart was settled without an answer."},
    [:flezha, :backoff, :wait] =>
      {:flezha_convergence, :backoff_wait, :metric, "A retry waited before its next attempt."},
    [:flezha, :backoff, :reset] =>
      {:flezha_convergence, :backoff_reset, :metric, "A success reset a retry's wait."},
    [:flezha, :hub, :start_failed] =>
      {:flezha_boot, :hub_start_failed, :event, "A namespace's hub failed to start."},
    [:flezha, :boot, :pre_recovery_replay] =>
      {:flezha_boot, :pre_recovery_replay, :event, "A hub began replaying its relays at boot."},
    [:flezha, :plugin, :websocket, :shipped] =>
      {:flezha_ws, :shipped, :metric, "Messages were sent over a websocket."},
    [:flezha, :plugin, :websocket, :retried] =>
      {:flezha_ws, :retried, :event, "A websocket send failed and will be retried."},
    [:flezha, :plugin, :websocket, :failed] =>
      {:flezha_ws, :failed, :event, "A websocket send was given up."},
    [:flezha, :plugin, :websocket, :overflowed] =>
      {:flezha_ws, :overflowed, :event, "A websocket's queue was full and messages were dropped."},
    [:flezha, :correction, :refused] =>
      {:flezha_correction, :refused, :event, "The controller refused to apply a correction."},
    [:flezha, :runner, :connected] =>
      {:flezha_runner, :runner_connected, :event, "A runner connected."},
    [:flezha, :runner, :disconnected] =>
      {:flezha_runner, :runner_disconnected, :event, "A runner disconnected."},
    [:flezha, :runner, :dropped] =>
      {:flezha_runner, :runner_dropped, :event, "An operator dropped a runner process."},
    [:flezha, :runner, :readmitted] =>
      {:flezha_runner, :runner_readmitted, :event, "A dropped runner process was let back in."},
    [:flezha, :runner, :drop_refused] =>
      {:flezha_runner, :runner_drop_refused, :event,
       "A dropped runner process tried to reconnect and was refused."},
    [:flezha, :runner, :factory_language_conflict] =>
      {:flezha_runner, :factory_language_conflict, :event,
       "A runner offered a factory already served in another language."},
    [:flezha, :runner, :factory_run_conflict] =>
      {:flezha_runner, :factory_run_conflict, :event,
       "A runner offered a factory that conflicts with one already running."},
    [:flezha, :runner, :quota_refused] =>
      {:flezha_runner, :quota_refused, :event,
       "A runner was refused because the namespace's runner quota is full."},
    [:flezha, :runner, :admin_request] =>
      {:flezha_runner, :admin_request, :event, "A runner sent an administration request."},
    [:flezha, :runner, :foreign_frame_refused] =>
      {:flezha_runner, :foreign_frame_refused, :event,
       "A runner sent a frame for a relay it does not own; it was refused."},
    [:flezha, :schedule, :spawn_failed] =>
      {:flezha_schedule, :spawn_failed, :event, "A schedule fired but its relay failed to start."},
    [:flezha, :stream, :fanout_warning] =>
      {:flezha_stream, :fanout_warning, :event,
       "A stream found more than one process for one relay."},
    [:flezha, :state, :claim_superseded] =>
      {:flezha_durability, :claim_superseded, :event,
       "Another node took over shipping a relay's state."},
    [:flezha, :state, :compaction_failed] =>
      {:flezha_state, :compaction_failed, :event, "A Cell's log compaction failed."},
    [:flezha, :state, :compaction_gave_up] =>
      {:flezha_state, :compaction_gave_up, :event, "A Cell's log compaction was given up."},
    [:flezha, :state, :disk_quota_breach] =>
      {:flezha_state, :disk_quota_breach, :event, "A namespace's state passed its disk quota."},
    [:flezha, :state, :inbox_duplicate] =>
      {:flezha_state, :inbox_duplicate, :event, "A relay received a message it already had."},
    [:flezha, :state, :input_log_frozen_watermark] =>
      {:flezha_state, :input_log_frozen_watermark, :event,
       "A relay's input log stopped advancing."},
    [:flezha, :state, :input_log_syncer_died] =>
      {:flezha_state, :input_log_syncer_died, :event, "A relay's input log syncer crashed."},
    [:flezha, :state, :record_frame, :refused] =>
      {:flezha_state, :record_frame_refused, :event,
       "A damaged record at the end of a durable log was refused."},
    [:flezha, :state, :restore_refused] =>
      {:flezha_state, :restore_refused, :event, "A relay's state could not be restored."},
    [:flezha, :state, :wal_pull_failed] =>
      {:flezha_state, :wal_pull_failed, :event, "Pulling a log from another node failed."},
    [:flezha, :state, :record_frame, :repaired] =>
      {:flezha_state, :record_frame_repaired, :event,
       "A torn record at the end of a durable log was discarded."},
    [:flezha, :state, :wal_rewind] =>
      {:flezha_state, :wal_rewind, :event, "A Cell's log was rewound to an earlier sequence."}
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
  @unwritten %{}

  # Derived at COMPILE time from the detector table, so adding a detector
  # recompiles this module and its event is bridged with no edit here.
  @derived_lines Map.new(Table.event_descriptions(), fn {event, meaning} ->
                   {event, {:flezha_health, List.last(event), :event, meaning}}
                 end)

  @derived Map.new(@derived_lines, fn {event, {source, name, kind, _meaning}} ->
             {event, {source, name, kind}}
           end)

  @all_lines Map.merge(@derived_lines, @lines)
  @all_mapping Map.new(@all_lines, fn {event, {source, name, kind, _meaning}} ->
                 {event, {source, name, kind}}
               end)

  # Every source a line names, with where it sits and what it is.
  @sources %{
    flezha_boot: {:controller, "Controller boot: recovering relays and starting hubs."},
    flezha_shutdown: {:controller, "Graceful shutdown of the controller and its phases."},
    flezha_error: {:controller, "Errors that reached a caller without a catalogue code."},
    flezha_state: {:storage, "Each Cell's write-ahead log: writes, compaction and repairs."},
    flezha_durability:
      {:storage, "Copies of relay state shipped to and restored from remote storage."},
    flezha_relay: {:relays, "Relays starting, stopping, parking and exiting."},
    flezha_runner: {:relays, "Runner sessions: connects, disconnects, drops and refusals."},
    flezha_call: {:relays, "Calls to relays: answers, timeouts and refusals."},
    flezha_steps: {:relays, "Steps sessions and their failed turns."},
    flezha_dlq: {:relays, "Dead-letter queues: messages parked and removed."},
    flezha_stream: {:relays, "Runner streams carrying messages to and from relays."},
    flezha_schedule: {:relays, "Schedules that start relays on a timetable."},
    flezha_health: {:relays, "Health checks' findings about relays, runners and plugins."},
    flezha_convergence: {:relays, "The loop that keeps running relays matching what was asked."},
    flezha_correction: {:relays, "Corrections the controller refused to apply."},
    flezha_plugin: {:plugins, "Plugins entering and leaving their lifecycle phases."},
    flezha_plugin_packages: {:plugins, "Plugin packages loaded or refused at boot."},
    flezha_hooks: {:plugins, "Notices handed to controller plugins."},
    flezha_ws: {:plugins, "The websocket plugin: connections, subscriptions and sends."},
    flezha_relay_forward:
      {:plugins, "The relay forward plugin, which hands messages to the next relay."},
    flezha_http_poll: {:plugins, "The HTTP poll plugin, which reads a URL on a timer."},
    flezha_http_push: {:plugins, "The HTTP push plugin, which sends relay output to a URL."},
    flezha_hook_webhook:
      {:plugins, "The webhook plugin, which sends controller notices to a URL."},
    flezha_file_source: {:plugins, "The file source plugin, which reads lines from a file."},
    flezha_file_sink: {:plugins, "The file sink plugin, which writes relay output to a file."},
    flezha_postgres_poll:
      {:plugins, "The Postgres poll plugin, which reads new rows from a table."}
  }

  for {_event, {source, _name, _kind}} <- @all_mapping, not Map.has_key?(@sources, source) do
    raise CompileError,
      description: "bridged source #{inspect(source)} has no area and description in @sources"
  end

  # The controller hooks' lines: plugin notices, not telemetry, which only the
  # sinks naming their source hear (`Bus.emit_named/1`).
  @hook_mapping for {_event, {:flezha_hooks, _name, _kind}} = row <- @all_mapping,
                    into: %{},
                    do: row

  # The shared provider verbs: a failure is read one at a time, the rest are counts.
  @shared_verbs for {verb, meaning} <- ProviderBehaviour.verb_descriptions(),
                    do: {verb, if(verb == :failed, do: :event, else: :metric), meaning}

  # ---------- Public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  What the bridge registers for each source its lines name: kinds, area,
  description and every signal with its meaning. `providers` are the
  provider-derived lines, as `provider_lines/0` answers them. Registered on
  bridge start so `Bus` enforces cardinality budgets against them and a
  reader can tell what each one says.
  """
  @spec registrations(map()) :: [map()]
  def registrations(providers \\ provider_lines()) do
    (Map.values(@all_lines) ++ Map.values(providers))
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.map(fn {source, lines} ->
      {area, description} = Map.get_lazy(@sources, source, fn -> package_source(source) end)

      %{
        name: source,
        kinds: lines |> Enum.map(&elem(&1, 2)) |> Enum.uniq(),
        flavour: :declarative,
        area: area,
        description: description,
        signals:
          Source.signals(
            for {_source, name, kind, meaning} <- Enum.uniq_by(lines, &elem(&1, 1)),
                do: {name, {kind, meaning}}
          )
      }
    end)
  end

  defp package_source(source) do
    name = source |> Atom.to_string() |> String.replace_prefix("flezha_", "")
    {:plugins, "The #{name} plugin, installed from a package."}
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
  def provider_mapping(), do: strip(provider_lines())

  @doc "`provider_mapping/0` with each line's meaning."
  @spec provider_lines() :: %{[atom()] => {atom(), atom(), Signal.kind(), String.t()}}
  def provider_lines() do
    for {_id, module} <- Providers.installed(),
        Code.ensure_loaded?(module),
        decl = module.__provider__(),
        {verb, kind, meaning} <- @shared_verbs ++ own_verbs(decl),
        # A name from code a verified package ships, never input.
        event = ProviderBehaviour.event(String.to_atom(decl.name), verb),
        not Map.has_key?(@all_mapping, event),
        into: %{},
        do: {event, {:"flezha_#{decl.name}", verb, kind, meaning}}
  end

  # A package built before `telemetry:` existed declares no own verbs, and one
  # built before a verb could carry a meaning declares a bare kind.
  defp own_verbs(decl) do
    for {verb, declared} <- Map.get(decl, :telemetry, []) do
      case declared do
        {kind, meaning} -> {verb, kind, meaning}
        kind -> {verb, kind, "No description from the plugin."}
      end
    end
  end

  defp strip(lines),
    do:
      Map.new(lines, fn {event, {source, name, kind, _meaning}} ->
        {event, {source, name, kind}}
      end)

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

    provider_lines = provider_lines()
    providers = strip(provider_lines)

    # A refused registration leaves its signals unbudgeted and unlisted, so it
    # is said rather than dropped.
    for entry <- registrations(provider_lines),
        {:error, reason} <- [Registry.register_source(entry)] do
      Logger.error(
        "[observability.telemetry_bridge] source #{inspect(entry.name)} not registered: " <>
          inspect(reason)
      )
    end

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

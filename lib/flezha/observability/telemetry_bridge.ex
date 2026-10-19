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

  # The hand-kept half: event => {source, name, kind, words}, `words` being what
  # `Flezha.Observability.Source.signals/1` takes beside a signal's kind. The
  # words are what a reader is told; they never reach the per-event path, which
  # reads `@all_mapping`, the same lines without them.
  @lines %{
    [:flezha, :state, :wal, :backpressure] =>
      {:flezha_state, :wal_backpressure, :metric,
       label: "Writes held back",
       outcome: :failure,
       meaning: "A relay wrote faster than its saved state could keep up, so its writes waited."},
    [:flezha, :state, :fsync, :tick] =>
      {:flezha_state, :fsync_tick, :event,
       label: "Flushed to disk",
       meaning: "Pending writes were flushed to disk together.",
       sentence: "Flushed {count:pending write} to disk"},
    [:flezha, :boot, :recovery_complete] =>
      {:flezha_boot, :recovery_complete, :event,
       label: "Relays restored",
       meaning: "A namespace finished bringing its relays back after the controller started."},
    [:flezha, :boot, :reconcile_duplicate] =>
      {:flezha_boot, :reconcile_duplicate, :event,
       label: "Duplicate stopped",
       outcome: :failure,
       meaning:
         "A relay was found running on more than one node, and the extra copies were stopped."},
    [:flezha, :shutdown, :requested] =>
      {:flezha_shutdown, :requested, :event,
       label: "Shutdown started", meaning: "Someone asked the controller to stop gracefully."},
    [:flezha, :shutdown, :phase_started] =>
      {:flezha_shutdown, :phase_started, :event,
       label: "Step started", meaning: "A step of the shutdown began."},
    [:flezha, :shutdown, :phase_completed] =>
      {:flezha_shutdown, :phase_completed, :event,
       label: "Step finished", meaning: "A step of the shutdown finished."},
    [:flezha, :shutdown, :drain_progress] =>
      {:flezha_shutdown, :drain_progress, :event,
       label: "Relays moved",
       meaning: "A namespace finished moving its relays to other nodes before the stop."},
    [:flezha, :shutdown, :timeout] =>
      {:flezha_shutdown, :timeout, :event,
       label: "Step timed out",
       outcome: :failure,
       meaning: "A step of the shutdown ran out of time."},
    [:flezha, :shutdown, :complete] =>
      {:flezha_shutdown, :complete, :event,
       label: "Shutdown finished", meaning: "The controller finished stopping gracefully."},
    [:flezha, :plugin_package, :loaded] =>
      {:flezha_plugin_packages, :loaded, :event,
       label: "Loaded", meaning: "A plugin package was loaded when the controller started."},
    [:flezha, :plugin_package, :refused] =>
      {:flezha_plugin_packages, :refused, :event,
       label: "Refused",
       outcome: :failure,
       meaning:
         "A plugin package was refused when the controller started, so its plugins are missing."},
    [:flezha, :plugin, :websocket, :idle_reconnect] =>
      {:flezha_ws, :idle_reconnect, :event,
       label: "Reconnected after silence",
       meaning: "A websocket was silent for too long, so it was reconnected.",
       sentence: "{plugin_id} reconnected after {silence_ms} ms of silence"},
    [:flezha, :plugin, :websocket, :resubscribe] =>
      {:flezha_ws, :resubscribe, :event,
       label: "Subscribed again",
       meaning: "A websocket sent its subscriptions again after its feed went quiet.",
       sentence: "{plugin_id} subscribed again"},
    [:flezha, :plugin, :websocket, :reconcile] =>
      {:flezha_ws, :reconcile, :event,
       label: "Subscriptions checked",
       meaning: "A websocket compared its subscriptions with what its feed reports.",
       sentence: "{plugin_id} checked its subscriptions: {added} added, {removed} removed"},
    [:flezha, :state, :seq_gap] =>
      {:flezha_state, :seq_gap, :event,
       label: "Gap in sequence",
       outcome: :failure,
       meaning: "A relay's saved state arrived with a gap in its sequence numbers."},
    [:flezha, :state, :credit_anomaly] =>
      {:flezha_state, :credit_anomaly, :event,
       label: "Wrote past its allowance",
       outcome: :failure,
       meaning:
         "A runner wrote more than the controller allowed it to: a fault in the runner, not load."},
    [:flezha, :state, :output_stall] =>
      {:flezha_state, :output_stall, :event,
       label: "No output",
       outcome: :failure,
       meaning: "A relay has produced no output for longer than allowed."},
    [:flezha, :state, :compaction_timeout] =>
      {:flezha_state, :compaction_timeout, :event,
       label: "Clean-up timed out",
       outcome: :failure,
       meaning: "Shrinking a relay's saved state took too long and was stopped."},
    [:flezha, :state, :compaction_trigger] =>
      {:flezha_state, :compaction_trigger, :event,
       label: "Clean-up started", meaning: "A relay's saved state grew enough to be shrunk."},
    [:flezha, :state, :baseline_seed] =>
      {:flezha_state, :baseline_seed, :event,
       label: "First write", meaning: "A relay's saved state took its first sequence number."},
    [:flezha, :state, :wal_realign] =>
      {:flezha_state, :wal_realign, :event,
       label: "Sequence realigned",
       meaning: "A relay's saved state moved its sequence number to match its runner's."},
    [:flezha, :state, :stale_writer_refused] =>
      {:flezha_state, :stale_writer_refused, :event,
       label: "Old writer refused",
       outcome: :failure,
       meaning: "A runner that no longer owns a relay tried to write its state and was refused."},
    [:flezha, :state, :rpo_exceeded] =>
      {:flezha_durability, :rpo_exceeded, :event,
       label: "Backups too far behind",
       outcome: :failure,
       meaning: "Remote copies fell further behind than the recovery point allows."},
    [:flezha, :state, :ship_failed] =>
      {:flezha_durability, :ship_failed, :event,
       label: "Upload failed",
       outcome: :failure,
       meaning: "Sending a copy of a relay's state to remote storage failed."},
    [:flezha, :state, :artifact_shipped] =>
      {:flezha_durability, :artifact_shipped, :metric,
       label: "Uploaded", meaning: "A copy of a relay's state was sent to remote storage."},
    [:flezha, :state, :remote_commit] =>
      {:flezha_durability, :remote_commit, :metric,
       label: "Upload confirmed", meaning: "Remote storage confirmed it holds a copy."},
    [:flezha, :state, :hydrated] =>
      {:flezha_durability, :hydrated, :event,
       label: "Restored", meaning: "A relay's state was restored from remote storage."},
    [:flezha, :state, :hydration_failed] =>
      {:flezha_durability, :hydration_failed, :event,
       label: "Restore failed",
       outcome: :failure,
       meaning: "Restoring a relay's state from remote storage failed."},
    [:flezha, :state, :artifact_missing] =>
      {:flezha_durability, :artifact_missing, :event,
       label: "Copy missing",
       outcome: :failure,
       meaning: "Remote storage lacks a copy that a relay's saved state refers to."},
    [:flezha, :state, :durability_gc] =>
      {:flezha_durability, :durability_gc, :metric,
       label: "Old copies removed",
       meaning: "Copies no longer needed were removed from remote storage.",
       sentence: "Removed {collected:old copy} from remote storage"},
    [:flezha, :error, :uncatalogued] =>
      {:flezha_error, :uncatalogued, :event,
       label: "Undocumented error",
       outcome: :failure,
       meaning: "A caller got an error with no documented code: a defect to report."},
    [:flezha, :stream, :drop] =>
      {:flezha_stream, :drop, :metric,
       label: "Dropped",
       outcome: :failure,
       meaning: "A message for a relay was dropped because the relay had no open stream.",
       sentence: "A message for {relay_id} was dropped"},
    [:flezha, :convergence, :intent_mismatch] =>
      {:flezha_convergence, :intent_mismatch, :event,
       label: "Leftover state",
       outcome: :failure,
       meaning: "Saved state exists for relays nobody asked to run."},
    [:flezha, :dlq, :parked] =>
      {:flezha_dlq, :dlq_parked_total, :metric,
       label: "Set aside",
       outcome: :failure,
       meaning: "A message failed and was set aside in a dead-letter queue.",
       sentence: "{tasklet_id} set aside {count:message}"},
    [:flezha, :dlq, :retention_dropped] =>
      {:flezha_dlq, :dlq_dropped_by_retention_total, :metric,
       label: "Expired", meaning: "Dead letters older than the retention limit were deleted."},
    [:flezha, :dlq, :usage] =>
      {:flezha_dlq, :dlq_usage, :metric,
       label: "Queue size",
       meaning: "How many dead letters a queue holds, and how many bytes.",
       sentence: "{tasklet_id} holds {entries:dead letter}"},
    [:flezha, :plugin, :relay_forward, :shipped] =>
      {:flezha_relay_forward, :delivered, :metric,
       label: "Forwarded", meaning: "Messages were handed to the next relay."},
    [:flezha, :plugin, :relay_forward, :retried] =>
      {:flezha_relay_forward, :retry, :metric,
       label: "Retrying", meaning: "A forward failed and will be tried again."},
    [:flezha, :plugin, :relay_forward, :queue_depth] =>
      {:flezha_relay_forward, :queue_depth, :metric,
       label: "Waiting to forward",
       meaning: "How many messages are waiting to be forwarded.",
       sentence: "{depth:message} waiting to be forwarded"},
    [:flezha, :plugin, :relay_forward, :overflowed] =>
      {:flezha_relay_forward, :saturated, :event,
       label: "Queue full",
       outcome: :failure,
       meaning: "The forward queue was full and turned messages away."},
    [:flezha, :plugin, :http_poll, :shipped] =>
      {:flezha_http_poll, :injected, :metric,
       label: "Fetched",
       meaning: "A poll got a new response and handed it to the relay.",
       sentence: "{plugin_id} fetched {bytes:byte} in {latency_ms} ms"},
    [:flezha, :plugin, :http_poll, :unchanged] =>
      {:flezha_http_poll, :unchanged, :metric,
       label: "Nothing new",
       meaning: "A poll's response matched the last one, so it was skipped.",
       sentence: "{plugin_id} found nothing new"},
    [:flezha, :plugin, :http_poll, :failed] =>
      {:flezha_http_poll, :failed, :event,
       label: "Poll failed", outcome: :failure, meaning: "A poll request failed."},
    [:flezha, :plugin, :file_source, :shipped] =>
      {:flezha_file_source, :injected, :metric,
       label: "Read",
       meaning: "A line read from the file was handed to the relay.",
       sentence: "{plugin_id} read a line from {path}"},
    [:flezha, :plugin, :file_source, :truncated] =>
      {:flezha_file_source, :truncated, :event,
       label: "File restarted",
       meaning: "The file was cut short or replaced, so it is read again from the start.",
       sentence: "{plugin_id} reads {path} again from the start"},
    [:flezha, :plugin, :file_source, :failed] =>
      {:flezha_file_source, :failed, :event,
       label: "Read failed", outcome: :failure, meaning: "Reading the file failed."},
    [:flezha, :plugin, :postgres_poll, :shipped] =>
      {:flezha_postgres_poll, :injected, :metric,
       label: "Read rows",
       meaning: "New rows read from the table were handed to the relay.",
       sentence: "{plugin_id} read {count:row} from {table}"},
    [:flezha, :plugin, :postgres_poll, :failed] =>
      {:flezha_postgres_poll, :failed, :event,
       label: "Poll failed", outcome: :failure, meaning: "Reading the table failed."},
    [:flezha, :plugin, :postgres_poll, :caught_up] =>
      {:flezha_postgres_poll, :caught_up, :event,
       label: "Caught up",
       meaning: "The poll reached the newest row in the table.",
       sentence: "{plugin_id} caught up with {table}"},
    [:flezha, :plugin, :file_sink, :shipped] =>
      {:flezha_file_sink, :flushed, :metric,
       label: "Saved",
       meaning: "Records were written to the file.",
       sentence: "{plugin_id} saved {count:record} to {path}"},
    [:flezha, :plugin, :file_sink, :refused] =>
      {:flezha_file_sink, :refused, :event,
       label: "Record refused",
       outcome: :failure,
       meaning: "A record was refused because it cannot be written as one line.",
       sentence: "{plugin_id} refused a record for {path}"},
    [:flezha, :plugin, :file_sink, :failed] =>
      {:flezha_file_sink, :failed, :event,
       label: "Write failed",
       outcome: :failure,
       meaning: "Writing to the file failed; it will be tried again.",
       sentence: "{plugin_id} could not write to {path}"},
    [:flezha, :relay, :started] =>
      {:flezha_relay, :relay_started, :event,
       label: "Started",
       meaning: "A relay was started.",
       sentence: "{relay} started in {namespace}"},
    [:flezha, :relay, :stopped] =>
      {:flezha_relay, :relay_stopped, :event,
       label: "Stopped",
       meaning: "A relay was stopped or finished its work.",
       sentence: "{relay} stopped in {namespace}"},
    # Only the sinks naming this source hear it (`Flezha.Observability.HookSink`).
    [:flezha, :dlq, :dead_lettered] =>
      {:flezha_hooks, :on_dead_letter, :event,
       label: "Dead letter",
       outcome: :failure,
       meaning: "A message became a dead letter, and controller plugins were told."},
    [:flezha, :plugin, :hook_webhook, :shipped] =>
      {:flezha_hook_webhook, :shipped, :metric,
       label: "Sent", meaning: "A webhook request was sent.", sentence: "Sent {count:request}"},
    [:flezha, :plugin, :hook_webhook, :retried] =>
      {:flezha_hook_webhook, :retried, :metric,
       label: "Retrying", meaning: "A webhook request failed and will be tried again."},
    [:flezha, :plugin, :hook_webhook, :failed] =>
      {:flezha_hook_webhook, :failed, :event,
       label: "Given up",
       outcome: :failure,
       meaning: "A webhook request failed too often and was given up."},
    [:flezha, :plugin, :hook_webhook, :overflowed] =>
      {:flezha_hook_webhook, :overflowed, :metric,
       label: "Queue full",
       outcome: :failure,
       meaning: "The webhook queue was full, so requests were dropped."},
    [:flezha, :plugin, :http_push, :shipped] =>
      {:flezha_http_push, :shipped, :metric,
       label: "Sent", meaning: "An HTTP request was sent.", sentence: "Sent {count:request}"},
    [:flezha, :plugin, :http_push, :retried] =>
      {:flezha_http_push, :retried, :metric,
       label: "Retrying", meaning: "An HTTP request failed and will be tried again."},
    [:flezha, :plugin, :http_push, :failed] =>
      {:flezha_http_push, :failed, :event,
       label: "Given up",
       outcome: :failure,
       meaning: "An HTTP request failed too often and was given up."},
    [:flezha, :plugin, :http_push, :overflowed] =>
      {:flezha_http_push, :overflowed, :metric,
       label: "Queue full",
       outcome: :failure,
       meaning: "The HTTP queue was full, so requests were dropped."},
    [:flezha, :plugin, :phase] =>
      {:flezha_plugin, :phase, :event,
       label: "Phase changed",
       meaning:
         "A plugin moved from one phase of its life to another, such as from prepared to running.",
       sentence: "{plugin_id} went from {from} to {to}"},
    [:flezha, :relay, :halted] =>
      {:flezha_relay, :relay_halted_total, :metric,
       label: "Halted",
       outcome: :failure,
       meaning: "A relay was halted after a failure and stays stopped until someone acts.",
       sentence: "{relay_id} halted in {namespace}"},
    [:flezha, :steps, :turn_dead_lettered] =>
      {:flezha_steps, :flezha_steps_turns_dead_lettered_total, :metric,
       label: "Turn set aside",
       outcome: :failure,
       meaning: "A turn of a steps relay failed and became a dead letter; the relay carries on."},
    [:flezha, :call, :answered] =>
      {:flezha_call, :flezha_call_answer_duration, :metric,
       label: "Answered",
       meaning: "A call to a relay was answered; the details say how long it took.",
       sentence: "{relay_id} answered a call"},
    [:flezha, :call, :timeout] =>
      {:flezha_call, :flezha_call_timeouts_total, :metric,
       label: "Timed out",
       outcome: :failure,
       meaning: "A call to a relay got no answer in time.",
       sentence: "A call to {relay_id} timed out"},
    [:flezha, :call, :answer_dropped] =>
      {:flezha_call, :flezha_call_answers_dropped_total, :metric,
       label: "Answer unclaimed",
       outcome: :failure,
       meaning: "A relay answered a call that nobody was waiting for any more."},
    [:flezha, :call, :refused] =>
      {:flezha_call, :flezha_call_refused_total, :metric,
       label: "Refused",
       outcome: :failure,
       meaning: "A call to a relay was refused.",
       sentence: "A call to {relay_id} was refused"},
    [:flezha, :call, :recovered_unanswered] =>
      {:flezha_call, :flezha_call_recovered_unanswered_total, :metric,
       label: "Closed unanswered",
       outcome: :failure,
       meaning: "A call picked up again after a restart was closed without an answer."},
    [:flezha, :backoff, :wait] =>
      {:flezha_convergence, :backoff_wait, :metric,
       label: "Waiting to retry", meaning: "A retry waited before its next attempt."},
    [:flezha, :backoff, :reset] =>
      {:flezha_convergence, :backoff_reset, :metric,
       label: "Retry wait reset", meaning: "A success reset a retry's waiting time."},
    [:flezha, :hub, :start_failed] =>
      {:flezha_boot, :hub_start_failed, :event,
       label: "Namespace failed to start",
       outcome: :failure,
       meaning: "A namespace's hub failed to start, so its relays are not running."},
    [:flezha, :boot, :pre_recovery_replay] =>
      {:flezha_boot, :pre_recovery_replay, :event,
       label: "Restoring relays",
       meaning: "A namespace began bringing its relays back after the controller started."},
    [:flezha, :plugin, :websocket, :shipped] =>
      {:flezha_ws, :shipped, :metric,
       label: "Sent",
       meaning: "Messages were sent over a websocket.",
       sentence: "{plugin_id} sent {count:message}"},
    [:flezha, :plugin, :websocket, :retried] =>
      {:flezha_ws, :retried, :event,
       label: "Retrying", meaning: "A websocket send failed and will be tried again."},
    [:flezha, :plugin, :websocket, :failed] =>
      {:flezha_ws, :failed, :event,
       label: "Send failed",
       outcome: :failure,
       meaning: "A websocket send failed and was given up."},
    [:flezha, :plugin, :websocket, :overflowed] =>
      {:flezha_ws, :overflowed, :event,
       label: "Queue full",
       outcome: :failure,
       meaning: "A websocket's queue was full, so messages were dropped."},
    [:flezha, :correction, :refused] =>
      {:flezha_correction, :refused, :event,
       label: "Correction refused",
       outcome: :failure,
       meaning: "The controller refused to apply a correction to a relay."},
    [:flezha, :runner, :connected] =>
      {:flezha_runner, :runner_connected, :event,
       label: "Connected",
       meaning: "A runner opened its connection to the controller.",
       sentence: "{runner} connected to {namespace}"},
    [:flezha, :runner, :disconnected] =>
      {:flezha_runner, :runner_disconnected, :event,
       label: "Disconnected",
       meaning: "A runner closed its connection.",
       sentence: "{runner} disconnected from {namespace}"},
    [:flezha, :runner, :dropped] =>
      {:flezha_runner, :runner_dropped, :event,
       label: "Dropped by an operator",
       meaning: "An operator dropped a runner, so it cannot reconnect until it is let back in."},
    [:flezha, :runner, :readmitted] =>
      {:flezha_runner, :runner_readmitted, :event,
       label: "Let back in", meaning: "An operator let a dropped runner connect again."},
    [:flezha, :runner, :drop_refused] =>
      {:flezha_runner, :runner_drop_refused, :event,
       label: "Refused after drop",
       outcome: :failure,
       meaning: "A runner an operator dropped tried to come back and was turned away."},
    [:flezha, :runner, :factory_language_conflict] =>
      {:flezha_runner, :factory_language_conflict, :event,
       label: "Language clash",
       outcome: :failure,
       meaning:
         "A runner offered a factory that another runner already serves in another language."},
    [:flezha, :runner, :factory_run_conflict] =>
      {:flezha_runner, :factory_run_conflict, :event,
       label: "Factory clash",
       outcome: :failure,
       meaning: "A runner offered a factory that conflicts with one already running."},
    [:flezha, :runner, :quota_refused] =>
      {:flezha_runner, :quota_refused, :event,
       label: "Runner quota full",
       outcome: :failure,
       meaning: "A runner was turned away because its namespace has as many runners as allowed."},
    [:flezha, :runner, :admin_request] =>
      {:flezha_runner, :admin_request, :event,
       label: "Admin request", meaning: "A runner sent an administration request."},
    [:flezha, :runner, :foreign_frame_refused] =>
      {:flezha_runner, :foreign_frame_refused, :event,
       label: "Not its relay",
       outcome: :failure,
       meaning: "A runner sent a message for a relay it does not own, and it was refused."},
    [:flezha, :schedule, :spawn_failed] =>
      {:flezha_schedule, :spawn_failed, :event,
       label: "Start failed",
       outcome: :failure,
       meaning: "A schedule came due, but its relay failed to start."},
    [:flezha, :stream, :fanout_warning] =>
      {:flezha_stream, :fanout_warning, :event,
       label: "Duplicate process",
       outcome: :failure,
       meaning: "A stream found more than one process serving one relay."},
    [:flezha, :state, :claim_superseded] =>
      {:flezha_durability, :claim_superseded, :event,
       label: "Taken over",
       meaning: "Another node took over sending a relay's state to remote storage."},
    [:flezha, :state, :compaction_failed] =>
      {:flezha_state, :compaction_failed, :event,
       label: "Clean-up failed",
       outcome: :failure,
       meaning: "Shrinking a relay's saved state failed; it will be tried again."},
    [:flezha, :state, :compaction_gave_up] =>
      {:flezha_state, :compaction_gave_up, :event,
       label: "Clean-up given up",
       outcome: :failure,
       meaning: "Shrinking a relay's saved state failed too often and was given up."},
    [:flezha, :state, :disk_quota_breach] =>
      {:flezha_state, :disk_quota_breach, :event,
       label: "Over disk quota",
       outcome: :failure,
       meaning: "A namespace's saved state grew past its disk quota."},
    [:flezha, :state, :inbox_duplicate] =>
      {:flezha_state, :inbox_duplicate, :event,
       label: "Duplicate ignored",
       meaning: "A relay received a message it already had and ignored the copy."},
    [:flezha, :state, :input_log_frozen_watermark] =>
      {:flezha_state, :input_log_frozen_watermark, :event,
       label: "Input stuck",
       outcome: :failure,
       meaning: "A relay's record of the input it received stopped moving forward."},
    [:flezha, :state, :input_log_syncer_died] =>
      {:flezha_state, :input_log_syncer_died, :event,
       label: "Input saver crashed",
       outcome: :failure,
       meaning: "The process that saves a relay's received input crashed."},
    [:flezha, :state, :record_frame, :refused] =>
      {:flezha_state, :record_frame_refused, :event,
       label: "Damaged record refused",
       outcome: :failure,
       meaning: "A damaged record at the end of a relay's saved log was refused."},
    [:flezha, :state, :restore_refused] =>
      {:flezha_state, :restore_refused, :event,
       label: "Restore refused",
       outcome: :failure,
       meaning: "A relay's saved state could not be restored."},
    [:flezha, :state, :wal_pull_failed] =>
      {:flezha_state, :wal_pull_failed, :event,
       label: "Copy from node failed",
       outcome: :failure,
       meaning: "Copying a relay's saved state from another node failed."},
    [:flezha, :state, :record_frame, :repaired] =>
      {:flezha_state, :record_frame_repaired, :event,
       label: "Torn record dropped",
       outcome: :failure,
       meaning: "A half-written record at the end of a relay's saved log was dropped."},
    [:flezha, :state, :wal_rewind] =>
      {:flezha_state, :wal_rewind, :event,
       label: "Rewound", meaning: "A relay's saved state was rewound to an earlier point."}
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
  @derived_lines Map.new(Table.event_descriptions(), fn {event, words} ->
                   {event, {:flezha_health, List.last(event), :event, words}}
                 end)

  @derived Map.new(@derived_lines, fn {event, {source, name, kind, _words}} ->
             {event, {source, name, kind}}
           end)

  @all_lines Map.merge(@derived_lines, @lines)
  @all_mapping Map.new(@all_lines, fn {event, {source, name, kind, _words}} ->
                 {event, {source, name, kind}}
               end)

  # Every source a line names: where it sits, its plain title and what it is.
  @sources %{
    flezha_boot:
      {:controller, "Controller start-up",
       "Bringing relays back and starting each namespace's hub when the controller starts."},
    flezha_shutdown:
      {:controller, "Controller shutdown",
       "A graceful stop of the controller, step by step, moving relays off first."},
    flezha_error:
      {:controller, "Undocumented errors",
       "Errors a caller got without a documented code; each one is a defect to report."},
    flezha_state:
      {:storage, "Saved relay state",
       "Each relay's state saved on this node: writes, clean-ups and repairs."},
    flezha_durability:
      {:storage, "Remote backups",
       "Copies of relay state sent to remote storage and restored from it."},
    flezha_relay:
      {:relays, "Relay lifecycle", "Relays starting, stopping, and halting after a failure."},
    flezha_runner:
      {:relays, "Runner connections",
       "Runners opening and closing their connection to the controller, and the ones it turned away."},
    flezha_call:
      {:relays, "Relay calls", "Calls made to relays: answers, timeouts and refusals."},
    flezha_steps:
      {:relays, "Steps sessions", "Turns of steps relays that failed and were set aside."},
    flezha_dlq:
      {:relays, "Dead letters",
       "Messages that failed and were set aside in dead-letter queues, and those that expired."},
    flezha_stream:
      {:relays, "Runner streams",
       "The streams that carry messages between runners and their relays."},
    flezha_schedule: {:relays, "Schedules", "Schedules that start relays on a timetable."},
    flezha_health:
      {:relays, "Health findings",
       "What the health checks found about relays, runners and plugins."},
    flezha_convergence:
      {:relays, "Self-repair",
       "The loop that keeps running relays matching what was asked, and its retries."},
    flezha_correction:
      {:relays, "Refused corrections",
       "Corrections to a relay that the controller would not apply."},
    flezha_plugin:
      {:plugins, "Plugin lifecycle", "Plugins moving between the phases of their life."},
    flezha_plugin_packages:
      {:plugins, "Plugin packages",
       "Plugin packages the controller loaded or refused when it started."},
    flezha_hooks:
      {:plugins, "Notices to controller plugins",
       "Notices handed to controller plugins, such as a message becoming a dead letter."},
    flezha_ws:
      {:plugins, "Websocket plugin",
       "The websocket plugin: its connections, subscriptions and sends."},
    flezha_relay_forward:
      {:plugins, "Relay forward plugin",
       "The plugin that hands a relay's messages to the next relay."},
    flezha_http_poll:
      {:plugins, "HTTP poll plugin",
       "The plugin that reads a URL on a timer and hands what changed to a relay."},
    flezha_http_push:
      {:plugins, "HTTP push plugin", "The plugin that sends a relay's output to a URL."},
    flezha_hook_webhook:
      {:plugins, "Webhook plugin", "The plugin that sends controller notices to a URL."},
    flezha_file_source:
      {:plugins, "File source plugin", "The plugin that reads lines from a file into a relay."},
    flezha_file_sink:
      {:plugins, "File sink plugin", "The plugin that writes a relay's output to a file."},
    flezha_postgres_poll:
      {:plugins, "Postgres poll plugin",
       "The plugin that reads new rows from a table into a relay."}
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
  @shared_verbs for {verb, words} <- ProviderBehaviour.verb_descriptions(),
                    do: {verb, if(verb == :failed, do: :event, else: :metric), words}

  # ---------- Public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  What the bridge registers for each source its lines name: kinds, area,
  title, description and every signal with its words. `providers` are the
  provider-derived lines, as `provider_lines/0` answers them. Registered on
  bridge start so `Bus` enforces cardinality budgets against them and a
  reader can tell what each one says.
  """
  @spec registrations(map()) :: [map()]
  def registrations(providers \\ provider_lines()) do
    (Map.values(@all_lines) ++ Map.values(providers))
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.map(fn {source, lines} ->
      {area, title, description} =
        Map.get_lazy(@sources, source, fn -> package_source(source) end)

      %{
        name: source,
        kinds: lines |> Enum.map(&elem(&1, 2)) |> Enum.uniq(),
        flavour: :declarative,
        area: area,
        title: title,
        description: description,
        signals:
          Source.signals(
            for {_source, name, kind, words} <- Enum.uniq_by(lines, &elem(&1, 1)),
                do: {name, {kind, words}}
          )
      }
    end)
  end

  defp package_source(source) do
    name = source |> Atom.to_string() |> String.replace_prefix("flezha_", "")
    {:plugins, "#{Source.humanize(name)} plugin", "The #{name} plugin, installed from a package."}
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

  @doc "`provider_mapping/0` with each line's words: a meaning, or the long form."
  @spec provider_lines() ::
          %{[atom()] => {atom(), atom(), Signal.kind(), String.t() | keyword()}}
  def provider_lines() do
    for {_id, module} <- Providers.installed(),
        Code.ensure_loaded?(module),
        decl = module.__provider__(),
        {verb, kind, words} <- @shared_verbs ++ own_verbs(decl),
        # A name from code a verified package ships, never input.
        event = ProviderBehaviour.event(String.to_atom(decl.name), verb),
        not Map.has_key?(@all_mapping, event),
        into: %{},
        do: {event, {:"flezha_#{decl.name}", verb, kind, words}}
  end

  # A package built before `telemetry:` existed declares no own verbs, and one
  # built before a verb could carry a meaning declares a bare kind.
  defp own_verbs(decl) do
    for {verb, declared} <- Map.get(decl, :telemetry, []) do
      case declared do
        {kind, words} -> {verb, kind, words}
        kind -> {verb, kind, "No description from the plugin."}
      end
    end
  end

  defp strip(lines),
    do:
      Map.new(lines, fn {event, {source, name, kind, _words}} ->
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

  @doc "`mapping/0` with each line's words, as `{source, name, kind, words}`."
  @spec lines() :: %{[atom()] => {atom(), atom(), Signal.kind(), String.t() | keyword()}}
  def lines(), do: @all_lines

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

defmodule Flezha.Observability.TelemetryBridge.Lines do
  @moduledoc """
  The words of the bridge's hand-kept half, and of the provider verbs every
  plugin shares: what a reader is told each bridged signal says.

  `hand_kept/0` maps an event to `{source, name, kind, words}`, the words being
  a meaning or the words `Flezha.Observability.Source.signals/1` takes beside a
  signal's kind. The bridge reads both at compile time.
  """

  @hand_kept %{
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

  @shared_verbs [
    shipped: [
      label: "Delivered",
      outcome: :normal,
      meaning: "The plugin passed items on, into its relay or out to where it sends them."
    ],
    retried: [
      label: "Retrying",
      outcome: :normal,
      meaning: "A delivery failed and will be tried again; nothing is lost yet."
    ],
    failed: [
      label: "Failed",
      outcome: :failure,
      meaning: "A delivery or a read failed."
    ],
    overflowed: [
      label: "Queue full",
      outcome: :failure,
      meaning: "The plugin's queue was full, so items were dropped."
    ]
  ]

  @doc "The hand-kept lines, event => `{source, name, kind, words}`."
  @spec hand_kept() :: %{[atom()] => {atom(), atom(), atom(), String.t() | keyword()}}
  def hand_kept(), do: @hand_kept

  @doc """
  What each verb `Flezha.Plugin.ProviderBehaviour.verbs/0` names says, the same
  for every provider.
  """
  @spec shared_verbs() :: keyword(keyword())
  def shared_verbs(), do: @shared_verbs
end

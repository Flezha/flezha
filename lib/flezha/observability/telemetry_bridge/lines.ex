defmodule Flezha.Observability.TelemetryBridge.Lines do
  @moduledoc """
  The bridge's hand-kept half, and the words of the provider verbs every plugin
  shares: what a reader is told each bridged signal says.

  `sources/0` maps each source to `{area, title, description, lines}`, a line
  `{event, name, kind, words}`, the words being a meaning or the words
  `Flezha.Observability.Source.signals/1` takes beside a signal's kind (with
  an optional `level:`). The bridge reads both at compile time.
  """

  @sources %{
    flezha_boot:
      {:controller, "Controller start-up",
       "Bringing relays back and starting each namespace's hub when the controller starts.",
       [
         {[:flezha, :boot, :recovery_complete], :recovery_complete, :event,
          label: "Relays restored",
          meaning: "A namespace finished bringing its relays back after the controller started."},
         {[:flezha, :boot, :reconcile_duplicate], :reconcile_duplicate, :event,
          label: "Duplicate stopped",
          outcome: :failure,
          meaning:
            "A relay was found running on more than one node, and the extra copies were stopped."},
         {[:flezha, :hub, :start_failed], :hub_start_failed, :event,
          label: "Namespace failed to start",
          outcome: :failure,
          level: :error,
          meaning: "A namespace's hub failed to start, so its relays are not running."},
         {[:flezha, :boot, :pre_recovery_replay], :pre_recovery_replay, :event,
          label: "Restoring relays",
          meaning: "A namespace began bringing its relays back after the controller started."}
       ]},
    flezha_shutdown:
      {:controller, "Controller shutdown",
       "A graceful stop of the controller, step by step, moving relays off first.",
       [
         {[:flezha, :shutdown, :requested], :requested, :event,
          label: "Shutdown started", meaning: "Someone asked the controller to stop gracefully."},
         {[:flezha, :shutdown, :phase_started], :phase_started, :event,
          label: "Step started", meaning: "A step of the shutdown began."},
         {[:flezha, :shutdown, :phase_completed], :phase_completed, :event,
          label: "Step finished", meaning: "A step of the shutdown finished."},
         {[:flezha, :shutdown, :drain_progress], :drain_progress, :event,
          label: "Relays moved",
          meaning: "A namespace finished moving its relays to other nodes before the stop."},
         {[:flezha, :shutdown, :timeout], :timeout, :event,
          label: "Step timed out",
          outcome: :failure,
          meaning: "A step of the shutdown ran out of time."},
         {[:flezha, :shutdown, :complete], :complete, :event,
          label: "Shutdown finished", meaning: "The controller finished stopping gracefully."}
       ]},
    flezha_error:
      {:controller, "Undocumented errors",
       "Errors a caller got without a documented code; each one is a defect to report.",
       [
         {[:flezha, :error, :uncatalogued], :uncatalogued, :event,
          label: "Undocumented error",
          outcome: :failure,
          meaning: "A caller got an error with no documented code: a defect to report."}
       ]},
    flezha_state:
      {:storage, "Saved relay state",
       "Each relay's state saved on this node: writes, clean-ups and repairs.",
       [
         {[:flezha, :state, :wal, :backpressure], :wal_backpressure, :metric,
          label: "Writes held back",
          outcome: :failure,
          meaning:
            "A relay wrote faster than its saved state could keep up, so its writes waited."},
         {[:flezha, :state, :fsync, :tick], :fsync_tick, :event,
          label: "Flushed to disk",
          meaning: "Pending writes were flushed to disk together.",
          sentence: "Flushed {count:pending write} to disk"},
         {[:flezha, :state, :seq_gap], :seq_gap, :event,
          label: "Gap in sequence",
          outcome: :failure,
          meaning: "A relay's saved state arrived with a gap in its sequence numbers."},
         {[:flezha, :state, :credit_anomaly], :credit_anomaly, :event,
          label: "Wrote past its allowance",
          outcome: :failure,
          meaning:
            "A runner wrote more than the controller allowed it to: a fault in the runner, not load."},
         {[:flezha, :state, :output_stall], :output_stall, :event,
          label: "No output",
          outcome: :failure,
          meaning: "A relay has produced no output for longer than allowed."},
         {[:flezha, :state, :compaction_timeout], :compaction_timeout, :event,
          label: "Clean-up timed out",
          outcome: :failure,
          meaning: "Shrinking a relay's saved state took too long and was stopped."},
         {[:flezha, :state, :compaction_trigger], :compaction_trigger, :event,
          label: "Clean-up started", meaning: "A relay's saved state grew enough to be shrunk."},
         {[:flezha, :state, :baseline_seed], :baseline_seed, :event,
          label: "First write", meaning: "A relay's saved state took its first sequence number."},
         {[:flezha, :state, :wal_realign], :wal_realign, :event,
          label: "Sequence realigned",
          meaning: "A relay's saved state moved its sequence number to match its runner's."},
         {[:flezha, :state, :stale_writer_refused], :stale_writer_refused, :event,
          label: "Old writer refused",
          outcome: :failure,
          meaning:
            "A runner that no longer owns a relay tried to write its state and was refused."},
         {[:flezha, :state, :compaction_failed], :compaction_failed, :event,
          label: "Clean-up failed",
          outcome: :failure,
          meaning: "Shrinking a relay's saved state failed; it will be tried again."},
         {[:flezha, :state, :compaction_gave_up], :compaction_gave_up, :event,
          label: "Clean-up given up",
          outcome: :failure,
          level: :error,
          meaning: "Shrinking a relay's saved state failed too often and was given up."},
         {[:flezha, :state, :disk_quota_breach], :disk_quota_breach, :event,
          label: "Over disk quota",
          outcome: :failure,
          level: :error,
          meaning: "A namespace's saved state grew past its disk quota."},
         {[:flezha, :state, :inbox_duplicate], :inbox_duplicate, :event,
          label: "Duplicate ignored",
          meaning: "A relay received a message it already had and ignored the copy."},
         {[:flezha, :state, :input_log_frozen_watermark], :input_log_frozen_watermark, :event,
          label: "Input stuck",
          outcome: :failure,
          meaning: "A relay's record of the input it received stopped moving forward."},
         {[:flezha, :state, :input_log_syncer_died], :input_log_syncer_died, :event,
          label: "Input saver crashed",
          outcome: :failure,
          meaning: "The process that saves a relay's received input crashed."},
         {[:flezha, :state, :record_frame, :refused], :record_frame_refused, :event,
          label: "Damaged record refused",
          outcome: :failure,
          level: :error,
          meaning: "A damaged record at the end of a relay's saved log was refused."},
         {[:flezha, :state, :restore_refused], :restore_refused, :event,
          label: "Restore refused",
          outcome: :failure,
          level: :error,
          meaning: "A relay's saved state could not be restored."},
         {[:flezha, :state, :wal_pull_failed], :wal_pull_failed, :event,
          label: "Copy from node failed",
          outcome: :failure,
          meaning: "Copying a relay's saved state from another node failed."},
         {[:flezha, :state, :record_frame, :repaired], :record_frame_repaired, :event,
          label: "Torn record dropped",
          outcome: :failure,
          level: :warning,
          meaning: "A half-written record at the end of a relay's saved log was dropped."},
         {[:flezha, :state, :wal_rewind], :wal_rewind, :event,
          label: "Rewound", meaning: "A relay's saved state was rewound to an earlier point."}
       ]},
    flezha_durability:
      {:storage, "Remote backups",
       "Copies of relay state sent to remote storage and restored from it.",
       [
         {[:flezha, :state, :rpo_exceeded], :rpo_exceeded, :event,
          label: "Backups too far behind",
          outcome: :failure,
          level: :error,
          meaning: "Remote copies fell further behind than the recovery point allows."},
         {[:flezha, :state, :ship_failed], :ship_failed, :event,
          label: "Upload failed",
          outcome: :failure,
          meaning: "Sending a copy of a relay's state to remote storage failed."},
         {[:flezha, :state, :artifact_shipped], :artifact_shipped, :metric,
          label: "Uploaded", meaning: "A copy of a relay's state was sent to remote storage."},
         {[:flezha, :state, :remote_commit], :remote_commit, :metric,
          label: "Upload confirmed", meaning: "Remote storage confirmed it holds a copy."},
         {[:flezha, :state, :hydrated], :hydrated, :event,
          label: "Restored", meaning: "A relay's state was restored from remote storage."},
         {[:flezha, :state, :hydration_failed], :hydration_failed, :event,
          label: "Restore failed",
          outcome: :failure,
          level: :error,
          meaning: "Restoring a relay's state from remote storage failed."},
         {[:flezha, :state, :artifact_missing], :artifact_missing, :event,
          label: "Copy missing",
          outcome: :failure,
          level: :error,
          meaning: "Remote storage lacks a copy that a relay's saved state refers to."},
         {[:flezha, :state, :durability_gc], :gc, :metric,
          label: "Old copies removed",
          meaning: "Copies no longer needed were removed from remote storage.",
          sentence: "Removed {collected:old copy} from remote storage"},
         {[:flezha, :state, :claim_superseded], :claim_superseded, :event,
          label: "Taken over",
          meaning: "Another node took over sending a relay's state to remote storage."}
       ]},
    flezha_relay:
      {:relays, "Relay lifecycle", "Relays starting, stopping, and halting after a failure.",
       [
         {[:flezha, :relay, :started], :relay_started, :event,
          label: "Started",
          meaning: "A relay was started.",
          sentence: "{relay} started in {namespace}"},
         {[:flezha, :relay, :stopped], :relay_stopped, :event,
          label: "Stopped",
          meaning: "A relay was stopped or finished its work.",
          sentence: "{relay} stopped in {namespace}"},
         {[:flezha, :relay, :halted], :halted_total, :metric,
          label: "Halted",
          outcome: :failure,
          meaning: "A relay was halted after a failure and stays stopped until someone acts.",
          sentence: "{relay_id} halted in {namespace}"}
       ]},
    flezha_runner:
      {:relays, "Runner connections",
       "Runners opening and closing their connection to the controller, and the ones it turned away.",
       [
         {[:flezha, :runner, :connected], :runner_connected, :event,
          label: "Connected",
          meaning: "A runner opened its connection to the controller.",
          sentence: "{runner} connected to {namespace}"},
         {[:flezha, :runner, :disconnected], :runner_disconnected, :event,
          label: "Disconnected",
          meaning: "A runner closed its connection.",
          sentence: "{runner} disconnected from {namespace}"},
         {[:flezha, :runner, :dropped], :runner_dropped, :event,
          label: "Dropped by an operator",
          meaning: "An operator dropped a runner, so it cannot reconnect until it is let back in."},
         {[:flezha, :runner, :readmitted], :runner_readmitted, :event,
          label: "Let back in", meaning: "An operator let a dropped runner connect again."},
         {[:flezha, :runner, :drop_refused], :runner_drop_refused, :event,
          label: "Refused after drop",
          outcome: :failure,
          meaning: "A runner an operator dropped tried to come back and was turned away."},
         {[:flezha, :runner, :factory_language_conflict], :factory_language_conflict, :event,
          label: "Language clash",
          outcome: :failure,
          level: :warning,
          meaning:
            "A runner offered a factory that another runner already serves in another language."},
         {[:flezha, :runner, :factory_run_conflict], :factory_run_conflict, :event,
          label: "Factory clash",
          outcome: :failure,
          level: :warning,
          meaning: "A runner offered a factory that conflicts with one already running."},
         {[:flezha, :runner, :quota_refused], :quota_refused, :event,
          label: "Runner quota full",
          outcome: :failure,
          level: :warning,
          meaning:
            "A runner was turned away because its namespace has as many runners as allowed."},
         {[:flezha, :runner, :admin_request], :admin_request, :event,
          label: "Admin request", meaning: "A runner sent an administration request."},
         {[:flezha, :runner, :foreign_frame_refused], :foreign_frame_refused, :event,
          label: "Not its relay",
          outcome: :failure,
          meaning: "A runner sent a message for a relay it does not own, and it was refused."}
       ]},
    flezha_call:
      {:relays, "Relay calls", "Calls made to relays: answers, timeouts and refusals.",
       [
         {[:flezha, :call, :answered], :answer_duration, :metric,
          label: "Answered",
          meaning: "A call to a relay was answered; the details say how long it took.",
          sentence: "{relay_id} answered a call"},
         {[:flezha, :call, :timeout], :timeouts_total, :metric,
          label: "Timed out",
          outcome: :failure,
          meaning: "A call to a relay got no answer in time.",
          sentence: "A call to {relay_id} timed out"},
         {[:flezha, :call, :answer_dropped], :answers_dropped_total, :metric,
          label: "Answer unclaimed",
          outcome: :failure,
          meaning: "A relay answered a call that nobody was waiting for any more."},
         {[:flezha, :call, :refused], :refused_total, :metric,
          label: "Refused",
          outcome: :failure,
          meaning: "A call to a relay was refused.",
          sentence: "A call to {relay_id} was refused"},
         {[:flezha, :call, :recovered_unanswered], :recovered_unanswered_total, :metric,
          label: "Closed unanswered",
          outcome: :failure,
          meaning: "A call picked up again after a restart was closed without an answer."}
       ]},
    flezha_steps:
      {:relays, "Steps sessions", "Turns of steps relays that failed and were set aside.",
       [
         {[:flezha, :steps, :turn_dead_lettered], :turns_dead_lettered_total, :metric,
          label: "Turn set aside",
          outcome: :failure,
          meaning:
            "A turn of a steps relay failed and became a dead letter; the relay carries on."}
       ]},
    flezha_dlq:
      {:relays, "Dead letters",
       "Messages that failed and were set aside in dead-letter queues, and those that expired.",
       [
         {[:flezha, :dlq, :parked], :parked_total, :metric,
          label: "Set aside",
          outcome: :failure,
          meaning: "A message failed and was set aside in a dead-letter queue.",
          sentence: "{tasklet_id} set aside {count:message}"},
         {[:flezha, :dlq, :retention_dropped], :dropped_by_retention_total, :metric,
          label: "Expired", meaning: "Dead letters older than the retention limit were deleted."},
         {[:flezha, :dlq, :usage], :usage, :metric,
          label: "Queue size",
          meaning: "How many dead letters a queue holds, and how many bytes.",
          sentence: "{tasklet_id} holds {entries:dead letter}"}
       ]},
    flezha_stream:
      {:relays, "Runner streams",
       "The streams that carry messages between runners and their relays.",
       [
         {[:flezha, :stream, :drop], :drop, :metric,
          label: "Dropped",
          outcome: :failure,
          meaning: "A message for a relay was dropped because the relay had no open stream.",
          sentence: "A message for {relay_id} was dropped"},
         {[:flezha, :stream, :fanout_warning], :fanout_warning, :event,
          label: "Duplicate process",
          outcome: :failure,
          meaning: "A stream found more than one process serving one relay."}
       ]},
    flezha_schedule:
      {:relays, "Schedules", "Schedules that start relays on a timetable.",
       [
         {[:flezha, :schedule, :spawn_failed], :spawn_failed, :event,
          label: "Start failed",
          outcome: :failure,
          level: :error,
          meaning: "A schedule came due, but its relay failed to start."}
       ]},
    flezha_health:
      {:relays, "Health findings",
       "What the health checks found about relays, runners and plugins.", []},
    flezha_convergence:
      {:relays, "Self-repair",
       "The loop that keeps running relays matching what was asked, and its retries.",
       [
         {[:flezha, :convergence, :intent_mismatch], :intent_mismatch, :event,
          label: "Leftover state",
          outcome: :failure,
          level: :warning,
          meaning: "Saved state exists for relays nobody asked to run."},
         {[:flezha, :backoff, :wait], :backoff_wait, :metric,
          label: "Waiting to retry", meaning: "A retry waited before its next attempt."},
         {[:flezha, :backoff, :reset], :backoff_reset, :metric,
          label: "Retry wait reset", meaning: "A success reset a retry's waiting time."}
       ]},
    flezha_correction:
      {:relays, "Refused corrections",
       "Corrections to a relay that the controller would not apply.",
       [
         {[:flezha, :correction, :refused], :refused, :event,
          label: "Correction refused",
          outcome: :failure,
          level: :warning,
          meaning: "The controller refused to apply a correction to a relay."}
       ]},
    flezha_plugin:
      {:plugins, "Plugin lifecycle", "Plugins moving between the phases of their life.",
       [
         {[:flezha, :plugin, :phase], :phase, :event,
          label: "Phase changed",
          meaning:
            "A plugin moved from one phase of its life to another, such as from prepared to running.",
          sentence: "{plugin_id} went from {from} to {to}"}
       ]},
    flezha_plugin_packages:
      {:plugins, "Plugin packages",
       "Plugin packages the controller loaded or refused when it started.",
       [
         {[:flezha, :plugin_package, :loaded], :loaded, :event,
          label: "Loaded", meaning: "A plugin package was loaded when the controller started."},
         {[:flezha, :plugin_package, :refused], :refused, :event,
          label: "Refused",
          outcome: :failure,
          meaning:
            "A plugin package was refused when the controller started, so its plugins are missing."}
       ]},
    flezha_hooks:
      {:plugins, "Notices to controller plugins",
       "Notices handed to controller plugins, such as a message becoming a dead letter.",
       [
         # Only the sinks naming this source hear it (`Flezha.Observability.HookSink`).
         {[:flezha, :dlq, :dead_lettered], :on_dead_letter, :event,
          label: "Dead letter",
          outcome: :failure,
          meaning: "A message became a dead letter, and controller plugins were told."}
       ]},
    flezha_ws:
      {:plugins, "Websocket plugin",
       "The websocket plugin: its connections, subscriptions and sends.",
       [
         {[:flezha, :plugin, :websocket, :idle_reconnect], :idle_reconnect, :event,
          label: "Reconnected after silence",
          meaning: "A websocket was silent for too long, so it was reconnected.",
          sentence: "{plugin_id} reconnected after {silence_ms} ms of silence"},
         {[:flezha, :plugin, :websocket, :resubscribe], :resubscribe, :event,
          label: "Subscribed again",
          meaning: "A websocket sent its subscriptions again after its feed went quiet.",
          sentence: "{plugin_id} subscribed again"},
         {[:flezha, :plugin, :websocket, :reconcile], :reconcile, :event,
          label: "Subscriptions checked",
          meaning: "A websocket compared its subscriptions with what its feed reports.",
          sentence: "{plugin_id} checked its subscriptions: {added} added, {removed} removed"},
         {[:flezha, :plugin, :websocket, :shipped], :shipped, :metric,
          label: "Sent",
          meaning: "Messages were sent over a websocket.",
          sentence: "{plugin_id} sent {count:message}"},
         {[:flezha, :plugin, :websocket, :retried], :retried, :event,
          label: "Retrying", meaning: "A websocket send failed and will be tried again."},
         {[:flezha, :plugin, :websocket, :failed], :failed, :event,
          label: "Send failed",
          outcome: :failure,
          meaning: "A websocket send failed and was given up."},
         {[:flezha, :plugin, :websocket, :overflowed], :overflowed, :event,
          label: "Queue full",
          outcome: :failure,
          meaning: "A websocket's queue was full, so messages were dropped."}
       ]},
    flezha_relay_forward:
      {:plugins, "Relay forward plugin",
       "The plugin that hands a relay's messages to the next relay.",
       [
         {[:flezha, :plugin, :relay_forward, :shipped], :shipped, :metric,
          label: "Forwarded", meaning: "Messages were handed to the next relay."},
         {[:flezha, :plugin, :relay_forward, :retried], :retried, :metric,
          label: "Retrying", meaning: "A forward failed and will be tried again."},
         {[:flezha, :plugin, :relay_forward, :queue_depth], :queue_depth, :metric,
          label: "Waiting to forward",
          meaning: "How many messages are waiting to be forwarded.",
          sentence: "{depth:message} waiting to be forwarded"},
         {[:flezha, :plugin, :relay_forward, :overflowed], :saturated, :event,
          label: "Queue full",
          outcome: :failure,
          meaning: "The forward queue was full and turned messages away."}
       ]},
    flezha_http_poll:
      {:plugins, "HTTP poll plugin",
       "The plugin that reads a URL on a timer and hands what changed to a relay.",
       [
         {[:flezha, :plugin, :http_poll, :shipped], :shipped, :metric,
          label: "Fetched",
          meaning: "A poll got a new response and handed it to the relay.",
          sentence: "{plugin_id} fetched {bytes:byte} in {latency_ms} ms"},
         {[:flezha, :plugin, :http_poll, :unchanged], :unchanged, :metric,
          label: "Nothing new",
          meaning: "A poll's response matched the last one, so it was skipped.",
          sentence: "{plugin_id} found nothing new"},
         {[:flezha, :plugin, :http_poll, :failed], :failed, :event,
          label: "Poll failed", outcome: :failure, meaning: "A poll request failed."}
       ]},
    flezha_http_push:
      {:plugins, "HTTP push plugin", "The plugin that sends a relay's output to a URL.",
       [
         {[:flezha, :plugin, :http_push, :shipped], :shipped, :metric,
          label: "Sent", meaning: "An HTTP request was sent.", sentence: "Sent {count:request}"},
         {[:flezha, :plugin, :http_push, :retried], :retried, :metric,
          label: "Retrying", meaning: "An HTTP request failed and will be tried again."},
         {[:flezha, :plugin, :http_push, :failed], :failed, :event,
          label: "Given up",
          outcome: :failure,
          meaning: "An HTTP request failed too often and was given up."},
         {[:flezha, :plugin, :http_push, :overflowed], :overflowed, :metric,
          label: "Queue full",
          outcome: :failure,
          meaning: "The HTTP queue was full, so requests were dropped."}
       ]},
    flezha_hook_webhook:
      {:plugins, "Webhook plugin", "The plugin that sends controller notices to a URL.",
       [
         {[:flezha, :plugin, :hook_webhook, :shipped], :shipped, :metric,
          label: "Sent", meaning: "A webhook request was sent.", sentence: "Sent {count:request}"},
         {[:flezha, :plugin, :hook_webhook, :retried], :retried, :metric,
          label: "Retrying", meaning: "A webhook request failed and will be tried again."},
         {[:flezha, :plugin, :hook_webhook, :failed], :failed, :event,
          label: "Given up",
          outcome: :failure,
          meaning: "A webhook request failed too often and was given up."},
         {[:flezha, :plugin, :hook_webhook, :overflowed], :overflowed, :metric,
          label: "Queue full",
          outcome: :failure,
          meaning: "The webhook queue was full, so requests were dropped."}
       ]},
    flezha_file_source:
      {:plugins, "File source plugin", "The plugin that reads lines from a file into a relay.",
       [
         {[:flezha, :plugin, :file_source, :shipped], :shipped, :metric,
          label: "Read",
          meaning: "A line read from the file was handed to the relay.",
          sentence: "{plugin_id} read a line from {path}"},
         {[:flezha, :plugin, :file_source, :truncated], :truncated, :event,
          label: "File restarted",
          meaning: "The file was cut short or replaced, so it is read again from the start.",
          sentence: "{plugin_id} reads {path} again from the start"},
         {[:flezha, :plugin, :file_source, :failed], :failed, :event,
          label: "Read failed", outcome: :failure, meaning: "Reading the file failed."}
       ]},
    flezha_file_sink:
      {:plugins, "File sink plugin", "The plugin that writes a relay's output to a file.",
       [
         {[:flezha, :plugin, :file_sink, :shipped], :shipped, :metric,
          label: "Saved",
          meaning: "Records were written to the file.",
          sentence: "{plugin_id} saved {count:record} to {path}"},
         {[:flezha, :plugin, :file_sink, :refused], :refused, :event,
          label: "Record refused",
          outcome: :failure,
          meaning: "A record was refused because it cannot be written as one line.",
          sentence: "{plugin_id} refused a record for {path}"},
         {[:flezha, :plugin, :file_sink, :failed], :failed, :event,
          label: "Write failed",
          outcome: :failure,
          meaning: "Writing to the file failed; it will be tried again.",
          sentence: "{plugin_id} could not write to {path}"}
       ]},
    flezha_postgres_poll:
      {:plugins, "Postgres poll plugin",
       "The plugin that reads new rows from a table into a relay.",
       [
         {[:flezha, :plugin, :postgres_poll, :shipped], :shipped, :metric,
          label: "Read rows",
          meaning: "New rows read from the table were handed to the relay.",
          sentence: "{plugin_id} read {count:row} from {table}"},
         {[:flezha, :plugin, :postgres_poll, :failed], :failed, :event,
          label: "Poll failed", outcome: :failure, meaning: "Reading the table failed."},
         {[:flezha, :plugin, :postgres_poll, :caught_up], :caught_up, :event,
          label: "Caught up",
          meaning: "The poll reached the newest row in the table.",
          sentence: "{plugin_id} caught up with {table}"}
       ]}
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

  @doc "Every bridged source, `source => {area, title, description, [{event, name, kind, words}]}`."
  @spec sources() :: %{atom() => {atom(), String.t(), String.t(), [tuple()]}}
  def sources(), do: @sources

  @doc """
  What each verb `Flezha.Plugin.ProviderBehaviour.verbs/0` names says, the same
  for every provider.
  """
  @spec shared_verbs() :: keyword(keyword())
  def shared_verbs(), do: @shared_verbs
end

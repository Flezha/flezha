%{
  derived: %{
    [:flezha, :convergence, :heal] => {:flezha_health, :heal, :event},
    [:flezha, :convergence, :relay_stuck] => {:flezha_health, :relay_stuck, :event},
    [:flezha, :convergence, :safe_call] => {:flezha_health, :safe_call, :event},
    [:flezha, :data_plane, :stall_localized] => {:flezha_health, :stall_localized, :event},
    [:flezha, :health, :verdict] => {:flezha_health, :verdict, :event},
    [:flezha, :hub, :unavailable] => {:flezha_health, :unavailable, :event},
    [:flezha, :plugin, :health, :fail_open] => {:flezha_health, :fail_open, :event},
    [:flezha, :relay, :halted] => {:flezha_health, :halted, :event},
    [:flezha, :runner, :evicted] => {:flezha_health, :evicted, :event},
    [:flezha, :runner, :liveness_timeout] => {:flezha_health, :liveness_timeout, :event},
    [:flezha, :runner, :reconnected] => {:flezha_health, :reconnected, :event},
    [:flezha, :runner, :reconnected_reassigned] =>
      {:flezha_health, :reconnected_reassigned, :event},
    [:flezha, :runner, :restarted] => {:flezha_health, :restarted, :event},
    [:flezha, :state, :input_silence] => {:flezha_health, :input_silence, :event},
    [:flezha, :state, :output_stall] => {:flezha_health, :output_stall, :event},
    [:flezha, :steps, :failed] => {:flezha_health, :failed, :event},
    [:flezha, :steps, :suspend_requested] => {:flezha_health, :suspend_requested, :event},
    [:flezha, :steps, :suspended] => {:flezha_health, :suspended, :event},
    [:flezha, :transport, :connect_rejected] => {:flezha_health, :connect_rejected, :event}
  },
  lines: %{
    [:flezha, :backoff, :reset] =>
      {:flezha_convergence, :backoff_reset, :metric,
       [
         label: "Retry wait reset",
         meaning: "A success reset a retry's waiting time."
       ]},
    [:flezha, :backoff, :wait] =>
      {:flezha_convergence, :backoff_wait, :metric,
       [
         label: "Waiting to retry",
         meaning: "A retry waited before its next attempt."
       ]},
    [:flezha, :boot, :pre_recovery_replay] =>
      {:flezha_boot, :pre_recovery_replay, :event,
       [
         label: "Restoring relays",
         meaning: "A namespace began bringing its relays back after the controller started."
       ]},
    [:flezha, :boot, :reconcile_duplicate] =>
      {:flezha_boot, :reconcile_duplicate, :event,
       [
         label: "Duplicate stopped",
         outcome: :failure,
         meaning:
           "A relay was found running on more than one node, and the extra copies were stopped."
       ]},
    [:flezha, :boot, :recovery_complete] =>
      {:flezha_boot, :recovery_complete, :event,
       [
         label: "Relays restored",
         meaning: "A namespace finished bringing its relays back after the controller started."
       ]},
    [:flezha, :call, :answer_dropped] =>
      {:flezha_call, :answers_dropped_total, :metric,
       [
         label: "Answer unclaimed",
         outcome: :failure,
         meaning: "A relay answered a call that nobody was waiting for any more."
       ]},
    [:flezha, :call, :answered] =>
      {:flezha_call, :answer_duration, :metric,
       [
         label: "Answered",
         meaning: "A call to a relay was answered; the details say how long it took.",
         sentence: "{relay_id} answered a call"
       ]},
    [:flezha, :call, :recovered_unanswered] =>
      {:flezha_call, :recovered_unanswered_total, :metric,
       [
         label: "Closed unanswered",
         outcome: :failure,
         meaning: "A call picked up again after a restart was closed without an answer."
       ]},
    [:flezha, :call, :refused] =>
      {:flezha_call, :refused_total, :metric,
       [
         label: "Refused",
         outcome: :failure,
         meaning: "A call to a relay was refused.",
         sentence: "A call to {relay_id} was refused"
       ]},
    [:flezha, :call, :timeout] =>
      {:flezha_call, :timeouts_total, :metric,
       [
         label: "Timed out",
         outcome: :failure,
         meaning: "A call to a relay got no answer in time.",
         sentence: "A call to {relay_id} timed out"
       ]},
    [:flezha, :convergence, :heal] =>
      {:flezha_health, :heal, :event,
       [label: "Repaired", meaning: "The controller ran a repair step."]},
    [:flezha, :convergence, :intent_mismatch] =>
      {:flezha_convergence, :intent_mismatch, :event,
       [
         label: "Leftover state",
         outcome: :failure,
         level: :warning,
         meaning: "Saved state exists for relays nobody asked to run."
       ]},
    [:flezha, :convergence, :relay_stuck] =>
      {:flezha_health, :relay_stuck, :event,
       [
         label: "Stuck",
         outcome: :failure,
         level: :warning,
         meaning: "A relay is making no progress."
       ]},
    [:flezha, :convergence, :safe_call] =>
      {:flezha_health, :safe_call, :event,
       [label: "Probed", meaning: "A short call checked whether a relay answers."]},
    [:flezha, :correction, :refused] =>
      {:flezha_correction, :refused, :event,
       [
         label: "Correction refused",
         outcome: :failure,
         level: :warning,
         meaning: "The controller refused to apply a correction to a relay."
       ]},
    [:flezha, :data_plane, :stall_localized] =>
      {:flezha_health, :stall_localized, :event,
       [
         label: "Stall traced",
         outcome: :failure,
         level: :warning,
         meaning: "A stalled message flow was traced to one connection, stream or relay."
       ]},
    [:flezha, :dlq, :dead_lettered] =>
      {:flezha_hooks, :on_dead_letter, :event,
       [
         label: "Dead letter",
         outcome: :failure,
         meaning: "A message became a dead letter, and controller plugins were told."
       ]},
    [:flezha, :dlq, :parked] =>
      {:flezha_dlq, :parked_total, :metric,
       [
         label: "Set aside",
         outcome: :failure,
         meaning: "A message failed and was set aside in a dead-letter queue.",
         sentence: "{tasklet_id} set aside {count:message}"
       ]},
    [:flezha, :dlq, :retention_dropped] =>
      {:flezha_dlq, :dropped_by_retention_total, :metric,
       [
         label: "Expired",
         meaning: "Dead letters older than the retention limit were deleted."
       ]},
    [:flezha, :dlq, :usage] =>
      {:flezha_dlq, :usage, :metric,
       [
         label: "Queue size",
         meaning: "How many dead letters a queue holds, and how many bytes.",
         sentence: "{tasklet_id} holds {entries:dead letter}"
       ]},
    [:flezha, :error, :uncatalogued] =>
      {:flezha_error, :uncatalogued, :event,
       [
         label: "Undocumented error",
         outcome: :failure,
         meaning: "A caller got an error with no documented code: a defect to report."
       ]},
    [:flezha, :health, :verdict] =>
      {:flezha_health, :verdict, :event,
       [
         label: "Health changed",
         meaning: "The health of a relay, runner or plugin changed.",
         sentence: "{id} is now {state}"
       ]},
    [:flezha, :hub, :start_failed] =>
      {:flezha_boot, :hub_start_failed, :event,
       [
         label: "Namespace failed to start",
         outcome: :failure,
         level: :error,
         meaning: "A namespace's hub failed to start, so its relays are not running."
       ]},
    [:flezha, :hub, :unavailable] =>
      {:flezha_health, :unavailable, :event,
       [
         label: "Namespace down",
         outcome: :failure,
         level: :warning,
         meaning: "A namespace's hub was down when something asked for it."
       ]},
    [:flezha, :plugin, :file_sink, :failed] =>
      {:flezha_file_sink, :failed, :event,
       [
         label: "Failed",
         outcome: :failure,
         meaning: "A delivery or a read failed."
       ]},
    [:flezha, :plugin, :file_sink, :refused] =>
      {:flezha_file_sink, :refused, :event,
       [
         label: "Record refused",
         outcome: :failure,
         meaning: "A record was refused because it cannot be written as one line.",
         sentence: "{plugin_id} refused a record for {path}"
       ]},
    [:flezha, :plugin, :file_sink, :shipped] =>
      {:flezha_file_sink, :shipped, :metric,
       [
         label: "Delivered",
         outcome: :normal,
         meaning: "The plugin passed items on, into its relay or out to where it sends them."
       ]},
    [:flezha, :plugin, :file_source, :failed] =>
      {:flezha_file_source, :failed, :event,
       [
         label: "Failed",
         outcome: :failure,
         meaning: "A delivery or a read failed."
       ]},
    [:flezha, :plugin, :file_source, :shipped] =>
      {:flezha_file_source, :shipped, :metric,
       [
         label: "Delivered",
         outcome: :normal,
         meaning: "The plugin passed items on, into its relay or out to where it sends them."
       ]},
    [:flezha, :plugin, :file_source, :truncated] =>
      {:flezha_file_source, :truncated, :event,
       [
         label: "File restarted",
         meaning: "The file was cut short or replaced, so it is read again from the start.",
         sentence: "{plugin_id} reads {path} again from the start"
       ]},
    [:flezha, :plugin, :health, :fail_open] =>
      {:flezha_health, :fail_open, :event,
       [
         label: "Health check failed",
         outcome: :failure,
         meaning:
           "A plugin's health check failed or timed out, so it is treated as healthy for now."
       ]},
    [:flezha, :plugin, :hook_webhook, :failed] =>
      {:flezha_hook_webhook, :failed, :event,
       [
         label: "Failed",
         outcome: :failure,
         meaning: "A delivery or a read failed."
       ]},
    [:flezha, :plugin, :hook_webhook, :overflowed] =>
      {:flezha_hook_webhook, :overflowed, :metric,
       [
         label: "Queue full",
         outcome: :failure,
         meaning: "The plugin's queue was full, so items were dropped."
       ]},
    [:flezha, :plugin, :hook_webhook, :retried] =>
      {:flezha_hook_webhook, :retried, :metric,
       [
         label: "Retrying",
         outcome: :normal,
         meaning: "A delivery failed and will be tried again; nothing is lost yet."
       ]},
    [:flezha, :plugin, :hook_webhook, :shipped] =>
      {:flezha_hook_webhook, :shipped, :metric,
       [
         label: "Delivered",
         outcome: :normal,
         meaning: "The plugin passed items on, into its relay or out to where it sends them."
       ]},
    [:flezha, :plugin, :http_poll, :failed] =>
      {:flezha_http_poll, :failed, :event,
       [
         label: "Failed",
         outcome: :failure,
         meaning: "A delivery or a read failed."
       ]},
    [:flezha, :plugin, :http_poll, :shipped] =>
      {:flezha_http_poll, :shipped, :metric,
       [
         label: "Delivered",
         outcome: :normal,
         meaning: "The plugin passed items on, into its relay or out to where it sends them."
       ]},
    [:flezha, :plugin, :http_poll, :unchanged] =>
      {:flezha_http_poll, :unchanged, :metric,
       [
         label: "Nothing new",
         meaning: "A poll's response matched the last one, so it was skipped.",
         sentence: "{plugin_id} found nothing new"
       ]},
    [:flezha, :plugin, :http_push, :failed] =>
      {:flezha_http_push, :failed, :event,
       [
         label: "Failed",
         outcome: :failure,
         meaning: "A delivery or a read failed."
       ]},
    [:flezha, :plugin, :http_push, :overflowed] =>
      {:flezha_http_push, :overflowed, :metric,
       [
         label: "Queue full",
         outcome: :failure,
         meaning: "The plugin's queue was full, so items were dropped."
       ]},
    [:flezha, :plugin, :http_push, :retried] =>
      {:flezha_http_push, :retried, :metric,
       [
         label: "Retrying",
         outcome: :normal,
         meaning: "A delivery failed and will be tried again; nothing is lost yet."
       ]},
    [:flezha, :plugin, :http_push, :shipped] =>
      {:flezha_http_push, :shipped, :metric,
       [
         label: "Delivered",
         outcome: :normal,
         meaning: "The plugin passed items on, into its relay or out to where it sends them."
       ]},
    [:flezha, :plugin, :phase] =>
      {:flezha_plugin, :phase, :event,
       [
         label: "Phase changed",
         meaning:
           "A plugin moved from one phase of its life to another, such as from prepared to running.",
         sentence: "{plugin_id} went from {from} to {to}"
       ]},
    [:flezha, :plugin, :postgres_poll, :caught_up] =>
      {:flezha_postgres_poll, :caught_up, :event,
       [
         label: "Caught up",
         meaning: "The poll reached the newest row in the table.",
         sentence: "{plugin_id} caught up with {table}"
       ]},
    [:flezha, :plugin, :postgres_poll, :failed] =>
      {:flezha_postgres_poll, :failed, :event,
       [
         label: "Failed",
         outcome: :failure,
         meaning: "A delivery or a read failed."
       ]},
    [:flezha, :plugin, :postgres_poll, :shipped] =>
      {:flezha_postgres_poll, :shipped, :metric,
       [
         label: "Delivered",
         outcome: :normal,
         meaning: "The plugin passed items on, into its relay or out to where it sends them."
       ]},
    [:flezha, :plugin, :relay_forward, :overflowed] =>
      {:flezha_relay_forward, :saturated, :event,
       [
         label: "Queue full",
         outcome: :failure,
         meaning: "The forward queue was full and turned messages away."
       ]},
    [:flezha, :plugin, :relay_forward, :queue_depth] =>
      {:flezha_relay_forward, :queue_depth, :metric,
       [
         label: "Waiting to forward",
         meaning: "How many messages are waiting to be forwarded.",
         sentence: "{depth:message} waiting to be forwarded"
       ]},
    [:flezha, :plugin, :relay_forward, :retried] =>
      {:flezha_relay_forward, :retried, :metric,
       [
         label: "Retrying",
         outcome: :normal,
         meaning: "A delivery failed and will be tried again; nothing is lost yet."
       ]},
    [:flezha, :plugin, :relay_forward, :shipped] =>
      {:flezha_relay_forward, :shipped, :metric,
       [
         label: "Delivered",
         outcome: :normal,
         meaning: "The plugin passed items on, into its relay or out to where it sends them."
       ]},
    [:flezha, :plugin, :websocket, :failed] =>
      {:flezha_ws, :failed, :event,
       [
         label: "Send failed",
         outcome: :failure,
         meaning: "A websocket send failed and was given up."
       ]},
    [:flezha, :plugin, :websocket, :idle_reconnect] =>
      {:flezha_ws, :idle_reconnect, :event,
       [
         label: "Reconnected after silence",
         meaning: "A websocket was silent for too long, so it was reconnected.",
         sentence: "{plugin_id} reconnected after {silence_ms} ms of silence"
       ]},
    [:flezha, :plugin, :websocket, :overflowed] =>
      {:flezha_ws, :overflowed, :event,
       [
         label: "Queue full",
         outcome: :failure,
         meaning: "A websocket's queue was full, so messages were dropped."
       ]},
    [:flezha, :plugin, :websocket, :reconcile] =>
      {:flezha_ws, :reconcile, :event,
       [
         label: "Subscriptions checked",
         meaning: "A websocket compared its subscriptions with what its feed reports.",
         sentence: "{plugin_id} checked its subscriptions: {added} added, {removed} removed"
       ]},
    [:flezha, :plugin, :websocket, :resubscribe] =>
      {:flezha_ws, :resubscribe, :event,
       [
         label: "Subscribed again",
         meaning: "A websocket sent its subscriptions again after its feed went quiet.",
         sentence: "{plugin_id} subscribed again"
       ]},
    [:flezha, :plugin, :websocket, :retried] =>
      {:flezha_ws, :retried, :event,
       [
         label: "Retrying",
         meaning: "A websocket send failed and will be tried again."
       ]},
    [:flezha, :plugin, :websocket, :shipped] =>
      {:flezha_ws, :shipped, :metric,
       [
         label: "Sent",
         meaning: "Messages were sent over a websocket.",
         sentence: "{plugin_id} sent {count:message}"
       ]},
    [:flezha, :plugin_package, :loaded] =>
      {:flezha_plugin_packages, :loaded, :event,
       [
         label: "Loaded",
         meaning: "A plugin package was loaded when the controller started."
       ]},
    [:flezha, :plugin_package, :refused] =>
      {:flezha_plugin_packages, :refused, :event,
       [
         label: "Refused",
         outcome: :failure,
         meaning:
           "A plugin package was refused when the controller started, so its plugins are missing."
       ]},
    [:flezha, :relay, :halted] =>
      {:flezha_relay, :halted_total, :metric,
       [
         label: "Halted",
         outcome: :failure,
         meaning: "A relay was halted after a failure and stays stopped until someone acts.",
         sentence: "{relay_id} halted in {namespace}"
       ]},
    [:flezha, :relay, :started] =>
      {:flezha_relay, :relay_started, :event,
       [
         label: "Started",
         meaning: "A relay was started.",
         sentence: "{relay} started in {namespace}"
       ]},
    [:flezha, :relay, :stopped] =>
      {:flezha_relay, :relay_stopped, :event,
       [
         label: "Stopped",
         meaning: "A relay was stopped or finished its work.",
         sentence: "{relay} stopped in {namespace}"
       ]},
    [:flezha, :runner, :admin_request] =>
      {:flezha_runner, :admin_request, :event,
       [
         label: "Admin request",
         meaning: "A runner sent an administration request."
       ]},
    [:flezha, :runner, :connected] =>
      {:flezha_runner, :runner_connected, :event,
       [
         label: "Connected",
         meaning: "A runner opened its connection to the controller.",
         sentence: "{runner} connected to {namespace}"
       ]},
    [:flezha, :runner, :disconnected] =>
      {:flezha_runner, :runner_disconnected, :event,
       [
         label: "Disconnected",
         meaning: "A runner closed its connection.",
         sentence: "{runner} disconnected from {namespace}"
       ]},
    [:flezha, :runner, :drop_refused] =>
      {:flezha_runner, :runner_drop_refused, :event,
       [
         label: "Refused after drop",
         outcome: :failure,
         meaning: "A runner an operator dropped tried to come back and was turned away."
       ]},
    [:flezha, :runner, :dropped] =>
      {:flezha_runner, :runner_dropped, :event,
       [
         label: "Dropped by an operator",
         meaning: "An operator dropped a runner, so it cannot reconnect until it is let back in."
       ]},
    [:flezha, :runner, :evicted] =>
      {:flezha_health, :evicted, :event,
       [
         label: "Evicted",
         outcome: :failure,
         meaning: "The controller removed a runner."
       ]},
    [:flezha, :runner, :factory_language_conflict] =>
      {:flezha_runner, :factory_language_conflict, :event,
       [
         label: "Language clash",
         outcome: :failure,
         level: :warning,
         meaning:
           "A runner offered a factory that another runner already serves in another language."
       ]},
    [:flezha, :runner, :factory_run_conflict] =>
      {:flezha_runner, :factory_run_conflict, :event,
       [
         label: "Factory clash",
         outcome: :failure,
         level: :warning,
         meaning: "A runner offered a factory that conflicts with one already running."
       ]},
    [:flezha, :runner, :foreign_frame_refused] =>
      {:flezha_runner, :foreign_frame_refused, :event,
       [
         label: "Not its relay",
         outcome: :failure,
         meaning: "A runner sent a message for a relay it does not own, and it was refused."
       ]},
    [:flezha, :runner, :liveness_timeout] =>
      {:flezha_health, :liveness_timeout, :event,
       [
         label: "Stopped answering",
         outcome: :failure,
         level: :warning,
         meaning: "A runner stopped answering the controller's keepalive."
       ]},
    [:flezha, :runner, :quota_refused] =>
      {:flezha_runner, :quota_refused, :event,
       [
         label: "Runner quota full",
         outcome: :failure,
         level: :warning,
         meaning: "A runner was turned away because its namespace has as many runners as allowed."
       ]},
    [:flezha, :runner, :readmitted] =>
      {:flezha_runner, :runner_readmitted, :event,
       [
         label: "Let back in",
         meaning: "An operator let a dropped runner connect again."
       ]},
    [:flezha, :runner, :reconnected] =>
      {:flezha_health, :reconnected, :event,
       [
         label: "Reconnected",
         meaning: "A runner reconnected and kept its relays."
       ]},
    [:flezha, :runner, :reconnected_reassigned] =>
      {:flezha_health, :reconnected_reassigned, :event,
       [
         label: "Reconnected too late",
         outcome: :failure,
         meaning: "A runner reconnected after its relays had already moved elsewhere."
       ]},
    [:flezha, :runner, :restarted] =>
      {:flezha_health, :restarted, :event,
       [
         label: "Restarted",
         outcome: :failure,
         level: :warning,
         meaning: "A runner restarted."
       ]},
    [:flezha, :schedule, :spawn_failed] =>
      {:flezha_schedule, :spawn_failed, :event,
       [
         label: "Start failed",
         outcome: :failure,
         level: :error,
         meaning: "A schedule came due, but its relay failed to start."
       ]},
    [:flezha, :shutdown, :complete] =>
      {:flezha_shutdown, :complete, :event,
       [
         label: "Shutdown finished",
         meaning: "The controller finished stopping gracefully."
       ]},
    [:flezha, :shutdown, :drain_progress] =>
      {:flezha_shutdown, :drain_progress, :event,
       [
         label: "Relays moved",
         meaning: "A namespace finished moving its relays to other nodes before the stop."
       ]},
    [:flezha, :shutdown, :phase_completed] =>
      {:flezha_shutdown, :phase_completed, :event,
       [label: "Step finished", meaning: "A step of the shutdown finished."]},
    [:flezha, :shutdown, :phase_started] =>
      {:flezha_shutdown, :phase_started, :event,
       [label: "Step started", meaning: "A step of the shutdown began."]},
    [:flezha, :shutdown, :requested] =>
      {:flezha_shutdown, :requested, :event,
       [
         label: "Shutdown started",
         meaning: "Someone asked the controller to stop gracefully."
       ]},
    [:flezha, :shutdown, :timeout] =>
      {:flezha_shutdown, :timeout, :event,
       [
         label: "Step timed out",
         outcome: :failure,
         meaning: "A step of the shutdown ran out of time."
       ]},
    [:flezha, :state, :artifact_missing] =>
      {:flezha_durability, :artifact_missing, :event,
       [
         label: "Copy missing",
         outcome: :failure,
         level: :error,
         meaning: "Remote storage lacks a copy that a relay's saved state refers to."
       ]},
    [:flezha, :state, :artifact_shipped] =>
      {:flezha_durability, :artifact_shipped, :metric,
       [
         label: "Uploaded",
         meaning: "A copy of a relay's state was sent to remote storage."
       ]},
    [:flezha, :state, :baseline_seed] =>
      {:flezha_state, :baseline_seed, :event,
       [
         label: "First write",
         meaning: "A relay's saved state took its first sequence number."
       ]},
    [:flezha, :state, :claim_superseded] =>
      {:flezha_durability, :claim_superseded, :event,
       [
         label: "Taken over",
         meaning: "Another node took over sending a relay's state to remote storage."
       ]},
    [:flezha, :state, :compaction_failed] =>
      {:flezha_state, :compaction_failed, :event,
       [
         label: "Clean-up failed",
         outcome: :failure,
         meaning: "Shrinking a relay's saved state failed; it will be tried again."
       ]},
    [:flezha, :state, :compaction_gave_up] =>
      {:flezha_state, :compaction_gave_up, :event,
       [
         label: "Clean-up given up",
         outcome: :failure,
         level: :error,
         meaning: "Shrinking a relay's saved state failed too often and was given up."
       ]},
    [:flezha, :state, :compaction_timeout] =>
      {:flezha_state, :compaction_timeout, :event,
       [
         label: "Clean-up timed out",
         outcome: :failure,
         meaning: "Shrinking a relay's saved state took too long and was stopped."
       ]},
    [:flezha, :state, :compaction_trigger] =>
      {:flezha_state, :compaction_trigger, :event,
       [
         label: "Clean-up started",
         meaning: "A relay's saved state grew enough to be shrunk."
       ]},
    [:flezha, :state, :credit_anomaly] =>
      {:flezha_state, :credit_anomaly, :event,
       [
         label: "Wrote past its allowance",
         outcome: :failure,
         meaning:
           "A runner wrote more than the controller allowed it to: a fault in the runner, not load."
       ]},
    [:flezha, :state, :disk_quota_breach] =>
      {:flezha_state, :disk_quota_breach, :event,
       [
         label: "Over disk quota",
         outcome: :failure,
         level: :error,
         meaning: "A namespace's saved state grew past its disk quota."
       ]},
    [:flezha, :state, :durability_gc] =>
      {:flezha_durability, :gc, :metric,
       [
         label: "Old copies removed",
         meaning: "Copies no longer needed were removed from remote storage.",
         sentence: "Removed {collected:old copy} from remote storage"
       ]},
    [:flezha, :state, :fsync, :tick] =>
      {:flezha_state, :fsync_tick, :event,
       [
         label: "Flushed to disk",
         meaning: "Pending writes were flushed to disk together.",
         sentence: "Flushed {count:pending write} to disk"
       ]},
    [:flezha, :state, :hydrated] =>
      {:flezha_durability, :hydrated, :event,
       [
         label: "Restored",
         meaning: "A relay's state was restored from remote storage."
       ]},
    [:flezha, :state, :hydration_failed] =>
      {:flezha_durability, :hydration_failed, :event,
       [
         label: "Restore failed",
         outcome: :failure,
         level: :error,
         meaning: "Restoring a relay's state from remote storage failed."
       ]},
    [:flezha, :state, :inbox_duplicate] =>
      {:flezha_state, :inbox_duplicate, :event,
       [
         label: "Duplicate ignored",
         meaning: "A relay received a message it already had and ignored the copy."
       ]},
    [:flezha, :state, :input_log_frozen_watermark] =>
      {:flezha_state, :input_log_frozen_watermark, :event,
       [
         label: "Input stuck",
         outcome: :failure,
         meaning: "A relay's record of the input it received stopped moving forward."
       ]},
    [:flezha, :state, :input_log_syncer_died] =>
      {:flezha_state, :input_log_syncer_died, :event,
       [
         label: "Input saver crashed",
         outcome: :failure,
         meaning: "The process that saves a relay's received input crashed."
       ]},
    [:flezha, :state, :input_silence] =>
      {:flezha_health, :input_silence, :event,
       [
         label: "No input",
         outcome: :failure,
         level: :warning,
         meaning: "A relay has received no input for longer than allowed."
       ]},
    [:flezha, :state, :output_stall] =>
      {:flezha_state, :output_stall, :event,
       [
         label: "No output",
         outcome: :failure,
         meaning: "A relay has produced no output for longer than allowed."
       ]},
    [:flezha, :state, :record_frame, :refused] =>
      {:flezha_state, :record_frame_refused, :event,
       [
         label: "Damaged record refused",
         outcome: :failure,
         level: :error,
         meaning: "A damaged record at the end of a relay's saved log was refused."
       ]},
    [:flezha, :state, :record_frame, :repaired] =>
      {:flezha_state, :record_frame_repaired, :event,
       [
         label: "Torn record dropped",
         outcome: :failure,
         level: :warning,
         meaning: "A half-written record at the end of a relay's saved log was dropped."
       ]},
    [:flezha, :state, :remote_commit] =>
      {:flezha_durability, :remote_commit, :metric,
       [
         label: "Upload confirmed",
         meaning: "Remote storage confirmed it holds a copy."
       ]},
    [:flezha, :state, :restore_refused] =>
      {:flezha_state, :restore_refused, :event,
       [
         label: "Restore refused",
         outcome: :failure,
         level: :error,
         meaning: "A relay's saved state could not be restored."
       ]},
    [:flezha, :state, :rpo_exceeded] =>
      {:flezha_durability, :rpo_exceeded, :event,
       [
         label: "Backups too far behind",
         outcome: :failure,
         level: :error,
         meaning: "Remote copies fell further behind than the recovery point allows."
       ]},
    [:flezha, :state, :seq_gap] =>
      {:flezha_state, :seq_gap, :event,
       [
         label: "Gap in sequence",
         outcome: :failure,
         meaning: "A relay's saved state arrived with a gap in its sequence numbers."
       ]},
    [:flezha, :state, :ship_failed] =>
      {:flezha_durability, :ship_failed, :event,
       [
         label: "Upload failed",
         outcome: :failure,
         meaning: "Sending a copy of a relay's state to remote storage failed."
       ]},
    [:flezha, :state, :stale_writer_refused] =>
      {:flezha_state, :stale_writer_refused, :event,
       [
         label: "Old writer refused",
         outcome: :failure,
         meaning: "A runner that no longer owns a relay tried to write its state and was refused."
       ]},
    [:flezha, :state, :wal, :backpressure] =>
      {:flezha_state, :wal_backpressure, :metric,
       [
         label: "Writes held back",
         outcome: :failure,
         meaning: "A relay wrote faster than its saved state could keep up, so its writes waited."
       ]},
    [:flezha, :state, :wal_pull_failed] =>
      {:flezha_state, :wal_pull_failed, :event,
       [
         label: "Copy from node failed",
         outcome: :failure,
         meaning: "Copying a relay's saved state from another node failed."
       ]},
    [:flezha, :state, :wal_realign] =>
      {:flezha_state, :wal_realign, :event,
       [
         label: "Sequence realigned",
         meaning: "A relay's saved state moved its sequence number to match its runner's."
       ]},
    [:flezha, :state, :wal_rewind] =>
      {:flezha_state, :wal_rewind, :event,
       [
         label: "Rewound",
         meaning: "A relay's saved state was rewound to an earlier point."
       ]},
    [:flezha, :steps, :failed] =>
      {:flezha_health, :failed, :event,
       [
         label: "Steps relay failed",
         outcome: :failure,
         level: :warning,
         meaning: "A steps relay failed."
       ]},
    [:flezha, :steps, :suspend_requested] =>
      {:flezha_health, :suspend_requested, :event,
       [
         label: "Pause asked",
         meaning: "A steps relay's runner asked to pause it at the next safe point."
       ]},
    [:flezha, :steps, :suspended] =>
      {:flezha_health, :suspended, :event,
       [label: "Paused", meaning: "A steps relay paused at a safe point."]},
    [:flezha, :steps, :turn_dead_lettered] =>
      {:flezha_steps, :turns_dead_lettered_total, :metric,
       [
         label: "Turn set aside",
         outcome: :failure,
         meaning: "A turn of a steps relay failed and became a dead letter; the relay carries on."
       ]},
    [:flezha, :stream, :drop] =>
      {:flezha_stream, :drop, :metric,
       [
         label: "Dropped",
         outcome: :failure,
         meaning: "A message for a relay was dropped because the relay had no open stream.",
         sentence: "A message for {relay_id} was dropped"
       ]},
    [:flezha, :stream, :fanout_warning] =>
      {:flezha_stream, :fanout_warning, :event,
       [
         label: "Duplicate process",
         outcome: :failure,
         meaning: "A stream found more than one process serving one relay."
       ]},
    [:flezha, :transport, :connect_rejected] =>
      {:flezha_health, :connect_rejected, :event,
       [
         label: "Connection rejected",
         outcome: :failure,
         meaning: "A connection to the controller was rejected."
       ]}
  },
  mapping: %{
    [:flezha, :backoff, :reset] => {:flezha_convergence, :backoff_reset, :metric},
    [:flezha, :backoff, :wait] => {:flezha_convergence, :backoff_wait, :metric},
    [:flezha, :boot, :pre_recovery_replay] => {:flezha_boot, :pre_recovery_replay, :event},
    [:flezha, :boot, :reconcile_duplicate] => {:flezha_boot, :reconcile_duplicate, :event},
    [:flezha, :boot, :recovery_complete] => {:flezha_boot, :recovery_complete, :event},
    [:flezha, :call, :answer_dropped] => {:flezha_call, :answers_dropped_total, :metric},
    [:flezha, :call, :answered] => {:flezha_call, :answer_duration, :metric},
    [:flezha, :call, :recovered_unanswered] =>
      {:flezha_call, :recovered_unanswered_total, :metric},
    [:flezha, :call, :refused] => {:flezha_call, :refused_total, :metric},
    [:flezha, :call, :timeout] => {:flezha_call, :timeouts_total, :metric},
    [:flezha, :convergence, :heal] => {:flezha_health, :heal, :event},
    [:flezha, :convergence, :intent_mismatch] => {:flezha_convergence, :intent_mismatch, :event},
    [:flezha, :convergence, :relay_stuck] => {:flezha_health, :relay_stuck, :event},
    [:flezha, :convergence, :safe_call] => {:flezha_health, :safe_call, :event},
    [:flezha, :correction, :refused] => {:flezha_correction, :refused, :event},
    [:flezha, :data_plane, :stall_localized] => {:flezha_health, :stall_localized, :event},
    [:flezha, :dlq, :dead_lettered] => {:flezha_hooks, :on_dead_letter, :event},
    [:flezha, :dlq, :parked] => {:flezha_dlq, :parked_total, :metric},
    [:flezha, :dlq, :retention_dropped] => {:flezha_dlq, :dropped_by_retention_total, :metric},
    [:flezha, :dlq, :usage] => {:flezha_dlq, :usage, :metric},
    [:flezha, :error, :uncatalogued] => {:flezha_error, :uncatalogued, :event},
    [:flezha, :health, :verdict] => {:flezha_health, :verdict, :event},
    [:flezha, :hub, :start_failed] => {:flezha_boot, :hub_start_failed, :event},
    [:flezha, :hub, :unavailable] => {:flezha_health, :unavailable, :event},
    [:flezha, :plugin, :file_sink, :failed] => {:flezha_file_sink, :failed, :event},
    [:flezha, :plugin, :file_sink, :refused] => {:flezha_file_sink, :refused, :event},
    [:flezha, :plugin, :file_sink, :shipped] => {:flezha_file_sink, :shipped, :metric},
    [:flezha, :plugin, :file_source, :failed] => {:flezha_file_source, :failed, :event},
    [:flezha, :plugin, :file_source, :shipped] => {:flezha_file_source, :shipped, :metric},
    [:flezha, :plugin, :file_source, :truncated] => {:flezha_file_source, :truncated, :event},
    [:flezha, :plugin, :health, :fail_open] => {:flezha_health, :fail_open, :event},
    [:flezha, :plugin, :hook_webhook, :failed] => {:flezha_hook_webhook, :failed, :event},
    [:flezha, :plugin, :hook_webhook, :overflowed] =>
      {:flezha_hook_webhook, :overflowed, :metric},
    [:flezha, :plugin, :hook_webhook, :retried] => {:flezha_hook_webhook, :retried, :metric},
    [:flezha, :plugin, :hook_webhook, :shipped] => {:flezha_hook_webhook, :shipped, :metric},
    [:flezha, :plugin, :http_poll, :failed] => {:flezha_http_poll, :failed, :event},
    [:flezha, :plugin, :http_poll, :shipped] => {:flezha_http_poll, :shipped, :metric},
    [:flezha, :plugin, :http_poll, :unchanged] => {:flezha_http_poll, :unchanged, :metric},
    [:flezha, :plugin, :http_push, :failed] => {:flezha_http_push, :failed, :event},
    [:flezha, :plugin, :http_push, :overflowed] => {:flezha_http_push, :overflowed, :metric},
    [:flezha, :plugin, :http_push, :retried] => {:flezha_http_push, :retried, :metric},
    [:flezha, :plugin, :http_push, :shipped] => {:flezha_http_push, :shipped, :metric},
    [:flezha, :plugin, :phase] => {:flezha_plugin, :phase, :event},
    [:flezha, :plugin, :postgres_poll, :caught_up] => {:flezha_postgres_poll, :caught_up, :event},
    [:flezha, :plugin, :postgres_poll, :failed] => {:flezha_postgres_poll, :failed, :event},
    [:flezha, :plugin, :postgres_poll, :shipped] => {:flezha_postgres_poll, :shipped, :metric},
    [:flezha, :plugin, :relay_forward, :overflowed] =>
      {:flezha_relay_forward, :saturated, :event},
    [:flezha, :plugin, :relay_forward, :queue_depth] =>
      {:flezha_relay_forward, :queue_depth, :metric},
    [:flezha, :plugin, :relay_forward, :retried] => {:flezha_relay_forward, :retried, :metric},
    [:flezha, :plugin, :relay_forward, :shipped] => {:flezha_relay_forward, :shipped, :metric},
    [:flezha, :plugin, :websocket, :failed] => {:flezha_ws, :failed, :event},
    [:flezha, :plugin, :websocket, :idle_reconnect] => {:flezha_ws, :idle_reconnect, :event},
    [:flezha, :plugin, :websocket, :overflowed] => {:flezha_ws, :overflowed, :event},
    [:flezha, :plugin, :websocket, :reconcile] => {:flezha_ws, :reconcile, :event},
    [:flezha, :plugin, :websocket, :resubscribe] => {:flezha_ws, :resubscribe, :event},
    [:flezha, :plugin, :websocket, :retried] => {:flezha_ws, :retried, :event},
    [:flezha, :plugin, :websocket, :shipped] => {:flezha_ws, :shipped, :metric},
    [:flezha, :plugin_package, :loaded] => {:flezha_plugin_packages, :loaded, :event},
    [:flezha, :plugin_package, :refused] => {:flezha_plugin_packages, :refused, :event},
    [:flezha, :relay, :halted] => {:flezha_relay, :halted_total, :metric},
    [:flezha, :relay, :started] => {:flezha_relay, :relay_started, :event},
    [:flezha, :relay, :stopped] => {:flezha_relay, :relay_stopped, :event},
    [:flezha, :runner, :admin_request] => {:flezha_runner, :admin_request, :event},
    [:flezha, :runner, :connected] => {:flezha_runner, :runner_connected, :event},
    [:flezha, :runner, :disconnected] => {:flezha_runner, :runner_disconnected, :event},
    [:flezha, :runner, :drop_refused] => {:flezha_runner, :runner_drop_refused, :event},
    [:flezha, :runner, :dropped] => {:flezha_runner, :runner_dropped, :event},
    [:flezha, :runner, :evicted] => {:flezha_health, :evicted, :event},
    [:flezha, :runner, :factory_language_conflict] =>
      {:flezha_runner, :factory_language_conflict, :event},
    [:flezha, :runner, :factory_run_conflict] => {:flezha_runner, :factory_run_conflict, :event},
    [:flezha, :runner, :foreign_frame_refused] =>
      {:flezha_runner, :foreign_frame_refused, :event},
    [:flezha, :runner, :liveness_timeout] => {:flezha_health, :liveness_timeout, :event},
    [:flezha, :runner, :quota_refused] => {:flezha_runner, :quota_refused, :event},
    [:flezha, :runner, :readmitted] => {:flezha_runner, :runner_readmitted, :event},
    [:flezha, :runner, :reconnected] => {:flezha_health, :reconnected, :event},
    [:flezha, :runner, :reconnected_reassigned] =>
      {:flezha_health, :reconnected_reassigned, :event},
    [:flezha, :runner, :restarted] => {:flezha_health, :restarted, :event},
    [:flezha, :schedule, :spawn_failed] => {:flezha_schedule, :spawn_failed, :event},
    [:flezha, :shutdown, :complete] => {:flezha_shutdown, :complete, :event},
    [:flezha, :shutdown, :drain_progress] => {:flezha_shutdown, :drain_progress, :event},
    [:flezha, :shutdown, :phase_completed] => {:flezha_shutdown, :phase_completed, :event},
    [:flezha, :shutdown, :phase_started] => {:flezha_shutdown, :phase_started, :event},
    [:flezha, :shutdown, :requested] => {:flezha_shutdown, :requested, :event},
    [:flezha, :shutdown, :timeout] => {:flezha_shutdown, :timeout, :event},
    [:flezha, :state, :artifact_missing] => {:flezha_durability, :artifact_missing, :event},
    [:flezha, :state, :artifact_shipped] => {:flezha_durability, :artifact_shipped, :metric},
    [:flezha, :state, :baseline_seed] => {:flezha_state, :baseline_seed, :event},
    [:flezha, :state, :claim_superseded] => {:flezha_durability, :claim_superseded, :event},
    [:flezha, :state, :compaction_failed] => {:flezha_state, :compaction_failed, :event},
    [:flezha, :state, :compaction_gave_up] => {:flezha_state, :compaction_gave_up, :event},
    [:flezha, :state, :compaction_timeout] => {:flezha_state, :compaction_timeout, :event},
    [:flezha, :state, :compaction_trigger] => {:flezha_state, :compaction_trigger, :event},
    [:flezha, :state, :credit_anomaly] => {:flezha_state, :credit_anomaly, :event},
    [:flezha, :state, :disk_quota_breach] => {:flezha_state, :disk_quota_breach, :event},
    [:flezha, :state, :durability_gc] => {:flezha_durability, :gc, :metric},
    [:flezha, :state, :fsync, :tick] => {:flezha_state, :fsync_tick, :event},
    [:flezha, :state, :hydrated] => {:flezha_durability, :hydrated, :event},
    [:flezha, :state, :hydration_failed] => {:flezha_durability, :hydration_failed, :event},
    [:flezha, :state, :inbox_duplicate] => {:flezha_state, :inbox_duplicate, :event},
    [:flezha, :state, :input_log_frozen_watermark] =>
      {:flezha_state, :input_log_frozen_watermark, :event},
    [:flezha, :state, :input_log_syncer_died] => {:flezha_state, :input_log_syncer_died, :event},
    [:flezha, :state, :input_silence] => {:flezha_health, :input_silence, :event},
    [:flezha, :state, :output_stall] => {:flezha_state, :output_stall, :event},
    [:flezha, :state, :record_frame, :refused] => {:flezha_state, :record_frame_refused, :event},
    [:flezha, :state, :record_frame, :repaired] =>
      {:flezha_state, :record_frame_repaired, :event},
    [:flezha, :state, :remote_commit] => {:flezha_durability, :remote_commit, :metric},
    [:flezha, :state, :restore_refused] => {:flezha_state, :restore_refused, :event},
    [:flezha, :state, :rpo_exceeded] => {:flezha_durability, :rpo_exceeded, :event},
    [:flezha, :state, :seq_gap] => {:flezha_state, :seq_gap, :event},
    [:flezha, :state, :ship_failed] => {:flezha_durability, :ship_failed, :event},
    [:flezha, :state, :stale_writer_refused] => {:flezha_state, :stale_writer_refused, :event},
    [:flezha, :state, :wal, :backpressure] => {:flezha_state, :wal_backpressure, :metric},
    [:flezha, :state, :wal_pull_failed] => {:flezha_state, :wal_pull_failed, :event},
    [:flezha, :state, :wal_realign] => {:flezha_state, :wal_realign, :event},
    [:flezha, :state, :wal_rewind] => {:flezha_state, :wal_rewind, :event},
    [:flezha, :steps, :failed] => {:flezha_health, :failed, :event},
    [:flezha, :steps, :suspend_requested] => {:flezha_health, :suspend_requested, :event},
    [:flezha, :steps, :suspended] => {:flezha_health, :suspended, :event},
    [:flezha, :steps, :turn_dead_lettered] =>
      {:flezha_steps, :turns_dead_lettered_total, :metric},
    [:flezha, :stream, :drop] => {:flezha_stream, :drop, :metric},
    [:flezha, :stream, :fanout_warning] => {:flezha_stream, :fanout_warning, :event},
    [:flezha, :transport, :connect_rejected] => {:flezha_health, :connect_rejected, :event}
  },
  registrations: [
    %{
      area: :controller,
      description:
        "Bringing relays back and starting each namespace's hub when the controller starts.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_boot,
      signals: [
        %{
          description: "A namespace's hub failed to start, so its relays are not running.",
          kind: :event,
          label: "Namespace failed to start",
          name: :hub_start_failed,
          outcome: :failure
        },
        %{
          description: "A namespace began bringing its relays back after the controller started.",
          kind: :event,
          label: "Restoring relays",
          name: :pre_recovery_replay,
          outcome: :normal
        },
        %{
          description:
            "A relay was found running on more than one node, and the extra copies were stopped.",
          kind: :event,
          label: "Duplicate stopped",
          name: :reconcile_duplicate,
          outcome: :failure
        },
        %{
          description:
            "A namespace finished bringing its relays back after the controller started.",
          kind: :event,
          label: "Relays restored",
          name: :recovery_complete,
          outcome: :normal
        }
      ],
      title: "Controller start-up"
    },
    %{
      area: :relays,
      description: "Calls made to relays: answers, timeouts and refusals.",
      flavour: :declarative,
      kinds: [:metric],
      name: :flezha_call,
      signals: [
        %{
          description: "A call to a relay was answered; the details say how long it took.",
          kind: :metric,
          label: "Answered",
          name: :answer_duration,
          outcome: :normal,
          sentence: "{relay_id} answered a call"
        },
        %{
          description: "A relay answered a call that nobody was waiting for any more.",
          kind: :metric,
          label: "Answer unclaimed",
          name: :answers_dropped_total,
          outcome: :failure
        },
        %{
          description: "A call picked up again after a restart was closed without an answer.",
          kind: :metric,
          label: "Closed unanswered",
          name: :recovered_unanswered_total,
          outcome: :failure
        },
        %{
          description: "A call to a relay was refused.",
          kind: :metric,
          label: "Refused",
          name: :refused_total,
          outcome: :failure,
          sentence: "A call to {relay_id} was refused"
        },
        %{
          description: "A call to a relay got no answer in time.",
          kind: :metric,
          label: "Timed out",
          name: :timeouts_total,
          outcome: :failure,
          sentence: "A call to {relay_id} timed out"
        }
      ],
      title: "Relay calls"
    },
    %{
      area: :relays,
      description: "The loop that keeps running relays matching what was asked, and its retries.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_convergence,
      signals: [
        %{
          description: "A success reset a retry's waiting time.",
          kind: :metric,
          label: "Retry wait reset",
          name: :backoff_reset,
          outcome: :normal
        },
        %{
          description: "A retry waited before its next attempt.",
          kind: :metric,
          label: "Waiting to retry",
          name: :backoff_wait,
          outcome: :normal
        },
        %{
          description: "Saved state exists for relays nobody asked to run.",
          kind: :event,
          label: "Leftover state",
          name: :intent_mismatch,
          outcome: :failure
        }
      ],
      title: "Self-repair"
    },
    %{
      area: :relays,
      description: "Corrections to a relay that the controller would not apply.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_correction,
      signals: [
        %{
          description: "The controller refused to apply a correction to a relay.",
          kind: :event,
          label: "Correction refused",
          name: :refused,
          outcome: :failure
        }
      ],
      title: "Refused corrections"
    },
    %{
      area: :relays,
      description:
        "Messages that failed and were set aside in dead-letter queues, and those that expired.",
      flavour: :declarative,
      kinds: [:metric],
      name: :flezha_dlq,
      signals: [
        %{
          description: "Dead letters older than the retention limit were deleted.",
          kind: :metric,
          label: "Expired",
          name: :dropped_by_retention_total,
          outcome: :normal
        },
        %{
          description: "A message failed and was set aside in a dead-letter queue.",
          kind: :metric,
          label: "Set aside",
          name: :parked_total,
          outcome: :failure,
          sentence: "{tasklet_id} set aside {count:message}"
        },
        %{
          description: "How many dead letters a queue holds, and how many bytes.",
          kind: :metric,
          label: "Queue size",
          name: :usage,
          outcome: :normal,
          sentence: "{tasklet_id} holds {entries:dead letter}"
        }
      ],
      title: "Dead letters"
    },
    %{
      area: :storage,
      description: "Copies of relay state sent to remote storage and restored from it.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_durability,
      signals: [
        %{
          description: "Remote storage lacks a copy that a relay's saved state refers to.",
          kind: :event,
          label: "Copy missing",
          name: :artifact_missing,
          outcome: :failure
        },
        %{
          description: "A copy of a relay's state was sent to remote storage.",
          kind: :metric,
          label: "Uploaded",
          name: :artifact_shipped,
          outcome: :normal
        },
        %{
          description: "Another node took over sending a relay's state to remote storage.",
          kind: :event,
          label: "Taken over",
          name: :claim_superseded,
          outcome: :normal
        },
        %{
          description: "Copies no longer needed were removed from remote storage.",
          kind: :metric,
          label: "Old copies removed",
          name: :gc,
          outcome: :normal,
          sentence: "Removed {collected:old copy} from remote storage"
        },
        %{
          description: "A relay's state was restored from remote storage.",
          kind: :event,
          label: "Restored",
          name: :hydrated,
          outcome: :normal
        },
        %{
          description: "Restoring a relay's state from remote storage failed.",
          kind: :event,
          label: "Restore failed",
          name: :hydration_failed,
          outcome: :failure
        },
        %{
          description: "Remote storage confirmed it holds a copy.",
          kind: :metric,
          label: "Upload confirmed",
          name: :remote_commit,
          outcome: :normal
        },
        %{
          description: "Remote copies fell further behind than the recovery point allows.",
          kind: :event,
          label: "Backups too far behind",
          name: :rpo_exceeded,
          outcome: :failure
        },
        %{
          description: "Sending a copy of a relay's state to remote storage failed.",
          kind: :event,
          label: "Upload failed",
          name: :ship_failed,
          outcome: :failure
        }
      ],
      title: "Remote backups"
    },
    %{
      area: :controller,
      description:
        "Errors a caller got without a documented code; each one is a defect to report.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_error,
      signals: [
        %{
          description: "A caller got an error with no documented code: a defect to report.",
          kind: :event,
          label: "Undocumented error",
          name: :uncatalogued,
          outcome: :failure
        }
      ],
      title: "Undocumented errors"
    },
    %{
      area: :plugins,
      description: "The plugin that writes a relay's output to a file.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_file_sink,
      signals: [
        %{
          description: "A delivery or a read failed.",
          kind: :event,
          label: "Failed",
          name: :failed,
          outcome: :failure
        },
        %{
          description: "A record was refused because it cannot be written as one line.",
          kind: :event,
          label: "Record refused",
          name: :refused,
          outcome: :failure,
          sentence: "{plugin_id} refused a record for {path}"
        },
        %{
          description:
            "The plugin passed items on, into its relay or out to where it sends them.",
          kind: :metric,
          label: "Delivered",
          name: :shipped,
          outcome: :normal
        }
      ],
      title: "File sink plugin"
    },
    %{
      area: :plugins,
      description: "The plugin that reads lines from a file into a relay.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_file_source,
      signals: [
        %{
          description: "A delivery or a read failed.",
          kind: :event,
          label: "Failed",
          name: :failed,
          outcome: :failure
        },
        %{
          description:
            "The plugin passed items on, into its relay or out to where it sends them.",
          kind: :metric,
          label: "Delivered",
          name: :shipped,
          outcome: :normal
        },
        %{
          description: "The file was cut short or replaced, so it is read again from the start.",
          kind: :event,
          label: "File restarted",
          name: :truncated,
          outcome: :normal,
          sentence: "{plugin_id} reads {path} again from the start"
        }
      ],
      title: "File source plugin"
    },
    %{
      area: :relays,
      description: "What the health checks found about relays, runners and plugins.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_health,
      signals: [
        %{
          description: "A connection to the controller was rejected.",
          kind: :event,
          label: "Connection rejected",
          name: :connect_rejected,
          outcome: :failure
        },
        %{
          description: "The controller removed a runner.",
          kind: :event,
          label: "Evicted",
          name: :evicted,
          outcome: :failure
        },
        %{
          description:
            "A plugin's health check failed or timed out, so it is treated as healthy for now.",
          kind: :event,
          label: "Health check failed",
          name: :fail_open,
          outcome: :failure
        },
        %{
          description: "A steps relay failed.",
          kind: :event,
          label: "Steps relay failed",
          name: :failed,
          outcome: :failure
        },
        %{
          description: "The controller ran a repair step.",
          kind: :event,
          label: "Repaired",
          name: :heal,
          outcome: :normal
        },
        %{
          description: "A relay has received no input for longer than allowed.",
          kind: :event,
          label: "No input",
          name: :input_silence,
          outcome: :failure
        },
        %{
          description: "A runner stopped answering the controller's keepalive.",
          kind: :event,
          label: "Stopped answering",
          name: :liveness_timeout,
          outcome: :failure
        },
        %{
          description: "A runner reconnected and kept its relays.",
          kind: :event,
          label: "Reconnected",
          name: :reconnected,
          outcome: :normal
        },
        %{
          description: "A runner reconnected after its relays had already moved elsewhere.",
          kind: :event,
          label: "Reconnected too late",
          name: :reconnected_reassigned,
          outcome: :failure
        },
        %{
          description: "A relay is making no progress.",
          kind: :event,
          label: "Stuck",
          name: :relay_stuck,
          outcome: :failure
        },
        %{
          description: "A runner restarted.",
          kind: :event,
          label: "Restarted",
          name: :restarted,
          outcome: :failure
        },
        %{
          description: "A short call checked whether a relay answers.",
          kind: :event,
          label: "Probed",
          name: :safe_call,
          outcome: :normal
        },
        %{
          description: "A stalled message flow was traced to one connection, stream or relay.",
          kind: :event,
          label: "Stall traced",
          name: :stall_localized,
          outcome: :failure
        },
        %{
          description: "A steps relay's runner asked to pause it at the next safe point.",
          kind: :event,
          label: "Pause asked",
          name: :suspend_requested,
          outcome: :normal
        },
        %{
          description: "A steps relay paused at a safe point.",
          kind: :event,
          label: "Paused",
          name: :suspended,
          outcome: :normal
        },
        %{
          description: "A namespace's hub was down when something asked for it.",
          kind: :event,
          label: "Namespace down",
          name: :unavailable,
          outcome: :failure
        },
        %{
          description: "The health of a relay, runner or plugin changed.",
          kind: :event,
          label: "Health changed",
          name: :verdict,
          outcome: :normal,
          sentence: "{id} is now {state}"
        }
      ],
      title: "Health findings"
    },
    %{
      area: :plugins,
      description: "The plugin that sends controller notices to a URL.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_hook_webhook,
      signals: [
        %{
          description: "A delivery or a read failed.",
          kind: :event,
          label: "Failed",
          name: :failed,
          outcome: :failure
        },
        %{
          description: "The plugin's queue was full, so items were dropped.",
          kind: :metric,
          label: "Queue full",
          name: :overflowed,
          outcome: :failure
        },
        %{
          description: "A delivery failed and will be tried again; nothing is lost yet.",
          kind: :metric,
          label: "Retrying",
          name: :retried,
          outcome: :normal
        },
        %{
          description:
            "The plugin passed items on, into its relay or out to where it sends them.",
          kind: :metric,
          label: "Delivered",
          name: :shipped,
          outcome: :normal
        }
      ],
      title: "Webhook plugin"
    },
    %{
      area: :plugins,
      description:
        "Notices handed to controller plugins, such as a message becoming a dead letter.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_hooks,
      signals: [
        %{
          description: "A message became a dead letter, and controller plugins were told.",
          kind: :event,
          label: "Dead letter",
          name: :on_dead_letter,
          outcome: :failure
        }
      ],
      title: "Notices to controller plugins"
    },
    %{
      area: :plugins,
      description: "The plugin that reads a URL on a timer and hands what changed to a relay.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_http_poll,
      signals: [
        %{
          description: "A delivery or a read failed.",
          kind: :event,
          label: "Failed",
          name: :failed,
          outcome: :failure
        },
        %{
          description:
            "The plugin passed items on, into its relay or out to where it sends them.",
          kind: :metric,
          label: "Delivered",
          name: :shipped,
          outcome: :normal
        },
        %{
          description: "A poll's response matched the last one, so it was skipped.",
          kind: :metric,
          label: "Nothing new",
          name: :unchanged,
          outcome: :normal,
          sentence: "{plugin_id} found nothing new"
        }
      ],
      title: "HTTP poll plugin"
    },
    %{
      area: :plugins,
      description: "The plugin that sends a relay's output to a URL.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_http_push,
      signals: [
        %{
          description: "A delivery or a read failed.",
          kind: :event,
          label: "Failed",
          name: :failed,
          outcome: :failure
        },
        %{
          description: "The plugin's queue was full, so items were dropped.",
          kind: :metric,
          label: "Queue full",
          name: :overflowed,
          outcome: :failure
        },
        %{
          description: "A delivery failed and will be tried again; nothing is lost yet.",
          kind: :metric,
          label: "Retrying",
          name: :retried,
          outcome: :normal
        },
        %{
          description:
            "The plugin passed items on, into its relay or out to where it sends them.",
          kind: :metric,
          label: "Delivered",
          name: :shipped,
          outcome: :normal
        }
      ],
      title: "HTTP push plugin"
    },
    %{
      area: :plugins,
      description: "Plugins moving between the phases of their life.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_plugin,
      signals: [
        %{
          description:
            "A plugin moved from one phase of its life to another, such as from prepared to running.",
          kind: :event,
          label: "Phase changed",
          name: :phase,
          outcome: :normal,
          sentence: "{plugin_id} went from {from} to {to}"
        }
      ],
      title: "Plugin lifecycle"
    },
    %{
      area: :plugins,
      description: "Plugin packages the controller loaded or refused when it started.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_plugin_packages,
      signals: [
        %{
          description: "A plugin package was loaded when the controller started.",
          kind: :event,
          label: "Loaded",
          name: :loaded,
          outcome: :normal
        },
        %{
          description:
            "A plugin package was refused when the controller started, so its plugins are missing.",
          kind: :event,
          label: "Refused",
          name: :refused,
          outcome: :failure
        }
      ],
      title: "Plugin packages"
    },
    %{
      area: :plugins,
      description: "The plugin that reads new rows from a table into a relay.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_postgres_poll,
      signals: [
        %{
          description: "The poll reached the newest row in the table.",
          kind: :event,
          label: "Caught up",
          name: :caught_up,
          outcome: :normal,
          sentence: "{plugin_id} caught up with {table}"
        },
        %{
          description: "A delivery or a read failed.",
          kind: :event,
          label: "Failed",
          name: :failed,
          outcome: :failure
        },
        %{
          description:
            "The plugin passed items on, into its relay or out to where it sends them.",
          kind: :metric,
          label: "Delivered",
          name: :shipped,
          outcome: :normal
        }
      ],
      title: "Postgres poll plugin"
    },
    %{
      area: :relays,
      description: "Relays starting, stopping, and halting after a failure.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_relay,
      signals: [
        %{
          description: "A relay was halted after a failure and stays stopped until someone acts.",
          kind: :metric,
          label: "Halted",
          name: :halted_total,
          outcome: :failure,
          sentence: "{relay_id} halted in {namespace}"
        },
        %{
          description: "A relay was started.",
          kind: :event,
          label: "Started",
          name: :relay_started,
          outcome: :normal,
          sentence: "{relay} started in {namespace}"
        },
        %{
          description: "A relay was stopped or finished its work.",
          kind: :event,
          label: "Stopped",
          name: :relay_stopped,
          outcome: :normal,
          sentence: "{relay} stopped in {namespace}"
        }
      ],
      title: "Relay lifecycle"
    },
    %{
      area: :plugins,
      description: "The plugin that hands a relay's messages to the next relay.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_relay_forward,
      signals: [
        %{
          description: "How many messages are waiting to be forwarded.",
          kind: :metric,
          label: "Waiting to forward",
          name: :queue_depth,
          outcome: :normal,
          sentence: "{depth:message} waiting to be forwarded"
        },
        %{
          description: "A delivery failed and will be tried again; nothing is lost yet.",
          kind: :metric,
          label: "Retrying",
          name: :retried,
          outcome: :normal
        },
        %{
          description: "The forward queue was full and turned messages away.",
          kind: :event,
          label: "Queue full",
          name: :saturated,
          outcome: :failure
        },
        %{
          description:
            "The plugin passed items on, into its relay or out to where it sends them.",
          kind: :metric,
          label: "Delivered",
          name: :shipped,
          outcome: :normal
        }
      ],
      title: "Relay forward plugin"
    },
    %{
      area: :relays,
      description:
        "Runners opening and closing their connection to the controller, and the ones it turned away.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_runner,
      signals: [
        %{
          description: "A runner sent an administration request.",
          kind: :event,
          label: "Admin request",
          name: :admin_request,
          outcome: :normal
        },
        %{
          description:
            "A runner offered a factory that another runner already serves in another language.",
          kind: :event,
          label: "Language clash",
          name: :factory_language_conflict,
          outcome: :failure
        },
        %{
          description: "A runner offered a factory that conflicts with one already running.",
          kind: :event,
          label: "Factory clash",
          name: :factory_run_conflict,
          outcome: :failure
        },
        %{
          description: "A runner sent a message for a relay it does not own, and it was refused.",
          kind: :event,
          label: "Not its relay",
          name: :foreign_frame_refused,
          outcome: :failure
        },
        %{
          description:
            "A runner was turned away because its namespace has as many runners as allowed.",
          kind: :event,
          label: "Runner quota full",
          name: :quota_refused,
          outcome: :failure
        },
        %{
          description: "A runner opened its connection to the controller.",
          kind: :event,
          label: "Connected",
          name: :runner_connected,
          outcome: :normal,
          sentence: "{runner} connected to {namespace}"
        },
        %{
          description: "A runner closed its connection.",
          kind: :event,
          label: "Disconnected",
          name: :runner_disconnected,
          outcome: :normal,
          sentence: "{runner} disconnected from {namespace}"
        },
        %{
          description: "A runner an operator dropped tried to come back and was turned away.",
          kind: :event,
          label: "Refused after drop",
          name: :runner_drop_refused,
          outcome: :failure
        },
        %{
          description:
            "An operator dropped a runner, so it cannot reconnect until it is let back in.",
          kind: :event,
          label: "Dropped by an operator",
          name: :runner_dropped,
          outcome: :normal
        },
        %{
          description: "An operator let a dropped runner connect again.",
          kind: :event,
          label: "Let back in",
          name: :runner_readmitted,
          outcome: :normal
        }
      ],
      title: "Runner connections"
    },
    %{
      area: :relays,
      description: "Schedules that start relays on a timetable.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_schedule,
      signals: [
        %{
          description: "A schedule came due, but its relay failed to start.",
          kind: :event,
          label: "Start failed",
          name: :spawn_failed,
          outcome: :failure
        }
      ],
      title: "Schedules"
    },
    %{
      area: :controller,
      description: "A graceful stop of the controller, step by step, moving relays off first.",
      flavour: :declarative,
      kinds: [:event],
      name: :flezha_shutdown,
      signals: [
        %{
          description: "The controller finished stopping gracefully.",
          kind: :event,
          label: "Shutdown finished",
          name: :complete,
          outcome: :normal
        },
        %{
          description: "A namespace finished moving its relays to other nodes before the stop.",
          kind: :event,
          label: "Relays moved",
          name: :drain_progress,
          outcome: :normal
        },
        %{
          description: "A step of the shutdown finished.",
          kind: :event,
          label: "Step finished",
          name: :phase_completed,
          outcome: :normal
        },
        %{
          description: "A step of the shutdown began.",
          kind: :event,
          label: "Step started",
          name: :phase_started,
          outcome: :normal
        },
        %{
          description: "Someone asked the controller to stop gracefully.",
          kind: :event,
          label: "Shutdown started",
          name: :requested,
          outcome: :normal
        },
        %{
          description: "A step of the shutdown ran out of time.",
          kind: :event,
          label: "Step timed out",
          name: :timeout,
          outcome: :failure
        }
      ],
      title: "Controller shutdown"
    },
    %{
      area: :storage,
      description: "Each relay's state saved on this node: writes, clean-ups and repairs.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_state,
      signals: [
        %{
          description: "A relay's saved state took its first sequence number.",
          kind: :event,
          label: "First write",
          name: :baseline_seed,
          outcome: :normal
        },
        %{
          description: "Shrinking a relay's saved state failed; it will be tried again.",
          kind: :event,
          label: "Clean-up failed",
          name: :compaction_failed,
          outcome: :failure
        },
        %{
          description: "Shrinking a relay's saved state failed too often and was given up.",
          kind: :event,
          label: "Clean-up given up",
          name: :compaction_gave_up,
          outcome: :failure
        },
        %{
          description: "Shrinking a relay's saved state took too long and was stopped.",
          kind: :event,
          label: "Clean-up timed out",
          name: :compaction_timeout,
          outcome: :failure
        },
        %{
          description: "A relay's saved state grew enough to be shrunk.",
          kind: :event,
          label: "Clean-up started",
          name: :compaction_trigger,
          outcome: :normal
        },
        %{
          description:
            "A runner wrote more than the controller allowed it to: a fault in the runner, not load.",
          kind: :event,
          label: "Wrote past its allowance",
          name: :credit_anomaly,
          outcome: :failure
        },
        %{
          description: "A namespace's saved state grew past its disk quota.",
          kind: :event,
          label: "Over disk quota",
          name: :disk_quota_breach,
          outcome: :failure
        },
        %{
          description: "Pending writes were flushed to disk together.",
          kind: :event,
          label: "Flushed to disk",
          name: :fsync_tick,
          outcome: :normal,
          sentence: "Flushed {count:pending write} to disk"
        },
        %{
          description: "A relay received a message it already had and ignored the copy.",
          kind: :event,
          label: "Duplicate ignored",
          name: :inbox_duplicate,
          outcome: :normal
        },
        %{
          description: "A relay's record of the input it received stopped moving forward.",
          kind: :event,
          label: "Input stuck",
          name: :input_log_frozen_watermark,
          outcome: :failure
        },
        %{
          description: "The process that saves a relay's received input crashed.",
          kind: :event,
          label: "Input saver crashed",
          name: :input_log_syncer_died,
          outcome: :failure
        },
        %{
          description: "A relay has produced no output for longer than allowed.",
          kind: :event,
          label: "No output",
          name: :output_stall,
          outcome: :failure
        },
        %{
          description: "A damaged record at the end of a relay's saved log was refused.",
          kind: :event,
          label: "Damaged record refused",
          name: :record_frame_refused,
          outcome: :failure
        },
        %{
          description: "A half-written record at the end of a relay's saved log was dropped.",
          kind: :event,
          label: "Torn record dropped",
          name: :record_frame_repaired,
          outcome: :failure
        },
        %{
          description: "A relay's saved state could not be restored.",
          kind: :event,
          label: "Restore refused",
          name: :restore_refused,
          outcome: :failure
        },
        %{
          description: "A relay's saved state arrived with a gap in its sequence numbers.",
          kind: :event,
          label: "Gap in sequence",
          name: :seq_gap,
          outcome: :failure
        },
        %{
          description:
            "A runner that no longer owns a relay tried to write its state and was refused.",
          kind: :event,
          label: "Old writer refused",
          name: :stale_writer_refused,
          outcome: :failure
        },
        %{
          description:
            "A relay wrote faster than its saved state could keep up, so its writes waited.",
          kind: :metric,
          label: "Writes held back",
          name: :wal_backpressure,
          outcome: :failure
        },
        %{
          description: "Copying a relay's saved state from another node failed.",
          kind: :event,
          label: "Copy from node failed",
          name: :wal_pull_failed,
          outcome: :failure
        },
        %{
          description: "A relay's saved state moved its sequence number to match its runner's.",
          kind: :event,
          label: "Sequence realigned",
          name: :wal_realign,
          outcome: :normal
        },
        %{
          description: "A relay's saved state was rewound to an earlier point.",
          kind: :event,
          label: "Rewound",
          name: :wal_rewind,
          outcome: :normal
        }
      ],
      title: "Saved relay state"
    },
    %{
      area: :relays,
      description: "Turns of steps relays that failed and were set aside.",
      flavour: :declarative,
      kinds: [:metric],
      name: :flezha_steps,
      signals: [
        %{
          description:
            "A turn of a steps relay failed and became a dead letter; the relay carries on.",
          kind: :metric,
          label: "Turn set aside",
          name: :turns_dead_lettered_total,
          outcome: :failure
        }
      ],
      title: "Steps sessions"
    },
    %{
      area: :relays,
      description: "The streams that carry messages between runners and their relays.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_stream,
      signals: [
        %{
          description: "A message for a relay was dropped because the relay had no open stream.",
          kind: :metric,
          label: "Dropped",
          name: :drop,
          outcome: :failure,
          sentence: "A message for {relay_id} was dropped"
        },
        %{
          description: "A stream found more than one process serving one relay.",
          kind: :event,
          label: "Duplicate process",
          name: :fanout_warning,
          outcome: :failure
        }
      ],
      title: "Runner streams"
    },
    %{
      area: :plugins,
      description: "The websocket plugin: its connections, subscriptions and sends.",
      flavour: :declarative,
      kinds: [:event, :metric],
      name: :flezha_ws,
      signals: [
        %{
          description: "A websocket send failed and was given up.",
          kind: :event,
          label: "Send failed",
          name: :failed,
          outcome: :failure
        },
        %{
          description: "A websocket was silent for too long, so it was reconnected.",
          kind: :event,
          label: "Reconnected after silence",
          name: :idle_reconnect,
          outcome: :normal,
          sentence: "{plugin_id} reconnected after {silence_ms} ms of silence"
        },
        %{
          description: "A websocket's queue was full, so messages were dropped.",
          kind: :event,
          label: "Queue full",
          name: :overflowed,
          outcome: :failure
        },
        %{
          description: "A websocket compared its subscriptions with what its feed reports.",
          kind: :event,
          label: "Subscriptions checked",
          name: :reconcile,
          outcome: :normal,
          sentence: "{plugin_id} checked its subscriptions: {added} added, {removed} removed"
        },
        %{
          description: "A websocket sent its subscriptions again after its feed went quiet.",
          kind: :event,
          label: "Subscribed again",
          name: :resubscribe,
          outcome: :normal,
          sentence: "{plugin_id} subscribed again"
        },
        %{
          description: "A websocket send failed and will be tried again.",
          kind: :event,
          label: "Retrying",
          name: :retried,
          outcome: :normal
        },
        %{
          description: "Messages were sent over a websocket.",
          kind: :metric,
          label: "Sent",
          name: :shipped,
          outcome: :normal,
          sentence: "{plugin_id} sent {count:message}"
        }
      ],
      title: "Websocket plugin"
    }
  ],
  unexported: [
    [:flezha, :state, :iter_commit],
    [:flezha, :state, :task_commit],
    [:flezha, :convergence, :tick],
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
  ],
  unwritten: %{}
}

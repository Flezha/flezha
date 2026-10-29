# Telemetry and metrics

The controller describes what it does as signals: counts and measurements (metrics), and
single happenings such as a refusal or a restart (events). Each signal belongs to a source,
such as `flezha_call` for relay calls or `flezha_http_poll` for one plugin. The tables below
list every signal the controller's telemetry carries, grouped by area, with what each one
says.

**Contents**

1. [Where signals go](#where-signals-go)
2. [Series names](#series-names)
3. [Controller](#controller)
4. [Storage](#storage)
5. [Relays](#relays)
6. [Plugins](#plugins)

## Where signals go

`flz observe sources` lists every source the controllers of the cluster have registered, with
its signals and what each one says, and `flz observe tail <source>` shows its signals as they
happen, from every controller, each with the controller that recorded it. The
machine sources (`beam_vm`, `host`, `processhub`, `flezha_transport`) and the access-control,
audit-log and export sources are listed there too; they are not part of the tables below.

`FLEZHA_PROMETHEUS_ENABLED=true` serves the metrics on
`FLEZHA_PROMETHEUS_BIND`:`FLEZHA_PROMETHEUS_PORT` (default `127.0.0.1:9568`) at `/metrics`,
and `FLEZHA_OTLP_ENABLED=true` sends metrics and events to `FLEZHA_OTLP_ENDPOINT`.

## Series names

A metric's series is its source and its name joined by `_`: the `shipped` metric of the
`flezha_http_poll` source is `flezha_http_poll_shipped`. A plugin's metric is named by the
verb it counts (`shipped`, `retried`, `overflowed`), so the same verb reads the same in every
plugin. A plugin installed from a package reports under `flezha_<plugin name>`; its page lists
its own signals.
Compiling 34 files (.ex)

## Controller

<!-- bridge-signals:begin controller -->
| Source | Signal | Kind | Series | Meaning |
| --- | --- | --- | --- | --- |
| `flezha_boot` | `hub_start_failed` | event |  | A namespace's hub failed to start, so its relays are not running. |
| `flezha_boot` | `pre_recovery_replay` | event |  | A namespace began bringing its relays back after the controller started. |
| `flezha_boot` | `reconcile_duplicate` | event |  | A relay was found running on more than one node, and the extra copies were stopped. |
| `flezha_boot` | `recovery_complete` | event |  | A namespace finished bringing its relays back after the controller started. |
| `flezha_error` | `uncatalogued` | event |  | A caller got an error with no documented code: a defect to report. |
| `flezha_shutdown` | `complete` | event |  | The controller finished stopping gracefully. |
| `flezha_shutdown` | `drain_progress` | event |  | A namespace finished moving its relays to other nodes before the stop. |
| `flezha_shutdown` | `phase_completed` | event |  | A step of the shutdown finished. |
| `flezha_shutdown` | `phase_started` | event |  | A step of the shutdown began. |
| `flezha_shutdown` | `requested` | event |  | Someone asked the controller to stop gracefully. |
| `flezha_shutdown` | `timeout` | event |  | A step of the shutdown ran out of time. |
<!-- bridge-signals:end -->

## Storage

<!-- bridge-signals:begin storage -->
| Source | Signal | Kind | Series | Meaning |
| --- | --- | --- | --- | --- |
| `flezha_durability` | `artifact_missing` | event |  | Remote storage lacks a copy that a relay's saved state refers to. |
| `flezha_durability` | `artifact_shipped` | metric | `flezha_durability_artifact_shipped` | A copy of a relay's state was sent to remote storage. |
| `flezha_durability` | `claim_superseded` | event |  | Another node took over sending a relay's state to remote storage. |
| `flezha_durability` | `gc` | metric | `flezha_durability_gc` | Copies no longer needed were removed from remote storage. |
| `flezha_durability` | `hydrated` | event |  | A relay's state was restored from remote storage. |
| `flezha_durability` | `hydration_failed` | event |  | Restoring a relay's state from remote storage failed. |
| `flezha_durability` | `remote_commit` | metric | `flezha_durability_remote_commit` | Remote storage confirmed it holds a copy. |
| `flezha_durability` | `rpo_exceeded` | event |  | Remote copies fell further behind than the recovery point allows. |
| `flezha_durability` | `ship_failed` | event |  | Sending a copy of a relay's state to remote storage failed. |
| `flezha_state` | `baseline_seed` | event |  | A relay's saved state took its first sequence number. |
| `flezha_state` | `compaction_failed` | event |  | Shrinking a relay's saved state failed; it will be tried again. |
| `flezha_state` | `compaction_gave_up` | event |  | Shrinking a relay's saved state failed too often and was given up. |
| `flezha_state` | `compaction_timeout` | event |  | Shrinking a relay's saved state took too long and was stopped. |
| `flezha_state` | `compaction_trigger` | event |  | A relay's saved state grew enough to be shrunk, or a shrink it was too busy for is asked again. |
| `flezha_state` | `credit_anomaly` | event |  | A runner wrote more than the controller allowed it to: a fault in the runner, not load. |
| `flezha_state` | `disk_quota_breach` | event |  | A namespace's saved state grew past its disk quota. |
| `flezha_state` | `fsync_tick` | event |  | Pending writes were flushed to disk together. |
| `flezha_state` | `inbox_duplicate` | event |  | A relay received a message it already had and ignored the copy. |
| `flezha_state` | `input_log_frozen_watermark` | event |  | A relay's record of the input it received stopped moving forward. |
| `flezha_state` | `input_log_syncer_died` | event |  | The process that saves a relay's received input crashed. |
| `flezha_state` | `output_stall` | event |  | A relay has produced no output for longer than allowed. |
| `flezha_state` | `record_frame_refused` | event |  | A damaged record at the end of a relay's saved log was refused. |
| `flezha_state` | `record_frame_repaired` | event |  | A half-written record at the end of a relay's saved log was dropped. |
| `flezha_state` | `restore_refused` | event |  | A relay's saved state could not be restored. |
| `flezha_state` | `seq_gap` | event |  | A relay's saved state arrived with a gap in its sequence numbers. |
| `flezha_state` | `stale_writer_refused` | event |  | A runner that no longer owns a relay tried to write its state and was refused. |
| `flezha_state` | `wal_backpressure` | metric | `flezha_state_wal_backpressure` | A relay wrote faster than its saved state could keep up, so its writes waited. |
| `flezha_state` | `wal_pull_failed` | event |  | Copying a relay's saved state from another node failed. |
| `flezha_state` | `wal_realign` | event |  | A relay's saved state moved its sequence number to match its runner's. |
| `flezha_state` | `wal_rewind` | event |  | A relay's saved state was rewound to an earlier point. |
<!-- bridge-signals:end -->

## Relays

<!-- bridge-signals:begin relays -->
| Source | Signal | Kind | Series | Meaning |
| --- | --- | --- | --- | --- |
| `flezha_call` | `answer_duration` | metric | `flezha_call_answer_duration` | A call to a relay was answered; the details say how long it took. |
| `flezha_call` | `answers_dropped_total` | metric | `flezha_call_answers_dropped_total` | A relay answered a call that nobody was waiting for any more. |
| `flezha_call` | `recovered_unanswered_total` | metric | `flezha_call_recovered_unanswered_total` | A call picked up again after a restart was closed without an answer. |
| `flezha_call` | `refused_total` | metric | `flezha_call_refused_total` | A call to a relay was refused. |
| `flezha_call` | `timeouts_total` | metric | `flezha_call_timeouts_total` | A call to a relay got no answer in time. |
| `flezha_convergence` | `backoff_reset` | metric | `flezha_convergence_backoff_reset` | A success reset a retry's waiting time. |
| `flezha_convergence` | `backoff_wait` | metric | `flezha_convergence_backoff_wait` | A retry waited before its next attempt. |
| `flezha_convergence` | `intent_mismatch` | event |  | Saved state exists for relays nobody asked to run. |
| `flezha_correction` | `refused` | event |  | The controller refused to apply a correction to a relay. |
| `flezha_dlq` | `dropped_by_retention_total` | metric | `flezha_dlq_dropped_by_retention_total` | Dead letters older than the retention limit were deleted. |
| `flezha_dlq` | `parked_total` | metric | `flezha_dlq_parked_total` | A message failed and was set aside in a dead-letter queue. |
| `flezha_dlq` | `usage` | metric | `flezha_dlq_usage` | How many dead letters a queue holds, and how many bytes. |
| `flezha_health` | `connect_rejected` | event |  | A connection to the controller was rejected. |
| `flezha_health` | `evicted` | event |  | The controller removed a runner. |
| `flezha_health` | `fail_open` | event |  | A plugin's health check failed or timed out, so it is treated as healthy for now. |
| `flezha_health` | `failed` | event |  | A steps relay failed. |
| `flezha_health` | `heal` | event |  | The controller ran a repair step. |
| `flezha_health` | `input_silence` | event |  | A relay has received no input for longer than allowed. |
| `flezha_health` | `liveness_timeout` | event |  | A runner stopped answering the controller's keepalive. |
| `flezha_health` | `reconnected` | event |  | A runner reconnected and kept its relays. |
| `flezha_health` | `reconnected_reassigned` | event |  | A runner reconnected after its relays had already moved elsewhere. |
| `flezha_health` | `relay_stuck` | event |  | A relay is making no progress. |
| `flezha_health` | `restarted` | event |  | A runner restarted. |
| `flezha_health` | `safe_call` | event |  | A short call checked whether a relay answers. |
| `flezha_health` | `stall_localized` | event |  | A stalled message flow was traced to one connection, stream or relay. |
| `flezha_health` | `suspend_requested` | event |  | A steps relay's runner asked to pause it at the next safe point. |
| `flezha_health` | `suspended` | event |  | A steps relay paused at a safe point. |
| `flezha_health` | `unavailable` | event |  | A namespace's hub was down when something asked for it. |
| `flezha_health` | `verdict` | event |  | The health of a relay, runner or plugin changed. |
| `flezha_relay` | `halted_total` | metric | `flezha_relay_halted_total` | A relay was halted after a failure and stays stopped until someone acts. |
| `flezha_relay` | `relay_started` | event |  | A relay was started. |
| `flezha_relay` | `relay_stopped` | event |  | A relay was stopped or finished its work. |
| `flezha_runner` | `admin_request` | event |  | A runner sent an administration request. |
| `flezha_runner` | `factory_language_conflict` | event |  | A runner offered a factory that another runner already serves in another language. |
| `flezha_runner` | `factory_run_conflict` | event |  | A runner offered a factory that conflicts with one already running. |
| `flezha_runner` | `foreign_frame_refused` | event |  | A runner sent a message for a relay it does not own, and it was refused. |
| `flezha_runner` | `quota_refused` | event |  | A runner was turned away because its namespace has as many runners as allowed. |
| `flezha_runner` | `runner_connected` | event |  | A runner opened its connection to the controller. |
| `flezha_runner` | `runner_disconnected` | event |  | A runner closed its connection. |
| `flezha_runner` | `runner_drop_refused` | event |  | A runner an operator dropped tried to come back and was turned away. |
| `flezha_runner` | `runner_dropped` | event |  | An operator dropped a runner, so it cannot reconnect until it is let back in. |
| `flezha_runner` | `runner_readmitted` | event |  | An operator let a dropped runner connect again. |
| `flezha_schedule` | `spawn_failed` | event |  | A schedule came due, but its relay failed to start. |
| `flezha_steps` | `turns_dead_lettered_total` | metric | `flezha_steps_turns_dead_lettered_total` | A turn of a steps relay failed and became a dead letter; the relay carries on. |
| `flezha_stream` | `channel_down` | event |  | A runner stream's channel process died; its replacement asks the runner for new streams. |
| `flezha_stream` | `drop` | metric | `flezha_stream_drop` | A message for a relay was dropped because the relay had no open stream. |
| `flezha_stream` | `fanout_warning` | event |  | A stream found more than one process serving one relay. |
<!-- bridge-signals:end -->

## Plugins

<!-- bridge-signals:begin plugins -->
| Source | Signal | Kind | Series | Meaning |
| --- | --- | --- | --- | --- |
| `flezha_file_sink` | `failed` | event |  | A delivery or a read failed. |
| `flezha_file_sink` | `refused` | event |  | A record was refused because it cannot be written as one line. |
| `flezha_file_sink` | `shipped` | metric | `flezha_file_sink_shipped` | The plugin passed items on, into its relay or out to where it sends them. |
| `flezha_file_source` | `failed` | event |  | A delivery or a read failed. |
| `flezha_file_source` | `shipped` | metric | `flezha_file_source_shipped` | The plugin passed items on, into its relay or out to where it sends them. |
| `flezha_file_source` | `truncated` | event |  | The file was cut short or replaced, so it is read again from the start. |
| `flezha_hook_webhook` | `failed` | event |  | A delivery or a read failed. |
| `flezha_hook_webhook` | `overflowed` | metric | `flezha_hook_webhook_overflowed` | The plugin's queue was full, so items were dropped. |
| `flezha_hook_webhook` | `retried` | metric | `flezha_hook_webhook_retried` | A delivery failed and will be tried again; nothing is lost yet. |
| `flezha_hook_webhook` | `shipped` | metric | `flezha_hook_webhook_shipped` | The plugin passed items on, into its relay or out to where it sends them. |
| `flezha_hooks` | `on_dead_letter` | event |  | A message became a dead letter, and controller plugins were told. |
| `flezha_http_poll` | `failed` | event |  | A delivery or a read failed. |
| `flezha_http_poll` | `shipped` | metric | `flezha_http_poll_shipped` | The plugin passed items on, into its relay or out to where it sends them. |
| `flezha_http_poll` | `unchanged` | metric | `flezha_http_poll_unchanged` | A poll's response matched the last one, so it was skipped. |
| `flezha_http_push` | `failed` | event |  | A delivery or a read failed. |
| `flezha_http_push` | `overflowed` | metric | `flezha_http_push_overflowed` | The plugin's queue was full, so items were dropped. |
| `flezha_http_push` | `retried` | metric | `flezha_http_push_retried` | A delivery failed and will be tried again; nothing is lost yet. |
| `flezha_http_push` | `shipped` | metric | `flezha_http_push_shipped` | The plugin passed items on, into its relay or out to where it sends them. |
| `flezha_plugin` | `phase` | event |  | A plugin moved from one phase of its life to another, such as from prepared to running. |
| `flezha_plugin_packages` | `loaded` | event |  | A plugin package was loaded when the controller started. |
| `flezha_plugin_packages` | `refused` | event |  | A plugin package was refused when the controller started, so its plugins are missing. |
| `flezha_postgres_poll` | `caught_up` | event |  | The poll reached the newest row in the table. |
| `flezha_postgres_poll` | `failed` | event |  | A delivery or a read failed. |
| `flezha_postgres_poll` | `shipped` | metric | `flezha_postgres_poll_shipped` | The plugin passed items on, into its relay or out to where it sends them. |
| `flezha_relay_forward` | `queue_depth` | metric | `flezha_relay_forward_queue_depth` | How many messages are waiting to be forwarded. |
| `flezha_relay_forward` | `retried` | metric | `flezha_relay_forward_retried` | A delivery failed and will be tried again; nothing is lost yet. |
| `flezha_relay_forward` | `saturated` | event |  | The forward queue was full and turned messages away. |
| `flezha_relay_forward` | `shipped` | metric | `flezha_relay_forward_shipped` | The plugin passed items on, into its relay or out to where it sends them. |
| `flezha_ws` | `failed` | event |  | A websocket send failed and was given up. |
| `flezha_ws` | `idle_reconnect` | event |  | A websocket was silent for too long, so it was reconnected. |
| `flezha_ws` | `overflowed` | event |  | A websocket's queue was full, so messages were dropped. |
| `flezha_ws` | `reconcile` | event |  | A websocket compared its subscriptions with what its feed reports. |
| `flezha_ws` | `resubscribe` | event |  | A websocket sent its subscriptions again after its feed went quiet. |
| `flezha_ws` | `retried` | event |  | A websocket send failed and will be tried again. |
| `flezha_ws` | `shipped` | metric | `flezha_ws_shipped` | Messages were sent over a websocket. |
<!-- bridge-signals:end -->

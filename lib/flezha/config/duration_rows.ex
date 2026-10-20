defmodule Flezha.Config.DurationRows do
  @moduledoc """
  The duration rows, as data.

  Every timeout, interval, grace and budget the controller and the Elixir SDK
  wait on, in the shape `Flezha.Config.Keys` merges into the one table. They sit
  in a module of their own so the table can read them without depending on
  `Flezha.Config.Durations`, which reads the table back: a declaration names
  nobody, which is what keeps the direction a line rather than a ring.

  Names are folded by NAME, never by value: eighteen rows that happen to be
  `5_000` are eighteen rows, because a probe bound and a call bound answer
  different questions and move apart the moment either is tuned. A row with a
  `:derived` carries no default of its own, so the relation cannot drift.
  `key: true` means the application-env key is the row's own name; a variable is
  written out, because it is a published name and a grep is what holds the
  documentation to it.
  """

  @declared [
    #
    # Milliseconds, and the derivations that keep a relation from drifting: a
    # silence window that is `k` × its heartbeat, a budget that sits inside
    # another by a fixed headroom. Names are folded by NAME, never by value:
    # eighteen rows that happen to be `5_000` are eighteen rows, because a probe
    # bound and a call bound answer different questions and move apart the
    # moment either is tuned.
    # ---- cluster ----------------------------------------------------------
    {:cluster_formation_timeout_ms, :ms, 15_000,
     key: true, env: "FLEZHA_CLUSTER_FORMATION_TIMEOUT_MS"},
    {:cluster_formation_wait_ms, :ms, nil,
     derived: {:headroom, :cluster_formation_timeout_ms, 5_000}},
    {:cluster_ready_poll_ms, :ms, 50, []},
    {:k8s_discovery_request_ms, :ms, 15_000, []},
    # One authorization write or snapshot exchanged with another controller.
    {:authz_replica_call_ms, :ms, 5_000, []},
    {:reconnect_max_backoff_ms, :ms, 30_000,
     key: true, env: "FLEZHA_RECONNECT_MAX_BACKOFF_MS", runtimes: [:controller, :rust]},

    # ---- control plane: liveness, calls, drains ----------------------------
    {:control_liveness_interval_ms, :ms, 15_000,
     key: true, env: "FLEZHA_CONTROL_LIVENESS_INTERVAL_MS"},
    # The window MUST stay comfortably above the SDK keepalive cadence: the
    # long-lived test runners re-arm `last_seen` only through their keepalive
    # ping, so a small window would falsely evict a healthy one.
    {:control_liveness_window_ms, :ms, nil,
     key: true,
     env: "FLEZHA_CONTROL_LIVENESS_WINDOW_MS",
     derived: {:factor, :control_liveness_interval_ms, 3}},
    {:control_call_timeout_ms, :ms, 5_000, []},
    {:listen_port_call_ms, :ms, 1_000, []},
    {:transport_stats_call_ms, :ms, 1_000, []},
    {:stream_pool_bind_call_ms, :ms, 5_000, []},
    {:control_unit_reply_timeout_ms, :ms, 5_000, []},
    {:control_unit_reply_sweep_interval_ms, :ms, 15_000, key: true},
    {:control_unit_pending_reply_timeout_ms, :ms, 90_000, key: true},
    {:credit_handshake_timeout_ms, :ms, 10_000, []},
    {:drain_owed_ms, :ms, 60_000, []},
    {:drain_owed_poll_ms, :ms, 50, []},
    {:io_stream_liveness_interval_ms, :ms, 10_000, key: true},
    {:io_stream_liveness_threshold_ms, :ms, nil,
     key: true, derived: {:factor, :io_stream_liveness_interval_ms, 3}},
    {:send_gate_hold_ms, :ms, 5_000, key: true},
    {:stream_pool_budget_ms, :ms, 1_000, []},
    {:stream_pool_flush_budget_ms, :ms, 5_000, []},

    # ---- runners: lookup, marks, replay ------------------------------------
    {:boot_replay_timeout_ms, :ms, 60_000, key: true, env: "FLEZHA_BOOT_REPLAY_TIMEOUT_MS"},
    # How long a caller waits for the boot sequence to finish, so a wedged
    # hook is a named exit rather than a hang.
    {:boot_sequence_await_ms, :ms, 30_000, []},
    {:boot_await_poll_ms, :ms, 200, []},
    # How long a controller-side `flz` verb waits for the caller's node to
    # name its `:standard_error`, before it writes to the controller's own.
    # The caller has just called us, so this is a liveness bound, not a wait.
    {:cli_caller_device_ms, :ms, 5_000, []},
    # How long a streaming verb runs when its request names no window.
    {:cli_stream_window_ms, :ms, 30_000, []},
    {:runner_lookup_ms, :ms, 5_000, []},
    {:runner_evict_call_ms, :ms, 5_000, []},
    {:drop_marks_call_ms, :ms, 5_000, []},
    {:runner_cleanup_slack_ms, :ms, 10_000, []},
    {:runner_min_call_ms, :ms, 25, []},
    {:runner_poll_call_ms, :ms, 250, []},
    {:runner_poll_interval_ms, :ms, 50, []},
    # How long the per-runner relay count a placement reads may go without
    # being rebuilt from the hub registry. Its own row rather than a borrowed
    # one: a cache's staleness bound and a lookup's timeout answer different
    # questions and move apart the moment either is tuned.
    {:placement_load_refresh_ms, :ms, 1_000, key: true},
    {:runner_retiring_mark_ttl_ms, :ms, 30_000,
     key: true, env: "FLEZHA_RUNNER_RETIRING_MARK_TTL_MS"},
    # How long this controller keeps declaring a factory to the namespace hub
    # after its last runner for it goes. A runner that comes back inside the
    # grace moves no relay at all; past it, the hub reassigns them to a
    # controller that can still run the factory. A disconnect the controller
    # asked for skips the grace. Measured on this box, kill to placeable again:
    # 125-185 ms for a Rust runner, 475-573 ms for a Java one, so five seconds
    # covers a restart an order of magnitude over the slowest of them while
    # keeping a genuine loss degraded for no longer than that.
    {:group_withdraw_grace_ms, :ms, 5_000, key: true, env: "FLEZHA_GROUP_WITHDRAW_GRACE_MS"},
    {:superseded_mark_ttl_ms, :ms, 600_000, []},

    # ---- relays: the calls the loop and the manager make ----------------
    {:hub_await_recovery_ms, :ms, 60_000, []},
    {:hub_alive_poll_ms, :ms, 10, []},
    {:hub_alive_wait_ms, :ms, 5_000, []},
    # A relay's start on its runner: a cold SDK (a JVM's warmup and the AddRelay
    # round trip) takes seconds, and a genuine stall still ends.
    {:relay_start_call_ms, :ms, 30_000, []},
    {:replay_call_ms, :ms, 60_000, []},
    {:rewind_call_ms, :ms, 30_000, []},
    {:hub_recovery_poll_ms, :ms, 50, []},
    # The hub kills a request at this bound, a relay stop's included, so it
    # outwaits the Overwatch's shutdown bound.
    {:hub_cross_node_request_ms, :ms, nil, derived: {:headroom, :overwatch_shutdown_ms, 1_000}},
    # The declared-set rewrite a plugin mutation issues, and the same bound plus
    # headroom for the hop when the leader is another node: the remote call must
    # outlive the call it carries, or it reports a timeout the leader did not
    # have.
    {:hub_declared_mutate_ms, :ms, 5_000, []},
    {:hub_declared_mutate_rpc_ms, :ms, nil, derived: {:headroom, :hub_declared_mutate_ms, 500}},
    {:hub_start_child_ms, :ms, 15_000, key: true},
    {:migrate_timeout_ms, :ms, 2_000, []},
    {:migration_consent_timeout_ms, :ms, 5_000,
     key: {:migration_consent, :consent_timeout_ms}, env: "FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS"},
    {:migration_retry_interval_ms, :ms, 2_000,
     key: {:migration_consent, :retry_interval_ms}, env: "FLEZHA_MIGRATION_RETRY_INTERVAL_MS"},
    {:relay_call_timeout_ms, :ms, 1_000, []},
    # What an outside caller waits past its call's deadline, so the callee's
    # own `call.timeout` always reaches it first.
    {:call_grace_ms, :ms, 1_000, key: true},
    # The deadline of a `Relay.call_channel` whose caller names none.
    {:call_channel_deadline_ms, :ms, 30_000, key: true},
    # How long an input log keeps a record carrying a call past the call's
    # deadline, so a request sent again is absorbed and a caller's recovery
    # still finds its `Response`.
    {:call_retention_margin_ms, :ms, 300_000, key: true, env: "FLEZHA_CALL_RETENTION_MARGIN_MS"},
    # How long a relay call's delivery worker waits with an empty queue
    # before it stops.
    {:call_worker_idle_ms, :ms, 30_000, key: true},
    {:overwatch_call_timeout_ms, :ms, 5_000, key: true},
    # A relay's activity read waits on its runner inside the caller's own
    # wait on the Overwatch, so the caller sees the runner's silence named.
    {:tasklet_activity_read_ms, :ms, nil,
     derived: {:headroom, :overwatch_call_timeout_ms, -1_000}},
    {:overwatch_ping_timeout_ms, :ms, 2_000, key: true, env: "FLEZHA_OVERWATCH_PING_TIMEOUT_MS"},
    {:overwatch_task_warn_ms, :ms, 2_000, key: true},
    {:overwatch_runner_wait_ms, :ms, 30_000, key: true},
    {:partition_peer_call_ms, :ms, 30_000, []},
    {:partition_quiesce_poll_ms, :ms, 250, []},
    {:partition_quiesce_timeout_ms, :ms, 30_000, []},
    {:process_hub_cluster_settle_ms, :ms, 2_000,
     key: true, env: "FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS"},
    {:process_hub_reconcile_grace_ms, :ms, 45_000,
     key: true, env: "FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS"},
    {:process_hub_reconcile_interval_ms, :ms, 30_000,
     key: true, env: "FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS"},
    {:route_call_timeout_ms, :ms, 30_000, []},
    {:stream_bind_wait_ms, :ms, 30_000, key: true},

    # ---- convergence -------------------------------------------------------
    {:convergence_cell_stall_ms, :ms, 300_000,
     key: true, env: "FLEZHA_CONVERGENCE_CELL_STALL_MS", min: 1},
    {:convergence_output_stall_grace_ms, :ms, 60_000,
     key: true, env: "FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS"},
    {:convergence_tick_ms, :ms, 5_000, key: true, env: "FLEZHA_CONVERGENCE_TICK_MS"},
    {:convergence_tick_call_ms, :ms, 30_000, []},
    {:convergence_unmet_intent_grace_ms, :ms, 120_000,
     key: true, env: "FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS"},
    {:plugin_stable_uptime_ms, :ms, 60_000, key: true},
    # How long a shared plugin whose last relay went down or moved away
    # waits for one to come back before it stops.
    {:plugin_idle_stop_ms, :ms, 60_000, key: true, env: "FLEZHA_PLUGIN_IDLE_STOP_MS"},
    # How often a retiring copy of a `single` plugin asks its successor whether
    # its upstream is open.
    {:plugin_successor_poll_ms, :ms, 50, key: true},

    # ---- plugins -----------------------------------------------------------
    # Headroom above the Overwatch call for provider health probes: each
    # plugin's probe is bounded on its own, and a relay's plugins are
    # probed in series, so a few wedged ones still fit.
    {:health_probe_budget_ms, :ms, nil, derived: {:factor, :provider_probe_timeout_ms, 3}},
    {:http_listener_idle_ms, :ms, 5_000, []},
    {:http_poll_min_interval_ms, :ms, 1_000, []},
    {:plugin_call_timeout_ms, :ms, 2_000, []},
    # A call into a provider's own worker: its counters, a hand-over, a stop.
    {:plugin_worker_call_ms, :ms, 5_000, []},
    # Whether a running worker may be handed to a new user, asked of the worker.
    {:worker_reuse_probe_ms, :ms, 1_000, []},
    # A call that opens, swaps or replaces a plugin's shared broker client,
    # which waits on the broker.
    {:plugin_client_call_ms, :ms, 30_000, []},
    # A record handed to a producing worker that answers once it is delivered.
    {:plugin_enqueue_call_ms, :ms, 35_000, []},
    # Strictly inside `route_call_timeout_ms`, which bounds a plugin verb run
    # on another controller, and strictly outside `plugin_call_timeout_ms`: an
    # inner bound equal to the outer one is no bound at all — both give up at
    # the same instant.
    {:plugin_disable_fanout_ms, :ms, 3_000, []},
    # One plugin package download, whatever its redirects.
    {:plugin_fetch_ms, :ms, 60_000, []},
    # How long an enable waits for the namespace's other controllers to say
    # whether they hold the provider.
    {:plugin_peer_check_ms, :ms, 2_000, []},
    # How long a listener's receiver waits after its plugin was released before
    # it asks the registry what the release was, and between asks: a move or a
    # restart brings the plugin's worker back well inside it.
    {:listener_release_check_ms, :ms, 5_000, key: true},
    {:provider_probe_timeout_ms, :ms, 500, []},
    {:ws_idle_timeout_ms, :ms, 30_000, []},
    {:ws_min_tick_interval_ms, :ms, 1_000, []},
    {:ws_resubscribe_after_ms, :ms, 0, []},
    {:ws_sink_handoff_timeout_ms, :ms, 5_000, []},
    {:ws_subscription_interval_ms, :ms, 30_000, []},
    {:ws_subscription_min_interval_ms, :ms, 1_000, []},
    {:ws_subscription_timeout_ms, :ms, 10_000, []},
    # One S3 request, for the durability tier and the file plugins alike.
    {:s3_recv_ms, :ms, 30_000, []},

    # ---- schedules ---------------------------------------------------------
    {:schedule_misfire_grace_ms, :ms, 120_000,
     key: {:schedules, :misfire_grace_ms}, env: "FLEZHA_SCHEDULE_MISFIRE_GRACE_MS"},
    {:schedule_rearm_chunk_ms, :ms, 86_400_000, []},
    {:schedule_reassert_interval_ms, :ms, 60_000, []},
    {:schedule_retract_timeout_ms, :ms, 5_000, []},
    {:schedule_status_call_ms, :ms, 1_000, []},
    {:schedule_store_call_ms, :ms, 5_000, []},

    # ---- state: logs, watchdogs, retention ---------------------------------
    {:agent_fsync_interval_ms, :ms, 5, key: true},
    {:agent_fsync_max_interval_ms, :ms, 1_000, []},
    {:agent_fsync_min_interval_ms, :ms, 1, []},
    {:input_silence_timeout_ms, :ms, 0, key: true, env: "FLEZHA_INPUT_SILENCE_TIMEOUT_MS"},
    {:compact_after_age_ms, :ms, 300_000,
     key: true, env: "FLEZHA_COMPACT_AFTER_AGE_MS", env_default: [test: 0]},
    {:compaction_timeout_ms, :ms, 30_000, key: true},
    {:correction_debounce_ms, :ms, 200, key: true},
    {:dlq_replay_grace_ms, :ms, 5_000, key: {:dlq, :replay_grace_ms}},
    {:dlq_replay_poll_ms, :ms, 100, []},
    {:dlq_sweep_interval_ms, :ms, 3_600_000, key: {:dlq, :retention_check_interval_ms}},
    {:dlq_store_call_ms, :ms, 30_000, []},
    {:dlq_sweep_call_ms, :ms, 60_000, []},
    {:state_sweep_call_ms, :ms, 5_000, []},
    {:input_log_hold_window_ms, :ms, 0, key: true, env: "FLEZHA_INPUT_LOG_HOLD_WINDOW_MS"},
    {:outbox_resettle_ms, :ms, 60_000, key: true},
    # The user key keeps the `default_` prefix it was published under; the
    # row's name is the quantity.
    {:output_stall_timeout_ms, :ms, 60_000, key: :default_output_stall_timeout_ms},
    {:output_watchdog_interval_ms, :ms, 10_000,
     key: true, env: "FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS"},
    {:restore_await_timeout_ms, :ms, 15_000, key: true},
    {:time_travel_retention_check_interval_ms, :ms, 60_000,
     key: true, env: "FLEZHA_RETENTION_CHECK_INTERVAL_MS"},
    {:input_log_call_ms, :ms, 5_000, []},
    {:wal_transfer_call_ms, :ms, 30_000, []},
    {:wal_writer_call_ms, :ms, 5_000, []},
    # A drain syncs each dirty writer through its own bounded call.
    {:fsync_batcher_call_ms, :ms, nil, derived: {:headroom, :wal_writer_call_ms, 1_000}},
    {:outbox_retry_ms, :ms, 1_000, []},

    # ---- durability tier ---------------------------------------------------
    # An unreferenced artifact no node holds local state for (a stopped relay,
    # one whose state moved) is collected at this age: far past the grace, where
    # no ship can still be in flight.
    {:durability_gc_abandoned_after_ms, :ms, 604_800_000,
     key: {:durability_tier, :gc_abandoned_after_ms},
     env: "FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS",
     min: 1},
    # No unreferenced artifact younger than this is collected, whoever owns it:
    # an upload in flight is young, so a peer mid-ship needs no coordination.
    {:durability_gc_grace_ms, :ms, 3_600_000,
     key: {:durability_tier, :gc_grace_ms}, env: "FLEZHA_DURABILITY_GC_GRACE_MS", min: 1},
    {:durability_gc_interval_ms, :ms, 3_600_000,
     key: {:durability_tier, :gc_interval_ms}, env: "FLEZHA_DURABILITY_GC_INTERVAL_MS", min: 1},
    {:durability_pg_query_ms, :ms, 30_000, []},
    # Time since a reference's last successful ship at which the RPO alarm fires.
    {:durability_rpo_target_ms, :ms, 120_000,
     key: {:durability_tier, :rpo_target_ms}, env: "FLEZHA_DURABILITY_RPO_TARGET_MS", min: 1},
    {:durability_ship_call_ms, :ms, nil, derived: {:headroom, :sdk_commit_ack_ms, -5_000}},
    # The backstop sweep: how stale a quiet cell's remote copy may get.
    {:durability_ship_interval_ms, :ms, 30_000,
     key: {:durability_tier, :ship_interval_ms}, env: "FLEZHA_DURABILITY_SHIP_INTERVAL_MS", min: 1},

    # ---- shutdown ----------------------------------------------------------
    {:drain_budget_slack_ms, :ms, 1_000, []},
    # How long a stopping controller waits for each running durable task.
    {:durable_task_shutdown_ms, :ms, 5_000, []},
    {:forced_handover_grace_ms, :ms, 2_000, []},
    {:graceful_shutdown_timeout_ms, :ms, 60_000,
     key: true, env: "FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS"},
    {:orchestrator_grace_ms, :ms, nil,
     runtimes: [:shell], derived: {:headroom, :graceful_shutdown_timeout_ms, 15_000}},
    {:shutdown_finalise_call_ms, :ms, 5_000, []},
    {:signal_handler_status_call_ms, :ms, 5_000, []},
    # A call to the coordinator, whose status reads the signal handler's inside it.
    {:shutdown_coordinator_call_ms, :ms, nil,
     derived: {:headroom, :signal_handler_status_call_ms, 1_000}},
    {:stop_release_timeout_ms, :ms, 15_000, []},
    # Nested 1 s apart: a relay stop's hub stop, after its flush of at most 1 s,
    # outwaits the Overwatch's shutdown bound, which outwaits its teardown.
    {:overwatch_shutdown_ms, :ms, nil, derived: {:headroom, :stop_release_timeout_ms, -2_000}},
    {:stop_process_budget_ms, :ms, nil, derived: {:headroom, :stop_release_timeout_ms, -3_000}},

    # ---- authentication and authorization ----------------------------------
    {:authz_store_call_ms, :ms, 5_000, []},
    # How far ahead of an expiry a token is announced as expiring, and how often
    # the registry looks. A lapse is then a monitoring alert rather than the
    # moment a user discovers they cannot issue another credential.
    {:token_expiry_warning_ms, :ms, 86_400_000,
     key: {:authn_authz, :token_expiry_warning_ms}, env: "FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS"},
    {:token_expiry_check_interval_ms, :ms, 300_000,
     key: {:authn_authz, :token_expiry_check_interval_ms}},

    # ---- observability -----------------------------------------------------
    {:audit_log_block_timeout_ms, :ms, 30_000, []},
    {:audit_log_writer_call_ms, :ms, 5_000, []},
    {:audit_log_write_ms, :ms, 30_000, []},
    {:audit_log_rotate_ms, :ms, 10_000, []},
    # A rotation on request waits on the writer's own rotation.
    {:audit_log_rotate_call_ms, :ms, nil, derived: {:headroom, :audit_log_rotate_ms, 1_000}},
    {:audit_log_sweep_call_ms, :ms, 30_000, []},
    {:observability_registry_call_ms, :ms, 5_000, []},
    {:observability_sweep_interval_ms, :ms, 5_000,
     key: {:observability, :retention_check_interval_ms}},
    {:otlp_request_timeout_ms, :ms, 10_000, []},
    # A post waits for every attempt of its backoff schedule.
    {:otlp_post_call_ms, :ms, 60_000, []},
    {:port_counter_reset_ms, :ms, 30_000, []},

    # ---- the Elixir SDK ----------------------------------------------------
    # The SDK declares these itself (`Flezha.SDK.Knob`); a row here is what its
    # values are held to.
    {:control_keepalive_ms, :ms, 15_000,
     env: "FLEZHA_CONTROL_KEEPALIVE_MS", runtimes: [:controller, :rust]},
    {:data_heartbeat_ms, :ms, 10_000,
     env: "FLEZHA_DATA_HEARTBEAT_MS", runtimes: [:controller, :rust]},
    {:sdk_commit_ack_ms, :ms, 30_000,
     key: true, env: "FLEZHA_STEPS_ACK_TIMEOUT_MS", runtimes: [:controller, :rust]},
    # A call into one of the runner's own processes: the runner or a local run.
    # None of them runs author code on the answering clause, so a wait here is
    # a wedged process rather than a busy one.
    {:sdk_control_call_ms, :ms, 5_000, runtimes: [:elixir_sdk]},
    # How long a tasklet's spawn request waits for the controller's answer.
    {:sdk_spawn_reply_ms, :ms, 30_000, runtimes: [:elixir_sdk]}
  ]

  @doc "The duration rows, in the table's own declaration shape."
  @spec declared() :: [tuple()]
  def declared(), do: @declared
end

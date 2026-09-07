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
    {:k8s_discovery_request_ms, :ms, 15_000, []},
    {:reconnect_max_backoff_ms, :ms, 30_000, key: true, env: "FLEZHA_RECONNECT_MAX_BACKOFF_MS"},

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
    {:local_stream_call_ms, :ms, 5_000, []},
    {:stream_pool_bind_call_ms, :ms, 5_000, []},
    {:control_unit_reply_timeout_ms, :ms, 5_000, []},
    {:control_unit_heartbeat_interval_ms, :ms, 15_000, key: true},
    {:control_unit_pending_reply_timeout_ms, :ms, 90_000, key: true},
    {:credit_handshake_timeout_ms, :ms, 10_000, []},
    {:drain_owed_ms, :ms, 60_000, []},
    {:io_stream_liveness_interval_ms, :ms, 10_000, key: true},
    {:io_stream_liveness_threshold_ms, :ms, nil,
     key: true, derived: {:factor, :io_stream_liveness_interval_ms, 3}},
    {:send_gate_hold_ms, :ms, 5_000, key: true},
    {:stream_pool_budget_ms, :ms, 1_000, []},
    {:stream_pool_flush_budget_ms, :ms, 5_000, []},

    # ---- runners: lookup, marks, replay ------------------------------------
    {:boot_replay_timeout_ms, :ms, 60_000, key: true, env: "FLEZHA_BOOT_REPLAY_TIMEOUT_MS"},
    {:boot_await_poll_ms, :ms, 200, []},
    # How long a controller-side `flz` verb waits for the caller's node to
    # name its `:standard_error`, before it writes to the controller's own.
    # The caller has just called us, so this is a liveness bound, not a wait.
    {:cli_operator_device_ms, :ms, 5_000, []},
    {:runner_lookup_ms, :ms, 5_000, []},
    {:runner_cleanup_slack_ms, :ms, 10_000, []},
    {:runner_min_call_ms, :ms, 25, []},
    {:runner_poll_call_ms, :ms, 250, []},
    {:runner_poll_interval_ms, :ms, 50, []},
    {:runner_retiring_mark_ttl_ms, :ms, 30_000,
     key: true, env: "FLEZHA_RUNNER_RETIRING_MARK_TTL_MS"},
    {:superseded_mark_ttl_ms, :ms, 600_000, []},

    # ---- operatives: the calls the loop and the manager make ----------------
    {:hub_await_recovery_ms, :ms, 60_000, []},
    {:hub_cross_node_request_ms, :ms, 10_000, []},
    {:hub_runner_assign_wait_ms, :ms, 2_000, key: true},
    {:hub_start_child_ms, :ms, 15_000, []},
    {:migrate_timeout_ms, :ms, 2_000, []},
    {:migration_consent_timeout_ms, :ms, 5_000,
     key: {:migration_consent, :consent_timeout_ms}, env: "FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS"},
    {:migration_retry_interval_ms, :ms, 2_000,
     key: {:migration_consent, :retry_interval_ms}, env: "FLEZHA_MIGRATION_RETRY_INTERVAL_MS"},
    {:operative_call_timeout_ms, :ms, 1_000, []},
    {:overwatch_call_timeout_ms, :ms, 5_000, key: true},
    {:overwatch_ping_timeout_ms, :ms, 2_000, key: true, env: "FLEZHA_OVERWATCH_PING_TIMEOUT_MS"},
    {:overwatch_proc_warn_ms, :ms, 2_000, key: true},
    {:overwatch_runner_wait_ms, :ms, 30_000, key: true},
    {:partition_peer_call_ms, :ms, 30_000, []},
    {:partition_quiesce_poll_ms, :ms, 250, []},
    {:partition_quiesce_timeout_ms, :ms, 30_000, []},
    {:process_hub_reconcile_grace_ms, :ms, 45_000,
     key: true, env: "FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS"},
    {:process_hub_reconcile_interval_ms, :ms, 30_000,
     key: true, env: "FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS"},
    {:route_call_timeout_ms, :ms, 30_000, []},
    {:stream_bind_wait_ms, :ms, 30_000, key: true},

    # ---- convergence -------------------------------------------------------
    {:convergence_entity_stall_ms, :ms, 300_000,
     key: true, env: "FLEZHA_CONVERGENCE_ENTITY_STALL_MS"},
    {:convergence_output_stall_grace_ms, :ms, 60_000,
     key: true, env: "FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS"},
    {:convergence_tick_ms, :ms, 5_000, key: true, env: "FLEZHA_CONVERGENCE_TICK_MS"},
    {:convergence_unmet_intent_grace_ms, :ms, 120_000,
     key: true, env: "FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS"},
    {:plugin_stable_uptime_ms, :ms, 60_000, []},

    # ---- plugins -----------------------------------------------------------
    # Headroom above the Overwatch call for provider health probes: each
    # plugin's probe is bounded on its own, and an operative's plugins are
    # probed in series, so a few wedged ones still fit.
    {:health_probe_budget_ms, :ms, nil, derived: {:factor, :provider_probe_timeout_ms, 3}},
    {:http_listener_idle_ms, :ms, 5_000, []},
    {:http_poll_min_interval_ms, :ms, 1_000, []},
    {:installer_auto_save_ms, :ms, 5_000, []},
    {:kafka_linger_ms, :ms, 5_000, []},
    {:kafka_worker_call_ms, :ms, 5_000, []},
    {:plugin_call_timeout_ms, :ms, 2_000, []},
    # Strictly inside `plugin_service_call_ms` and strictly outside
    # `plugin_call_timeout_ms`: an inner bound equal to the outer one is no
    # bound at all — both give up at the same instant.
    {:plugin_disable_fanout_ms, :ms, 3_000, []},
    {:plugin_service_call_ms, :ms, 5_000, []},
    {:provider_probe_timeout_ms, :ms, 500, []},
    {:ws_idle_timeout_ms, :ms, 30_000, []},
    {:ws_min_tick_interval_ms, :ms, 1_000, []},
    {:ws_resubscribe_after_ms, :ms, 0, []},
    {:ws_sink_handoff_timeout_ms, :ms, 5_000, []},
    {:ws_subscription_interval_ms, :ms, 30_000, []},
    {:ws_subscription_min_interval_ms, :ms, 1_000, []},
    {:ws_subscription_timeout_ms, :ms, 10_000, []},

    # ---- schedules ---------------------------------------------------------
    {:schedule_misfire_grace_ms, :ms, 120_000,
     key: {:schedules, :misfire_grace_ms}, env: "FLEZHA_SCHEDULE_MISFIRE_GRACE_MS"},
    {:schedule_rearm_chunk_ms, :ms, 86_400_000, []},
    {:schedule_reassert_interval_ms, :ms, 60_000, []},
    {:schedule_retract_timeout_ms, :ms, 5_000, []},
    {:schedule_status_call_ms, :ms, 1_000, []},

    # ---- state: logs, watchdogs, retention ---------------------------------
    {:agent_fsync_interval_ms, :ms, 5, key: true},
    {:agent_fsync_max_interval_ms, :ms, 1_000, []},
    {:agent_fsync_min_interval_ms, :ms, 1, []},
    {:catastrophic_dataflow_timeout_ms, :ms, 60_000, key: true},
    {:compact_after_age_ms, :ms, 300_000,
     key: true, env: "FLEZHA_COMPACT_AFTER_AGE_MS", env_default: [test: 0]},
    {:compaction_timeout_ms, :ms, 30_000, key: true},
    {:correction_debounce_ms, :ms, 200, key: true},
    {:dlq_replay_grace_ms, :ms, 5_000, key: {:dlq, :replay_grace_ms}},
    {:dlq_sweep_interval_ms, :ms, 3_600_000, key: {:dlq, :retention_check_interval_ms}},
    {:input_log_hold_window_ms, :ms, 0, key: true, env: "FLEZHA_INPUT_LOG_HOLD_WINDOW_MS"},
    {:outbox_resettle_ms, :ms, 60_000, key: true},
    # The operator key keeps the `default_` prefix it was published under; the
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
    {:outbox_retry_ms, :ms, 1_000, []},

    # ---- durability tier ---------------------------------------------------
    {:durability_gc_abandoned_after_ms, :ms, 604_800_000,
     key: {:durability_tier, :gc_abandoned_after_ms},
     env: "FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS"},
    {:durability_gc_grace_ms, :ms, 3_600_000,
     key: {:durability_tier, :gc_grace_ms}, env: "FLEZHA_DURABILITY_GC_GRACE_MS"},
    {:durability_gc_interval_ms, :ms, 3_600_000,
     key: {:durability_tier, :gc_interval_ms}, env: "FLEZHA_DURABILITY_GC_INTERVAL_MS"},
    {:durability_pg_query_ms, :ms, 30_000, []},
    {:durability_rpo_target_ms, :ms, 120_000,
     key: {:durability_tier, :rpo_target_ms}, env: "FLEZHA_DURABILITY_RPO_TARGET_MS"},
    {:durability_s3_recv_ms, :ms, 30_000, []},
    {:durability_ship_call_ms, :ms, nil, derived: {:headroom, :sdk_commit_ack_ms, -5_000}},
    {:durability_ship_interval_ms, :ms, 30_000,
     key: {:durability_tier, :ship_interval_ms}, env: "FLEZHA_DURABILITY_SHIP_INTERVAL_MS"},

    # ---- shutdown ----------------------------------------------------------
    {:drain_budget_slack_ms, :ms, 1_000, []},
    {:forced_handover_grace_ms, :ms, 2_000, []},
    {:graceful_shutdown_timeout_ms, :ms, 60_000,
     key: true, env: "FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS"},
    {:orchestrator_grace_ms, :ms, nil,
     runtimes: [:shell], derived: {:headroom, :graceful_shutdown_timeout_ms, 15_000}},
    {:shutdown_finalise_call_ms, :ms, 5_000, []},
    {:signal_handler_status_call_ms, :ms, 5_000, []},
    {:stop_release_timeout_ms, :ms, 15_000, []},
    # A process teardown runs inside the operative stop, keeping 1 s of slack
    # for the stop itself to answer.
    {:stop_process_budget_ms, :ms, nil, derived: {:headroom, :stop_release_timeout_ms, -1_000}},

    # ---- observability -----------------------------------------------------
    {:audit_log_block_timeout_ms, :ms, 30_000, []},
    {:audit_log_writer_call_ms, :ms, 5_000, []},
    {:observability_sweep_interval_ms, :ms, 5_000,
     key: {:observability, :retention_check_interval_ms}},
    {:otlp_request_timeout_ms, :ms, 10_000, []},
    {:port_counter_reset_ms, :ms, 30_000, []},

    # ---- the Elixir SDK ----------------------------------------------------
    {:control_keepalive_ms, :ms, 15_000, runtimes: [:controller, :elixir_sdk, :rust]},
    {:data_heartbeat_ms, :ms, 10_000,
     env: "FLEZHA_DATA_HEARTBEAT_MS", runtimes: [:controller, :elixir_sdk, :rust]},
    {:entry_idle_tick_max_ms, :ms, 1_000, []},
    {:entry_idle_tick_min_ms, :ms, 250, []},
    {:sdk_commit_ack_ms, :ms, 30_000,
     env: "FLEZHA_WORKFLOW_ACK_TIMEOUT_MS", runtimes: [:controller, :elixir_sdk, :rust]},
    # A call into one of the runner's own control processes: the RunnerServer,
    # an Operative, a local run, the entry-idle watcher. None of them runs
    # author code on the answering clause, so a wait here is a wedged process
    # rather than a busy one.
    {:sdk_control_call_ms, :ms, 5_000, []},
    # A call into an Entity that answers from its queue rather than from its
    # state — the same question the serialise wait asks, kept as a row of its
    # own because a state read and a lifecycle call move apart the moment either
    # is tuned.
    {:sdk_entity_call_ms, :ms, 5_000, []},
    # The three calls whose duration grows with the state, not with the queue in
    # front of them. A refused call is not a cancelled one — the Entity finishes
    # the work either way — so each waits inside the Controller surface that
    # asked, and never the flat control bound. An inner bound equal to the outer
    # is no bound at all.
    {:sdk_entity_restore_ms, :ms, nil, derived: {:headroom, :restore_await_timeout_ms, -5_000}},
    {:sdk_entity_snapshot_ms, :ms, nil, derived: {:headroom, :compaction_timeout_ms, -5_000}},
    {:sdk_entity_apply_ms, :ms, nil,
     derived: {:headroom, :control_unit_reply_timeout_ms, -1_000}},
    {:sdk_idle_probe_ms, :ms, 50, []},
    {:sdk_max_reap_interval_ms, :ms, 30_000, []},
    {:sdk_serialize_wait_ms, :ms, 5_000, []},
    # The task yield outlasts the read it awaits, so a wedged Entity answers the
    # read's own bound rather than the yield's.
    {:sdk_serialize_yield_ms, :ms, nil, derived: {:headroom, :sdk_serialize_wait_ms, 1_000}},
    {:sdk_shutdown_ack_ms, :ms, 2_000, []}
  ]

  @doc "The duration rows, in the table's own declaration shape."
  @spec declared() :: [tuple()]
  def declared(), do: @declared
end

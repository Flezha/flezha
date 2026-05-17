import Config

config :flezha,
  testnet_enabled: true,
  concurrent_channel_count: 1,
  # Graceful shutdown is off in tests so `Application.stop/1` calls
  # at teardown remain fast. Dedicated integration tests under
  # `test/integration/shutdown/` exercise it explicitly.
  graceful_shutdown_enabled: false,
  graceful_shutdown_timeout_ms: 5_000,
  # Observability foundation: PubSub sink is off by default in tests so
  # subscribers in unrelated test modules don't accidentally receive
  # cross-test signal traffic. Tests that exercise the PubSub sink flip
  # this on explicitly via `Application.put_env/3`.
  observability: [
    enabled: true,
    ring_buffer_retention_ms: 900_000,
    ring_buffer_max_entries_per_source: 10_000,
    overflow_policy: :drop,
    cardinality_budget_per_source: 1_000,
    pubsub_sink_enabled: false
  ],
  # Tests bring up their own executors and relays — auto-recovery would
  # trigger replay against a not-yet-connected executor, cascading the
  # ProcessHub coordinator into shutdown. The persistent registry still
  # records mutations; only the at-boot replay state machine is off.
  auto_restore_enabled: false,
  # Bounded retry on top of `:start_relay` keeps test wall-clock down —
  # one short attempt is enough, and the parked-retry timer is disabled
  # so a failed startup does not periodically re-fire during a test.
  overwatch_retry_backoff_ms: [10, 10, 10],
  overwatch_retry_park_interval_ms: 0,
  overwatch_ping_timeout_ms: 500,
  # Tests that hit the real test executors leave ping enabled and rely on
  # the SDK's `Ping` handler. Tests that drive the retry/parked-state
  # paths in isolation use `Application.put_env(:flezha,
  # :overwatch_ping_fn, ...)` to inject a fake.
  overwatch_ping_enabled: true,
  process_hub_registry_root: Path.join([System.tmp_dir!(), "flezha_test_process_hub"]),
  pre_hooks: [
    fn ->
      # Wipe any stale per-test ProcessHub registry from previous runs so
      # auto-recovery never sees rows it cannot replay.
      :flezha
      |> Application.get_env(:process_hub_registry_root)
      |> case do
        nil -> :ok
        path -> File.rm_rf(path)
      end
    end,
    fn ->
      # Check if local node is running.
      if !Node.alive?() do
        # Start the local node
        {_, 0} = System.cmd("epmd", ["-daemon"])
        Node.start(:"test_agent_node1@127.0.0.1", :longnames)
      end
    end,
    fn ->
      Flezha.Service.EventManager.add_handler(:executor_connected_event)
    end,
    fn ->
      # Update the socket files path.
      Application.put_env(:flezha, :socket_files_path, "app_data/sockets/#{node()}/")
    end
  ],
  post_hooks: [
    fn ->
      for _x <- 1..2 do
        receive do
          {:executor_connected_event, {"default", _}} -> :ok
        after
          20_000 -> raise "Executor not connected"
        end
      end
    end
  ]

config :logger,
  level: :error

config :kafka_ex,
  brokers: [
    {"kafka", 9092}
  ],
  disable_default_worker: true

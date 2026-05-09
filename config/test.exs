import Config

config :flezha,
  testnet_enabled: true,
  concurrent_channel_count: 1,
  # Tests bring up their own executors and relays — auto-recovery would
  # trigger replay against a not-yet-connected executor, cascading the
  # ProcessHub coordinator into shutdown. The persistent registry still
  # records mutations; only the at-boot replay state machine is off.
  auto_restore_enabled: false,
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

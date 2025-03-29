import Config

config :aflecha,
  testnet_enabled: true,
  pre_hooks: [
    fn ->
      # Check if local node is running.
      if !Node.alive?() do
        # Start the local node
        {_, 0} = System.cmd("epmd", ["-daemon"])
        Node.start(:"test_agent_node1@127.0.0.1", :longnames)
      end
    end,
    fn ->
      Aflecha.Service.EventManager.add_handler(:operator_connected_event)
    end,
    fn ->
      # Update the socket files path.
      Application.put_env(:aflecha, :socket_files_path, "app_data/sockets/#{node()}/")
    end
  ],
  post_hooks: [
    fn ->
      for _x <- 1..2 do
        receive do
          {:operator_connected_event, {"default", _}} -> :ok
        after
          3000 -> raise "Operator not connected"
        end
      end
    end
  ]

config :logger,
  level: :warning

config :kafka_ex,
  brokers: [
    {"kafka", 9092}
  ],
  disable_default_worker: true

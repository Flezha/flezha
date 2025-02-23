import Config

config :aflecha,
  testnet_enabled: true,
  pre_hooks: [
    fn ->
      # Start the local node
      {_, 0} = System.cmd("epmd", ["-daemon"])
      Node.start(:"test_agent_node1@127.0.0.1", :longnames)
    end,
    fn ->
      Aflecha.Service.EventManager.add_handler(:factory_registered_event)
      Aflecha.Service.EventManager.add_handler(:operator_connected_event)
    end
  ],
  post_hooks: [
    fn ->
      for _x <- 1..10 do
        receive do
          {:factory_registered_event, {_group_id, _operator_id, _factory_id}} ->
            :ok
        after
          3000 -> raise "Factory not registered"
        end
      end
    end,
    fn ->
      for _x <- 1..2 do
        receive do
          {:operator_connected_event, {"default", _}} ->
            :ok
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

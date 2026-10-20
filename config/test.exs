import Config

# Every knob this environment used to set is an `env_default: [test: …]` on its
# row in `Flezha.Config.Keys`. What is left here is what is not a knob: the two
# boot hooks' own bodies, and two other applications' configuration.
config :flezha,
  pre_hooks: [
    fn ->
      if !Node.alive?() do
        {_, 0} = System.cmd("epmd", ["-daemon"])
        Node.start(FlezhaTest.Helper.NodeTestHelper.node_name(), :longnames)
      end
    end,
    fn ->
      boot = self()

      :ok =
        :telemetry.attach(
          :flezha_test_boot_runners,
          [:flezha, :runner, :connected],
          fn _event, _measurements, metadata, _config ->
            if metadata.namespace == "default",
              do: send(boot, {:runner_connected, "default", metadata.runner})
          end,
          nil
        )
    end
  ],
  post_hooks: [
    fn ->
      for _x <- 1..2 do
        receive do
          {:runner_connected, "default", _} -> :ok
        after
          20_000 -> raise "Runner not connected"
        end
      end

      :telemetry.detach(:flezha_test_boot_runners)
    end
  ]

config :flezha_sdk, socket_nif: true

config :logger,
  level: :error

# The console shows errors and nothing else. The primary level above is not
# enough on its own: a test that asserts on a warning has to lower the primary
# level for a capture handler to see the event at all, and that window is
# global, so every concurrently running test's warnings would reach the console
# with it. The handler's own level is what the console is actually judged by.
config :logger, :default_handler, level: :error

defmodule Flezha.Service.CliManager.ListenTest do
  @moduledoc """
  `flz operator listen` and `flz operator send --await` on the controller,
  with the plugin manager and the delivery stubbed: what a frame prints as,
  what ends the loop, and that the listener is detached on every route out.

  The live operator, the real forwarder and a partition group are
  `test/system/operator_listen_e2e_test.exs`.
  """
  # async: false: Mimic global mode, because the verb runs in a process the test spawns.
  use ExUnit.Case, async: false
  use Mimic
  import ExUnit.CaptureIO
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Service.{CliManager, OperatorManager, PluginManager}

  setup :set_mimic_global_until_exit

  setup do
    test = self()

    stub(PluginManager, :enable_plugin, fn id, plugin ->
      send(test, {:call, :enable, {id, plugin, self()}})
      {:ok, "plugin enabled"}
    end)

    stub(PluginManager, :disable_plugin_group, fn id ->
      send(test, {:call, :detach, id})
      {:ok, "plugin disabled"}
    end)

    stub(OperatorManager, :send_io_confirming, fn _op, _ns, _message, _metadata ->
      send(test, {:call, :deliver, nil})
      :ok
    end)

    %{parent: test}
  end

  describe "operator_listen/5" do
    test "text prints the channel, the payload and a keyed frame's stamps" do
      out =
        in_caller(fn ->
          forward("hello", %{"channel" => "echo"})

          forward("k1=v1", %{
            "channel" => "sink",
            "flz.cell" => "op/p/k1",
            "flz.seq" => "3.0",
            "flz.key" => "k1"
          })

          forward("again", %{"input_type" => "2"})
          CliManager.operator_listen("ns", "op", "output", duration_ms: 50)
        end)

      assert out ==
               "echo\thello\n" <>
                 "sink\tk1=v1\tcell=op/p/k1 seq=3.0 key=k1\n" <>
                 "-\tagain\n"
    end

    test "json carries the whole map, and bytes that are not text in a readable form" do
      out =
        in_caller(fn ->
          forward("hello", %{"channel" => "echo", "flz.key" => "k"})
          forward(<<255, 0>>, %{"flz.key" => <<255>>})
          CliManager.operator_listen("ns", "op", "output", output: "json", duration_ms: 50)
        end)

      assert out |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1) == [
               %{
                 "channel" => "echo",
                 "payload" => "hello",
                 "metadata" => %{"channel" => "echo", "flz.key" => "k"}
               },
               %{
                 "channel" => nil,
                 "payload_base64" => "/wA=",
                 "metadata" => %{"flz.key" => "<<255>>"}
               }
             ]
    end

    test "the forwarder is attached to the chain the direction names, then detached" do
      in_caller(fn -> CliManager.operator_listen("", "op", "input", duration_ms: 10) end)

      assert [{:enable, {id, plugin, listener}}, {:detach, id}] = calls()

      assert %Plugin{
               provider: MonitoredIOForward,
               namespace: "default",
               operator_id: "op",
               hooks: [{:on_input, []}],
               opts: %{recv_pid: ^listener, plugin_id: ^id}
             } = plugin
    end

    test "a heartbeat is answered while the loop runs", %{parent: test} do
      in_caller(fn ->
        send(self(), {:heartbeat_keepalive, test})
        CliManager.operator_listen("ns", "op", "output", duration_ms: 50)
      end)

      assert_received {:heartbeat_keepalive, :ok}
    end

    test "the caller's group leader going away ends the loop and detaches" do
      leader = spawn(fn -> Process.sleep(:infinity) end)

      listener =
        spawn(fn ->
          Process.group_leader(self(), leader)
          CliManager.operator_listen("ns", "op", "output", duration_ms: 60_000)
        end)

      assert_receive {:call, :enable, {id, _plugin, ^listener}}
      ref = Process.monitor(listener)
      Process.exit(leader, :kill)

      assert_receive {:call, :detach, ^id}
      assert_receive {:DOWN, ^ref, :process, ^listener, :normal}
    end

    test "an exit signal ends the loop and detaches, and a normal one does not", %{parent: test} do
      listener =
        spawn(fn ->
          capture_io(fn ->
            CliManager.operator_listen("ns", "op", "output", duration_ms: 60_000)
          end)
        end)

      assert_receive {:call, :enable, {id, _plugin, ^listener}}

      # The heartbeat is answered only once the loop has read past the normal exit.
      Process.exit(listener, :normal)
      send(listener, {:heartbeat_keepalive, test})
      assert_receive {:heartbeat_keepalive, :ok}
      refute_received {:call, :detach, _}

      Process.exit(listener, :shutdown)
      assert_receive {:call, :detach, ^id}
    end

    test "a refused enable is reported, and whatever it left is detached" do
      for {answer, said, exit} <- [
            {{:error, {:group_enable_partial, 1, 2, ["op#p1"]}},
             "enabled on 1 of 2 partitions and failed on op#p1 (plugin.group_enable_partial)", 7},
            {{:ok, :already_enabled}, "(plugin.id_taken)", 6}
          ] do
        stub(PluginManager, :enable_plugin, fn _id, _plugin -> answer end)

        {reason, err} =
          refused(fn -> CliManager.operator_listen("ns", "op", "output", []) end)

        assert reason == {:shutdown, exit}
        assert err =~ said
        assert [{:detach, "io_listener_" <> _}] = calls()
      end
    end

    test "a direction it does not know is refused before anything is attached" do
      for {direction, word} <- [{"sideways", "direction"}] do
        {reason, err} = refused(fn -> CliManager.operator_listen("ns", "op", direction, []) end)

        assert reason == {:shutdown, 2}
        assert err =~ "unknown #{word} 'sideways'"
      end

      assert calls() == []
    end
  end

  describe "operator_send/5 with await_ms" do
    test "attaches before it delivers, prints the verdict, then what came back", %{parent: test} do
      stub(OperatorManager, :send_io_confirming, fn "op", "ns", "hello", %{} ->
        send(test, {:call, :deliver, nil})
        forward("hello", %{"channel" => "echo"})
        :ok
      end)

      out = in_caller(fn -> CliManager.operator_send("ns", "op", "hello", "", await_ms: 50) end)

      assert out == "Delivered to 'op'.\necho\thello\n"
      assert [:enable, :deliver, :detach] = Enum.map(calls(), &elem(&1, 0))
    end

    test "with nothing emitted it prints the verdict alone" do
      out = in_caller(fn -> CliManager.operator_send("ns", "op", "hello", "", await_ms: 20) end)

      assert out == "Delivered to 'op'.\n"
      assert [:enable, :deliver, :detach] = Enum.map(calls(), &elem(&1, 0))
    end

    test "without it nothing is attached" do
      out = in_caller(fn -> CliManager.operator_send("ns", "op", "hello", "", []) end)

      assert out == "Delivered to 'op'.\n"
      assert [{:deliver, nil}] = calls()
    end
  end

  # The verb reads its own mailbox, so it runs in a process of its own and the
  # test's mailbox is left to the stubs.
  defp in_caller(fun), do: Task.await(Task.async(fn -> capture_io(fun) end), 5_000)

  defp refused(fun) do
    Task.await(
      Task.async(fn ->
        err = capture_io(:stderr, fn -> send(self(), {:reason, catch_exit(fun.())}) end)
        assert_received {:reason, reason}
        {reason, err}
      end),
      5_000
    )
  end

  # The forwarder sends to its `recv_pid`, which is the verb's own process.
  defp forward(payload, metadata), do: send(self(), {:message_forward, {payload, metadata}})

  defp calls() do
    receive do
      {:call, name, detail} -> [{name, detail} | calls()]
    after
      0 -> []
    end
  end
end

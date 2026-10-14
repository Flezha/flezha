defmodule Flezha.Service.Cli.ListenTest do
  @moduledoc """
  `flz relay listen` and `flz relay send --await` on the controller,
  with the plugin manager and the delivery stubbed: what a frame, a part and a
  dead-letter notice print as, what ends the loop, and that the listener is
  detached on every route out. The listener itself is
  `test/flezha/service/listener_test.exs`.

  The live relay, the real forwarder and a partition group are
  `test/system/relay_listen_e2e_test.exs`.
  """
  # async: false: Mimic global mode, because the verb runs in a process the test spawns.
  use ExUnit.Case, async: false
  use Mimic
  import ExUnit.CaptureIO
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Cluster.Directory
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Service.{Cli, HubManager, RelayManager, PluginManager}
  alias FlezhaTest.Helper.CliCapture

  setup :set_mimic_global_until_exit

  setup do
    test = self()

    # The forwarder sends to its `recv_pid`, the listener's receiver: the frames
    # the verb's process staged are sent there once it is attached.
    stub(PluginManager, :enable_plugin, fn id, plugin ->
      send(test, {:call, :enable, {id, plugin, self()}})
      for frame <- Process.get(:frames, []), do: send(plugin.opts.recv_pid, frame)
      {:ok, "plugin enabled"}
    end)

    stub(PluginManager, :detach, fn _ns, _op, id ->
      send(test, {:call, :detach, id})
      :ok
    end)

    # The relay is registered: the listener's existence read lets it attach.
    stub(Directory, :hub_nodes, fn _ns -> [node()] end)
    stub(PartitionGroup, :members, fn _ns, _op -> [] end)
    stub(HubManager, :declared_relays, fn _ns -> [] end)
    stub(HubManager, :relay_bindings, fn _ns, _op -> {:ok, []} end)

    stub(RelayManager, :send_io_confirming, fn _op, _ns, _message, _metadata ->
      send(test, {:call, :deliver, nil})
      :ok
    end)

    %{parent: test}
  end

  describe "relay_listen/5" do
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
          Cli.call("Relay.listen", Jason.encode!(["ns", "op", "output", %{window_ms: 50}]), [])
        end)

      assert out ==
               "echo\thello\n" <>
                 "sink\tk1=v1\tcell=op/p/k1 seq=3.0 key=k1\n" <>
                 "-\tagain\n"
    end

    test "text tells a part and a dead-letter notice from an output" do
      out =
        in_caller(fn ->
          preview("hel", %{"channel" => "answer", "flz.part" => "0", "flz.part_attempt" => "1"})
          forward("hello", %{"channel" => "answer"})
          notice("in")
          Cli.call("Relay.listen", Jason.encode!(["ns", "op", "output", %{window_ms: 50}]), [])
        end)

      assert [part, output, notice] = String.split(out, "\n", trim: true)
      assert part == "answer\thel\tpart=0 attempt=1"
      assert output == "answer\thello"
      assert [channel, message, fields] = String.split(notice, "\t")
      assert channel == "in"
      assert message =~ "was dead-lettered as 'op/t/e1'"
      assert fields == "error=input.dead_lettered entry=op/t/e1"
    end

    test "json names the kind of every line, and a notice carries its error" do
      out =
        in_caller(fn ->
          preview("hel", %{"channel" => "answer", "flz.part" => "0", "flz.part_attempt" => "1"})
          forward("hello", %{"channel" => "answer"})
          notice("in")

          Cli.call("Relay.listen", Jason.encode!(["ns", "op", "output", %{window_ms: 50}]),
            output: "json"
          )
        end)

      assert [part, output, notice] =
               out |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

      assert %{"kind" => "part", "payload" => "hel"} = part
      assert %{"kind" => "output", "payload" => "hello"} = output
      assert %{"kind" => "dead_lettered", "channel" => "in", "error" => error} = notice
      assert %{"code" => "input.dead_lettered", "details" => %{"entry" => "op/t/e1"}} = error
      refute Map.has_key?(notice, "payload")
    end

    test "json carries the whole map, and bytes that are not text in a readable form" do
      out =
        in_caller(fn ->
          forward("hello", %{"channel" => "echo", "flz.key" => "k"})
          forward(<<255, 0>>, %{"flz.key" => <<255>>})

          Cli.call("Relay.listen", Jason.encode!(["ns", "op", "output", %{window_ms: 50}]),
            output: "json"
          )
        end)

      assert out |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1) == [
               %{
                 "kind" => "output",
                 "channel" => "echo",
                 "payload" => "hello",
                 "metadata" => %{"channel" => "echo", "flz.key" => "k"}
               },
               %{
                 "kind" => "output",
                 "channel" => nil,
                 "payload_base64" => "/wA=",
                 "metadata" => %{"flz.key" => "<<255>>"}
               }
             ]
    end

    test "text prints each call and how it ended" do
      out = in_caller(fn -> calls_listened([]) end)

      assert String.split(out, "\n", trim: true) == [
               "in\thello\tcall=id1 source=cli",
               "-\tturn\tcall=id2 source=relay caller=orders",
               "out\thi\tcall=id1 answered ms=4",
               "in\t" <> timeout_message() <> "\tcall=id3 error=call.timeout ms=100",
               "-\t" <> busy_message() <> "\tcall=id2 error=call.busy ms=0"
             ]
    end

    test "json names each call event's kind and call, with an error for an outcome" do
      out = in_caller(fn -> calls_listened(output: "json") end)

      assert [call, relay_call, answered, timed_out, refused] =
               out |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

      assert call == %{
               "kind" => "call",
               "call" => "id1",
               "channel" => "in",
               "payload" => "hello",
               "metadata" => %{
                 "channel" => "in",
                 "flz.call_id" => "id1",
                 "flz.call_source" => "cli",
                 "flz.call_at" => 1_700_000_000_000,
                 "flz.call_deadline_ms" => 5_000
               }
             }

      assert %{"kind" => "call", "call" => "id2", "channel" => nil} = relay_call
      assert %{"kind" => "answered", "call" => "id1", "payload" => "hi"} = answered
      assert %{"kind" => "timed_out", "call" => "id3", "error" => error} = timed_out
      assert %{"code" => "call.timeout"} = error
      refute Map.has_key?(timed_out, "payload")

      assert %{"kind" => "refused", "call" => "id2", "error" => %{"code" => "call.busy"}} =
               refused
    end

    test "a tally prints one line per report, its rates since the line before" do
      reports = [
        {"op", 1, %{"a" => 10, "b" => 0, "" => 0}},
        {"op", 1, %{"a" => 30, "b" => 5, "" => 2}},
        {"op", 2, %{"a" => 1, "b" => 0, "" => 0}}
      ]

      stub(PluginManager, :enable_plugin, fn _id, plugin ->
        spawn(fn ->
          for {member, epoch, totals} <- reports do
            send(plugin.opts.recv_pid, {:tally_report, member, epoch, totals})
            Process.sleep(150)
          end
        end)

        {:ok, "plugin enabled"}
      end)

      args =
        Jason.encode!([
          "ns",
          "op",
          "tally",
          %{window_ms: 700, tally_ms: 100, channels: ["a", "b"]}
        ])

      out = in_caller(fn -> Cli.call("Relay.listen", args, []) end)
      assert [first, second, restarted] = String.split(out, "\n", trim: true)

      assert first == "a=10 (-)\tb=0 (-)", "no rate before a second report, no empty (other)"
      assert [other, a, b] = String.split(second, "\t")
      assert other =~ ~r"^\(other\)=2 \(\d+\.\d/s\)$"
      assert a =~ ~r"^a=30 \(\d+\.\d/s\)$"
      assert b =~ ~r"^b=5 \(\d+\.\d/s\)$"
      assert restarted == "a=1 (-)\tb=0 (-)", "a new epoch starts the rates again"

      json = in_caller(fn -> Cli.call("Relay.listen", args, output: "json") end)
      assert [line | _] = json |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

      assert %{
               "kind" => "tally",
               "totals" => %{"a" => 10},
               "rates" => %{},
               "epochs" => %{"op" => 1}
             } =
               line
    end

    test "the forwarder is attached to the chain the direction names, then detached" do
      in_caller(fn ->
        Cli.call(
          "Relay.listen",
          Jason.encode!(["default", "op", "input", %{window_ms: 10}]),
          []
        )
      end)

      assert [{:enable, {id, plugin, listener}}, {:detach, id}] = calls()

      assert %Plugin{
               provider: MonitoredIOForward,
               namespace: "default",
               relay_id: "op",
               hooks: [{:on_input, []}],
               opts: %{recv_pid: receiver, plugin_id: ^id}
             } = plugin

      assert is_pid(receiver) and receiver != listener
    end

    test "an output listener takes the output chain and the previews" do
      in_caller(fn ->
        Cli.call("Relay.listen", Jason.encode!(["ns", "op", "output", %{window_ms: 10}]), [])
      end)

      assert [{:enable, {_id, plugin, _}}, {:detach, _}] = calls()
      assert plugin.hooks == [{:on_output, []}, {:on_preview, []}]
    end

    test "the caller's group leader going away ends the loop and detaches" do
      leader = spawn(fn -> Process.sleep(:infinity) end)

      listener =
        spawn(fn ->
          Process.group_leader(self(), leader)

          Cli.call(
            "Relay.listen",
            Jason.encode!(["ns", "op", "output", %{window_ms: 60_000}]),
            []
          )
        end)

      assert_receive {:call, :enable, {id, _plugin, ^listener}}
      ref = Process.monitor(listener)
      Process.exit(leader, :kill)

      assert_receive {:call, :detach, ^id}
      assert_receive {:DOWN, ^ref, :process, ^listener, :normal}
    end

    test "a refused enable is reported, and whatever it left is detached" do
      for {answer, said, exit} <- [
            {{:error, {:group_enable_partial, 1, 2, ["op#p1"]}},
             "enabled on 1 of 2 partitions and failed on op#p1 (plugin.group_enable_partial)", 7},
            {{:ok, :already_enabled}, "(plugin.id_taken)", 6}
          ] do
        stub(PluginManager, :enable_plugin, fn _id, _plugin -> answer end)

        {reason, err} =
          refused(fn ->
            Cli.call("Relay.listen", Jason.encode!(["ns", "op", "output", %{}]), [])
          end)

        assert reason == {:shutdown, exit}
        assert err =~ said
        assert [{:detach, "io_listener_" <> _}] = calls()
      end
    end

    test "a direction it does not know is refused before anything is attached" do
      for {direction, word} <- [{"sideways", "direction"}] do
        {reason, err} =
          refused(fn ->
            Cli.call("Relay.listen", Jason.encode!(["ns", "op", direction, %{}]), [])
          end)

        assert reason == {:shutdown, 2}
        assert err =~ "unknown #{word} 'sideways'"
      end

      assert calls() == []
    end
  end

  describe "relay_send/5 with await_ms" do
    # `op` is a plain relay: the send reaches it by its own id.
    setup do
      stub(RelayManager, :resolve_member, fn "ns", "op", nil -> {:ok, "op"} end)
      :ok
    end

    test "attaches before it delivers, prints the verdict, then what came back", %{parent: test} do
      stub(RelayManager, :send_io_confirming, fn "op", "ns", "hello", %{} ->
        send(test, {:call, :deliver, nil})
        send(Process.get(:receiver), {:message_forward, {"hello", %{"channel" => "echo"}}})
        :ok
      end)

      stub(PluginManager, :enable_plugin, fn id, plugin ->
        send(test, {:call, :enable, {id, plugin, self()}})
        Process.put(:receiver, plugin.opts.recv_pid)
        {:ok, "plugin enabled"}
      end)

      out =
        in_caller(fn ->
          Cli.call("Relay.send", Jason.encode!(["ns", "op", "hello", "", %{}]), await_ms: 50)
        end)

      assert out == "Delivered to 'op'.\necho\thello\n"
      assert [:enable, :deliver, :detach] = Enum.map(calls(), &elem(&1, 0))
    end

    test "with nothing emitted it prints the verdict alone" do
      out =
        in_caller(fn ->
          Cli.call("Relay.send", Jason.encode!(["ns", "op", "hello", "", %{}]), await_ms: 20)
        end)

      assert out == "Delivered to 'op'.\n"
      assert [:enable, :deliver, :detach] = Enum.map(calls(), &elem(&1, 0))
    end

    # Fails when: the refusal is rendered inside the listen, whose exit the door
    # then reports a second time as an internal failure with the wrong exit code.
    test "a refused send is reported once, with its own code, and detaches" do
      stub(RelayManager, :resolve_member, fn "ns", "op", nil ->
        {:error, {:relay_not_found, "ns", "op"}}
      end)

      {reason, err} =
        refused(fn ->
          Cli.call("Relay.send", Jason.encode!(["ns", "op", "hello", "", %{}]), await_ms: 50)
        end)

      assert reason == {:shutdown, 5}
      assert [_one] = String.split(err, "error:", trim: true)
      assert err =~ "(relay.not_found)"
      assert [:enable, :detach] = Enum.map(calls(), &elem(&1, 0))
    end

    test "without it nothing is attached" do
      out =
        in_caller(fn ->
          Cli.call("Relay.send", Jason.encode!(["ns", "op", "hello", "", %{}]), [])
        end)

      assert out == "Delivered to 'op'.\n"
      assert [{:deliver, nil}] = calls()
    end
  end

  # The verb reads its own mailbox, so it runs in a process of its own and the
  # test's mailbox is left to the stubs.
  defp in_caller(fun), do: Task.await(Task.async(fn -> capture_io(fun) end), 5_000)

  defp refused(fun) do
    {{:exit, reason}, _out, err} = Task.await(Task.async(fn -> CliCapture.run(fun) end), 5_000)
    {reason, err}
  end

  # Staged in the verb's process; the enable stub hands them to the receiver.
  defp forward(payload, metadata), do: stage({:message_forward, {payload, metadata}})
  defp preview(payload, metadata), do: stage({:preview_forward, {payload, metadata}})

  defp notice(channel) do
    error =
      Flezha.Error.new("input.dead_lettered",
        relay: "op",
        channel: channel,
        entry: "op/t/e1",
        attempts: 3,
        error: "boom"
      )

    preview(Flezha.Error.render(error, :json), %{"channel" => channel})
  end

  defp calls_listened(opts) do
    call_frame("hello", "call", %{
      "channel" => "in",
      "flz.call_id" => "id1",
      "flz.call_source" => "cli",
      "flz.call_at" => 1_700_000_000_000,
      "flz.call_deadline_ms" => 5_000
    })

    call_frame("turn", "call", %{
      "flz.call_id" => "id2",
      "flz.call_source" => "relay",
      "flz.call_caller" => "orders"
    })

    call_frame("hi", "answered", %{
      "channel" => "out",
      "flz.call_id" => "id1",
      "flz.call_duration_ms" => 4
    })

    call_frame(Flezha.Error.render(timeout_error(), :json), "timed_out", %{
      "channel" => "in",
      "flz.call_id" => "id3",
      "flz.call_duration_ms" => 100
    })

    call_frame(Flezha.Error.render(busy_error(), :json), "refused", %{
      "flz.call_id" => "id2",
      "flz.call_duration_ms" => 0
    })

    Cli.call("Relay.listen", Jason.encode!(["ns", "op", "calls", %{window_ms: 50}]), opts)
  end

  defp timeout_error, do: Flezha.Error.new("call.timeout", relay: "op", deadline_ms: 100)
  defp busy_error, do: Flezha.Error.new("call.busy", relay: "op", pending_max: 1)
  defp timeout_message, do: timeout_error().message
  defp busy_message, do: busy_error().message

  defp call_frame(payload, kind, metadata),
    do: forward(payload, Map.put(metadata, "flz.call_event", kind))

  defp stage(frame), do: Process.put(:frames, Process.get(:frames, []) ++ [frame])

  defp calls() do
    receive do
      {:call, name, detail} -> [{name, detail} | calls()]
    after
      0 -> []
    end
  end
end

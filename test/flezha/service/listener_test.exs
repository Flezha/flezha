defmodule Flezha.Service.ListenerTest do
  @moduledoc """
  `Flezha.Service.Listener.listen/4` with the plugin manager and the registry
  stubbed: a test process stands in for a plugin's request process, and the
  frames the attached forwarder would send are sent to the relay it names.
  What a caller receives and in which process, what ends a listener, its
  bound, and that every way out detaches.

  The live operator is `test/system/part_stream_e2e_test.exs`.
  """
  # async: false: Mimic global mode (the relay reads the stubbed registry) and
  # the listener bounds are application env.
  use FlezhaTest.Case, fixtures: [:env], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Cluster.Directory
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Service.{HubManager, Listener, PluginManager}

  setup :set_mimic_global_until_exit

  setup do
    test = self()

    stub(PluginManager, :enable_plugin, fn id, plugin ->
      send(test, {:attached, id, plugin.opts.recv_pid, plugin.hooks})
      {:ok, "plugin enabled"}
    end)

    stub(PluginManager, :detach, fn ns, op, id ->
      send(test, {:detached, ns, op, id})
      :ok
    end)

    stub(PartitionGroup, :members, fn _ns, _op -> [] end)
    stub(Directory, :hub_nodes, fn _ns -> [node()] end)
    stub(HubManager, :declared_operators, fn _ns -> [] end)
    :ok
  end

  # A listener in a process of its own, fed `frames` once it is attached;
  # its outcome and every event come back to the test.
  defp listening(opts, handler \\ nil) do
    test = self()
    handler = handler || fn event -> send(test, {:event, self(), event}) && :continue end

    pid =
      spawn(fn ->
        outcome = Listener.listen("ns", "op", opts, handler)
        send(test, {:outcome, outcome})
      end)

    assert_receive {:attached, id, relay, hooks}, 2_000
    %{pid: pid, relay: relay, id: id, hooks: hooks}
  end

  defp output(relay, payload), do: send(relay, {:message_forward, {payload, %{"channel" => "c"}}})

  defp part(relay, payload, n),
    do: send(relay, {:preview_forward, {payload, %{"channel" => "c", "flz.part" => "#{n}"}}})

  test "outputs reach the handler in the calling process, and :done detaches" do
    %{pid: pid, relay: relay} = listening([])
    output(relay, "one")
    output(relay, "two")

    assert_receive {:event, ^pid, {:output, "one", %{"channel" => "c"}}}, 2_000
    assert_receive {:event, ^pid, {:output, "two", _}}, 2_000
    Process.exit(pid, :kill)

    %{relay: relay, id: id} = listening([], fn _event -> :done end)
    output(relay, "x")
    assert_receive {:outcome, {:ok, :done}}, 2_000
    assert_receive {:detached, "ns", "op", ^id}
    refute Process.alive?(relay)
  end

  test "an output listener hooks the output and the preview chains, an input one the input" do
    assert %{hooks: [{:on_output, []}, {:on_preview, []}]} = listening(window_ms: 1)
    assert_receive {:outcome, {:ok, :window_closed}}, 2_000
    assert %{hooks: [{:on_input, []}]} = listening(window_ms: 1, direction: :input)
    assert_receive {:outcome, {:ok, :window_closed}}, 2_000
  end

  test "a part, a notice and an input are told apart" do
    %{pid: pid, relay: relay} = listening([])
    part(relay, "he", 0)
    notice = Flezha.Error.new("input.dead_lettered", operator: "op", channel: "in", entry: "e1")
    send(relay, {:preview_forward, {Flezha.Error.render(notice, :json), %{"channel" => "in"}}})

    assert_receive {:event, ^pid, {:part, "he", %{"flz.part" => "0"}}}, 2_000
    assert_receive {:event, ^pid, {:dead_lettered, %Flezha.Error{} = error, %{"channel" => "in"}}}
    assert error.code == "input.dead_lettered"
    assert error.details["entry"] == "e1"
    Process.exit(pid, :kill)
  end

  test "a handler that raises detaches and the raise reaches the caller" do
    {pid, ref} =
      spawn_monitor(fn ->
        try do
          Listener.listen("ns", "op", [], fn _event -> raise "boom" end)
        rescue
          raised -> exit({:raised, raised})
        end
      end)

    assert_receive {:attached, id, relay, _}, 2_000
    output(relay, "x")
    assert_receive {:DOWN, ^ref, :process, ^pid, {:raised, %RuntimeError{message: "boom"}}}, 2_000
    assert_receive {:detached, "ns", "op", ^id}
  end

  test "a refused attach answers the enable's own error and leaves nothing" do
    stub(PluginManager, :enable_plugin, fn _id, _plugin ->
      {:error, {:operator_not_found, "ns", "op"}}
    end)

    assert {:error, %Flezha.Error{code: "operator.not_found"}} =
             Listener.listen("ns", "op", [], fn _ -> :continue end)

    assert_received {:detached, "ns", "op", _id}
  end

  test "the relay answers the heartbeat, and dies with its caller" do
    %{pid: pid, relay: relay} = listening([])
    send(relay, {:heartbeat_keepalive, self()})
    assert_receive {:heartbeat_keepalive, :ok}, 2_000

    ref = Process.monitor(relay)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^relay, :killed}, 2_000
  end

  test "the watched process going away ends the listener" do
    watched = spawn(fn -> Process.sleep(:infinity) end)
    %{id: id} = listening(watch: watched)
    Process.exit(watched, :kill)
    assert_receive {:outcome, {:ok, :caller_down}}, 2_000
    assert_receive {:detached, "ns", "op", ^id}
  end

  describe "a bounded backlog" do
    setup do
      Application.put_env(:flezha, :listener_pending_frames, 8)
    end

    test "a slow reader loses parts, not outputs, and goes on" do
      test = self()

      slow = fn event ->
        send(test, {:event, self(), event})
        Process.sleep(20)
        :continue
      end

      %{pid: pid, relay: relay} = listening([], slow)
      for n <- 0..99, do: part(relay, "p#{n}", n)
      output(relay, "answer")

      numbers = parts_until_output(pid, [])
      assert length(numbers) < 100
      assert numbers == Enum.sort(numbers)
      assert :sys.get_state(relay).frames <= 8
      refute_received {:outcome, _}
      Process.exit(pid, :kill)
    end

    test "a reader that falls behind on outputs is ended with listener.overrun" do
      slow = fn _event ->
        Process.sleep(200)
        :continue
      end

      %{relay: relay, id: id} = listening([], slow)
      for n <- 1..100, do: output(relay, "o#{n}")

      assert_receive {:outcome, {:error, %Flezha.Error{code: "listener.overrun"} = error}}, 5_000
      assert error.details["frames"] == 8
      assert_receive {:detached, "ns", "op", ^id}
    end
  end

  describe "a released plugin" do
    setup do
      Application.put_env(:flezha, :listener_release_check_ms, 20)
    end

    test "a row still carrying it is a move: the listener goes on" do
      %{pid: pid, relay: relay, id: id} = listening(window_ms: 300)
      stub(HubManager, :operator_plugins, fn "ns", "op" -> {:ok, [{id, nil}]} end)

      send(relay, {:listener_released, id})
      send(relay, {:heartbeat_keepalive, self()})
      assert_receive {:heartbeat_keepalive, :ok}, 2_000
      assert_receive {:outcome, {:ok, :window_closed}}, 2_000
      refute Process.alive?(pid)
    end

    test "a registered operator without the row was disabled by someone else" do
      stub(HubManager, :operator_plugins, fn "ns", "op" -> {:ok, []} end)
      %{relay: relay, id: id} = listening([])
      send(relay, {:listener_released, id})

      assert_receive {:outcome, {:error, %Flezha.Error{code: "listener.detached"}}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}
    end

    test "an operator no longer registered ended, after what was held before it" do
      stub(HubManager, :operator_plugins, fn "ns", "op" ->
        {:error, {:operator_not_found, "ns", "op"}}
      end)

      %{pid: pid, relay: relay, id: id} = listening([])
      output(relay, "last")
      send(relay, {:listener_released, id})

      assert_receive {:event, ^pid, {:output, "last", _}}, 2_000
      assert_receive {:outcome, {:ok, :operator_ended}}, 2_000
    end
  end

  describe "idle_ms" do
    test "a quiet listener is told it is idle, and stays attached" do
      test = self()

      counting = fn
        :idle -> send(test, :idle) && :continue
        _event -> :continue
      end

      listening([idle_ms: 200, window_ms: 1_000], counting)
      assert_receive {:outcome, {:ok, :window_closed}}, 2_000
      idles = drain(:idle, 0)
      assert idles in 3..5
    end

    test "an idle answer of :done ends the listener and detaches" do
      %{id: id} = listening([idle_ms: 50], fn :idle -> :done end)
      assert_receive {:outcome, {:ok, :done}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}
    end

    test "events reset the interval" do
      test = self()

      handler = fn
        :idle -> send(test, :idle) && :continue
        event -> send(test, {:event, self(), event}) && :continue
      end

      %{pid: pid, relay: relay} = listening([idle_ms: 500], handler)

      for n <- 0..19 do
        part(relay, "p", n)
        Process.sleep(100)
      end

      refute_received :idle
      assert drain_events(pid, 0) == 20
      Process.exit(pid, :kill)
    end

    test "an option that is neither a positive integer nor :infinity is refused" do
      for bad <- [0, -1, "5"] do
        assert_raise ArgumentError, fn -> Listener.listen("ns", "op", [idle_ms: bad], & &1) end
      end
    end
  end

  defp parts_until_output(pid, numbers) do
    receive do
      {:event, ^pid, {:part, _payload, %{"flz.part" => n}}} ->
        parts_until_output(pid, [String.to_integer(n) | numbers])

      {:event, ^pid, {:output, "answer", _}} ->
        Enum.reverse(numbers)
    after
      10_000 -> flunk("the output never came")
    end
  end

  defp drain(message, count) do
    receive do
      ^message -> drain(message, count + 1)
    after
      0 -> count
    end
  end

  defp drain_events(pid, count) do
    receive do
      {:event, ^pid, _} -> drain_events(pid, count + 1)
    after
      0 -> count
    end
  end
end

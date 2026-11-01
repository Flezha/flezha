defmodule Flezha.Service.ListenerTest do
  @moduledoc """
  `Flezha.Service.Listener.listen/4` with the plugin manager and the registry
  stubbed: a test process stands in for a plugin's request process, and the
  frames the attached forwarder would send are sent to the receiver it names.
  What a caller receives and in which process, what ends a listener, its
  bound, and that every way out detaches.

  The live relay is `test/system/part_stream_e2e_test.exs`.
  """
  # async: false: Mimic global mode (the receiver reads the stubbed registry) and
  # the listener bounds are application env.
  use FlezhaTest.Case, fixtures: [:env], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Cluster.Directory
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Service.{HubManager, Listener, PluginManager, Relay, RelayApi}

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
    stub(HubManager, :declared_relays, fn _ns -> [] end)
    stub(HubManager, :relay_bindings, fn _ns, _op -> {:ok, []} end)
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

    assert_receive {:attached, id, receiver, hooks}, 2_000
    %{pid: pid, receiver: receiver, id: id, hooks: hooks}
  end

  defp output(receiver, payload),
    do: send(receiver, {:message_forward, {payload, %{"channel" => "c"}}})

  defp call_frame(receiver, payload, kind, metadata) do
    metadata = Map.merge(metadata, %{"flz.call_event" => kind, "flz.call_id" => "id1"})
    send(receiver, {:message_forward, {payload, metadata}})
  end

  defp part(receiver, payload, n),
    do: send(receiver, {:preview_forward, {payload, %{"channel" => "c", "flz.part" => "#{n}"}}})

  test "outputs reach the handler in the calling process, and :done detaches" do
    %{pid: pid, receiver: receiver} = listening([])
    output(receiver, "one")
    output(receiver, "two")

    assert_receive {:event, ^pid, {:output, "one", %{"channel" => "c"}}}, 2_000
    assert_receive {:event, ^pid, {:output, "two", _}}, 2_000
    Process.exit(pid, :kill)

    %{receiver: receiver, id: id} = listening([], fn _event -> :done end)
    output(receiver, "x")
    assert_receive {:outcome, {:ok, :done}}, 2_000
    assert_receive {:detached, "ns", "op", ^id}
    refute Process.alive?(receiver)
  end

  test "an output listener hooks the output and the preview chains, an input one the input" do
    assert %{hooks: [{:on_output, []}, {:on_preview, []}]} = listening(window_ms: 1)
    assert_receive {:outcome, {:ok, :window_closed}}, 2_000
    assert %{hooks: [{:on_input, []}]} = listening(window_ms: 1, direction: :input)
    assert_receive {:outcome, {:ok, :window_closed}}, 2_000
  end

  test "a calls listener hooks the calls alone" do
    assert %{hooks: [{:on_call, []}]} = listening(window_ms: 1, direction: :calls)
    assert_receive {:outcome, {:ok, :window_closed}}, 2_000

    assert_raise ArgumentError, ~r/:calls/, fn ->
      Listener.listen("ns", "op", [direction: :sideways], & &1)
    end
  end

  test "a call frame and each outcome arrive as their events" do
    %{pid: pid, receiver: receiver} = listening(direction: :calls)
    call_frame(receiver, "hello", "call", %{"flz.call_source" => "cli"})
    call_frame(receiver, "hi", "answered", %{"flz.call_duration_ms" => 3})

    for {code, kind} <- [{"call.timeout", "timed_out"}, {"call.busy", "refused"}] do
      document = Flezha.Error.render(Flezha.Error.new(code, relay: "op", deadline_ms: 1), :json)
      call_frame(receiver, document, kind, %{})
    end

    assert_receive {:event, ^pid, {:call, "hello", %{"flz.call_source" => "cli"} = called}}, 2_000
    refute Map.has_key?(called, "flz.call_event")
    assert_receive {:event, ^pid, {:answered, "hi", %{"flz.call_duration_ms" => 3}}}, 2_000
    assert_receive {:event, ^pid, {:timed_out, %Flezha.Error{code: "call.timeout"}, _}}, 2_000
    assert_receive {:event, ^pid, {:refused, %Flezha.Error{code: "call.busy"}, md}}, 2_000
    assert md == %{"flz.call_id" => "id1"}
    Process.exit(pid, :kill)
  end

  describe "a tally" do
    setup do
      test = self()

      stub(PluginManager, :enable_plugin, fn id, plugin ->
        send(test, {:tally_opts, plugin.opts})
        send(test, {:attached, id, plugin.opts.recv_pid, plugin.hooks})
        {:ok, "plugin enabled"}
      end)

      stub(RelayApi, :graph_of, fn "ns", "op" ->
        {:ok, %{graph: %{channels: [out("prices"), out("ticks", ""), out("quote", "call")]}}}
      end)

      :ok
    end

    test "hooks the output chain first and counts the announced output channels" do
      %{hooks: hooks} = listening(window_ms: 1, direction: :tally, tally_ms: 250)
      assert hooks == [{:on_output, [position: 0]}]
      assert_receive {:tally_opts, %{mode: "tally", channels: ["prices"], tally_ms: 250}}
      assert_receive {:outcome, {:ok, :window_closed}}, 2_000
    end

    test "named channels replace the announced ones, and none is refused" do
      listening(window_ms: 1, direction: :tally, channels: ["a", "b"])
      assert_receive {:tally_opts, %{channels: ["a", "b"]}}
      assert_receive {:outcome, {:ok, :window_closed}}, 2_000

      stub(RelayApi, :graph_of, fn _ns, _op -> {:ok, %{graph: nil}} end)

      for opts <- [[], [channels: []]] do
        assert {:error, %Flezha.Error{code: "listener.no_channels", details: %{"relay" => "op"}}} =
                 Listener.listen("ns", "op", [direction: :tally] ++ opts, & &1)
      end

      refute_received {:attached, _, _, _}
    end

    test "a group's member reports are summed, each with the epoch it counts in" do
      %{pid: pid, receiver: receiver} = listening(direction: :tally, tally_ms: 3_600_000)
      send(receiver, {:tally_report, "op#p0", 1, %{"prices" => 2, "" => 0}})

      assert_receive {:event, ^pid, {:tally, %{"prices" => 2}, %{epochs: %{"op#p0" => 1}}}},
                     2_000

      # Within the interval: held back, then handed on once with both members.
      send(receiver, {:tally_report, "op#p1", 7, %{"prices" => 3, "" => 1}})
      send(receiver, {:tally_report, "op#p0", 1, %{"prices" => 4, "" => 0}})
      :sys.get_state(receiver)
      refute_received {:event, ^pid, _}

      send(receiver, :tally_due)
      assert_receive {:event, ^pid, {:tally, totals, %{epochs: epochs, read_at_ms: at}}}, 2_000
      :sys.get_state(receiver)
      refute_received {:event, ^pid, _}
      assert totals == %{"prices" => 7, "" => 1}
      assert epochs == %{"op#p0" => 1, "op#p1" => 7}
      assert is_integer(at)
      Process.exit(pid, :kill)
    end

    test "every way out detaches it" do
      %{id: id} = listening(window_ms: 1, direction: :tally)
      assert_receive {:outcome, {:ok, :window_closed}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}

      watched = spawn(fn -> Process.sleep(:infinity) end)
      %{id: id} = listening(direction: :tally, watch: watched)
      Process.exit(watched, :kill)
      assert_receive {:outcome, {:ok, :caller_down}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}

      %{receiver: receiver, id: id} = listening([direction: :tally], fn _ -> :done end)
      send(receiver, {:tally_report, "op", 1, %{"prices" => 1}})
      assert_receive {:outcome, {:ok, :done}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}
    end
  end

  defp out(name, plugin_end \\ "output"), do: %{name: name, plugin_end: plugin_end}

  # Lets a stalled reader take one event at a time until it holds `wanted`.
  defp newest_tally(pid, wanted) do
    send(pid, :next)
    assert_receive {:event, ^pid, {:tally, %{"a" => n}, _}}, 2_000
    if n == wanted, do: n, else: newest_tally(pid, wanted)
  end

  test "a part, a notice and an input are told apart" do
    %{pid: pid, receiver: receiver} = listening([])
    part(receiver, "he", 0)
    notice = Flezha.Error.new("input.dead_lettered", relay: "op", channel: "in", entry: "e1")
    send(receiver, {:preview_forward, {Flezha.Error.render(notice, :json), %{"channel" => "in"}}})

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

    assert_receive {:attached, id, receiver, _}, 2_000
    output(receiver, "x")
    assert_receive {:DOWN, ^ref, :process, ^pid, {:raised, %RuntimeError{message: "boom"}}}, 2_000
    assert_receive {:detached, "ns", "op", ^id}
  end

  test "a refused attach answers the enable's own error and leaves nothing" do
    stub(PluginManager, :enable_plugin, fn _id, _plugin ->
      {:error, {:relay_not_found, "ns", "op"}}
    end)

    assert {:error, %Flezha.Error{code: "relay.not_found"}} =
             Listener.listen("ns", "op", [], fn _ -> :continue end)

    assert_received {:detached, "ns", "op", _id}
  end

  test "a target neither registered nor declared is refused before anything is enabled" do
    stub(HubManager, :relay_bindings, fn "ns", "ghost" ->
      {:error, {:relay_not_found, "ns", "ghost"}}
    end)

    assert {:error, %Flezha.Error{code: "relay.not_found"}} =
             Listener.listen("ns", "ghost", [], fn _ -> :continue end)

    refute_received {:attached, _, _, _}
    refute_received {:detached, _, _, _}
  end

  test "the receiver answers the heartbeat, and dies with its caller, detaching" do
    %{pid: pid, receiver: receiver} = listening([])
    send(receiver, {:heartbeat_keepalive, self()})
    assert_receive {:heartbeat_keepalive, :ok}, 2_000

    ref = Process.monitor(receiver)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^receiver, :normal}, 2_000
    assert_received {:detached, "ns", "op", _id}
  end

  test "the watched process going away ends the listener" do
    watched = spawn(fn -> Process.sleep(:infinity) end)
    %{id: id} = listening(watch: watched)
    Process.exit(watched, :kill)
    assert_receive {:outcome, {:ok, :caller_down}}, 2_000
    assert_receive {:detached, "ns", "op", ^id}
  end

  # What a consumer on another node does: `Relay.listen/5` over `:erpc`, with
  # a pid of its own as `:emit`, so no code of the consumer runs here.
  test "a pid as :emit receives each event as {:flezha_emit, event}, and its exit detaches" do
    test = self()
    sink = spawn(fn -> forward_to(test) end)
    context = %{source: :internal, emit: sink}

    spawn(fn ->
      outcome = :erpc.call(node(), Relay, :listen, ["ns", "op", :output, [], context])
      send(test, {:outcome, outcome})
    end)

    assert_receive {:attached, id, receiver, _hooks}, 2_000
    assert_receive {:forwarded, :attached}, 2_000
    output(receiver, "one")
    assert_receive {:forwarded, {:output, "one", %{"channel" => "c"}}}, 2_000

    Process.exit(sink, :kill)
    assert_receive {:outcome, {:ok, :caller_down}}, 2_000
    assert_receive {:detached, "ns", "op", ^id}
  end

  test "a calls listen over :erpc hands a pid each call as {:flezha_emit, event}" do
    test = self()
    sink = spawn(fn -> forward_to(test) end)
    context = %{source: :internal, emit: sink}

    spawn(fn ->
      outcome = :erpc.call(node(), Relay, :listen, ["ns", "op", :calls, [], context])
      send(test, {:outcome, outcome})
    end)

    assert_receive {:attached, _id, receiver, [{:on_call, []}]}, 2_000
    assert_receive {:forwarded, :attached}, 2_000
    call_frame(receiver, "hello", "call", %{})
    assert_receive {:forwarded, {:call, "hello", %{"flz.call_id" => "id1"}}}, 2_000
    Process.exit(sink, :kill)
    assert_receive {:outcome, {:ok, :caller_down}}, 2_000
  end

  describe "a steps backlog" do
    setup do
      Application.put_env(:flezha, :listener_pending_frames, 8)
    end

    test "a slow reader loses parts, not outputs, and goes on" do
      %{pid: pid, receiver: receiver} = listening([], held_reader(self()))
      for n <- 0..99, do: part(receiver, "p#{n}", n)
      output(receiver, "answer")
      assert :sys.get_state(receiver).backlog.frames <= 8

      send(pid, :go)
      numbers = parts_until_output(pid, [])
      assert length(numbers) < 100
      assert numbers == Enum.sort(numbers)
      refute_received {:outcome, _}
      Process.exit(pid, :kill)
    end

    test "a stalled reader of a tally loses older totals, never the listener" do
      test = self()

      stalled = fn event ->
        send(test, {:event, self(), event})

        receive do
          :next -> :continue
        end
      end

      stub(RelayApi, :graph_of, fn _ns, _op -> {:ok, %{graph: %{channels: [out("a")]}}} end)
      %{pid: pid, receiver: receiver} = listening([direction: :tally], stalled)

      # Each report handed on at once, as its interval's tick would.
      for n <- 1..100 do
        send(receiver, {:tally_report, "op", 1, %{"a" => n}})
        send(receiver, :tally_due)
      end

      assert :sys.get_state(receiver).backlog.frames <= 8
      refute_received {:outcome, _}
      assert newest_tally(pid, 100) == 100
      Process.exit(pid, :kill)
    end

    test "a reader that falls behind on call events is ended with listener.overrun" do
      %{pid: pid, receiver: receiver, id: id} =
        listening([direction: :calls], held_reader(self()))

      for n <- 1..100, do: call_frame(receiver, "c#{n}", "call", %{})
      assert :sys.get_state(receiver).ended

      send(pid, :go)
      assert_receive {:outcome, {:error, %Flezha.Error{code: "listener.overrun"}}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}
    end

    test "a reader that falls behind on outputs is ended with listener.overrun" do
      %{pid: pid, receiver: receiver, id: id} = listening([], held_reader(self()))
      for n <- 1..100, do: output(receiver, "o#{n}")
      assert :sys.get_state(receiver).ended

      send(pid, :go)
      assert_receive {:outcome, {:error, %Flezha.Error{code: "listener.overrun"} = error}}, 2_000
      assert error.details["frames"] == 8
      assert_receive {:detached, "ns", "op", ^id}
    end
  end

  describe "a released plugin" do
    # Each check is fired by hand; the release has to arm the next one.
    defp release(receiver, id) do
      send(receiver, {:listener_released, id})
      assert Map.has_key?(:sys.get_state(receiver).timers, :release_check)
      for _ <- 1..2, do: send(receiver, :release_check)
    end

    test "a row still carrying it is a move: the listener goes on" do
      %{pid: pid, receiver: receiver, id: id} = listening([])
      stub(HubManager, :relay_bindings, fn "ns", "op" -> {:ok, [{id, nil}]} end)

      release(receiver, id)
      output(receiver, "after the move")
      assert_receive {:event, ^pid, {:output, "after the move", _}}, 2_000
      assert Map.has_key?(:sys.get_state(receiver).timers, :release_check)

      send(receiver, {:heartbeat_keepalive, self()})
      assert_receive {:heartbeat_keepalive, :ok}, 2_000
      refute :sys.get_state(receiver).released
      refute_received {:outcome, _}
      Process.exit(pid, :kill)
    end

    test "a registered relay without the row was disabled by someone else" do
      stub(HubManager, :relay_bindings, fn "ns", "op" -> {:ok, []} end)
      %{receiver: receiver, id: id} = listening([])
      release(receiver, id)

      assert_receive {:outcome, {:error, %Flezha.Error{code: "listener.detached"}}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}
    end

    test "a relay no longer registered ended, after what was held before it" do
      %{pid: pid, receiver: receiver, id: id} = listening([])

      stub(HubManager, :relay_bindings, fn "ns", "op" ->
        {:error, {:relay_not_found, "ns", "op"}}
      end)

      output(receiver, "last")
      release(receiver, id)

      assert_receive {:event, ^pid, {:output, "last", _}}, 2_000
      assert_receive {:outcome, {:ok, :relay_ended}}, 2_000
    end
  end

  describe "idle_ms" do
    test "a quiet listener is told it is idle, and stays attached" do
      test = self()

      counting = fn
        :idle -> send(test, {:idle, now()}) && :continue
        {:output, "still attached", _} -> :done
      end

      %{receiver: receiver, id: id} = listening([idle_ms: 10, window_ms: 3_600_000], counting)
      assert_receive {:idle, first}, 2_000
      assert_receive {:idle, second}, 2_000
      assert second - first >= 10
      refute_received {:detached, _, _, _}

      output(receiver, "still attached")
      assert_receive {:outcome, {:ok, :done}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}
    end

    test "an idle answer of :done ends the listener and detaches" do
      %{id: id} = listening([idle_ms: 1], fn :idle -> :done end)
      assert_receive {:outcome, {:ok, :done}}, 2_000
      assert_receive {:detached, "ns", "op", ^id}
    end

    # Parts handed over for several intervals back to back: an idle timed from
    # anything but the last event comes less than an interval after one.
    test "events reset the interval" do
      test = self()
      tick = fn kind -> send(test, {:tick, kind, now()}) && :continue end

      handler = fn
        :idle -> tick.(:idle)
        {:part, _, _} -> tick.(:part)
      end

      %{pid: pid, receiver: receiver} = listening([idle_ms: 10], handler)
      ticks = feed_parts(receiver, now() + 40, 0, [])

      for [{:tick, _, before}, {:tick, :idle, at}] <- Enum.chunk_every(ticks, 2, 1, :discard),
          do: assert(at - before >= 10)

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

  defp forward_to(test) do
    receive do
      {:flezha_emit, event} -> send(test, {:forwarded, event})
    end

    forward_to(test)
  end

  # Hands over one part at a time until `until`, returning every tick in order.
  defp feed_parts(receiver, until, n, ticks) do
    part(receiver, "p", n)
    ticks = await_part(ticks)
    if now() < until, do: feed_parts(receiver, until, n + 1, ticks), else: Enum.reverse(ticks)
  end

  defp await_part(ticks) do
    receive do
      {:tick, :part, _at} = tick -> [tick | ticks]
      {:tick, :idle, _at} = tick -> await_part([tick | ticks])
    after
      2_000 -> flunk("a part was never handled")
    end
  end

  # A reader that takes its first event and then nothing until it is sent `:go`.
  defp held_reader(test) do
    fn event ->
      send(test, {:event, self(), event})
      unless Process.get(:let_go), do: receive(do: (:go -> Process.put(:let_go, true)))
      :continue
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end

defmodule FlezhaTest.System.RelayListenE2ETest do
  @moduledoc """
  `flz relay listen` and `flz relay send --await` against live
  relays bound to a live runner, through `Flezha.Service.Cli.call/3` in
  process the way `flz` reaches it: what a named sink, a keyed sink and the
  input chain print, a partition group on one stream, the plugin gone however
  the listener ends, and the order `send --await` attaches in.

  The same verbs with the plugin manager stubbed are
  `test/flezha/cli/listen_test.exs`.

  Tagged `:system`; run with
  `mix test --include system test/system/relay_listen_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 3, force_fresh_rebind: 2, stop_namespace: 1, unique_id_path: 1]

  alias Flezha.Relay.{Builder, PartitionGroup}
  alias Flezha.Partition
  alias Flezha.Plugin
  alias Flezha.Relay.KeyGroup
  alias Flezha.Service.{Admin, Cli, RelayManager}
  alias FlezhaTest.Fixtures.RelayFixture

  # The listener, the plugin it enables and what detaches it are controller
  # mechanics; the reference runner's fixtures are what they need.
  @moduletag reference_runtime: true
  @moduletag timeout: 300_000

  setup_all do
    namespace = "listen_e2e_#{System.unique_integer([:positive])}"
    boot_runner(namespace, unique_id_path("listen_e2e"), [])
    on_exit(fn -> stop_namespace(namespace) end)
    %{namespace: namespace}
  end

  describe "flz relay listen" do
    test "a named sink's frame prints its channel and payload", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      listener = listen(ns, id)

      deliver!(ns, id, "hello")

      assert printed_line(listener, &(&1 == "echo\thello"))
      end_by_caller(ns, listener, [id])
    end

    @tag capability: :state_persistence,
         skip: FlezhaTest.SystemCase.deferral(:state_persistence)
    test "a keyed sink's frame carries its cell, seq and key", %{namespace: ns} do
      id = start!(ns, "bench_keyed")
      listener = listen(ns, id)

      deliver!(ns, id, "k1:v1")

      assert printed_line(listener, &(&1 =~ ~r/^sink\tk1=v1\tcell=\S+ seq=\S+ key=k1$/))
      end_by_caller(ns, listener, [id])
    end

    @tag capability: :state_persistence,
         skip: FlezhaTest.SystemCase.deferral(:state_persistence)
    test "json carries the channel, the payload and the whole metadata map", %{namespace: ns} do
      id = start!(ns, "bench_keyed")
      listener = listen(ns, id, format: "json")

      deliver!(ns, id, "k2:v2")

      line = printed_line(listener, &String.contains?(&1, "k2=v2"))

      assert %{"channel" => "sink", "payload" => "k2=v2", "metadata" => metadata} =
               Jason.decode!(line)

      assert %{"channel" => "sink", "flz.key" => "k2", "flz.cell" => _, "flz.seq" => _} =
               metadata

      end_by_caller(ns, listener, [id])
    end

    test "with --input it prints what the relay ingests", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      listener = listen(ns, id, direction: "input")

      deliver!(ns, id, "again")

      assert printed_line(listener, &(&1 == "-\tagain"))
      end_by_caller(ns, listener, [id])
    end

    @tag capability: :partitioned, skip: FlezhaTest.SystemCase.deferral(:partitioned)
    test "a partition group prints every member's frames, and releases every member", %{
      namespace: ns
    } do
      {group, members} = start_group!(ns)

      # One key per member, each sent to the member that owns it.
      owners = Map.new(["SOL-USD", "BTC-USD"], &{&1, owner(group, &1)})
      assert owners |> Map.values() |> Enum.sort() == Enum.sort(members)

      listener = listen(ns, group, on: members)

      for {key, member} <- owners, do: deliver!(ns, member, "#{key}:listened")

      for key <- Map.keys(owners) do
        assert printed_line(listener, &String.starts_with?(&1, "input\t#{key}:listened\tcell="))
      end

      end_by_caller(ns, listener, members)
    end

    @tag capability: :partitioned, skip: FlezhaTest.SystemCase.deferral(:partitioned)
    test "a killed loop is released by the heartbeat, on every member", %{namespace: ns} do
      {group, members} = start_group!(ns)
      %{pid: pid} = listen(ns, group, on: members)

      Process.exit(pid, :kill)

      # Each member holds a heartbeat of its own, so releasing one stops nothing
      # the next one still watches.
      log =
        capture_log(fn ->
          for member <- members do
            assert wait_until(fn -> listeners(ns, member) == [] end, 30_000),
                   "the heartbeat left a listener on #{member}"
          end
        end)

      refute log =~ "crashed"
    end

    test "the caller going away ends the loop and detaches", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      end_by_caller(ns, listen(ns, id), [id])
    end

    test "the window closes, and nothing is left behind", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      %{pid: pid, ref: ref} = listen(ns, id, window: 2_000)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 30_000
      assert listeners(ns, id) == []
    end
  end

  describe "flz relay listen --tally" do
    test "counts each output channel of a live relay, and leaves nothing behind", %{
      namespace: ns
    } do
      id = start!(ns, "bench_echo")
      listener = listen(ns, id, direction: "tally")

      for n <- 1..3, do: deliver!(ns, id, "t#{n}")

      assert printed_line(listener, &(&1 =~ ~r/^echo=3 \(/))
      end_by_caller(ns, listener, [id])
    end

    @tag capability: :partitioned, skip: FlezhaTest.SystemCase.deferral(:partitioned)
    test "a group's counts are summed over its members, and every member is released", %{
      namespace: ns
    } do
      {group, members} = start_group!(ns)
      owners = Map.new(["SOL-USD", "BTC-USD"], &{&1, owner(group, &1)})
      assert owners |> Map.values() |> Enum.sort() == Enum.sort(members)
      listener = listen(ns, group, direction: "tally", on: members)

      for {key, member} <- owners, do: deliver!(ns, member, "#{key}:tallied")

      assert printed_line(listener, &(&1 =~ ~r/(^|\t)[^=\t]+=2 \(/))
      end_by_caller(ns, listener, members)
    end

    test "an Overwatch restart counts again from zero under a new epoch", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      test = self()

      spawn(fn ->
        Flezha.Service.Relay.listen(ns, id, :tally, [window_ms: 120_000, tally_ms: 200], %{
          source: :internal,
          emit: test
        })
      end)

      assert_receive {:flezha_emit, :attached}, 30_000
      deliver!(ns, id, "before")
      {_, first} = tally_until(fn totals, _epochs -> totals["echo"] == 1 end)

      force_fresh_rebind(ns, id)
      assert wait_until(fn -> listeners(ns, id) != [] and bound?(ns, id) end, 30_000)
      deliver!(ns, id, "after")

      # A count that went on would read 2 here.
      {_, restarted} =
        tally_until(fn totals, epochs -> epochs != first and totals["echo"] == 1 end)

      # The worker counting for the killed Overwatch reports no more.
      for _ <- 1..3, do: assert({_, ^restarted} = tally_until(fn _, _ -> true end))
    end
  end

  # The first tally within 30 s that `match` takes, as its totals and epochs.
  defp tally_until(match, deadline \\ System.monotonic_time(:millisecond) + 30_000) do
    receive do
      {:flezha_emit, {:tally, totals, %{epochs: epochs}}} ->
        if match.(totals, epochs), do: {totals, epochs}, else: tally_until(match, deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> flunk("no tally matched")
    end
  end

  describe "flz relay listen --calls" do
    test "every surface's call and its answer reach a calls listener, paired by id", %{
      namespace: ns
    } do
      id = start!(ns, "bench_echo")
      console = watch_calls(ns, id)

      capture_io(fn ->
        Cli.call(
          "Relay.call_channel",
          Jason.encode!([
            ns,
            id,
            "",
            "from-cli",
            %{"deadline_ms" => 10_000, "payload" => "text"}
          ]),
          []
        )
      end)

      assert_paired("from-cli", "cli")

      assert {:ok, "from-ui", _} =
               :erpc.call(node(), Flezha.Service.Relay, :call, [
                 ns,
                 id,
                 "from-ui",
                 "",
                 [deadline_ms: 10_000],
                 %{source: :ui}
               ])

      assert_paired("from-ui", "ui")

      for source <- [:http, :mcp] do
        message = "from-#{source}"
        args = [ns, id, message, "", %{"deadline_ms" => 10_000}]

        assert {:ok, _json} =
                 Admin.call("Relay.call", args, %{source: source, principal: "e2e"}, :given)

        assert_paired(message, Atom.to_string(source))
      end

      # The console going away ends the listen, which detaches as it returns.
      Process.exit(console, :kill)
      assert wait_until(fn -> listeners(ns, id) == [] end, 30_000)
    end

    test "flz relay listen --calls prints the call line and the answer line", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      listener = listen(ns, id, direction: "calls")

      assert {:ok, "printed", _} =
               Flezha.Service.Relay.call(ns, id, "printed", "", [deadline_ms: 10_000], %{
                 source: :ui
               })

      call = printed_line(listener, &String.starts_with?(&1, "-\tprinted\tcall="))
      [_, _, "call=" <> rest] = String.split(call, "\t")
      [call_id, "source=ui"] = String.split(rest, " ")
      assert printed_line(listener, &(&1 =~ ~r/^echo\tprinted\tcall=#{call_id} answered ms=\d+$/))
      end_by_caller(ns, listener, [id])
    end
  end

  describe "flz relay send --await" do
    test "prints the delivery verdict, then what came back", %{namespace: ns} do
      id = start!(ns, "bench_echo")

      out = capture_io(fn -> send_input(ns, id, "hello", await_ms: 2_000) end)

      assert lines(out) == ["Delivered to '#{id}'.", "echo\thello"]
      assert listeners(ns, id) == []
    end

    test "a listener attached after the delivery answers misses the frame", %{namespace: ns} do
      id = start!(ns, "bench_echo", [forward_to(self())])

      deliver!(ns, id, "early")
      assert_receive {:message_forward, "early"}, 30_000

      %{pid: pid, ref: ref} = listener = listen(ns, id, window: 2_000)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 30_000
      assert printed(listener) == []
    end
  end

  defp start!(ns, factory, plugins \\ []) do
    id = "#{factory}_#{System.unique_integer([:positive])}"
    bindings = RelayFixture.bindings(ns, id, plugins)
    builder = %Builder{id: id, factory_id: factory, init_values: %{}, opts: [bindings: bindings]}

    assert {:ok, _} = RelayManager.start_relay(ns, builder)
    assert wait_until(fn -> bound?(ns, id) end, 30_000), "#{id} was never bound"
    id
  end

  defp start_group!(ns) do
    group = "listen_grp_#{System.unique_integer([:positive])}"
    builder = %Builder{id: group, factory_id: "partition_pass", init_values: %{}, opts: []}

    assert {:ok, %{members: members}} = PartitionGroup.start(ns, builder, 2)
    for member <- members, do: assert(wait_until(fn -> bound?(ns, member) end, 30_000))
    {group, members}
  end

  defp bound?(ns, id) do
    match?(
      {:ok, %{runner_id: runner}} when is_binary(runner),
      RelayManager.get_overwatch_state(ns, id)
    )
  end

  defp deliver!(ns, id, payload) do
    assert capture_io(fn -> send_input(ns, id, payload, []) end) =~ "Delivered"
  end

  defp send_input(ns, id, payload, opts),
    do: Cli.call("Relay.send", Jason.encode!([ns, id, payload, ""]), opts)

  # A listener the way `flz` runs one: a process of its own, whose group leader
  # stands in for the `flz` node and collects what it prints. Answers once the
  # plugin is on every relay in `on`.
  defp listen(ns, id, opts \\ []) do
    {:ok, device} = StringIO.open("")

    {pid, ref} =
      spawn_monitor(fn ->
        Process.group_leader(self(), device)

        Cli.call(
          "Relay.listen",
          Jason.encode!([
            ns,
            id,
            Keyword.get(opts, :direction, "output"),
            %{"window_ms" => Keyword.get(opts, :window, 60_000)}
          ]),
          output: Keyword.get(opts, :format, "text")
        )
      end)

    for target <- Keyword.get(opts, :on, [id]) do
      assert wait_until(fn -> listeners(ns, target) != [] end, 30_000),
             "no listener was attached to #{target}"
    end

    %{pid: pid, ref: ref, device: device}
  end

  # A calls listener the way the console holds one: `Relay.listen/5` with a
  # pid as `:emit`, here a stand-in for the console that hands every event on
  # to the test. Answers the stand-in, whose exit ends the listen.
  defp watch_calls(ns, id) do
    test = self()
    console = spawn(fn -> hand_on(test) end)

    spawn(fn ->
      Flezha.Service.Relay.listen(ns, id, :calls, [window_ms: 120_000], %{
        source: :internal,
        emit: console
      })
    end)

    assert_receive {:flezha_emit, :attached}, 30_000
    console
  end

  defp hand_on(test) do
    receive do
      message -> send(test, message)
    end

    hand_on(test)
  end

  defp assert_paired(message, source) do
    assert_receive {:flezha_emit, {:call, ^message, %{"flz.call_id" => id} = called}}, 30_000
    assert called["flz.call_source"] == source

    assert_receive {:flezha_emit, {:answered, ^message, %{"flz.call_id" => ^id} = answered}},
                   30_000

    assert %{"channel" => "echo", "flz.call_duration_ms" => ms} = answered
    assert is_integer(ms)
  end

  # The way `flz` ends a listen: its node, the loop's group leader, goes away,
  # and the loop detaches as it returns.
  defp end_by_caller(ns, %{pid: pid, ref: ref, device: device}, targets) do
    Process.exit(device, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 30_000

    for target <- targets do
      assert wait_until(fn -> listeners(ns, target) == [] end, 30_000),
             "a listener was left on #{target}"
    end
  end

  # The listener plugins `flz relay info --json` shows on `id`.
  defp listeners(ns, id) do
    %{"relays" => relays} =
      Jason.decode!(
        capture_io(fn -> Cli.call("Relay.info", Jason.encode!([ns]), output: "json") end)
      )

    for %{"id" => ^id, "plugins" => plugins} <- relays,
        %{"id" => "io_listener_" <> _ = plugin} <- plugins,
        do: plugin
  end

  defp printed(%{device: device}), do: device |> StringIO.contents() |> elem(1) |> lines()

  defp printed_line(listener, match) do
    wait_until(fn -> Enum.find(printed(listener), match) end, 30_000) ||
      flunk("never printed; printed #{inspect(printed(listener))}")
  end

  defp lines(text), do: String.split(text, "\n", trim: true)

  defp owner(group, key),
    do: Partition.member_id(group, Partition.owner(Partition.ranges(2), KeyGroup.group_of(key)))

  defp forward_to(pid) do
    %Plugin{
      id: "e2e_sink",
      namespace: nil,
      relay_id: nil,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: pid},
      hooks: [{:on_output, []}]
    }
  end
end

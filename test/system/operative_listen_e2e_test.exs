defmodule FlezhaTest.System.OperativeListenE2ETest do
  @moduledoc """
  `flz operative listen` and `flz operative send --await` against live
  operatives bound to a live runner, through `CliManager` in process the way
  `flz` reaches it: what a named sink, a keyed sink and the input chain print,
  a partition group on one stream, the plugin gone however the listener ends,
  and the order `send --await` attaches in.

  The same verbs with the plugin manager stubbed are
  `test/flezha/cli/cli_manager_listen_test.exs`.

  Tagged `:system`; run with
  `mix test --include system test/system/operative_listen_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 3, stop_namespace: 1, unique_id_path: 1]

  alias Flezha.Operative.{Builder, PartitionGroup}
  alias Flezha.Partition
  alias Flezha.Plugin
  alias Flezha.Service.{CliManager, OperativeManager}

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

  describe "flz operative listen" do
    test "a named sink's frame prints its channel and payload", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      listener = listen(ns, id)

      deliver!(ns, id, "hello")

      assert printed_line(listener, &(&1 == "echo\thello"))
      end_by_exit(ns, listener, [id])
    end

    @tag capability: :state_persistence,
         skip: FlezhaTest.SystemCase.deferral(:state_persistence)
    test "a keyed sink's frame carries its entity, seq and key", %{namespace: ns} do
      id = start!(ns, "bench_keyed")
      listener = listen(ns, id)

      deliver!(ns, id, "k1:v1")

      assert printed_line(listener, &(&1 =~ ~r/^sink\tk1=v1\tentity=\S+ seq=\S+ key=k1$/))
      end_by_exit(ns, listener, [id])
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

      assert %{"channel" => "sink", "flz.key" => "k2", "flz.entity" => _, "flz.seq" => _} =
               metadata

      end_by_exit(ns, listener, [id])
    end

    test "with --input it prints what the operative ingests", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      listener = listen(ns, id, direction: "input")

      deliver!(ns, id, "again")

      assert printed_line(listener, &(&1 == "-\tagain"))
      end_by_exit(ns, listener, [id])
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
        assert printed_line(listener, &String.starts_with?(&1, "input\t#{key}:listened\tentity="))
      end

      # Each member holds a heartbeat of its own, so releasing one stops nothing
      # the next one still watches.
      refute capture_log(fn -> end_by_exit(ns, listener, members) end) =~ "crashed"
    end

    @tag capability: :partitioned, skip: FlezhaTest.SystemCase.deferral(:partitioned)
    test "a killed loop is released by the heartbeat, on every member", %{namespace: ns} do
      {group, members} = start_group!(ns)
      %{pid: pid} = listen(ns, group, on: members)

      Process.exit(pid, :kill)

      for member <- members do
        assert wait_until(fn -> listeners(ns, member) == [] end, 30_000),
               "the heartbeat left a listener on #{member}"
      end
    end

    test "the caller going away ends the loop and detaches", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      %{pid: pid, ref: ref, device: device} = listen(ns, id)

      Process.exit(device, :kill)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 30_000
      assert wait_until(fn -> listeners(ns, id) == [] end, 30_000)
    end

    test "the window closes, and nothing is left behind", %{namespace: ns} do
      id = start!(ns, "bench_echo")
      %{pid: pid, ref: ref} = listen(ns, id, window: 2_000)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 30_000
      assert listeners(ns, id) == []
    end
  end

  describe "flz operative send --await" do
    test "prints the delivery verdict, then what came back", %{namespace: ns} do
      id = start!(ns, "bench_echo")

      out = capture_io(fn -> CliManager.operative_send(ns, id, "hello", "", await_ms: 2_000) end)

      assert lines(out) == ["Delivered to '#{id}'.", "echo\thello"]
      assert listeners(ns, id) == []
    end

    test "with nothing emitted it prints the verdict alone", %{namespace: ns} do
      id = start!(ns, "compare_filter")

      out = capture_io(fn -> CliManager.operative_send(ns, id, "nope", "", await_ms: 500) end)

      assert lines(out) == ["Delivered to '#{id}'."]
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
    builder = %Builder{id: id, factory_id: factory, init_values: %{}, opts: [plugins: plugins]}

    assert {:ok, _} = OperativeManager.start_operative(ns, builder)
    assert wait_until(fn -> bound?(ns, id) end, 30_000), "#{id} was never bound"
    id
  end

  defp start_group!(ns) do
    group = "listen_grp_#{System.unique_integer([:positive])}"
    builder = %Builder{id: group, factory_id: "partition_relay", init_values: %{}, opts: []}

    assert {:ok, %{members: members}} = PartitionGroup.start(ns, builder, 2)
    for member <- members, do: assert(wait_until(fn -> bound?(ns, member) end, 30_000))
    {group, members}
  end

  defp bound?(ns, id) do
    match?(
      {:ok, %{runner_id: runner}} when is_binary(runner),
      OperativeManager.get_overwatch_state(ns, id)
    )
  end

  defp deliver!(ns, id, payload) do
    assert capture_io(fn -> CliManager.operative_send(ns, id, payload, "", []) end) =~ "Delivered"
  end

  # A listener the way `flz` runs one: a process of its own, whose group leader
  # stands in for the `flz` node and collects what it prints. Answers once the
  # plugin is on every operative in `on`.
  defp listen(ns, id, opts \\ []) do
    {:ok, device} = StringIO.open("")

    {pid, ref} =
      spawn_monitor(fn ->
        Process.group_leader(self(), device)

        CliManager.operative_listen(
          ns,
          id,
          Keyword.get(opts, :direction, "output"),
          Keyword.get(opts, :format, "text"),
          duration_ms: Keyword.get(opts, :window, 60_000)
        )
      end)

    for target <- Keyword.get(opts, :on, [id]) do
      assert wait_until(fn -> listeners(ns, target) != [] end, 30_000),
             "no listener was attached to #{target}"
    end

    %{pid: pid, ref: ref, device: device}
  end

  # The third route out: an exit signal to the loop's own process.
  defp end_by_exit(ns, %{pid: pid, ref: ref}, targets) do
    Process.exit(pid, :shutdown)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 30_000

    for target <- targets do
      assert wait_until(fn -> listeners(ns, target) == [] end, 30_000),
             "a listener was left on #{target}"
    end
  end

  # The listener plugins `flz operative info --json` shows on `id`.
  defp listeners(ns, id) do
    %{"operatives" => operatives} =
      Jason.decode!(capture_io(fn -> CliManager.operative_info(ns, "json", []) end))

    for %{"id" => ^id, "plugins" => plugins} <- operatives,
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
    do: Partition.member_id(group, Partition.owner(Partition.ranges(2), Partition.group_of(key)))

  defp forward_to(pid) do
    %Plugin{
      id: "e2e_sink",
      namespace: nil,
      operative_id: nil,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: pid},
      hooks: [{:on_output, []}]
    }
  end
end

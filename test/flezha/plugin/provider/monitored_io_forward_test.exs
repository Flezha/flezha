defmodule FlezhaTest.Plugin.Provider.MonitoredIOForwardTest do
  @moduledoc """
  The heartbeat interval is a declared option, converted before anything is
  started. In tally mode the plugin counts outputs per channel and its worker
  reports the totals, sending nothing per frame.

  It reaches `Process.send_after/3` inside the worker's `init/1`, so a value the
  declaration did not convert would reach a live process rather than a refusal.
  Preparation now starts nothing at all, which is the stronger form of the same
  guarantee: a value that cannot be interpreted is refused with no process in
  existence to crash.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  defp opts(extra) do
    Map.merge(
      %{recv_pid: self(), plugin_id: "mio-test", relay_id: "op", namespace: "ns"},
      extra
    )
  end

  defp prepare(extra) do
    with {:ok, opts} <- ProviderOptions.normalise_opts(MonitoredIOForward, opts(extra)),
         do: MonitoredIOForward.init_provider(opts)
  end

  test "a heartbeat interval it cannot interpret is refused, and nothing is linked" do
    Process.flag(:trap_exit, true)

    assert {:error, {:invalid_option_value, :heartbeat_interval, "abc"}} =
             prepare(%{heartbeat_interval: "abc"})

    refute_received {:EXIT, _pid, _reason}
  end

  test "the interval is accepted as an integer and as its CLI string" do
    for {value, expected} <- [{250, 250}, {"250", 250}] do
      assert {:ok, prepared} = prepare(%{heartbeat_interval: value})
      assert prepared.heartbeat_interval == expected
    end
  end

  test "preparation holds no process, and the declared worker is what runs" do
    assert {:ok, prepared} = prepare(%{})
    assert prepared.heartbeat_interval == 5000

    assert %{module: MonitoredIOForward, restart: :temporary} =
             MonitoredIOForward.__provider__().worker

    assert [{{nil, "mio-test"}, _worker_opts}] = MonitoredIOForward.worker_specs(prepared)
  end

  test "each member of a partition group holds a heartbeat of its own" do
    assert {:ok, prepared} = prepare(%{})

    keys =
      for index <- [0, 1] do
        [{key, _opts}] =
          MonitoredIOForward.worker_specs(Map.put(prepared, :partition_index, index))

        key
      end

    assert keys == [{{nil, "mio-test"}, 0}, {{nil, "mio-test"}, 1}]
  end

  describe "tally mode" do
    test "each output adds to its channel, an undeclared or unnamed one to \"\", and nothing is sent" do
      prepared = tally(tally_ms: 100)

      for {channel, n} <- [{"alerts", 10}, {"orders", 3}, {"elsewhere", 2}, {nil, 1}],
          _ <- 1..n do
        assert {:ok, %Ctx.IO{message: "m"}} =
                 MonitoredIOForward.on_output(frame(channel), prepared)
      end

      refute_received _anything
      start_worker(prepared)
      epoch = prepared.tally.epoch

      assert_receive {:tally_report, "op#p0", ^epoch, totals}, 1_000
      assert totals == %{"alerts" => 10, "orders" => 3, "" => 3}
    end

    test "a thousand outputs within one interval are one report" do
      prepared = tally(tally_ms: 60_000)
      for _ <- 1..1_000, do: MonitoredIOForward.on_output(frame("alerts"), prepared)

      # The interval's tick, sent now rather than waited for.
      send(start_worker(prepared), :tally_report)

      assert_receive {:tally_report, _member, _epoch, %{"alerts" => 1_000}}, 1_000
      refute_received {:tally_report, _, _, _}
      refute_received {:message_forward, _}
    end

    test "preparing it again restarts the count under a new epoch" do
      first = tally()
      MonitoredIOForward.on_output(frame("alerts"), first)

      # A restart re-prepares from the options the table holds, its counters among them.
      assert {:ok, held} = ProviderOptions.normalise_opts(MonitoredIOForward, first)
      assert {:ok, again} = MonitoredIOForward.init_provider(held)

      assert again.tally.epoch != first.tally.epoch
      assert :counters.get(again.tally.counters, again.tally.index["alerts"]) == 0
      assert :counters.get(first.tally.counters, first.tally.index["alerts"]) == 1
    end

    test "a tally's worker is keyed by its epoch, and goes with the Overwatch counting" do
      overwatch = spawn(fn -> Process.sleep(:infinity) end)
      prepared = tally(overwatcher_pid: overwatch)
      assert [{{{nil, "mio-test"}, epoch}, _opts}] = MonitoredIOForward.worker_specs(prepared)
      assert epoch == prepared.tally.epoch

      worker = start_worker(prepared)
      ref = Process.monitor(worker)
      Process.exit(overwatch, :kill)
      assert_receive {:DOWN, ^ref, :process, ^worker, :normal}, 1_000
    end

    test "a forwarding plugin keeps no counters and forwards each output" do
      assert {:ok, prepared} = prepare(%{})
      assert prepared.mode == "forward" and prepared.tally == nil
      MonitoredIOForward.on_output(frame("alerts"), prepared)
      assert_received {:message_forward, {"m", %{"channel" => "alerts"}}}
    end
  end

  defp tally(extra \\ []) do
    tally = %{mode: "tally", channels: ["alerts", "orders"], source_relay_id: "op#p0"}
    assert {:ok, prepared} = prepare(Map.merge(tally, Map.new(extra)))
    prepared
  end

  defp frame(nil), do: %Ctx.IO{message: "m"}
  defp frame(channel), do: %Ctx.IO{message: "m", metadata: %{"channel" => channel}}

  defp start_worker(prepared),
    do:
      start_supervised!(
        Supervisor.child_spec({MonitoredIOForward, Map.put(prepared, :name, nil)},
          restart: :temporary
        )
      )
end

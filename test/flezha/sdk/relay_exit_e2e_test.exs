defmodule Flezha.SDK.RelayExitE2ETest do
  @moduledoc """
  In-node end to end: an Elixir instance whose process dies, or whose Cell
  dies holding unfinished input, is reported by the runner, parked by its
  Overwatch and rebound by the convergence loop, which restores its Cells and
  re-forwards the input log above the confirmed floor.
  """
  # async: false: the live controller and its fleet; registered singletons.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Config.Backoff
  alias Flezha.Convergence.{Facts, Loop}
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{HubManager, RelayManager}
  alias Flezha.State.{Completion, InputLog, TestHelpers}
  alias FlezhaTest.Helper.NodeTestHelper

  @probe :relay_exit_e2e_probe

  @doc false
  def probe_run(message) do
    case Process.whereis(@probe) do
      nil -> :ok
      pid -> send(pid, message)
    end
  end

  # Every run it makes goes to the test with the offset it took and the state
  # it started from; a `:hold` input waits for the test inside `run/3`.
  defmodule Ledger do
    use Flezha.SDK.Tasklet, name: "ledger"

    @impl true
    def init_state(_key), do: []

    defchange took(s, value) do
      s ++ [value]
    end

    @impl true
    def run(input, s, ctx) do
      {_cell, offset} = ctx.origin
      Flezha.SDK.RelayExitE2ETest.probe_run({:ran, input, offset, s})

      # A poison input kills its Cell through a linked exit, which the ladder
      # cannot catch, until the test takes it off the path.
      if String.ends_with?(input, ":poison") and :persistent_term.get(:exit_e2e_poison, false) do
        Flezha.SDK.RelayExitE2ETest.probe_run({:poisoned, System.monotonic_time(:millisecond)})
        spawn_link(fn -> exit(:boom) end)
        Process.sleep(:infinity)
      end

      if String.ends_with?(input, ":hold") do
        Flezha.SDK.RelayExitE2ETest.probe_run({:holding, input, self()})

        receive do
          :release -> :ok
        after
          30_000 -> :ok
        end
      end

      {:ok, took(s, ctx, input)}
    end
  end

  defmodule LedgerOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "exit_ledger"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> key_by(&String.first/1) |> into(Ledger)
    end
  end

  # The first stage waits for the test; the run completes behind it.
  defmodule Gate do
    use Flezha.SDK.Tasklet, name: "gate"

    @impl true
    def run(input, s, ctx) do
      Flezha.SDK.RelayExitE2ETest.probe_run({:gate, self()})

      receive do
        :release -> :ok
      after
        30_000 -> :ok
      end

      emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule Finish do
    use Flezha.SDK.Tasklet, name: "finish"

    @impl true
    def run(_input, s, _ctx), do: {:ok, s}
  end

  defmodule GateSteps do
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def factory_id, do: "exit_gate_steps"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(Gate) |> named("gated") |> into(Finish)
    end
  end

  setup do
    ns = "relay_exit_#{System.unique_integer([:positive])}"
    Process.register(self(), @probe)
    ConvergenceSnapshot.reset_recovery_states()

    handle =
      Flezha.SDK.create_runner(ns, transport: :native)
      |> Flezha.SDK.register_factory(LedgerOp)
      |> Flezha.SDK.register_factory(GateSteps)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle, "SDK start failed: #{inspect(handle)}"

    on_exit(fn ->
      Flezha.SDK.close(handle)

      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    assert NodeTestHelper.wait_until(
             fn ->
               HubManager.hub_alive?(ns) and
                 FleetManager.runners_for_factory(ns, "exit_ledger") != [] and
                 FleetManager.runners_for_factory(ns, "exit_gate_steps") != []
             end,
             15_000
           )

    start_supervised!({Loop, tick_ms: 200})
    {:ok, ns: ns}
  end

  defp start_ledger(ns) do
    relay_id = "exit_ledger_#{System.unique_integer([:positive])}"
    builder = %Builder{id: relay_id, factory_id: "exit_ledger", init_values: %{}, opts: []}
    assert {:ok, _info} = RelayManager.start_relay(ns, builder)
    relay_id
  end

  defp send_in(ns, relay_id, input),
    do: RelayManager.send_io(relay_id, ns, input, %{"channel" => "in", "input_type" => "2"})

  # The process the runner has registered for the instance.
  defp instance_pid(relay_id) do
    case Map.get(:sys.get_state(Flezha.SDK.RunnerServer).relays, relay_id) do
      {pid, _factory} -> pid
      nil -> nil
    end
  end

  defp serving?(ns, relay_id) do
    match?(
      {:ok, %{init_error: nil, runner_id: runner}} when is_binary(runner),
      RelayManager.get_overwatch_state(ns, relay_id)
    )
  end

  # Rebound: a live process other than `old` is registered, and the Overwatch
  # serves it again.
  defp rebound?(ns, relay_id, old) do
    pid = instance_pid(relay_id)
    is_pid(pid) and pid != old and Process.alive?(pid) and serving?(ns, relay_id)
  end

  test "a killed Relay is rebound and takes input again", %{ns: ns} do
    relay_id = start_ledger(ns)
    send_in(ns, relay_id, "k:1")
    assert_receive {:ran, "k:1", _offset, []}, 10_000

    old = instance_pid(relay_id)

    # A Cell caught writing its slot as its Relay dies may crash on the
    # Relay's table; whether one is caught is timing, so nothing is asserted.
    capture_log(fn ->
      Process.exit(old, :kill)

      assert NodeTestHelper.wait_until(fn -> rebound?(ns, relay_id, old) end, 15_000),
             "the instance was never bound to a new Relay"
    end)

    send_in(ns, relay_id, "k:2")
    assert_receive {:ran, "k:2", _offset, ["k:1"]}, 10_000
  end

  test "inputs a dead Cell held run again after the rebind, and nothing else does", %{ns: ns} do
    relay_id = start_ledger(ns)
    send_in(ns, relay_id, "k:1")
    assert_receive {:ran, "k:1", _o1, []}, 10_000

    send_in(ns, relay_id, "k:hold")
    assert_receive {:ran, "k:hold", o2, ["k:1"]}, 10_000
    assert_receive {:holding, "k:hold", cell}, 10_000
    send_in(ns, relay_id, "k:3")
    send_in(ns, relay_id, "k:4")
    send_in(ns, relay_id, "j:5")
    assert_receive {:ran, "j:5", o5, []}, 10_000

    # `j`'s record is durable before `k`'s Cell dies.
    assert NodeTestHelper.wait_until(
             fn -> Enum.any?(InputLog.cell_offsets(relay_id), &(elem(&1, 1) == o5)) end,
             10_000
           )

    old = instance_pid(relay_id)

    {_, log} =
      with_log(fn ->
        Process.exit(cell, :kill)
        assert_receive {:ran, "k:hold", ^o2, ["k:1"]}, 15_000
      end)

    assert log =~ "{:cell_exited, :killed}"
    assert_receive {:holding, "k:hold", again}, 5_000
    send(again, :release)
    assert_receive {:ran, "k:3", o3, ["k:1", "k:hold"]}, 5_000
    assert_receive {:ran, "k:4", o4, ["k:1", "k:hold", "k:3"]}, 5_000
    assert o2 < o3 and o3 < o4 and o4 < o5
    refute_receive {:ran, _, _, _}, 500

    assert rebound?(ns, relay_id, old)

    assert NodeTestHelper.wait_until(fn -> InputLog.watermark(relay_id) == o5 end, 10_000),
           "the confirmed floor never reached #{o5}: #{inspect(InputLog.watermark(relay_id))}"
  end

  test "an input that kills its Cell on every bind is retried on the rung, and the ladder clears once it stays up",
       %{ns: ns} do
    on_exit(TestHelpers.override_env(:plugin_stable_uptime_ms, 1_000))
    :persistent_term.put(:exit_e2e_poison, true)
    on_exit(fn -> :persistent_term.erase(:exit_e2e_poison) end)

    relay_id = start_ledger(ns)

    {[_first, second, third, fourth], log} =
      with_log(fn ->
        send_in(ns, relay_id, "k:poison")

        poisoned =
          for _ <- 1..4 do
            assert_receive {:poisoned, at}, 20_000
            at
          end

        # Each bind reports its run before the poison bites, so the fifth
        # report is the bind after the fourth crash, the one that stays up.
        :persistent_term.put(:exit_e2e_poison, false)
        for _ <- 1..5, do: assert_receive({:ran, "k:poison", _offset, []}, 20_000)
        poisoned
      end)

    assert third - second >= Backoff.delay(:heal, 1)
    assert fourth - third >= Backoff.delay(:heal, 2)
    assert NodeTestHelper.occurrences(log, "** (stop) {:cell_exited, :boom}") == 4

    assert NodeTestHelper.wait_until(
             fn ->
               loop = :sys.get_state(Loop)

               not Map.has_key?(loop.facts, {:relay_parked, {ns, relay_id}}) and
                 Backoff.attempt(loop.backoff, Facts.key({:relay_parked, {ns, relay_id}})) ==
                   0
             end,
             15_000
           ),
           "the exit park never cleared after the instance stayed up"
  end

  test "a durable steps run whose driver is killed resumes and completes", %{ns: ns} do
    relay_id = "exit_gate_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: relay_id,
      factory_id: "exit_gate_steps",
      run: :steps,
      init_values: %{},
      opts: [seed_input: {"go", %{}}]
    }

    assert {:ok, _info} = RelayManager.start_relay(ns, builder)
    assert_receive {:gate, _cell}, 10_000

    old = instance_pid(relay_id)

    # As in the first test, a Cell caught mid-write may crash on the table.
    capture_log(fn ->
      Process.exit(old, :kill)
      assert_receive {:gate, cell}, 15_000
      send(cell, :release)
    end)

    assert NodeTestHelper.wait_until(
             fn -> match?({:ok, %{status: "ok"}}, Completion.read(relay_id)) end,
             15_000
           ),
           "the resumed run never completed: #{inspect(Completion.read(relay_id))}"
  end
end

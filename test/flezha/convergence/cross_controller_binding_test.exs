defmodule FlezhaTest.Convergence.CrossControllerBinding do
  use ExUnit.Case

  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Service.RelayManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.StepsResumeHarness
  alias FlezhaTest.Helper.Telemetry

  @ns "test_temp_namespace"

  setup do
    TestHelpers.reset_all_cells()
    ConvergenceSnapshot.reset_recovery_states()
    on_exit(fn -> Flezha.Runner.FleetManager.stop_namespace(@ns) end)
    :ok
  end

  # Each controller's loop places only the relays it hosts: with a runner on
  # both, this controller's loop never plans an op for the relay the peer hosts.
  @tag :multi_node
  @tag timeout: 120_000
  test "a healthy controller's loop leaves the relays another controller hosts alone" do
    {peer_pid, peer} = NodeTestHelper.start_healing_peer(:cross_healthy)

    _local_runner = NodeTestHelper.start_runner(@ns)
    {peer_runner, _os_pid} = NodeTestHelper.start_runner_remote_with_pid(peer, @ns)
    collector = spawn_link(fn -> StepsResumeHarness.collect_loop([]) end)
    op = StepsResumeHarness.start_steps_on_peer(peer, @ns, "keyed_loop_steps", collector)

    send_trade(op, "SOL-USD:1")

    assert NodeTestHelper.wait_until(
             fn -> StepsResumeHarness.committed_iters(op, peer) == [0] end,
             30_000
           )

    assert bound_runner(op) == peer_runner

    Telemetry.capture([[:flezha, :convergence, :heal], [:flezha, :convergence, :tick]],
      as: fn event, _measurements, meta -> {:loop_event, event, meta} end
    )

    # The loop emits each tick's heals before the tick itself, so five ticks
    # received carry every op this loop planned in them, past the misbound streak.
    heals = await_ticks(5, [])
    refute Enum.any?(heals, &(&1.target == op)), "this loop healed the peer's relay"
    assert bound_runner(op) == peer_runner

    # Not judged here, so not reported failed here either.
    assert %{state: :unknown} =
             Enum.find_value(ConvergenceSnapshot.current().relay_health, fn
               {{:relay, {_ns, ^op}}, verdict} -> verdict
               _ -> nil
             end)

    NodeTestHelper.stop_peer_node(peer_pid, peer)
  end

  defp await_ticks(0, heals), do: heals

  defp await_ticks(n, heals) do
    receive do
      {:loop_event, [:flezha, :convergence, :tick], _meta} -> await_ticks(n - 1, heals)
      {:loop_event, [:flezha, :convergence, :heal], meta} -> await_ticks(n, [meta | heals])
    after
      10_000 -> flunk("the loop stopped ticking")
    end
  end

  defp send_trade(op, text), do: StepsResumeHarness.send_turn(@ns, op, text, "trades")

  defp bound_runner(op) do
    case RelayManager.get_overwatch_state(@ns, op) do
      {:ok, %{runner_id: rid}} -> rid
      _ -> nil
    end
  end
end

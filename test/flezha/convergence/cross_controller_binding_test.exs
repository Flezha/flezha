defmodule FlezhaTest.Convergence.CrossControllerBinding do
  use ExUnit.Case

  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Init
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

    # Stopped while the peer still hosts it: stopping the peer first moves it
    # onto this node while the teardown stops this node's runner.
    NodeTestHelper.stop_namespace(@ns)
    NodeTestHelper.stop_peer_node(peer_pid, peer)
  end

  # Fails when: the bind this controller sends after the move arms the class's
  # whole window again, or reads a window end only the peer holds.
  # `keyed_loop_steps` declares 600 s, so the run ends here only because the
  # window counts from its first bind, on the peer.
  @tag :multi_node
  @tag timeout: 240_000
  test "a move whose new bind this controller sends keeps the window" do
    # An awake steps relay refuses consent, so its move off the peer is the
    # forced one at `max_defer_ms`. The hub reads it at its start, so the peer
    # takes it from this node's env as it boots.
    on_exit(
      TestHelpers.override_env(:migration_consent, max_defer_ms: 1_000, retry_interval_ms: 250)
    )

    {peer_pid, peer} = NodeTestHelper.start_healing_peer(:window_move)
    :erpc.call(peer, Application, :put_env, [:flezha, :group_withdraw_grace_ms, 500])
    _local_runner = NodeTestHelper.start_runner(@ns)
    {_peer_runner, os_pid} = NodeTestHelper.start_runner_remote_with_pid(peer, @ns)
    collector = spawn_link(fn -> StepsResumeHarness.collect_loop([]) end)
    op = StepsResumeHarness.start_steps_on_peer(peer, @ns, "keyed_loop_steps", collector)

    send_trade(op, "SOL-USD:1")

    assert NodeTestHelper.wait_until(
             fn -> StepsResumeHarness.committed_iters(op, peer) == [0] end,
             30_000
           )

    assert NodeTestHelper.wait_until(fn -> Init.window_end(@ns, op) != nil end, 10_000),
           "the peer's first bind recorded no window end"

    ends_at = Init.window_end(@ns, op)
    assert :erpc.call(peer, Init, :window_end, [@ns, op]) == ends_at

    StepsResumeHarness.spend_window(@ns, op)
    {_, 0} = System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)
    StepsResumeHarness.await_status(op, "cancelled", 150_000)

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

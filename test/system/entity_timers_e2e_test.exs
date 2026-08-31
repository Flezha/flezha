defmodule FlezhaTest.System.EntityTimersE2ETest do
  @moduledoc """
  End-to-end proof for per-Entity timers (`entity-timers`) against a live
  runner: an input arms an idle deadline, the quiet period
  elapses, the Operative's timer wheel fires it, and `on_timer`'s Change shows
  up in the Entity's state — with no further input to carry it.

  That last clause is the whole point. Every other stream path in the system
  is input-driven; this is the only one where a Procedure acts because time
  passed. A test that sent a second input to "wake" it would prove nothing.

  The second case re-seats the Entity from disk mid-quiet-period. The pending
  timer is folded state, so the restore rebuilds it from the same change log the
  Entity's state comes back through, re-arms the wheel, and the session still
  closes — on a fresh SDK-side instance, from a timer nobody re-registered.

  Tagged `:system` (boots a real subprocess); the Java arm additionally needs
  Run with `mix test --include system test/system/entity_timers_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :entity_timers

  alias Flezha.Payloader
  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.TierE2EHarness

  @factory "session_timeout"
  @procedure "session_timeout_procedure"

  # Short enough that the test waits on it, long enough that the crash case can
  # kill the runner before it fires.
  @timeout_ms "600"

  @tag :entity_timers_e2e
  test "a quiet session is closed by its own idle timer" do
    run_fires_case()
  end

  @tag :entity_timers_e2e
  test "a pending timer survives a re-seat from disk and still fires" do
    run_crash_case()
  end

  # One input, then silence. The only thing that can produce a `closed_at` is
  # the wheel firing `on_timer`.
  defp run_fires_case do
    namespace = "timers_fire"
    {port, ce_id} = start_runner(namespace)
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "timer_e2e_#{System.unique_integer([:positive])}"
    assert {:ok, info} = start_operative(namespace, operative_id)
    assert info.runner_id == ce_id
    await_readable(namespace, operative_id)

    OperativeManager.send_io(operative_id, namespace, "hello", %{"input_type" => "2"})

    assert wait_until(fn -> touched(namespace, operative_id) == ["hello"] end, 15_000),
           "the input must reach the Entity before the deadline is asserted on"

    assert wait_until(fn -> closed_at(namespace, operative_id) != [] end, 15_000),
           "the idle timer must fire with no further input, got #{inspect(state_of(namespace, operative_id))}"

    [fired_at] = closed_at(namespace, operative_id)

    assert is_integer(fired_at) and fired_at > 0,
           "`on_timer` records the SCHEDULED timestamp it was handed"
  end

  # Re-seat the Entity from disk while the timer is still pending. Killing the
  # Overwatch drives a fresh bind: the controller re-issues `AddOperative`,
  # which rebuilds the SDK-side instance — new slots, new wheel, no in-memory
  # pending set — and then `RestoreInitState`. So the only thing that can still
  # close the session is a timer folded back out of the change log.
  defp run_crash_case do
    namespace = "timers_crash"
    {port, _ce_id} = start_runner(namespace)
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "timer_crash_e2e_#{System.unique_integer([:positive])}"
    assert {:ok, _info} = start_operative(namespace, operative_id)
    await_readable(namespace, operative_id)

    OperativeManager.send_io(operative_id, namespace, "hello", %{"input_type" => "2"})

    assert wait_until(fn -> touched(namespace, operative_id) == ["hello"] end, 15_000),
           "the registration must be durable before the Entity is re-seated; state=#{inspect(state_of(namespace, operative_id))}"

    TierE2EHarness.force_fresh_rebind(namespace, operative_id)

    assert wait_until(fn -> touched(namespace, operative_id) == ["hello"] end, 30_000),
           "the restore must rebuild the Entity before its timer is asserted on"

    assert wait_until(fn -> closed_at(namespace, operative_id) != [] end, 30_000),
           "the restored Entity still owes the timer, and firing it needs no new input"
  end

  # `send_io` is fire-and-forget: an input sent before the Entity is live is
  # simply gone (this class has no input log and no durable inbox), and the
  # timer that input would have armed with it.
  defp await_readable(namespace, operative_id) do
    assert wait_until(fn -> state_of(namespace, operative_id) != nil end, 30_000),
           "the Entity must be live before an input is sent at it"
  end

  defp start_runner(namespace) do
    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(namespace)
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    NodeTestHelper.start_runner_port(namespace,
      env: [{"TIMER_TEST_TIMEOUT_MS", @timeout_ms}]
    )
  end

  defp start_operative(namespace, operative_id) do
    result =
      OperativeManager.start_operative(namespace, %Builder{
        id: operative_id,
        factory_id: @factory,
        init_values: %{},
        opts: []
      })

    result
  end

  defp state_of(namespace, operative_id) do
    case OperativeManager.get_entity_state(namespace, operative_id) do
      {:ok, json} ->
        case Payloader.json_to_map(json) do
          %{@procedure => entity} -> entity
          other -> other
        end

      _ ->
        nil
    end
  end

  defp touched(namespace, operative_id) do
    case state_of(namespace, operative_id) do
      %{"touched" => t} -> t
      _ -> nil
    end
  end

  defp closed_at(namespace, operative_id) do
    case state_of(namespace, operative_id) do
      %{"closedAt" => c} when is_list(c) -> c
      %{"closed_at" => c} when is_list(c) -> c
      _ -> []
    end
  end

  defp wait_until(fun, timeout_ms),
    do: NodeTestHelper.wait_until(fun, timeout_ms, 100)
end

defmodule FlezhaTest.System.StepsTimersE2ETest do
  @moduledoc """
  Cell timers inside a steps relay, end to end against a live runner of the
  selected SDK runtime over UDS.

  `timer_loop_steps` is `in -> tick -> tock -> out`: `after:<ms>` makes
  `tick` arm a processing-time timer and emit `armed:<at>`, its firing emits
  `fired:<at>`, and `tock` prefixes both onto the sink. `slow:<ms>` holds
  `tick` that long. `event_timer_steps` arms an event-time timer 100 after
  each message's event time and emits `seen:<t>`.

  A firing is a trigger of the run: it needs no message, it waits behind a
  pass in flight, it wakes a sleeping instance at its deadline, and it
  survives a runner crash.

  Tagged `:system` (boots real subprocesses); run with:

      mix test test/system/steps_timers_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :steps

  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, os_pid: 1, sigkill: 1, unique: 1, unique_id_path: 0]

  import FlezhaTest.Helper.StepsResumeHarness

  alias Flezha.Relay.Suspend
  alias Flezha.Runner.FleetManager
  alias Flezha.State.Completion
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 180_000

  describe "a timer inside a steps (uds)" do
    @describetag transport: :uds

    test "a firing reaches the sink through tock with no second message", ctx do
      {namespace, id, collector} = start_timer_loop(ctx, [])

      send_turn(namespace, id, "after:300")
      at = armed_at(collector, id)

      assert await_sink(collector, ["tock:armed:#{at}", "tock:fired:#{at}"]),
             "the firing never reached the sink: #{inspect(collected(collector))}"
    end

    test "a firing due while a slow pass runs reaches the sink after that pass's output",
         ctx do
      {namespace, id, collector} = start_timer_loop(ctx, [])

      # The timer comes due inside the slow pass, with room for the second
      # send to reach the instance first on a loaded runner.
      send_turn(namespace, id, "after:500")
      send_turn(namespace, id, "slow:1500")
      at = armed_at(collector, id)

      assert await_sink(collector, [
               "tock:armed:#{at}",
               "tock:slow:1500",
               "tock:fired:#{at}"
             ]),
             "the firing did not wait behind the slow pass: #{inspect(collected(collector))}"
    end

    test "a sleeping instance wakes at its timer and the firing reaches the sink", ctx do
      {namespace, id, collector} =
        start_timer_loop(ctx, extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "200"}])

      send_turn(namespace, id, "after:2000")
      at = armed_at(collector, id)

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 10_000),
             "the idle instance with a pending timer never slept"

      assert %{wake_deadline_ms: ^at} = Suspend.sleep_record(id)
      assert collected(collector) == ["tock:armed:#{at}"]

      assert await_sink(collector, ["tock:armed:#{at}", "tock:fired:#{at}"]),
             "the sleeper never woke at its timer: #{inspect(collected(collector))}"
    end

    test "a runner killed with a timer pending fires it after the restart", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_timer_crash")
      id = unique("timer_loop_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      {port_a, runner_a} = boot_runner(namespace, ctx, id_path, release?: false)
      start_loop(namespace, id, "timer_loop_steps", collector)

      send_turn(namespace, id, "after:4000")
      at = armed_at(collector, id)

      sigkill(os_pid(port_a))
      boot_runner(namespace, ctx, id_path, release?: false)

      assert NodeTestHelper.wait_until(
               fn -> runner_a not in FleetManager.active_runners(namespace) end,
               15_000
             )

      assert await_sink(collector, ["tock:armed:#{at}", "tock:fired:#{at}"], 60_000),
             "the restored timer never fired: #{inspect(collected(collector))}"
    end

    test "an event-time timer fires ahead of the message that advanced the high-water", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_event_timer")
      id = unique("event_timer_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path, release?: false)
      start_loop(namespace, id, "event_timer_steps", collector)

      send_turn(namespace, id, "1000")
      assert await_sink(collector, ["seen:1000"])
      send_turn(namespace, id, "1150")

      assert await_sink(collector, ["seen:1000", "fired:1100", "seen:1150"]),
             "the event-time firing did not run ahead of the second message: " <>
               inspect(collected(collector))
    end
  end

  defp start_timer_loop(ctx, opts) do
    collector = spawn_link(fn -> collect_loop([]) end)
    namespace = unique("steps_timers")
    id = unique("timer_loop_steps")
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, id, id_path) end)

    boot_runner(namespace, ctx, id_path, [release?: false] ++ opts)
    start_loop(namespace, id, "timer_loop_steps", collector)
    {namespace, id, collector}
  end

  defp start_loop(namespace, id, factory_id, collector) do
    start_steps(namespace, id, factory_id,
      first_message: nil,
      plugins: [forward_sink("out", collector)]
    )
  end

  # The timestamp `tick` armed, read off the first thing the sink received.
  defp armed_at(collector, id) do
    assert NodeTestHelper.wait_until(fn -> collected(collector) != [] end, 30_000),
           "nothing reached the sink; completion: #{inspect(Completion.read(id))}"

    ["tock:armed:" <> at | _] = collected(collector)
    String.to_integer(at)
  end

  defp await_sink(collector, expected, timeout \\ 30_000) do
    NodeTestHelper.wait_until(fn -> collected(collector) == expected end, timeout)
  end
end

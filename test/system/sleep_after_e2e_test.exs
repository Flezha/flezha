defmodule FlezhaTest.System.SleepAfterE2ETest do
  @moduledoc """
  A flow class that sleeps when idle, and the one persistence switch, end to
  end against a live runner of the selected SDK runtime over UDS.

  `sleeping_counter` folds `key:delta` inputs into one total per key and emits
  `key=total`: idle past `FLEZHA_FIXTURE_SLEEP_AFTER_MS` it sleeps, a send wakes it with
  its total intact, a controller-side restart while it sleeps changes nothing,
  and a stop removes it hard, as any stream's stop does. `stateful_counter`,
  bound beside it, declares nothing and never sleeps. The persistence switch
  is driven through `persistence_off_counter` and `optout_commit_block`: a
  class of either run that declares persistence off keeps no state on the
  controller and refuses a live read.

  Tagged `:system` (boots real subprocesses); run with:

      mix test test/system/sleep_after_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :state_persistence

  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, force_fresh_rebind: 2, kill_overwatch: 2, unique: 1, unique_id_path: 0]

  import FlezhaTest.Helper.StepsResumeHarness

  alias Flezha.Relay.{ClassRegistry, Replay, Suspend}
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.RelayTestHelper
  alias Flezha.Service.RelayManager.Init
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 180_000

  describe "a stream that sleeps (uds)" do
    @describetag transport: :uds

    test "sleeps when idle, wakes on a send with its total, and a restart asleep changes nothing",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("sleep_flow")
      id = unique("sleeping_counter")
      sibling = unique("stateful_counter")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
      )

      start_stream(namespace, id, "sleeping_counter",
        plugins: [forward_sink("totals", collector)]
      )

      start_stream(namespace, sibling, "stateful_counter", [])

      send_input(namespace, id, "a:2")
      assert NodeTestHelper.wait_until(fn -> collected(collector) == ["a=2"] end, 30_000)

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 20_000),
             "the idle stream never slept"

      assert Suspend.sleep_record(id) != nil
      refute suspended?(namespace, sibling), "a stream that does not declare sleeping stays bound"

      kill_overwatch(namespace, id)

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 30_000),
             "the sleep was not rediscovered from durable state"

      send_input(namespace, id, "a:3")

      assert NodeTestHelper.wait_until(fn -> collected(collector) == ["a=2", "a=5"] end, 60_000),
             "the woken stream did not fold the send onto its total: " <>
               inspect(collected(collector))

      # Once: nothing the sleep held is delivered twice. A held input folded
      # or delivered again lands ahead of a later send's output.
      send_input(namespace, id, "a:1")
      assert NodeTestHelper.wait_until(fn -> length(collected(collector)) >= 3 end, 30_000)
      assert collected(collector) == ["a=2", "a=5", "a=6"]
    end

    # A controller restart while the stream is awake loses every in-memory
    # floor row; the rebind re-forwards the retained log and nothing already
    # folded is folded again.
    test "a restart while awake folds nothing twice", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("sleep_flow_restart")
      id = unique("sleeping_counter")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "2000"}]
      )

      start_stream(namespace, id, "sleeping_counter",
        plugins: [forward_sink("totals", collector)]
      )

      send_input(namespace, id, "a:100")
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 30_000)

      send_input(namespace, id, "a:1")
      send_input(namespace, id, "a:2")

      assert NodeTestHelper.wait_until(
               fn -> collected(collector) == ["a=100", "a=101", "a=103"] end,
               30_000
             )

      refute suspended?(namespace, id)
      forget_floor_rows(id)
      force_fresh_rebind(namespace, id)
      send_input(namespace, id, "a:10")

      # A re-forwarded input folded again lands ahead of a later send's output.
      send_input(namespace, id, "a:1")
      assert NodeTestHelper.wait_until(fn -> length(collected(collector)) >= 5 end, 30_000)
      assert collected(collector) == ["a=100", "a=101", "a=103", "a=113", "a=114"]
    end

    test "a stop removes a sleeping stream and its directory", ctx do
      namespace = unique("sleep_flow_stop")
      id = unique("sleeping_counter")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
      )

      start_stream(namespace, id, "sleeping_counter", [])
      send_input(namespace, id, "a:1")
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 30_000)

      assert :ok = RelayManager.request_stop(namespace, id)
      refute registered?(namespace, id)
      refute File.dir?(Paths.class_dir(id))
    end
  end

  describe "the persistence switch (uds)" do
    @describetag transport: :uds

    test "a stream that declares persistence off keeps no state and refuses a live read", ctx do
      namespace = unique("persistence_off_flow")
      id = unique("persistence_off_counter")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path, release?: false)
      start_stream(namespace, id, "persistence_off_counter", [])
      for n <- 1..3, do: send_input(namespace, id, Integer.to_string(n))

      assert NodeTestHelper.wait_until(
               fn ->
                 RelayManager.get_cell_state(namespace, id) == {:error, {:persistence_off, id}}
               end,
               30_000
             ),
             "the live read of an opted-out stream was not refused as persistence_off"

      refute ClassRegistry.persistent?(id)
      assert Paths.cells_of(id) == [], "the controller holds no WAL for an opted-out class"
      assert Replay.resume_cell_count(id) == 0, "a rebind would ship no RestoreInitState"
    end

    # A message dead-lettered unread is consumed; the wake that clears the park
    # record and a restart before the next commit must not make it owed again.
    @tag capability: :steps, skip: FlezhaTest.SystemCase.deferral(:steps)
    test "a woken instance keeps an unread message released across a restart", ctx do
      namespace = unique("sleep_released")
      id = unique("typed_once_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      {_port, runner_id} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "3000"}]
        )

      start_steps(namespace, id, "typed_once_steps", first_message: nil)
      :ok = RelayTestHelper.cast_io(id, namespace, "not json", %{})

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 30_000)
      assert [%{"generation" => 1}] = DlqStore.list(id)

      {:ok, pid} = RelayManager.get_relay_pid(namespace, id)
      GenServer.call(pid, {:rebind, runner_id}, 30_000)

      assert NodeTestHelper.wait_until(fn -> Suspend.sleep_record(id) == nil end, 10_000),
             "the wake did not clear the park record"

      forget_floor_rows(id)
      kill_overwatch(namespace, id)

      # Running: the replacement's bind has re-forwarded what the log owes.
      assert NodeTestHelper.wait_until(
               fn ->
                 match?(
                   {:ok, %{phase: :running}},
                   RelayManager.get_overwatch_state(namespace, id)
                 )
               end,
               30_000
             )

      assert InputLog.undelivered(id) == [], "the dead-lettered message is owed again"
      assert [%{"generation" => 1}] = DlqStore.list(id)
    end

    @tag capability: :steps, skip: FlezhaTest.SystemCase.deferral(:steps)
    test "a steps relay that declares persistence off keeps no state and refuses a live read",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("persistence_off_steps")
      id = unique("optout_commit_block")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      # Without the release env its second task blocks, so the run stays bound.
      boot_runner(namespace, ctx, id_path, release?: false)

      start_steps(namespace, id, "optout_commit_block", plugins: [forward_sink("mid", collector)])

      assert NodeTestHelper.wait_until(fn -> collected(collector) != [] end, 30_000)

      assert RelayManager.get_cell_state(namespace, id) == {:error, {:persistence_off, id}}
      refute ClassRegistry.persistent?(id)
      refute ClassRegistry.sleeps?(id), "an opted-out steps does not sleep"
      assert Paths.cells_of(id) == []
      refute File.exists?(Paths.journal_path(id))
    end
  end

  describe "a run's window across a sleep (uds)" do
    @describetag transport: :uds

    # Fails when: the wake binds with the class's whole window, so a loop that
    # sleeps between turns never reaches its `max_duration_ms`.
    @tag capability: :steps, skip: FlezhaTest.SystemCase.deferral(:steps)
    test "A sleep does not restart the window", ctx do
      namespace = unique("window_sleep")
      id = unique("chat_loop_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
      )

      start_steps(namespace, id, "chat_loop_steps", first_message: nil)
      send_turn(namespace, id, "hello")
      assert NodeTestHelper.wait_until(fn -> committed_iters(id) == [0] end, 30_000)

      assert NodeTestHelper.wait_until(fn -> Init.window_end(namespace, id) != nil end, 10_000),
             "the first bind recorded no window end"

      ends_at = Init.window_end(namespace, id)

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 20_000)
      send_turn(namespace, id, "again")
      assert NodeTestHelper.wait_until(fn -> committed_iters(id) == [0, 1] end, 60_000)
      assert Init.window_end(namespace, id) == ends_at, "the wake recorded a window of its own"

      # The class's window is 600 s: only a wake that counts from the first
      # bind ends the run here.
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 20_000)
      spend_window(namespace, id)
      send_turn(namespace, id, "late")
      await_status(namespace, id, "cancelled", 60_000)
    end
  end

  # What a controller restart loses: the in-memory floor rows of the class.
  defp forget_floor_rows(id),
    do: :ets.match_delete(InputLog.watermark_table(), {{id, :_}, :_})
end

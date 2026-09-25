defmodule FlezhaTest.System.SleepWhenIdleE2ETest do
  @moduledoc """
  An unbounded class that sleeps when idle, and the one persistence switch, end to
  end against a live runner of the selected SDK runtime over UDS.

  `sleeping_counter` folds `key:delta` inputs into one total per key and emits
  `key=total`: idle past `FLEZHA_PARK_IDLE_MS` it sleeps, a send wakes it with
  its total intact, a controller-side restart while it sleeps changes nothing,
  and a stop removes it hard, as any stream's stop does. `stateful_counter`,
  bound beside it, declares nothing and never sleeps. The persistence switch
  is driven through `persistence_off_counter` and `optout_commit_block`: a
  class of either kind that declares persistence off keeps no state on the
  controller and refuses a live read.

  Tagged `:system` (boots real subprocesses); run with:

      mix test test/system/sleep_when_idle_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :state_persistence

  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, kill_overwatch: 2, unique: 1, unique_id_path: 0]

  import FlezhaTest.Helper.BoundedResumeHarness

  alias Flezha.Operator.{Builder, ClassRegistry, Replay}
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Paths
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 180_000

  describe "a stream that sleeps (uds)" do
    @describetag transport: :uds

    test "sleeps when idle, wakes on a send with its total, and a restart asleep changes nothing",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("sleep_unbounded")
      id = unique("sleeping_counter")
      sibling = unique("stateful_counter")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
      )

      start_stream(namespace, id, "sleeping_counter",
        plugins: [forward_sink("totals", collector)]
      )

      start_stream(namespace, sibling, "stateful_counter", [])

      send_input(namespace, id, "a:2")
      assert NodeTestHelper.wait_until(fn -> collected(collector) == ["a=2"] end, 30_000)

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 20_000),
             "the idle stream never slept"

      assert ClassRegistry.parked(id) != nil
      refute suspended?(namespace, sibling), "a stream that does not declare sleeping stays bound"

      kill_overwatch(namespace, id)

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 30_000),
             "the sleep was not rediscovered from durable state"

      send_input(namespace, id, "a:3")

      assert NodeTestHelper.wait_until(fn -> collected(collector) == ["a=2", "a=5"] end, 60_000),
             "the woken stream did not fold the send onto its total: " <>
               inspect(collected(collector))

      # Once: nothing the sleep held is delivered twice.
      Process.sleep(1_500)
      assert collected(collector) == ["a=2", "a=5"]
    end

    test "a stop removes a sleeping stream and its directory", ctx do
      namespace = unique("sleep_unbounded_stop")
      id = unique("sleeping_counter")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
      )

      start_stream(namespace, id, "sleeping_counter", [])
      send_input(namespace, id, "a:1")
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 30_000)

      assert :ok = OperatorManager.request_stop(namespace, id)
      refute registered?(namespace, id)
      refute File.dir?(Paths.class_dir(id))
    end
  end

  describe "the persistence switch (uds)" do
    @describetag transport: :uds

    test "a stream that declares persistence off keeps no state and refuses a live read", ctx do
      namespace = unique("persistence_off_unbounded")
      id = unique("persistence_off_counter")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path, release?: false)
      start_stream(namespace, id, "persistence_off_counter", [])
      for n <- 1..3, do: send_input(namespace, id, Integer.to_string(n))

      assert NodeTestHelper.wait_until(
               fn ->
                 OperatorManager.get_cell_state(namespace, id) == {:error, :persistence_off}
               end,
               30_000
             ),
             "the live read of an opted-out stream was not refused as persistence_off"

      refute ClassRegistry.persistent?(id)
      assert Paths.cells_of(id) == [], "the controller holds no WAL for an opted-out class"
      assert Replay.resume_cell_count(id) == 0, "a rebind would ship no RestoreInitState"
    end

    @tag capability: :bounded, skip: FlezhaTest.SystemCase.deferral(:bounded)
    test "a bounded operator that declares persistence off keeps no state and refuses a live read",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("persistence_off_bounded")
      id = unique("optout_commit_block")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      # Without the release env its second task blocks, so the run stays bound.
      boot_runner(namespace, ctx, id_path, release?: false)

      start_bounded(namespace, id, "optout_commit_block",
        plugins: [forward_sink("mid", collector)]
      )

      assert NodeTestHelper.wait_until(fn -> collected(collector) != [] end, 30_000)

      assert OperatorManager.get_cell_state(namespace, id) == {:error, :persistence_off}
      refute ClassRegistry.persistent?(id)
      refute ClassRegistry.sleep_when_idle?(id), "an opted-out bounded does not sleep"
      assert Paths.cells_of(id) == []
      refute File.exists?(Paths.journal_path(id))
    end
  end

  defp start_stream(namespace, id, factory_id, opts) do
    builder = %Builder{id: id, factory_id: factory_id, init_values: %{}, opts: opts}
    assert {:ok, _info} = OperatorManager.start_operator(namespace, builder)
  end

  defp send_input(namespace, id, text),
    do: :ok = OperatorManager.send_io(id, namespace, text, %{})
end

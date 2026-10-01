defmodule FlezhaTest.System.OperatorSettingsE2ETest do
  @moduledoc """
  The rows of the one operator class's settings tree no other drill covers,
  against a live runner of the selected SDK runtime: a flow instance that
  completes itself, a chained steps loop that ends itself, a steps loop that
  never ends until stopped, a `durable: remote` class refused on a controller
  with no durability backend, and a two-second `sleep_after` on a flow
  instance and on a steps instance.

  Tagged `:system` (boots real subprocesses); run with:

      mix test test/system/operator_settings_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :steps

  import FlezhaTest.Helper.TierE2EHarness, only: [cleanup: 3, unique: 1, unique_id_path: 0]
  import FlezhaTest.Helper.StepsResumeHarness

  alias Flezha.Operator.Builder
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Completion
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 180_000

  describe "the run (uds)" do
    @describetag transport: :uds

    @tag capability: :runner_lifecycle,
         skip: FlezhaTest.SystemCase.deferral(:runner_lifecycle)
    test "a flow instance ends itself with complete, after its Cells ran what they held", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("complete_flow")
      id = unique("complete_flow")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)
      boot_runner(namespace, ctx, id_path, release?: false)

      start_stream(namespace, id, "complete_flow",
        plugins: [forward_sink("complete_flow_out", collector)]
      )

      send_input(namespace, id, "a")
      send_input(namespace, id, "bye")

      assert NodeTestHelper.wait_until(fn -> collected(collector) == ["a", "bye"] end, 30_000),
             "the flow did not echo both inputs: #{inspect(collected(collector))}"

      await_status(id, "completed", 30_000)

      assert NodeTestHelper.wait_until(fn -> not registered?(namespace, id) end, 15_000),
             "the completed flow instance was never deregistered"
    end

    test "a chained steps loop runs each pass on the last one's handoff and ends on an empty one",
         ctx do
      namespace = unique("chained_steps")
      id = unique("chained_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)
      {port, _runner} = boot_runner(namespace, ctx, id_path, release?: false)

      start_steps(namespace, id, "chained_steps", first_message: "3")
      await_status(id, "ok", 60_000)

      seen = NodeTestHelper.read_port(port, &(passes(&1) == ~w(3 2 1 0)), 30_000)
      assert passes(seen) == ~w(3 2 1 0), "the passes were not 3, 2, 1, 0:\n#{seen}"

      assert NodeTestHelper.wait_until(fn -> not registered?(namespace, id) end, 15_000),
             "the ended loop was never deregistered"
    end

    test "a steps loop that never ends stays bound across its turns until it is stopped", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("endless_steps")
      id = unique("chat_loop_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)
      boot_runner(namespace, ctx, id_path, release?: false)

      start_steps(namespace, id, "chat_loop_steps",
        first_message: nil,
        plugins: [forward_sink("out", collector)]
      )

      send_turn(namespace, id, "one")
      await_outputs(collector, 1, id)
      send_turn(namespace, id, "two")
      await_outputs(collector, 2, id)

      assert registered?(namespace, id)
      assert Completion.read(id) == :error

      assert :ok = OperatorManager.stop_operator(namespace, id)
      refute registered?(namespace, id)
    end

    test "a durable: remote class is refused at start without a durability backend", ctx do
      namespace = unique("remote_refused")
      id = unique("sync_durable_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)
      boot_runner(namespace, ctx, id_path, release?: false)

      builder = %Builder{id: id, factory_id: "sync_durable_steps", run: :steps}

      assert {:error, {:start_failed, ^id, reason}} =
               OperatorManager.start_operator(namespace, builder)

      assert Flezha.Error.from_reason(reason, nil).code == "operator.remote_without_backend"
      refute registered?(namespace, id)
    end
  end

  describe "a two-second sleep_after (uds)" do
    @describetag transport: :uds

    @tag capability: :state_persistence,
         skip: FlezhaTest.SystemCase.deferral(:state_persistence)
    test "a flow instance and a steps instance each sleep once idle that long", ctx do
      namespace = unique("sleep_after_2s")
      flow = unique("sleeping_counter")
      steps = unique("stateless_loop_steps")
      id_path = unique_id_path()

      on_exit(fn ->
        cleanup(namespace, flow, id_path)
        cleanup(namespace, steps, id_path)
      end)

      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "2000"}]
      )

      collector = spawn_link(fn -> collect_loop([]) end)

      start_stream(namespace, flow, "sleeping_counter",
        plugins: [forward_sink("totals", collector)]
      )

      start_steps(namespace, steps, "stateless_loop_steps",
        first_message: nil,
        plugins: [forward_sink("out", collector)]
      )

      send_input(namespace, flow, "a:1")
      send_turn(namespace, steps, "hi")
      assert NodeTestHelper.wait_until(fn -> length(collected(collector)) == 2 end, 30_000)

      for id <- [flow, steps] do
        assert NodeTestHelper.wait_until(fn -> suspended?(namespace, id) end, 30_000),
               "#{id} never slept"
      end
    end
  end

  defp passes(seen), do: for([_, n] <- Regex.scan(~r/^chained_pass (\d+)$/m, seen), do: n)
end

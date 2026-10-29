defmodule FlezhaTest.System.RelaySettingsE2ETest do
  @moduledoc """
  The rows of the one relay class's settings tree no other drill covers,
  against a live runner of the selected SDK runtime: a flow instance that
  completes itself, a chained steps loop that ends itself, a steps loop that
  never ends until stopped, a `durable: remote` class refused on a controller
  with no durability backend, a two-second `sleep_after` on a flow
  instance and on a steps instance, a `max_duration_ms` window that a
  runner restart or a live move does not restart, and a finished run that keeps
  its state only when its class or the controller asks.

  Tagged `:system` (boots real subprocesses); run with:

      mix test test/system/relay_settings_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :steps

  import FlezhaTest.Helper.TierE2EHarness, only: [cleanup: 3, unique: 1, unique_id_path: 0]
  import FlezhaTest.Helper.StepsResumeHarness

  alias Flezha.Relay.Builder
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Init
  alias Flezha.Service.Steps
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 180_000

  describe "the run (uds)" do
    @describetag transport: :uds

    @tag capability: :runner_lifecycle,
         skip: FlezhaTest.SystemCase.deferral(:runner_lifecycle)
    test "a flow instance ends itself with complete, after its Cells ran what they held", ctx do
      {namespace, id} = complete_flow(ctx)

      # Its class declares no `keep_finished`: the relay history answers.
      assert %{state_kept: false} = await_status(namespace, id, "ok", 30_000)

      assert NodeTestHelper.wait_until(fn -> not registered?(namespace, id) end, 15_000),
             "the completed flow instance was never deregistered"

      assert NodeTestHelper.wait_until(fn -> not File.exists?(Paths.class_dir(id)) end, 15_000),
             "the completed flow instance's state directory was kept"
    end

    @tag capability: :runner_lifecycle,
         skip: FlezhaTest.SystemCase.deferral(:runner_lifecycle)
    test "a flow instance that completes keeps its record when the controller keeps by default",
         ctx do
      keep_finished()
      {namespace, id} = complete_flow(ctx)

      assert %{status: "completed"} =
               await_status(namespace, id, %{state_kept: true}, 30_000)

      assert Completion.exists?(id)
    end

    test "a chained steps loop runs each pass on the last one's handoff and ends on an empty one",
         ctx do
      namespace = unique("chained_steps")
      id = unique("chained_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)
      {port, _runner} = boot_runner(namespace, ctx, id_path, release?: false)

      start_steps(namespace, id, "chained_steps", first_message: "3")
      await_status(namespace, id, "ok", 60_000)

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
      assert Steps.outcome(namespace, id) == :error

      assert :ok = RelayManager.stop_relay(namespace, id)
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
               RelayManager.start_relay(namespace, builder)

      assert Flezha.Error.from_reason(reason, nil).code == "relay.remote_without_backend"
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

  # `chat_loop_steps` declares a 600 s window, so a run that ends here ends only
  # because its rebind counted the window from the first bind.
  describe "max_duration_ms across a rebind (uds)" do
    @describetag transport: :uds

    # Fails when: the replacement runner arms the class's whole window again.
    @tag capability: :runner_lifecycle,
         skip: FlezhaTest.SystemCase.deferral(:runner_lifecycle)
    test "a runner restart past the window ends the run", ctx do
      {namespace, id, id_path} = window_run()
      runner = boot_runner(namespace, ctx, id_path, release?: false)
      first_turn(namespace, id)

      spend_window(namespace, id)
      replace_runner(namespace, ctx, runner, id_path)
      await_status(namespace, id, "cancelled", 60_000)
    end

    # Fails when: the bind a live move runs arms the class's whole window again.
    test "a live move keeps the window", ctx do
      {namespace, id, id_path} = window_run()

      runners =
        for path <- [id_path, unique_id_path()],
            do: namespace |> boot_runner(ctx, path, release?: false) |> elem(1)

      info = first_turn(namespace, id)
      target = Enum.find(runners, &(&1 != info.runner_id))

      spend_window(namespace, id)
      {:ok, pid} = RelayManager.get_relay_pid(namespace, id)
      assert :ok = GenServer.call(pid, {:rebind, target}, 60_000)
      await_status(namespace, id, "cancelled", 60_000)
    end
  end

  # `commit_block_steps` commits a Cell in its first stage; released, it ends
  # `ok` at once.
  describe "keep_finished (uds)" do
    @describetag transport: :uds

    test "a steps run whose class declares nothing keeps nothing once it ends", ctx do
      {namespace, id} = commit_block_run(ctx, [])

      assert %{state_kept: false, error: ""} = await_status(namespace, id, "ok", 60_000)

      assert NodeTestHelper.wait_until(fn -> not File.exists?(Paths.class_dir(id)) end, 15_000),
             "the finished run's state directory was kept"
    end

    # Only the reference runner declares `keep_finished` from the drill's env.
    @tag reference_runtime: true
    test "a steps run whose class declares keep_finished keeps its state and record", ctx do
      {namespace, id} = commit_block_run(ctx, [{"FLEZHA_FIXTURE_KEEP_FINISHED_H", "24"}])

      assert %{finished_at: finished_at, expires_at: expires_at} =
               await_status(namespace, id, %{status: "ok", state_kept: true}, 60_000)

      assert expires_at - finished_at == 24 * 3600, "the declared window was not the one kept"
      assert Completion.exists?(id)

      assert [_ | _] = Path.wildcard(Path.join([Paths.class_dir(id), "**", "*.wal"])),
             "the kept run's Cell state is gone"
    end
  end

  defp complete_flow(ctx) do
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

    {namespace, id}
  end

  defp commit_block_run(ctx, env) do
    namespace = unique("keep_finished")
    id = unique("commit_block_steps")
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, id, id_path) end)
    boot_runner(namespace, ctx, id_path, release?: true, extra_env: env)
    start_steps(namespace, id, "commit_block_steps", [])
    {namespace, id}
  end

  defp window_run do
    namespace = unique("steps_window")
    id = unique("chat_loop_steps")
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, id, id_path) end)
    {namespace, id, id_path}
  end

  defp first_turn(namespace, id) do
    info = start_steps(namespace, id, "chat_loop_steps", first_message: nil)
    send_turn(namespace, id, "hello")
    assert NodeTestHelper.wait_until(fn -> committed_iters(id) == [0] end, 30_000)

    assert NodeTestHelper.wait_until(fn -> Init.window_end(namespace, id) != nil end, 10_000),
           "the first bind recorded no window end"

    info
  end

  defp passes(seen), do: for([_, n] <- Regex.scan(~r/^chained_pass (\d+)$/m, seen), do: n)
end

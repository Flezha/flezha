defmodule FlezhaTest.System.TaskKindE2ETest do
  @moduledoc """
  End-to-end proof of the steps run against a live runner of the selected
  SDK runtime: a start that names only the factory takes the kind its runner
  announced, flows `AddRelay` with kind `steps` → the SDK runs the pass
  once → emits `RelayCompleted` → the controller drives
  deregister-on-completion. The whole loop is observable as the unit
  disappearing from the durable registry without an explicit stop. Tagged
  `:system` (boots a real subprocess); run with
  `mix test --include system test/system/steps_kind_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :steps

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, ensure_port_dead: 1]

  import FlezhaTest.Helper.StepsResumeHarness,
    only: [
      collect_loop: 1,
      forward_sink: 2,
      await_outputs: 3,
      await_status: 4,
      keep_finished: 0,
      send_turn: 3
    ]

  alias Flezha.Identificator
  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.Logs
  alias Flezha.Service.RelayManager
  alias Flezha.State.Paths
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.MimicGlobal
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.TierE2EHarness
  alias ProcessHub.Service.ProcessRegistry

  @namespace "task_kind_e2e"
  @factory "echo_steps"

  # One fleet for the module: every scenario names its own relay.
  setup_all do
    {port, _ce_id} = NodeTestHelper.start_runner_port(@namespace)

    on_exit(fn ->
      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end

      ensure_port_dead(port)
    end)

    :ok
  end

  setup do
    on_exit(fn ->
      try do
        RelayManager.stop_all_relays(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  test "task runs once on its first message and is deregistered on completion" do
    relay_id = "task_e2e_#{System.unique_integer([:positive])}"

    # No kind: the factory's runner announced it.
    builder = %Builder{
      id: relay_id,
      factory_id: @factory,
      init_values: %{},
      opts: [seed_input: {"hi", %{}}]
    }

    # Start succeeds even for a near-instant task: a run-once unit may
    # complete (and self-deregister) before its binding is observable, and a
    # clean exit during start confirmation is not a failure.
    assert {:ok, _info} = RelayManager.start_relay(@namespace, builder)

    # The SDK runs the task once and ships `RelayCompleted`; the controller
    # tears it down. Deregister-on-completion: the unit drops out of the
    # durable registry with no explicit stop — proof the full loop ran.
    assert wait_until(fn -> not registered?(relay_id) end, 5_000),
           "task was never deregistered after completion"
  end

  # The hub writes a started relay's row only after the relay runs, and a task
  # takes its first message at once: held behind a busy hub registry, the task
  # completes before its row exists. The completion's teardown still removes it.
  # The bind's own runner tag reaches that registry first and finds no row, as
  # it does when its write is queued ahead of the start's.
  test "a task that completes before its hub row is written is still deregistered" do
    relay_id = "task_e2e_early_#{System.unique_integer([:positive])}"
    trace_teardown_lookup(relay_id)
    MimicGlobal.set_mimic_global_until_exit()

    Mimic.stub(ProcessRegistry, :update, fn
      _hub_id, ^relay_id, _update, _opts ->
        {:error, "No child found"}

      hub_id, id, update, opts ->
        Mimic.call_original(ProcessRegistry, :update, [hub_id, id, update, opts])
    end)

    registry = hub_registry()
    :ok = :sys.suspend(registry)
    on_exit(fn -> :sys.resume(registry) end)

    start =
      Task.async(fn ->
        RelayManager.start_relay(@namespace, %Builder{
          id: relay_id,
          factory_id: @factory,
          opts: [seed_input: {"hi", %{}}]
        })
      end)

    # The row is held back until the teardown's stop has looked the relay up.
    assert_receive {:trace, teardown, :call, {RelayManager, :stop_relay, [_, ^relay_id, _]}},
                   10_000

    assert_receive {:trace, ^teardown, :return_from, {RelayManager, :get_relay_pid, 2}, lookup},
                   10_000

    assert {:error, {:relay_not_found, _, _}} = lookup
    :ok = :sys.resume(registry)

    assert {:ok, _info} = Task.await(start, 30_000)

    assert wait_until(fn -> not registered?(relay_id) end, 5_000),
           "task was never deregistered after completion"
  end

  # A steps tasklet declaring static cells is offered every message, and the
  # cells `cells_for` names run it: two of the three here, each emitting its key.
  test "a static-cell steps tasklet runs on each cell its cells_for names" do
    relay_id = "static_cell_steps_#{System.unique_integer([:positive])}"
    collector = spawn_link(fn -> collect_loop([]) end)
    sink = forward_sink("static_cell_sink", collector)

    assert {:ok, _} =
             RelayManager.start_relay(@namespace, %Builder{
               id: relay_id,
               factory_id: "static_cell_steps",
               opts: [bindings: RelayFixture.bindings(@namespace, relay_id, [sink])]
             })

    send_turn(@namespace, relay_id, "go")

    delivered = collector |> await_outputs(2, relay_id) |> Enum.map(&elem(&1, 0))
    assert Enum.sort(delivered) == ["a:go", "b:go"]
  end

  # A tasklet spawns the `stateless_loop_steps` loop once with no id and
  # once with its input as the id, then prints each handle's result as
  # `spawn_result <unnamed|named> <id> <outcome>`. The runner's stdout is the
  # only place those results are observable, so the scenario owns its runner.
  test "a tasklet's spawn without an id gets a new generated id; one with an id spawns, then delivers" do
    namespace = "spawn_ids_e2e"
    {port, _runner_id} = NodeTestHelper.start_runner_port(namespace)

    on_exit(fn ->
      try do
        RelayManager.stop_all_relays(namespace)
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end

      ensure_port_dead(port)
    end)

    requester = "spawn_requester_#{System.unique_integer([:positive])}"
    named = "spawn-named-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             RelayManager.start_relay(namespace, %Builder{
               id: requester,
               factory_id: "spawn_requester"
             })

    for _ <- 1..2 do
      assert :ok = RelayManager.send_io_confirming(requester, namespace, named, %{})
    end

    seen = NodeTestHelper.read_port(port, &(length(spawn_results(&1)) >= 4), 60_000)
    results = spawn_results(seen)
    assert length(results) >= 4, "the runner printed fewer than 4 spawn results:\n#{seen}"

    assert [{first, "spawned"}, {second, "spawned"}] =
             for({"unnamed", id, outcome} <- results, do: {id, outcome})

    # `stateless_loop_steps` is trimmed to fit the id limit beside the suffix.
    assert first =~ ~r/\Astateless_loop-[a-z2-7]{16}\z/
    assert second =~ ~r/\Astateless_loop-[a-z2-7]{16}\z/
    refute first == second

    assert for({"named", id, outcome} <- results, do: {id, outcome}) ==
             [{named, "spawned"}, {named, "delivered"}]
  end

  # KB-381: a tasklet spawns a `once` steps relay whose entry channel `order`
  # decodes the payload's type. The spawned instance runs its one pass over
  # the decoded ticket (the runner prints `once_ran <text>`), completes with
  # status `ok` carrying the text, and is torn down. The runner's stdout is the
  # only place the spawn result and the pass are observable.
  test "a once steps spawned by a tasklet runs its pass on the payload and completes ok" do
    namespace = "spawn_once_e2e"
    # The result is read from the kept record.
    keep_finished()
    {port, _runner_id} = NodeTestHelper.start_runner_port(namespace)

    on_exit(fn ->
      try do
        RelayManager.stop_all_relays(namespace)
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end

      ensure_port_dead(port)
    end)

    requester = "spawn_once_requester_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             RelayManager.start_relay(namespace, %Builder{
               id: requester,
               factory_id: "spawn_once_requester"
             })

    assert :ok = RelayManager.send_io_confirming(requester, namespace, "ticket-7", %{})

    seen = NodeTestHelper.read_port(port, &(once_results(&1) != [] and &1 =~ "once_ran "), 60_000)
    assert [{spawned, "spawned"}] = once_results(seen), "no spawn result in:\n#{seen}"
    assert seen =~ "once_ran ticket-7", "the spawned pass never ran on its payload:\n#{seen}"

    assert %{result: "ticket-7"} =
             await_status(namespace, spawned, %{status: "ok", state_kept: true}, 30_000)

    assert wait_until(
             fn ->
               not match?({:ok, pid} when is_pid(pid), HubManager.get_pid(namespace, spawned))
             end,
             15_000
           ),
           "the completed instance was never deregistered"
  end

  # A crash re-run of the input that spawned a worker issues the same request
  # again: the runner dies with the spawn answered and the input not durably
  # consumed, and the re-run resolves to the worker the first run spawned.
  test "a crash re-run of a tasklet's spawn resolves to the worker it already spawned" do
    namespace = "spawn_rerun_e2e_#{System.unique_integer([:positive])}"
    id_path = TierE2EHarness.unique_id_path()
    runner_env = [{"FLEZHA_RUNNER_ID_PATH", id_path}]
    {port_a, _runner_a} = NodeTestHelper.start_runner_port(namespace, env: runner_env)
    requester = "spawn_hold_requester_#{System.unique_integer([:positive])}"
    on_exit(fn -> TierE2EHarness.cleanup(namespace, requester, id_path) end)

    assert {:ok, _} =
             RelayManager.start_relay(namespace, %Builder{
               id: requester,
               factory_id: "spawn_hold_requester"
             })

    assert :ok = RelayManager.send_io_confirming(requester, namespace, "job-1", %{})

    seen = NodeTestHelper.read_port(port_a, &(hold_results(&1) != []), 60_000)
    assert [{first, "spawned"}] = hold_results(seen), "no spawn result in:\n#{seen}"

    TierE2EHarness.sigkill(TierE2EHarness.os_pid(port_a))

    {port_b, _runner_b} =
      NodeTestHelper.start_runner_port(namespace,
        env: [{"FLEZHA_STEPS_E2E_RELEASE", "1"} | runner_env]
      )

    on_exit(fn -> ensure_port_dead(port_b) end)
    seen = NodeTestHelper.read_port(port_b, &(hold_results(&1) != []), 60_000)

    assert [{^first, _outcome}] = hold_results(seen),
           "the re-run did not resolve to #{first}:\n#{seen}"
  end

  # The same re-run once the worker ended and kept nothing: the relay history
  # still names it, so the identical spawn resolves to it and starts nothing.
  # Only the reference runner's requester spawns a worker that ends
  # (`FLEZHA_SPAWN_HOLD_ONCE`).
  @tag reference_runtime: true
  test "a crash re-run of a spawn whose worker ended and kept nothing starts nothing" do
    namespace = "spawn_rerun_ended_e2e_#{System.unique_integer([:positive])}"
    id_path = TierE2EHarness.unique_id_path()
    runner_env = [{"FLEZHA_RUNNER_ID_PATH", id_path}, {"FLEZHA_SPAWN_HOLD_ONCE", "1"}]
    {port_a, _runner_a} = NodeTestHelper.start_runner_port(namespace, env: runner_env)
    requester = "spawn_hold_requester_#{System.unique_integer([:positive])}"
    on_exit(fn -> TierE2EHarness.cleanup(namespace, requester, id_path) end)

    assert {:ok, _} =
             RelayManager.start_relay(namespace, %Builder{
               id: requester,
               factory_id: "spawn_hold_requester"
             })

    assert :ok = RelayManager.send_io_confirming(requester, namespace, "job-1", %{})

    seen = NodeTestHelper.read_port(port_a, &(hold_results(&1) != []), 60_000)
    assert [{worker, "spawned"}] = hold_results(seen), "no spawn result in:\n#{seen}"

    assert %{state_kept: false} = await_status(namespace, worker, "ok", 30_000)

    assert wait_until(fn -> not File.exists?(Paths.class_dir(worker)) end, 15_000),
           "the ended worker's state directory was kept"

    TierE2EHarness.sigkill(TierE2EHarness.os_pid(port_a))

    {port_b, _runner_b} =
      NodeTestHelper.start_runner_port(namespace,
        env: [{"FLEZHA_STEPS_E2E_RELEASE", "1"} | runner_env]
      )

    on_exit(fn -> ensure_port_dead(port_b) end)
    seen = NodeTestHelper.read_port(port_b, &(hold_results(&1) != []), 60_000)

    assert [{^worker, "spawned"}] = hold_results(seen),
           "the re-run did not resolve to #{worker}:\n#{seen}"

    refute RelayManager.exist?(namespace, worker)
    assert started_count(worker) == 1, "the re-run started the ended worker again"
  end

  defp started_count(relay_id) do
    {:ok, %{records: records}} =
      Logs.read(%{kind: "relay_history", filters: %{relay: relay_id}}, %{source: :internal})

    Enum.count(records, &(&1["event"] == "started"))
  end

  defp hold_results(seen) do
    for [_line, id, outcome] <- Regex.scan(~r/^spawn_result hold (\S+) (.+)$/m, seen),
        do: {id, String.trim(outcome)}
  end

  defp once_results(seen) do
    for [_line, id, outcome] <- Regex.scan(~r/^spawn_result once (\S+) (.+)$/m, seen),
        do: {id, String.trim(outcome)}
  end

  defp spawn_results(seen) do
    for [_line, label, id, outcome] <-
          Regex.scan(~r/^spawn_result (unnamed|named) (\S+) (.+)$/m, seen),
        do: {label, id, String.trim(outcome)}
  end

  defp hub_registry do
    hub = ProcessHub.Hub.get(Identificator.hub(@namespace))
    GenServer.whereis(hub.procs.process_registry)
  end

  # Tells this process of every stop of `relay_id` and of each hub lookup of it
  # with its result, made by processes started from now on: the completion's
  # teardown among them.
  defp trace_teardown_lookup(relay_id) do
    session = :trace.session_create(:steps_teardown_lookup, self(), [])
    on_exit(fn -> :trace.session_destroy(session) end)

    _ = :trace.process(session, :new_processes, true, [:call])
    stop = [{[:_, relay_id, :_], [], []}]
    lookup = [{[:_, relay_id], [], [{:return_trace}]}]
    1 = :trace.function(session, {RelayManager, :stop_relay, 3}, stop, [:local])
    1 = :trace.function(session, {RelayManager, :get_relay_pid, 2}, lookup, [:local])
  end

  defp registered?(relay_id) do
    match?({:ok, pid} when is_pid(pid), HubManager.get_pid(@namespace, relay_id))
  end
end

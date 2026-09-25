defmodule FlezhaTest.System.TaskKindE2ETest do
  @moduledoc """
  End-to-end proof of the bounded kind against a live runner of the selected
  SDK runtime: a start that names only the factory takes the kind its runner
  announced, flows `AddOperator` with kind `bounded` → the SDK runs the pass
  once → emits `BoundedCompleted` → the controller drives
  deregister-on-completion. The whole loop is observable as the unit
  disappearing from the durable registry without an explicit stop. Tagged
  `:system` (boots a real subprocess); run with
  `mix test --include system test/system/bounded_kind_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :bounded

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, ensure_port_dead: 1]

  import FlezhaTest.Helper.BoundedResumeHarness,
    only: [collect_loop: 1, forward_sink: 2, await_outputs: 3, send_turn: 3]

  alias Flezha.Operator.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Completion
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "task_kind_e2e"
  @factory "echo_bounded"

  # One fleet for the module: every scenario names its own operator.
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
        OperatorManager.stop_all_operators(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  test "task runs once on its first message and is deregistered on completion" do
    operator_id = "task_e2e_#{System.unique_integer([:positive])}"

    # No kind: the factory's runner announced it.
    builder = %Builder{
      id: operator_id,
      factory_id: @factory,
      init_values: %{},
      opts: [seed_input: {"hi", %{}}]
    }

    # Start succeeds even for a near-instant task: a run-once unit may
    # complete (and self-deregister) before its binding is observable, and a
    # clean exit during start confirmation is not a failure.
    assert {:ok, _info} = OperatorManager.start_operator(@namespace, builder)

    # The SDK runs the task once and ships `BoundedCompleted`; the controller
    # tears it down. Deregister-on-completion: the unit drops out of the
    # durable registry with no explicit stop — proof the full loop ran.
    assert wait_until(fn -> not registered?(operator_id) end, 5_000),
           "task was never deregistered after completion"
  end

  # A bounded tasklet declaring static cells is offered every message, and the
  # cells `cells_for` names run it: two of the three here, each emitting its key.
  test "a static-cell bounded tasklet runs on each cell its cells_for names" do
    operator_id = "static_cell_bounded_#{System.unique_integer([:positive])}"
    collector = spawn_link(fn -> collect_loop([]) end)

    assert {:ok, _} =
             OperatorManager.start_operator(@namespace, %Builder{
               id: operator_id,
               factory_id: "static_cell_bounded",
               opts: [plugins: [forward_sink("static_cell_sink", collector)]]
             })

    send_turn(@namespace, operator_id, "go")

    delivered = collector |> await_outputs(2, operator_id) |> Enum.map(&elem(&1, 0))
    assert Enum.sort(delivered) == ["a:go", "b:go"]
  end

  # A tasklet spawns the `stateless_loop_bounded` loop once with no id and
  # once with its input as the id, then prints each handle's result as
  # `spawn_result <unnamed|named> <id> <outcome>`. The runner's stdout is the
  # only place those results are observable, so the scenario owns its runner.
  test "a tasklet's spawn without an id gets a new generated id; one with an id spawns, then delivers" do
    namespace = "spawn_ids_e2e"
    {port, _runner_id} = NodeTestHelper.start_runner_port(namespace)

    on_exit(fn ->
      try do
        OperatorManager.stop_all_operators(namespace)
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end

      ensure_port_dead(port)
    end)

    requester = "spawn_requester_#{System.unique_integer([:positive])}"
    named = "spawn-named-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             OperatorManager.start_operator(namespace, %Builder{
               id: requester,
               factory_id: "spawn_requester"
             })

    for _ <- 1..2 do
      assert :ok = OperatorManager.send_io_confirming(requester, namespace, named, %{})
    end

    seen = NodeTestHelper.read_port(port, &(length(spawn_results(&1)) >= 4), 60_000)
    results = spawn_results(seen)
    assert length(results) >= 4, "the runner printed fewer than 4 spawn results:\n#{seen}"

    assert [{first, "spawned"}, {second, "spawned"}] =
             for({"unnamed", id, outcome} <- results, do: {id, outcome})

    # `stateless_loop_bounded` is trimmed to fit the id limit beside the suffix.
    assert first =~ ~r/\Astateless_loop-[a-z2-7]{16}\z/
    assert second =~ ~r/\Astateless_loop-[a-z2-7]{16}\z/
    refute first == second

    assert for({"named", id, outcome} <- results, do: {id, outcome}) ==
             [{named, "spawned"}, {named, "delivered"}]
  end

  # KB-381: a tasklet spawns a `once` bounded operator whose entry channel `order`
  # decodes the payload's type. The spawned instance runs its one pass over
  # the decoded ticket (the runner prints `once_ran <text>`), completes with
  # status `ok` carrying the text, and is torn down. The runner's stdout is the
  # only place the spawn result and the pass are observable.
  test "a once bounded spawned by a tasklet runs its pass on the payload and completes ok" do
    namespace = "spawn_once_e2e"
    {port, _runner_id} = NodeTestHelper.start_runner_port(namespace)

    on_exit(fn ->
      try do
        OperatorManager.stop_all_operators(namespace)
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end

      ensure_port_dead(port)
    end)

    requester = "spawn_once_requester_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             OperatorManager.start_operator(namespace, %Builder{
               id: requester,
               factory_id: "spawn_once_requester"
             })

    assert :ok = OperatorManager.send_io_confirming(requester, namespace, "ticket-7", %{})

    seen = NodeTestHelper.read_port(port, &(once_results(&1) != [] and &1 =~ "once_ran "), 60_000)
    assert [{spawned, "spawned"}] = once_results(seen), "no spawn result in:\n#{seen}"
    assert seen =~ "once_ran ticket-7", "the spawned pass never ran on its payload:\n#{seen}"

    assert wait_until(
             fn -> match?({:ok, %{status: "ok"}}, Completion.read(spawned)) end,
             30_000
           ),
           "the spawned once bounded did not complete ok: " <>
             inspect(Completion.read(spawned))

    assert {:ok, %{result: "ticket-7"}} = Completion.read(spawned)

    assert wait_until(
             fn ->
               not match?({:ok, pid} when is_pid(pid), HubManager.get_pid(namespace, spawned))
             end,
             15_000
           ),
           "the completed instance was never deregistered"
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

  defp registered?(operator_id) do
    match?({:ok, pid} when is_pid(pid), HubManager.get_pid(@namespace, operator_id))
  end
end

defmodule FlezhaTest.Service.OperativeManagerSpawnTest do
  @moduledoc """
  `OperativeManager.spawn_or_deliver/5` — the controller half of a procedure's
  `ctx.spawn_workflow` (`procedure-requested-spawn`).

  The registry is simulated by an ETS table whose `insert_new` is the same
  atomic claim ProcessHub's `start_child` performs, so the concurrency case
  exercises the real race resolution rather than a serialized stand-in.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.AuthZ.Quotas
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager

  @namespace "spawn-or-deliver-test"
  @factory "worker"

  # Stand-in for a started operative: answers the two calls the start path and
  # the delivery path make, and records every ingested payload.
  defmodule Instance do
    @moduledoc false
    use GenServer

    # Unlinked from its starter — the resolution may run in a Task, and a real
    # operative outlives whoever asked for it. It follows the test process
    # instead, so nothing leaks into the next test.
    def start(owner), do: GenServer.start(__MODULE__, owner)
    def ingested(pid), do: GenServer.call(pid, :ingested)

    @impl true
    def init(owner) do
      Process.monitor(owner)
      {:ok, []}
    end

    @impl true
    def handle_call(:registry_runner_set, _from, state), do: {:reply, {:ok, "runner-1"}, state}

    def handle_call({:ingest, message, _metadata}, _from, state),
      do: {:reply, :ok, [message | state]}

    def handle_call(:ingested, _from, state), do: {:reply, Enum.reverse(state), state}

    @impl true
    def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:stop, :normal, state}
  end

  setup :set_mimic_global

  setup do
    registry = :ets.new(:spawn_registry, [:public, :set])
    owner = self()

    stub(HubManager, :hub_alive?, fn _ -> true end)

    stub(HubManager, :get_pid, fn _ns, id ->
      case :ets.lookup(registry, id) do
        [{^id, pid}] -> {:ok, pid}
        [] -> {:error, "no pid found"}
      end
    end)

    stub(ProcessHub, :child_lookup, fn _hub, id ->
      case :ets.lookup(registry, id) do
        [{^id, pid}] -> {id, [{node(), pid}]}
        [] -> nil
      end
    end)

    stub(ProcessHub, :start_child, fn _hub, child_spec, _opts ->
      {:ok, pid} = Instance.start(owner)

      if :ets.insert_new(registry, {child_spec.id, pid}) do
        %ProcessHub.StartResult{started: [{child_spec.id, [{node(), pid}]}], errors: []}
      else
        %ProcessHub.StartResult{
          started: [],
          errors: [{child_spec.id, {:error, "operative id already in use"}}]
        }
      end
    end)

    stub(ProcessHub, :stop_children, fn _hub, _ids, _opts -> :ok end)
    stub(ProcessHub.Future, :await, fn result -> result end)
    stub(FleetManager, :runners_for_factory, fn _ns, _factory -> ["runner-1"] end)
    stub(Quotas, :check_allocation, fn _ns, _key, _n -> :ok end)
    stub(Quotas, :record_allocation, fn _ns, _key, _n -> :ok end)

    %{registry: registry}
  end

  defp instance_pid(registry, id) do
    [{^id, pid}] = :ets.lookup(registry, id)
    pid
  end

  test "a free id spawns, carrying the payload as the instance's seed input", %{
    registry: registry
  } do
    parent = self()

    expect(ProcessHub, :start_child, fn _hub, child_spec, _opts ->
      {_mod, _fun, [{_ns, operative}]} = child_spec.start
      send(parent, {:builder, operative})
      {:ok, pid} = Instance.start(parent)
      :ets.insert(registry, {child_spec.id, pid})
      %ProcessHub.StartResult{started: [{child_spec.id, [{node(), pid}]}], errors: []}
    end)

    assert :spawned =
             OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p1", :deliver)

    assert_receive {:builder, operative}
    assert operative.kind == :workflow
    assert operative.factory_id == @factory
    assert Keyword.get(operative.opts, :seed_input) == "p1"
  end

  test "a taken id receives the payload as one more input", %{registry: registry} do
    assert :spawned =
             OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p1", :deliver)

    assert :delivered =
             OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p2", :deliver)

    assert Instance.ingested(instance_pid(registry, "w1")) == ["p2"]
  end

  test "the error policy reports instead of delivering", %{registry: registry} do
    assert :spawned =
             OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p1", :deliver)

    assert :already_running =
             OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p2", :error)

    assert Instance.ingested(instance_pid(registry, "w1")) == []
  end

  test "a concurrent burst yields one instance and delivers every other payload", %{
    registry: registry
  } do
    outcomes =
      1..8
      |> Task.async_stream(
        fn n ->
          OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p#{n}", :deliver)
        end,
        max_concurrency: 8,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, outcome} -> outcome end)

    assert Enum.count(outcomes, &(&1 == :spawned)) == 1
    assert Enum.count(outcomes, &(&1 == :delivered)) == 7

    delivered = Instance.ingested(instance_pid(registry, "w1"))
    assert length(delivered) == 7
    assert length(Enum.uniq(delivered)) == 7
  end

  test "an id freed by teardown spawns a fresh instance rather than delivering", %{
    registry: registry
  } do
    assert :spawned =
             OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p1", :deliver)

    first = instance_pid(registry, "w1")

    :ets.delete(registry, "w1")

    assert :spawned =
             OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p2", :deliver)

    refute instance_pid(registry, "w1") == first
  end

  test "a factory no connected runner registers is reported, not spawned" do
    stub(FleetManager, :runners_for_factory, fn _ns, _factory -> [] end)

    assert :factory_unknown =
             OperativeManager.spawn_or_deliver(@namespace, "w1", "nobody", "p1", :deliver)
  end

  test "an exhausted quota refuses the spawn with the reason attached" do
    stub(Quotas, :check_allocation, fn _ns, key, _n ->
      {:error, {:quota_exceeded, key, %{limit: 1}}}
    end)

    assert {:refused, reason} =
             OperativeManager.spawn_or_deliver(@namespace, "w1", @factory, "p1", :deliver)

    assert reason =~ "quota_exceeded"
  end

  describe "start authority — the one id claim (D6)" do
    defp builder(id) do
      %Flezha.Operative.Builder{id: id, factory_id: @factory, kind: :stream, init_values: %{}}
    end

    test "a start that loses the claim concedes; the winner is left strictly alone", %{
      registry: registry
    } do
      assert {:ok, _} = OperativeManager.start_operative(@namespace, builder("r1"))
      winner = instance_pid(registry, "r1")

      # Stopping the id here would tear down the instance that WON the claim —
      # the start-authority violation the duplicate verdict exists to prevent.
      reject(&ProcessHub.stop_children/3)

      assert :duplicate = OperativeManager.start_operative(@namespace, builder("r1"))
      assert instance_pid(registry, "r1") == winner
      assert Process.alive?(winner)
    end

    test "a claim refused by the registry's own check also concedes", %{registry: registry} do
      assert {:ok, _} = OperativeManager.start_operative(@namespace, builder("r1"))
      winner = instance_pid(registry, "r1")

      # The pre-check (`Validator.operative_unique/2`) sees the row first, so
      # this is the branch a non-racing duplicate start takes.
      reject(&ProcessHub.stop_children/3)
      assert :duplicate = OperativeManager.start_operative(@namespace, builder("r1"))
      assert Process.alive?(winner)
    end

    test "a refusal with no live winner behind it stays an error" do
      # A registry row whose process is gone: conceding would silently accept a
      # start that produced nothing.
      stub(ProcessHub, :child_lookup, fn _hub, id -> {id, [{node(), self()}]} end)
      stub(HubManager, :get_pid, fn _ns, _id -> {:error, "no pid found"} end)

      assert {:error, %{reason: reason}} =
               OperativeManager.start_operative(@namespace, builder("ghost"))

      assert reason == Flezha.Service.Validator.id_in_use()
    end

    test "a concurrent burst of plain starts yields exactly one instance" do
      outcomes =
        1..8
        |> Task.async_stream(
          fn _n -> OperativeManager.start_operative(@namespace, builder("r_race")) end,
          max_concurrency: 8,
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, outcome} -> outcome end)

      assert Enum.count(outcomes, &match?({:ok, _}, &1)) == 1
      assert Enum.count(outcomes, &(&1 == :duplicate)) == 7
    end
  end
end

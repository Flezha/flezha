defmodule FlezhaTest.Service.OperativeManagerSpawnTest do
  @moduledoc """
  `OperativeManager.spawn_or_deliver/6` — the controller half of a procedure's
  `ctx.spawn_workflow` (`procedure-requested-spawn`).

  The registry is `FakeStartRegistry`, whose `insert_new` is the same atomic
  claim ProcessHub's `start_child` performs, so the concurrency case exercises
  the real race resolution rather than a serialized stand-in. Authorization is
  off here, so the caller context is judged and passes; the judgment itself is
  `FlezhaTest.Runner.ControlChannelSpawnAuthzTest`.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.Quotas
  alias Flezha.Cluster.Directory
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.OperativeManager.Placement
  alias Flezha.Service.Validator
  alias FlezhaTest.Helper.FakeStartRegistry
  alias FlezhaTest.Helper.FakeStartRegistry.Instance

  @namespace "spawn-or-deliver-test"
  @factory "worker"
  @caller %{source: :sdk, principal: "runner@" <> @namespace, namespace: @namespace}

  setup :set_mimic_global_until_exit

  setup do
    %{registry: FakeStartRegistry.install(runners: ["runner-1"])}
  end

  # The one request every case sends: instance "w1" in this module's namespace.
  defp spawn_or_deliver(factory, payload, if_running),
    do: OperativeManager.spawn_or_deliver(@namespace, "w1", factory, payload, if_running, @caller)

  defp instance_pid(registry, id) do
    {:ok, pid} = FakeStartRegistry.pid_of(registry, id)
    pid
  end

  test "a free id spawns, carrying the payload as the instance's seed input" do
    assert :spawned =
             spawn_or_deliver(@factory, "p1", :deliver)

    assert [operative] = FakeStartRegistry.started()
    assert operative.kind == :workflow
    assert operative.factory_id == @factory
    assert Keyword.get(operative.opts, :seed_input) == "p1"
  end

  test "a taken id receives the payload as one more input", %{registry: registry} do
    assert :spawned =
             spawn_or_deliver(@factory, "p1", :deliver)

    assert :delivered =
             spawn_or_deliver(@factory, "p2", :deliver)

    assert Instance.ingested(instance_pid(registry, "w1")) == ["p2"]
  end

  test "the error policy reports instead of delivering", %{registry: registry} do
    assert :spawned =
             spawn_or_deliver(@factory, "p1", :deliver)

    assert :already_running =
             spawn_or_deliver(@factory, "p2", :error)

    assert Instance.ingested(instance_pid(registry, "w1")) == []
  end

  test "a concurrent burst yields one instance and delivers every other payload", %{
    registry: registry
  } do
    outcomes =
      1..8
      |> Task.async_stream(
        fn n ->
          spawn_or_deliver(@factory, "p#{n}", :deliver)
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
             spawn_or_deliver(@factory, "p1", :deliver)

    first = instance_pid(registry, "w1")

    :ets.delete(registry, "w1")

    assert :spawned =
             spawn_or_deliver(@factory, "p2", :deliver)

    refute instance_pid(registry, "w1") == first
  end

  test "a factory no connected runner registers is reported, not spawned" do
    stub(FleetManager, :runners_for_factory, fn _ns, _factory -> [] end)

    assert :factory_unknown =
             spawn_or_deliver("nobody", "p1", :deliver)
  end

  test "a start forwarded to a node whose runner has gone answers factory_unknown" do
    # The directory still names a peer for the factory and the peer has lost its
    # runner. The pick lands on this node, so the real forward runs and the
    # refusal comes back exactly as a peer's would.
    stub(FleetManager, :runners_for_factory, fn _ns, _factory -> [] end)
    stub(Directory, :factory_nodes, fn _ns, _factory -> [:"stale-peer@127.0.0.1"] end)
    stub(Placement, :pick_runner, fn _nodes, _id -> node() end)

    assert :factory_unknown = spawn_or_deliver(@factory, "p1", :deliver)
    assert FakeStartRegistry.started() == []
  end

  test "an exhausted quota refuses the spawn with the reason attached" do
    stub(Quotas, :check_allocation, fn _ns, key, _n ->
      {:error, {:quota_exceeded, key, %{limit: 1}}}
    end)

    assert {:refused, reason} =
             spawn_or_deliver(@factory, "p1", :deliver)

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

      assert reason == Validator.id_in_use()
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

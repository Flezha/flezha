defmodule Flezha.Convergence.HealRebindContractTest do
  @moduledoc """
  The one migrate-commit contract (`Heal.rebind/2` — the `rebind_operative`
  primitive's body, and the runner-disconnect reroute's entry point): the
  target comes from the factory-filtered HRW placement, and the registry
  binding flips ONLY after the operative confirms `{:runner_migrate, target}`.
  A timed-out, refused, or unplaceable migrate leaves the durable row exactly
  as it was, for the next convergence tick.
  """

  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Cluster.Directory
  alias Flezha.Convergence.Heal
  alias Flezha.Operative.Overwatch
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias ProcessHub.Service.Migration
  alias ProcessHub.Service.ProcessRegistry

  setup :set_mimic_global_until_exit

  @namespace "heal_rebind_contract_test"
  @dead_runner "runner_that_disconnected"
  @live_runner "live_market_runner"

  # An Overwatch stand-in: replies the full struct — as the heal's snapshot
  # and consent reads require (the struct defaults are consent-:ready) — with
  # a scriptable `{:runner_migrate, _}` reply.
  defmodule FakeOverwatch do
    use GenServer

    def start_link(ow, migrate \\ :confirm), do: GenServer.start_link(__MODULE__, {ow, migrate})

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call(:get_overwatch_state, _from, {ow, _migrate} = state), do: {:reply, ow, state}

    def handle_call({:runner_migrate, _target}, _from, {_ow, :confirm} = state) do
      {:reply, :ok, state}
    end

    def handle_call({:runner_migrate, _target}, _from, {_ow, :refuse} = state) do
      {:reply, {:error, :busy}, state}
    end

    # Never replies: the caller's bounded call must time out (~2 s).
    def handle_call({:runner_migrate, _target}, _from, {_ow, :hang} = state) do
      {:noreply, state}
    end
  end

  setup do
    fleet_state = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet_state end) end)

    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    on_exit(fn -> HubManager.stop_hub(@namespace) end)

    HubRegistryTestHelper.seed_runner(@namespace, @live_runner, "market")
    :ok
  end

  test "a confirmed migrate flips the binding to the factory-filtered pick" do
    id = "confirming_op"
    inject(id, factory: "market", migrate: :confirm)

    assert Heal.rebind(@namespace, id) == :ok
    assert registry_tag(id) == @live_runner
  end

  test "a timed-out migrate leaves the registry binding unchanged" do
    id = "hanging_op"
    inject(id, factory: "market", migrate: :hang)

    assert Heal.rebind(@namespace, id) == :error
    assert registry_tag(id) == @dead_runner
  end

  test "a refused migrate leaves the registry binding unchanged" do
    id = "refusing_op"
    inject(id, factory: "market", migrate: :refuse)

    assert Heal.rebind(@namespace, id) == :error
    assert registry_tag(id) == @dead_runner
  end

  test "no runner advertising the factory means no bind at all — never a random one" do
    id = "unservable_op"
    inject(id, factory: "factory_nobody_serves", migrate: :confirm)

    assert Heal.rebind(@namespace, id) == :error
    assert registry_tag(id) == @dead_runner
  end

  describe "replace_operative" do
    test "executes the per-child handover to the HRW-picked eligible node" do
      test_pid = self()
      id = "replaceable_op"
      {:ok, pid} = start_shell(id, "market")
      inject_pid(id, pid)

      Directory
      |> stub(:factory_nodes, fn @namespace, "market" -> [node(), :peer_a@host, :peer_b@host] end)

      expected = Enum.max_by([:peer_a@host, :peer_b@host], &:erlang.phash2({id, &1}))

      Migration
      |> stub(:migrate_child, fn hub_id, ^id, target ->
        send(test_pid, {:migrate_child, hub_id, target})
        :ok
      end)

      op = %{primitive: :replace_operative, namespace: @namespace, target: id, args: %{}}
      assert Heal.execute(op) == :ok

      hub_id = Identificator.hub(@namespace)
      assert_receive {:migrate_child, ^hub_id, ^expected}, 500
    end

    test "a deferring consent leaves the operative in place for the next tick" do
      test_pid = self()
      id = "deferring_op"

      {:ok, pid} =
        FakeOverwatch.start_link(%Overwatch{
          id: id,
          namespace: @namespace,
          factory_id: "market",
          # A held correction defers migration consent.
          corrections: %{"k" => :pending}
        })

      inject_pid(id, pid)

      Directory |> stub(:factory_nodes, fn @namespace, "market" -> [:peer_a@host] end)

      Migration
      |> stub(:migrate_child, fn _hub, _id, _target ->
        send(test_pid, :moved)
        :ok
      end)

      op = %{primitive: :replace_operative, namespace: @namespace, target: id, args: %{}}
      assert Heal.execute(op) == :error
      refute_received :moved
    end

    test "no other node advertising the factory fails the heal without a move" do
      test_pid = self()
      id = "nowhere_to_go_op"
      {:ok, pid} = start_shell(id, "market")
      inject_pid(id, pid)

      Directory |> stub(:factory_nodes, fn @namespace, "market" -> [node()] end)

      Migration
      |> stub(:migrate_child, fn _hub, _id, _target ->
        send(test_pid, :moved)
        :ok
      end)

      op = %{primitive: :replace_operative, namespace: @namespace, target: id, args: %{}}
      assert Heal.execute(op) == :error
      refute_received :moved
    end
  end

  defp inject_pid(id, pid) do
    cspec = %{id: id, start: {HubRegistryTestHelper.StubChild, :start_link, [{@namespace, id}]}}
    hub_id = Identificator.hub(@namespace)

    :ok = ProcessRegistry.bulk_insert(hub_id, %{id => {cspec, [{node(), pid}], %{}}})
  end

  defp start_shell(id, factory) do
    FakeOverwatch.start_link(%Overwatch{
      id: id,
      namespace: @namespace,
      factory_id: factory,
      runner_id: nil
    })
  end

  defp inject(id, opts) do
    {:ok, pid} =
      FakeOverwatch.start_link(
        %Overwatch{
          id: id,
          namespace: @namespace,
          factory_id: Keyword.fetch!(opts, :factory),
          runner_id: @dead_runner
        },
        Keyword.fetch!(opts, :migrate)
      )

    cspec = %{
      id: id,
      start:
        {HubRegistryTestHelper.StubChild, :start_link,
         [{@namespace, @dead_runner, %{id: id, factory_id: Keyword.fetch!(opts, :factory)}}]}
    }

    hub_id = Identificator.hub(@namespace)

    :ok =
      ProcessRegistry.bulk_insert(hub_id, %{
        id => {cspec, [{node(), pid}], %{tag: @dead_runner}}
      })
  end

  defp registry_tag(id) do
    hub_id = Identificator.hub(@namespace)
    {_cspec, _node_pids, metadata} = Map.fetch!(ProcessRegistry.dump(hub_id), id)
    metadata[:tag]
  end
end

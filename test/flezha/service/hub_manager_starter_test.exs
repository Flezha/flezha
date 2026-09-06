defmodule Flezha.Service.HubManagerStarterTest do
  @moduledoc """
  A hub exists for every namespace this node already has evidence for.

  Before this, `Flezha.Runner.FleetManager.Session.ensure_hub/1` was the only
  thing on the node that created a namespace hub, so a restarted controller
  replayed nothing until a runner arrived — its durable registry sat on disk
  with nothing to read it. The starter derives its set from the two sources
  that already record the answer (the registry root and the namespace
  catalog), and creates through the unchanged `HubManager.start_hub/1`.

  The gate cases hold `Orchestrator.await_cluster_ready/0` open and prove that
  BOTH creating paths — the batch here and a runner connect — stop at it, so
  the one seam is one seam.
  """

  # async: false: writes Application env; a shared on-disk root; Mimic global mode.
  use FlezhaTest.Case, fixtures: [:auth_stores, :env], async: false
  use Mimic

  import FlezhaTest.Helper.HubRegistryTestHelper
  import FlezhaTest.Helper.MimicGlobal

  @moduletag :capture_log

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Boot.Readiness
  alias Flezha.Cluster.Orchestrator
  alias Flezha.Convergence.Snapshot
  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager.Session
  alias Flezha.Service.HubManager
  alias Flezha.Service.HubManager.Starter
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  setup :set_mimic_global_until_exit

  setup %{tmp: tmp} do
    root = Path.join(tmp, "process_hub")
    File.mkdir_p!(root)
    Application.put_env(:flezha, :process_hub_registry_root, root)
    Application.put_env(:flezha, :auto_restore_enabled, true)

    # Only what this module creates comes down again: `default` is the suite's
    # own hub and every later file depends on it.
    inherited = HubManager.get_running_namespaces()

    on_exit(fn ->
      (HubManager.get_running_namespaces() -- inherited)
      |> Enum.each(&HubManager.stop_hub/1)
    end)

    {:ok, root: root, inherited: inherited}
  end

  describe "the declared set" do
    test "a registry directory alone gets a hub", ctx do
      namespace = leave_registry_on_disk(ctx)

      refute namespace in HubManager.get_running_namespaces()
      assert namespace in HubManager.declared_namespaces()

      run_starter()

      assert namespace in HubManager.get_running_namespaces()
    end

    test "a catalog entry alone gets a hub", ctx do
      namespace = unique("catalog-only")
      {:ok, _} = NamespaceStore.create(namespace)

      refute File.exists?(Path.join(ctx.root, Identificator.hub(namespace) |> to_string()))
      assert namespace in HubManager.declared_namespaces()

      run_starter()

      assert namespace in HubManager.get_running_namespaces()
    end

    test "it is derived, never named: one catalog entry gives one, three give three" do
      # A fresh catalog seeds `_system` and nothing else, which is what makes
      # the set non-empty on a clean state root without any name being written
      # into the derivation.
      assert HubManager.declared_namespaces() == ["_system"]

      {:ok, _} = NamespaceStore.create("billing")
      {:ok, _} = NamespaceStore.create("team-orders")

      assert Enum.sort(HubManager.declared_namespaces()) == ["_system", "billing", "team-orders"]
    end

    test "the kill-switch empties it, and the starter then creates nothing", ctx do
      namespace = leave_registry_on_disk(ctx)
      {:ok, _} = NamespaceStore.create(unique("catalog-off"))

      Application.put_env(:flezha, :auto_restore_enabled, false)

      assert HubManager.declared_namespaces() == []

      run_starter()

      refute namespace in HubManager.get_running_namespaces()
      # Today's behaviour, unchanged: nothing to create, nothing to wait for.
      Starter.reset()
      assert Readiness.declared_hubs_started?()
    end
  end

  describe "readiness" do
    test "refuses until the starter has run, then flips with no restart", ctx do
      namespace = leave_registry_on_disk(ctx)
      Readiness.publish_sequence_complete()
      Starter.reset()

      # The vacuous-ready window this row closes: the boot sequence is
      # recorded, and the node has created none of the hubs it owes.
      refute Readiness.declared_hubs_started?()

      run_starter()

      assert namespace in HubManager.get_running_namespaces()
      assert Readiness.declared_hubs_started?()
    end

    test "a hub torn down later does not hold the node out of rotation", ctx do
      namespace = leave_registry_on_disk(ctx)
      run_starter()
      assert Readiness.declared_hubs_started?()

      # The explicit teardown, which is unguarded. The row answers whether
      # boot-time creation finished, not whether every hub is still up, or a
      # deliberate stop would strand a healthy controller at `503` with nothing
      # left to bring it back.
      HubManager.stop_hub(namespace)

      refute namespace in HubManager.get_running_namespaces()
      assert Readiness.declared_hubs_started?()
    end
  end

  describe "the vacancy reclaim" do
    test "leaves a replayed registry alone instead of deleting it", ctx do
      namespace = leave_registry_on_disk(ctx)
      run_starter()
      assert namespace in HubManager.get_running_namespaces()

      # What the convergence loop calls on every tick for a hub with no active
      # runners. It runs seconds after boot, and `stop_hub/1` wipes the
      # registry directory — so unguarded it deletes the durable state
      # boot-time creation just recovered, before any runner could connect.
      # The window the guard is about: the rows are on disk and the live
      # registry is still empty, because the reconcile has not run yet.
      assert File.stat!(registry_file(ctx, namespace)).size > 0
      assert ProcessHub.registry_dump(Identificator.hub(namespace)) == %{}

      HubManager.stop_hub_if_vacant(namespace)

      assert namespace in HubManager.get_running_namespaces()
      assert File.regular?(registry_file(ctx, namespace))
    end

    test "leaves a hub declared to hold operatives, even once recovery settles", ctx do
      namespace = leave_registry_on_disk(ctx)
      run_starter()
      start_durable(namespace, "declared_op")

      # The second window: the reconcile has run, so the rows are back, but
      # nothing has placed them — placing an operative needs a runner and this
      # controller has none, which is the state the whole change is about.
      Snapshot.observe_recovery(Identificator.hub(namespace), :normal)
      assert HubManager.declared_operatives(namespace) != []

      HubManager.stop_hub_if_vacant(namespace)

      assert namespace in HubManager.get_running_namespaces()
      assert File.regular?(registry_file(ctx, namespace))
    end

    test "still reclaims a settled hub that holds nothing" do
      namespace = unique("empty-hub")
      {:ok, _} = ensure_hub(namespace)

      # Recovery reached a terminal state with nothing in the registry: there
      # is no unreplayed row to lose, so the reclaim keeps its purpose.
      Snapshot.observe_recovery(Identificator.hub(namespace), :normal)

      HubManager.stop_hub_if_vacant(namespace)

      refute namespace in HubManager.get_running_namespaces()
    end
  end

  describe "a hub that will not start" do
    test "is retried with telemetry, and takes neither the owner nor the node down" do
      namespace = unique("refused")
      {:ok, _} = NamespaceStore.create(namespace)

      Telemetry.capture([:flezha, :hub, :start_failed])
      stub(HubManager, :start_hub, fn _namespace -> {:error, :refused_by_the_test} end)

      pid = run_starter()

      # One event per failed namespace, so the pattern names the one under test
      # rather than whichever the catalog walked first.
      assert_receive {:telemetry, [:flezha, :hub, :start_failed], _measure,
                      %{target: ^namespace} = meta}

      assert meta.reason =~ "refused_by_the_test"

      assert Process.alive?(pid)
      assert Process.alive?(Process.whereis(Flezha.Supervisor))
      refute namespace in HubManager.get_running_namespaces()

      # The readiness half of the same state: the node refuses traffic while
      # the hub is missing, and says which precondition holds it.
      refute Readiness.declared_hubs_started?()
      {verdict, rows} = Readiness.verdict()
      assert verdict == :recovering
      assert %{key: :declared_hubs_started, met: false} in rows
    end
  end

  describe "the cluster-formation gate" do
    test "the starter creates nothing until it clears", ctx do
      namespace = leave_registry_on_disk(ctx)
      hold_gate()

      {:ok, _pid} = start_supervised(Starter)

      assert_receive {:at_gate, waiter}, 2_000
      refute namespace in HubManager.get_running_namespaces()

      send(waiter, :release)

      assert_eventually(fn -> namespace in HubManager.get_running_namespaces() end)
    end

    test "a runner connect stops at the same gate" do
      namespace = unique("connect-gated")
      hold_gate()

      # Unlinked on purpose: past the gate the connect raises on the stream ref
      # this test does not have, and the hub creation it does FIRST is the step
      # under test.
      spawn(fn -> Session.catch_runner_connect(make_ref(), namespace, :uds) end)

      assert_receive {:at_gate, waiter}, 2_000
      refute namespace in HubManager.get_running_namespaces()

      send(waiter, :release)

      assert_eventually(fn -> namespace in HubManager.get_running_namespaces() end)
    end
  end

  # ---------- helpers ----------

  # The state a restarted controller is in: a registry on disk with no hub
  # over it. NOT `stop_hub/1`, which wipes the registry directory.
  defp leave_registry_on_disk(%{root: root}) do
    namespace = unique("registry-only")
    {:ok, _} = ensure_hub(namespace)
    inject_operative(namespace, "durable_op", [{node(), spawn(&NodeTestHelper.park/0)}])

    hub_id = Identificator.hub(namespace)
    Supervisor.terminate_child(HubManager, hub_id)
    Supervisor.delete_child(HubManager, hub_id)

    assert File.regular?(Path.join([root, to_string(hub_id), "registry.dets"]))
    namespace
  end

  defp registry_file(%{root: root}, namespace) do
    Path.join([root, to_string(Identificator.hub(namespace)), "registry.dets"])
  end

  # `handle_continue(:bootstrap, _)` runs before any call is served, so an
  # answered call is the first batch finished.
  defp run_starter() do
    {:ok, pid} = start_supervised(Starter)
    _ = :sys.get_state(pid)
    pid
  end

  # Park every caller of the gate and hand the test the parked process.
  defp hold_gate() do
    owner = self()

    stub(Orchestrator, :await_cluster_ready, fn ->
      send(owner, {:at_gate, self()})

      receive do
        :release -> :ok
      after
        5_000 -> :timeout
      end
    end)
  end

  defp assert_eventually(fun, timeout_ms \\ 2_000) do
    assert FlezhaTest.Helper.NodeTestHelper.wait_until(fun, timeout_ms)
  end

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"
end

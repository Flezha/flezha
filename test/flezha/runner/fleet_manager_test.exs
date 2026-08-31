defmodule FlezhaTest.Runner.FleetManagerGhostTest do
  @moduledoc """
  Regression coverage for the stream ghost-runner accumulation that
  appeared under rapid repeated runner kill/restart (chaos soak): a dead
  runner whose control channel closed was never removed from the active
  set when the disconnect handler's operative-reroute side effects raised
  (`:noproc`/`ArgumentError` against a hub mid-restart), so
  `active_runners/1` kept reporting corpses and operatives got assigned to a
  dead node.

  These tests drive `Flezha.Runner.FleetManager` directly with synthetic
  active-runner state (no real SDK / no app_data) so they can run in the
  presence of a live chaos cluster.
  """

  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks

  # async: false: the live controller and its fleet; node-global tables.

  use ExUnit.Case, async: false

  # A namespace with no running ProcessHub so the reroute side effects
  # (`HubManager.runner_operatives/2` → ETS match on an undefined table) raise,
  # exercising the crash-proof eviction path.
  @namespace "ghost_test_namespace_#{System.unique_integer([:positive])}"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  defp seed_active(runner_ids) do
    ts = System.system_time(:millisecond)
    entries = Map.new(runner_ids, fn id -> {id, {:active, ts}} end)

    :sys.replace_state(FleetManager, fn state ->
      put_in(state, [:runner_namespaces, @namespace], entries)
    end)
  end

  test "superseded-session marks are pruned past their TTL on the cleanup tick" do
    fresh = "fence_fresh_#{System.unique_integer([:positive])}"
    aged = "fence_aged_#{System.unique_integer([:positive])}"

    Marks.mark_session_superseded(fresh)
    Marks.mark_session_superseded(aged)

    # Age one mark past the 10-minute TTL, then drive the periodic cleanup.
    true =
      :ets.update_element(
        :flezha_superseded_sessions,
        aged,
        {2, :erlang.monotonic_time(:millisecond) - 700_000}
      )

    :ok = GenServer.call(FleetManager, :runner_cleanup)

    refute Marks.superseded_session?(aged)
    assert Marks.superseded_session?(fresh)
    :ets.delete(:flezha_superseded_sessions, fresh)
  end

  test "a closed runner is removed from the active set even when reroute side effects fail" do
    seed_active(["ghost_a", "ghost_b"])
    assert "ghost_a" in FleetManager.active_runners(@namespace)
    assert "ghost_b" in FleetManager.active_runners(@namespace)

    # Mirrors ControlChannel.handle_connection_closed/1.
    :ok = GenServer.call(FleetManager, {:runner_disconnected, @namespace, "ghost_a"})

    active = FleetManager.active_runners(@namespace)
    refute "ghost_a" in active, "dead runner must not linger as active"
    assert "ghost_b" in active, "live peer must survive the disconnect"
  end

  test "a live runner is protected from eviction, unless the classifier proved it wedged" do
    seed_active(["wedged_one", "healthy_one"])

    # The guard: a stale convergence snapshot must never evict a runner whose
    # connection is open.
    :ok = FleetManager.evict_runner("wedged_one")
    assert "wedged_one" in FleetManager.active_runners(@namespace)

    # The waiver: a pool-wedged runner is still connected — being connected is
    # exactly what stops anything else from dislodging it — so the verdict-
    # driven eviction has to get through, and only that one.
    :ok = FleetManager.evict_runner("wedged_one", proved_wedged: true)

    active = FleetManager.active_runners(@namespace)
    refute "wedged_one" in active
    assert "healthy_one" in active
  end

  test "active set does not accumulate ghosts under repeated kill/restart churn" do
    # Simulate N rounds of: fresh runner connects (added to active set by a
    # peer), prior one's control channel closes. The active set must never
    # grow past the live runners, regardless of reroute failures.
    seed_active(["live_peer", "round_0"])

    Enum.each(1..8, fn round ->
      :sys.replace_state(FleetManager, fn state ->
        update_in(state, [:runner_namespaces, @namespace], fn ns ->
          Map.put(ns, "round_#{round}", {:active, System.system_time(:millisecond)})
        end)
      end)

      :ok =
        GenServer.call(FleetManager, {:runner_disconnected, @namespace, "round_#{round - 1}"})
    end)

    # Drain the final outstanding id; "live_peer" + "round_8" remain.
    :ok = GenServer.call(FleetManager, {:runner_disconnected, @namespace, "round_8"})

    active = FleetManager.active_runners(@namespace)
    assert "live_peer" in active

    assert Enum.all?(active, fn id -> id == "live_peer" end),
           "no dead round_* ghosts may remain, got: #{inspect(active)}"
  end
end

defmodule Flezha.Runner.FleetManagerHubLifecycleTest do
  @moduledoc """
  A namespace hub outlives its runner while it still holds registered
  (durably-suspended) operatives. `runner_cleanup` reaps the dead runner ref but
  must NOT `stop_hub` while operatives remain — otherwise a parked fleet whose
  sole runner restarts loses every Overwatch and its durable registry, breaking
  the "still addressable" promise (workflow_operative.md). When the registry is
  empty, cleanup still tears the hub down.

  Drives `FleetManager` directly with a synthetic `:inactive` runner against a
  real (but operative-less) ProcessHub, injecting a registry row the way
  `HubManagerRegistryTest` does — no SDK / no runner process needed.
  """
  # async: false: writes Application env; the live controller and its fleet.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.HubRegistryTestHelper

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager

  setup do
    original = :sys.get_state(FleetManager)
    keepalive = Application.get_env(:flezha, :runner_keepalive)

    on_exit(fn ->
      :sys.replace_state(FleetManager, fn _ -> original end)
      restore_env(:runner_keepalive, keepalive)
    end)

    :ok
  end

  test "cleanup preserves a hub that still holds a registered operative" do
    ns = fresh_ns("keep")
    {:ok, _} = ensure_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    inject_operative(ns, "parked-1")
    assert HubManager.registered_operatives?(ns)

    seed_inactive(ns, "dead_runner")
    cleanup_now()

    assert HubManager.hub_alive?(ns) === true, "hub torn down despite a registered operative"
    refute Map.has_key?(runner_map(ns), "dead_runner"), "dead runner ref must still be reaped"
  end

  test "cleanup tears down a hub whose registry is empty" do
    ns = fresh_ns("empty")
    {:ok, _} = ensure_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    refute HubManager.registered_operatives?(ns)

    seed_inactive(ns, "dead_runner")
    cleanup_now()

    assert HubManager.hub_alive?(ns) === false, "an empty hub should still be reclaimed"
  end

  test "get_pid returns a clean error (never raises) when the hub is gone" do
    ns = fresh_ns("nohub")
    assert HubManager.get_pid(ns, "whoever") == {:error, "hub not alive"}
  end

  # --- helpers -----------------------------------------------------------

  defp fresh_ns(tag), do: "hub_lifecycle_#{tag}_#{System.unique_integer([:positive])}"

  defp cleanup_now() do
    Application.put_env(:flezha, :runner_keepalive, "0")
    :ok = GenServer.call(FleetManager, :runner_cleanup)
  end

  defp seed_inactive(ns, runner_id) do
    entry = {runner_id, {:inactive, System.system_time(:millisecond) - 1}}

    :sys.replace_state(FleetManager, fn state ->
      put_in(state, [:runner_namespaces, ns], Map.new([entry]))
    end)
  end

  defp runner_map(ns), do: :sys.get_state(FleetManager).runner_namespaces[ns] || %{}

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, val), do: Application.put_env(:flezha, key, val)
end

defmodule FlezhaTest.Runner.FleetManagerSdkLangTest do
  @moduledoc """
  The cross-SDK placement guard (`guard-cross-sdk-placement`).

  Durable `State`/`Change` bodies are per-SDK below the shared 16-byte
  envelope, and a restore ships those bodies verbatim to whichever runner bound
  the Entity. So placement must never hand a factory's operative to a runner of
  another language. The guard lives in the ONE filter every placement path
  reads (`placeable_runners/3`), which is what these tests drive through
  `runners_for_factory/3`.

  The homogeneous unit is the FACTORY, not the namespace: a namespace holding a
  Rust class and a Java class is legitimate and must keep working.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Factories
  alias Flezha.State.Paths
  alias Flezha.State.Restore
  alias FlezhaTest.Helper.FleetStateHelper
  alias Flezha.State.TestHelpers, as: StateTestHelpers

  # async: false: writes Application env; replaces the FleetManager's state.

  use ExUnit.Case, async: false
  use Mimic

  @rust 1
  @java 2
  @elixir 3

  setup do
    original = :sys.get_state(FleetManager)
    Logger.put_module_level([FleetManager, Factories], :warning)

    on_exit(fn ->
      Logger.delete_module_level([FleetManager, Factories])
      :sys.replace_state(FleetManager, fn _ -> original end)
    end)

    namespace = "sdk_lang_ns_#{System.unique_integer([:positive])}"
    {:ok, namespace: namespace}
  end

  defp factory(base), do: "#{base}_#{System.unique_integer([:positive])}"

  defp forget(ns, ids), do: on_exit(fn -> FleetStateHelper.forget_langs(ns, ids) end)

  describe "the placement filter" do
    test "places on a runner of the factory's own language", %{namespace: ns} do
      id = factory("orders")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"rust_runner" => [{id, @rust}]})

      assert FleetManager.runners_for_factory(ns, id) == ["rust_runner"]
    end

    test "excludes a runner advertising another language", %{namespace: ns} do
      id = factory("orders")
      forget(ns, [id])

      # The Rust runner connects first and establishes the factory; the Java
      # runner then advertises the same id — the exact shape the conformance
      # fleet has by design (49 factory ids are claimed by both).
      FleetStateHelper.seed(ns, %{"rust_runner" => [{id, @rust}]})
      :ok = FleetStateHelper.sync(ns, "java_runner", [{id, @java}])

      assert FleetManager.runners_for_factory(ns, id) == ["rust_runner"]
    end

    test "a dead runner's factory does not fall through to the other language", %{namespace: ns} do
      id = factory("orders")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"rust_runner" => [{id, @rust}], "java_runner" => []})
      :ok = FleetStateHelper.sync(ns, "java_runner", [{id, @java}])

      # The reroute off a dead runner reads the same filter as initial start.
      :ok = FleetStateHelper.sync(ns, "rust_runner", [])

      assert FleetManager.runners_for_factory(ns, id) == [],
             "an operative must stay unbound rather than reroute onto a runner " <>
               "that cannot read its state"
    end

    test "a runner advertising no language never places", %{namespace: ns} do
      id = factory("untagged")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"old_runner" => [{id, 0}]})

      assert FleetManager.runners_for_factory(ns, id) == []
    end

    test "a mixed-language namespace still places each factory", %{namespace: ns} do
      orders = factory("orders")
      billing = factory("billing")
      forget(ns, [orders, billing])

      FleetStateHelper.seed(ns, %{
        "rust_runner" => [{orders, @rust}],
        "java_runner" => [{billing, @java}]
      })

      assert FleetManager.runners_for_factory(ns, orders) == ["rust_runner"]
      assert FleetManager.runners_for_factory(ns, billing) == ["java_runner"]
    end
  end

  describe "the established language" do
    test "the first advertiser establishes a factory with no durable state", %{namespace: ns} do
      id = factory("stateless")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"elixir_runner" => [{id, @elixir}]})

      assert ClassRegistry.sdk_lang(ns, id) == @elixir
    end

    test "a lost record is re-derived from the advertisements, not read as a refusal",
         %{namespace: ns} do
      id = factory("lost_record")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"r1" => [{id, @rust}], "r2" => [{id, @rust}]})

      # Both halves of the record are losable: the ETS table is recreated
      # empty if its owner restarts, and the sidecar goes with the factory's
      # state directory.
      ClassRegistry.delete_sdk_lang(ns, id)

      assert Enum.sort(FleetManager.runners_for_factory(ns, id)) == ["r1", "r2"],
             "a missing record is no evidence of a conflict and must not refuse everyone"

      # The query derives; it does not write. The next advertisement re-records.
      assert ClassRegistry.sdk_lang(ns, id) == nil
      :ok = FleetStateHelper.sync(ns, "r1", [{id, @rust}])
      assert ClassRegistry.sdk_lang(ns, id) == @rust
    end

    test "disagreeing advertisements with nothing recorded place nowhere", %{namespace: ns} do
      id = factory("ambiguous")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"rust_runner" => [{id, @rust}], "java_runner" => [{id, @java}]})
      ClassRegistry.delete_sdk_lang(ns, id)

      assert FleetManager.runners_for_factory(ns, id) == [],
             "with no record and no agreement the controller cannot tell which " <>
               "language owns the factory"
    end

    test "a factory nobody advertises any more forgets its language", %{namespace: ns} do
      id = factory("dropped")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"r1" => [{id, @rust}]})
      assert ClassRegistry.sdk_lang(ns, id) == @rust

      :ok = FleetStateHelper.sync(ns, "r1", [])

      assert ClassRegistry.sdk_lang(ns, id) == nil,
             "clearing lets a factory re-establish — the escape hatch from a " <>
               "factory deliberately re-authored in another language"
    end

    test "a recorded language outranks a later advertiser", %{namespace: ns} do
      # The bind records the language the bytes on disk were written by,
      # correcting a factory a first advertiser established wrongly. From then
      # on the filter refuses every runner that cannot read those bytes.
      id = factory("stateful")
      forget(ns, [id])
      ClassRegistry.put_sdk_lang(ns, id, @java)

      FleetStateHelper.seed(ns, %{"rust_runner" => []})
      :ok = FleetStateHelper.sync(ns, "rust_runner", [{id, @rust}])

      assert ClassRegistry.sdk_lang(ns, id) == @java
      assert FleetManager.runners_for_factory(ns, id) == []
    end

    test "the bind establishes it from the bytes on disk, correcting a first advertiser",
         %{namespace: ns} do
      base = StateTestHelpers.tmp_app_data_dir()

      on_exit(fn ->
        Application.delete_env(:flezha, :state_root)
        File.rm_rf!(base)
      end)

      id = factory("stateful")
      forget(ns, [id])
      operative = "#{id}-0"
      entity = {operative, "p0", "k"}
      StateTestHelpers.write_wal_of_lang(entity, @java)

      assert entity in Paths.entities_of(operative), "fixture: #{base}"
      assert Restore.class_sdk_lang(operative) == @java
      assert Restore.class_sdk_lang("never_written") == nil

      # A Rust runner got there first and established the factory wrongly.
      ClassRegistry.put_sdk_lang(ns, id, @rust)

      :ok = Restore.establish_sdk_lang_from_state(ns, id, operative)

      assert ClassRegistry.sdk_lang(ns, id) == @java,
             "the bytes on disk outrank whichever runner advertised first"
    end
  end

  describe "the refusal" do
    test "is distinguishable from a factory that does not exist", %{namespace: ns} do
      id = factory("orders")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"rust_runner" => [{id, @rust}], "java_runner" => []})
      :ok = FleetStateHelper.sync(ns, "java_runner", [{id, @java}])
      :ok = FleetStateHelper.sync(ns, "rust_runner", [])

      assert FleetManager.placement_refusal(ns, id) == {:language, @rust}
      assert FleetManager.placement_refusal(ns, factory("never_declared")) == :no_such_factory
    end

    test "a conflicting advertisement is logged and stored, not silent", %{namespace: ns} do
      id = factory("orders")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"rust_runner" => [{id, @rust}]})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          :ok = FleetStateHelper.sync(ns, "java_runner", [{id, @java}])
        end)

      assert log =~ "java_runner"
      assert log =~ id

      # Stored so operator tooling can see it, excluded from placement.
      {:ok, factories} = FleetManager.list_factories(ns)
      assert Enum.any?(factories, &(&1.id == id))
      assert FleetManager.runners_for_factory(ns, id) == ["rust_runner"]
    end
  end
end

defmodule FlezhaTest.Runner.FleetManagerSyncFactoriesTest do
  @moduledoc """
  `{:sync_factories, ...}` cast handling (`runner-factory-sync`): a full-set
  replacement of one runner's declared factory list composed from the existing
  removal + store paths. Idempotent on redelivery, scoped to the sending
  runner, legal with an empty set, and warning — not blocking — when a dropped
  factory still has controller-known operatives bound to that runner (the
  suspended-instance removal hole's tripwire).

  Drives the live `Flezha.Runner.FleetManager` with synthetic state, mirroring
  `FleetManagerGhostTest`.
  """

  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Factories
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.FleetStateHelper

  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  @namespace "sync_factories_ns_#{System.unique_integer([:positive])}"

  setup do
    original = :sys.get_state(FleetManager)

    # `config/test.exs` pins the global Logger level to `:error`; raise this
    # module's level so the dropped-factory warning reaches the capture.
    Logger.put_module_level([FleetManager, Factories], :warning)

    on_exit(fn ->
      Logger.delete_module_level([FleetManager, Factories])
      :sys.replace_state(FleetManager, fn _ -> original end)
    end)

    :ok
  end

  defp seed(runner_factories), do: FleetStateHelper.seed(@namespace, runner_factories)

  defp sync(runner, factory_ids), do: FleetStateHelper.sync(@namespace, runner, factory_ids)

  defp factory_ids do
    {:ok, factories} = FleetManager.list_factories(@namespace)
    Enum.map(factories, & &1.id)
  end

  test "full-set replacement drops removed factories and adds new ones" do
    seed(%{"r1" => ["a", "b"]})

    :ok = sync("r1", ["a", "c"])

    assert factory_ids() == ["a", "c"]
    assert FleetManager.runners_for_factory(@namespace, "b") == []
    assert FleetManager.runners_for_factory(@namespace, "c") == ["r1"]
  end

  test "redelivery of the same set is idempotent" do
    seed(%{"r1" => ["a"]})

    :ok = sync("r1", ["a", "c"])
    :ok = sync("r1", ["a", "c"])

    assert factory_ids() == ["a", "c"]
    assert FleetManager.runners_for_factory(@namespace, "c") == ["r1"]
  end

  test "other runners' factory refs are untouched" do
    seed(%{"r1" => ["a"], "r2" => ["b"]})

    :ok = sync("r1", ["c"])

    assert FleetManager.runners_for_factory(@namespace, "a") == []
    assert FleetManager.runners_for_factory(@namespace, "b") == ["r2"]
    assert FleetManager.runners_for_factory(@namespace, "c") == ["r1"]
  end

  test "an empty set is legal and leaves the runner connected" do
    seed(%{"r1" => ["a", "b"]})

    :ok = sync("r1", [])

    assert factory_ids() == []
    assert "r1" in FleetManager.active_runners(@namespace)
  end

  test "a dropped factory with controller-known bound operatives logs a warning" do
    seed(%{"r1" => ["a", "b"]})

    set_mimic_global_until_exit()

    Mimic.stub(HubManager, :operative_rows, fn @namespace ->
      [
        %{id: "suspended_op", health: :unknown, runner: "r1", factory: "b", node: nil},
        %{id: "other_op", health: :unknown, runner: "r1", factory: "a", node: nil}
      ]
    end)

    log =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        :ok = sync("r1", ["a"])
      end)

    assert log =~ "dropped factory b"
    assert log =~ "suspended_op"
    assert log =~ @namespace
    assert log =~ "r1"
    refute log =~ "other_op", "factories still declared must not be warned about"
  end

  test "a suspended (unbound) operative warns only when no other runner has the factory" do
    seed(%{"r1" => ["b"], "r2" => ["b"]})

    set_mimic_global_until_exit()

    Mimic.stub(HubManager, :operative_rows, fn @namespace ->
      [%{id: "parked_op", health: :unknown, runner: nil, factory: "b", node: nil}]
    end)

    # r2 still advertises b: the suspended instance remains placeable — silent.
    quiet =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        :ok = sync("r1", [])
      end)

    refute quiet =~ "dropped factory"

    # r2 drops it too: nothing can place the instance anymore — warn.
    loud =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        :ok = sync("r2", [])
      end)

    assert loud =~ "dropped factory b"
    assert loud =~ "parked_op"
  end

  test "dropping a factory with no controller-known operatives is silent" do
    seed(%{"r1" => ["a", "b"]})

    log =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        :ok = sync("r1", ["a"])
      end)

    refute log =~ "dropped factory"
    assert factory_ids() == ["a"]
  end
end

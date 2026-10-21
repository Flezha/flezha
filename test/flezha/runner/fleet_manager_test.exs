defmodule FlezhaTest.Runner.FleetManagerGhostTest do
  @moduledoc """
  Regression coverage for the stream ghost-runner accumulation that
  appeared under rapid repeated runner kill/restart (chaos soak): a dead
  runner whose control channel closed was never removed from the active
  set when the disconnect handler's relay-reroute side effects raised
  (`:noproc`/`ArgumentError` against a hub mid-restart), so
  `active_runners/1` kept reporting corpses and relays got assigned to a
  dead node.

  These tests drive `Flezha.Runner.FleetManager` directly with synthetic
  active-runner state (no real SDK / no storage) so they can run in the
  presence of a live chaos cluster.
  """

  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Helper.FleetStateHelper
  alias Flezha.Runner.FleetManager.Marks

  # async: false: the live controller and its fleet; node-global tables.

  use ExUnit.Case, async: false

  # A namespace with no running ProcessHub so the reroute side effects
  # (`HubManager.runner_relays/2` → ETS match on an undefined table) raise,
  # exercising the crash-proof eviction path.
  @namespace "ghost_test_namespace_#{System.unique_integer([:positive])}"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  defp seed_active(runner_ids) do
    ts = System.monotonic_time(:millisecond)
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
    :ok = FleetStateHelper.disconnected(@namespace, "ghost_a")

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
          Map.put(ns, "round_#{round}", {:active, System.monotonic_time(:millisecond)})
        end)
      end)

      :ok =
        FleetStateHelper.disconnected(@namespace, "round_#{round - 1}")
    end)

    # Drain the final outstanding id; "live_peer" + "round_8" remain.
    :ok = FleetStateHelper.disconnected(@namespace, "round_8")

    active = FleetManager.active_runners(@namespace)
    assert "live_peer" in active

    assert Enum.all?(active, fn id -> id == "live_peer" end),
           "no dead round_* ghosts may remain, got: #{inspect(active)}"
  end
end

defmodule Flezha.Runner.FleetManagerHubLifecycleTest do
  @moduledoc """
  A namespace hub outlives its runner while it still holds registered
  (durably-suspended) relays. `runner_cleanup` reaps the dead runner ref but
  must NOT `stop_hub` while relays remain — otherwise a parked fleet whose
  sole runner restarts loses every Overwatch and its durable registry, breaking
  the "still addressable" promise (relay.md). When the registry is
  empty, cleanup still tears the hub down.

  Drives `FleetManager` directly with a synthetic `:inactive` runner against a
  real (but relay-less) ProcessHub, injecting a registry row the way
  `HubManagerRegistryTest` does — no SDK / no runner process needed.
  """
  # async: false: writes Application env; the live controller and its fleet.
  use FlezhaTest.Case, fixtures: [:env], async: false

  import FlezhaTest.Helper.HubRegistryTestHelper

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  test "cleanup preserves a hub that still holds a registered relay" do
    ns = fresh_ns("keep")
    {:ok, _} = ensure_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    inject_relay(ns, "parked-1")
    assert HubManager.registered_relays?(ns)

    seed_inactive(ns, "dead_runner")
    cleanup_now()

    assert HubManager.hub_alive?(ns) === true, "hub torn down despite a registered relay"
    refute Map.has_key?(runner_map(ns), "dead_runner"), "dead runner ref must still be reaped"
  end

  test "cleanup tears down a hub whose registry is empty" do
    ns = fresh_ns("empty")
    {:ok, _} = ensure_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    refute HubManager.registered_relays?(ns)

    seed_inactive(ns, "dead_runner")
    cleanup_now()

    assert HubManager.hub_alive?(ns) === false, "an empty hub should still be reclaimed"
  end

  test "cleanup preserves an empty hub while another runner is active in its namespace" do
    ns = fresh_ns("occupied")
    {:ok, _} = ensure_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    seed_inactive(ns, "dead_runner")

    :sys.replace_state(FleetManager, fn state ->
      put_in(state, [:runner_namespaces, ns, "live_runner"], {:active, 0})
    end)

    cleanup_now()

    assert HubManager.hub_alive?(ns) === true, "hub torn down under an active runner"
    refute Map.has_key?(runner_map(ns), "dead_runner"), "dead runner ref must still be reaped"
  end

  test "get_pid returns a clean error (never raises) when the hub is gone" do
    ns = fresh_ns("nohub")
    assert HubManager.get_pid(ns, "whoever") == {:error, {:hub_down, ns}}
  end

  # --- helpers -----------------------------------------------------------

  defp fresh_ns(tag), do: "hub_lifecycle_#{tag}_#{System.unique_integer([:positive])}"

  defp cleanup_now() do
    Application.put_env(:flezha, :runner_removal_grace_ms, 0)
    :ok = GenServer.call(FleetManager, :runner_cleanup)
  end

  defp seed_inactive(ns, runner_id) do
    entry = {runner_id, {:inactive, System.monotonic_time(:millisecond) - 1}}

    :sys.replace_state(FleetManager, fn state ->
      put_in(state, [:runner_namespaces, ns], Map.new([entry]))
    end)
  end

  defp runner_map(ns), do: :sys.get_state(FleetManager).runner_namespaces[ns] || %{}
end

defmodule FlezhaTest.Runner.FleetManagerSdkLangTest do
  @moduledoc """
  The cross-SDK placement guard (`guard-cross-sdk-placement`).

  Durable `State`/`Change` bodies are per-SDK below the shared 16-byte
  envelope, and a restore ships those bodies verbatim to whichever runner bound
  the Cell. So placement must never hand a factory's relay to a runner of
  another language. The guard lives in the ONE filter every placement path
  reads (`placeable_runners/3`), which is what these tests drive through
  `runners_for_factory/3`.

  The homogeneous unit is the FACTORY, not the namespace: a namespace holding a
  Rust class and a Java class is legitimate and must keep working.
  """

  alias Flezha.Observability.TelemetryBridge
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Factories
  alias Flezha.State.Paths
  alias Flezha.State.Restore
  alias Flezha.State.TestHelpers, as: StateTestHelpers
  alias FlezhaTest.Helper.FleetStateHelper

  # async: false: writes Application env; replaces the FleetManager's state.

  use ExUnit.Case, async: false
  use Mimic

  @rust 1
  @java 2
  @elixir 3

  setup do
    original = :sys.get_state(FleetManager)
    # A conflict's warning is its event's bridge line, written by the bridge.
    Logger.put_module_level([FleetManager, Factories, TelemetryBridge], :warning)

    on_exit(fn ->
      Logger.delete_module_level([FleetManager, Factories, TelemetryBridge])
      :sys.replace_state(FleetManager, fn _ -> original end)
    end)

    namespace = "sdk_lang_ns_#{System.unique_integer([:positive])}"
    {:ok, namespace: namespace}
  end

  defp factory(base), do: "#{base}_#{System.unique_integer([:positive])}"

  defp forget(ns, ids), do: on_exit(fn -> FleetStateHelper.forget_established(ns, ids) end)

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
             "a relay must stay unbound rather than reroute onto a runner " <>
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

      assert ClassRegistry.established(ns, id, :sdk_lang) == @elixir
    end

    test "a lost record is re-derived from the advertisements, not read as a refusal",
         %{namespace: ns} do
      id = factory("lost_record")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"r1" => [{id, @rust}], "r2" => [{id, @rust}]})

      # Both halves of the record are losable: the ETS table is recreated
      # empty if its owner restarts, and the sidecar goes with the factory's
      # state directory.
      ClassRegistry.delete_established(ns, id)

      assert Enum.sort(FleetManager.runners_for_factory(ns, id)) == ["r1", "r2"],
             "a missing record is no evidence of a conflict and must not refuse everyone"

      # The query derives; it does not write. The next advertisement re-records.
      assert ClassRegistry.established(ns, id, :sdk_lang) == nil
      :ok = FleetStateHelper.sync(ns, "r1", [{id, @rust}])
      assert ClassRegistry.established(ns, id, :sdk_lang) == @rust
    end

    test "disagreeing advertisements with nothing recorded place nowhere", %{namespace: ns} do
      id = factory("ambiguous")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"rust_runner" => [{id, @rust}], "java_runner" => [{id, @java}]})
      ClassRegistry.delete_established(ns, id)

      assert FleetManager.runners_for_factory(ns, id) == [],
             "with no record and no agreement the controller cannot tell which " <>
               "language owns the factory"
    end

    test "a factory nobody advertises any more forgets its language", %{namespace: ns} do
      id = factory("dropped")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"r1" => [{id, @rust}]})
      assert ClassRegistry.established(ns, id, :sdk_lang) == @rust

      :ok = FleetStateHelper.sync(ns, "r1", [])

      assert ClassRegistry.established(ns, id, :sdk_lang) == nil,
             "clearing lets a factory re-establish — the escape hatch from a " <>
               "factory deliberately re-authored in another language"
    end

    test "a recorded language outranks a later advertiser", %{namespace: ns} do
      # The bind records the language the bytes on disk were written by,
      # correcting a factory a first advertiser established wrongly. From then
      # on the filter refuses every runner that cannot read those bytes.
      id = factory("stateful")
      forget(ns, [id])
      ClassRegistry.put_established(ns, id, :sdk_lang, @java)

      FleetStateHelper.seed(ns, %{"rust_runner" => []})
      :ok = FleetStateHelper.sync(ns, "rust_runner", [{id, @rust}])

      assert ClassRegistry.established(ns, id, :sdk_lang) == @java
      assert FleetManager.runners_for_factory(ns, id) == []
    end

    test "the bind establishes it from the bytes on disk, correcting a first advertiser",
         %{namespace: ns} do
      base = StateTestHelpers.tmp_app_data_dir()

      id = factory("stateful")
      forget(ns, [id])
      relay = "#{id}-0"
      cell = {relay, "p0", "k"}
      StateTestHelpers.write_wal_of_lang(cell, @java)

      assert cell in Paths.cells_of(relay), "fixture: #{base}"
      assert Restore.class_sdk_lang(relay) == @java
      assert Restore.class_sdk_lang("never_written") == nil

      # A Rust runner got there first and established the factory wrongly.
      ClassRegistry.put_established(ns, id, :sdk_lang, @rust)

      :ok = Restore.establish_sdk_lang_from_state(ns, id, relay)

      assert ClassRegistry.established(ns, id, :sdk_lang) == @java,
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

      # Stored so user tooling can see it, excluded from placement.
      {:ok, factories} = FleetManager.list_factories(ns)
      assert Enum.any?(factories, &(&1.id == id))
      assert FleetManager.runners_for_factory(ns, id) == ["rust_runner"]

      # The row carries the same placeable runners, so a listing needs no
      # per-factory lookup.
      assert %{runners: ["rust_runner"]} = Enum.find(factories, &(&1.id == id))
    end
  end

  describe "the established run" do
    test "the first announcement establishes it, and a start reads it", %{namespace: ns} do
      id = factory("paper_order")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"r1" => [{id, @rust, :steps}]})

      assert ClassRegistry.established(ns, id, :run) == :steps
      assert FleetManager.factory_run(ns, id) == {:ok, :steps}
      assert {:ok, [%{id: ^id, run: :steps}]} = FleetManager.list_factories(ns)
    end

    test "a bind reads the run, loop shape and failure mode as placement does once the record is gone",
         %{namespace: ns} do
      id = factory("advertised_loop")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"r1" => []})

      steps = %{
        "id" => id,
        "sdk_lang" => @rust,
        "run" => "steps",
        "each" => "chained",
        "on_failure" => "keep_progress"
      }

      :ok = FleetStateHelper.announce(ns, "r1", [steps])
      # The record's table is recreated empty when its owner restarts.
      ClassRegistry.delete_established(ns, id)
      assert FleetManager.runners_for_factory(ns, id) == ["r1"]

      assert FleetManager.established(ns, id, [:run, :each, :on_failure]) ==
               %{run: :steps, each: :chained, on_failure: :keep_progress}
    end

    test "a runner announcing the other run is stored, logged and never placed",
         %{namespace: ns} do
      id = factory("prices")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"r1" => [{id, @rust, :flow}], "r2" => []})
      event = [:flezha, :runner, :factory_run_conflict]
      ref = :telemetry_test.attach_event_handlers(self(), [event])

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          :ok = FleetStateHelper.sync(ns, "r2", [{id, @rust, :steps}])
        end)

      assert log =~ "[flezha_runner] factory_run_conflict advertised=steps"
      assert log =~ "runner=r2"

      assert_received {^event, ^ref, %{count: 1},
                       %{factory: ^id, established: :flow, advertised: :steps}}

      assert FleetManager.runners_for_factory(ns, id) == ["r1"]

      :ok = FleetStateHelper.sync(ns, "r1", [])
      assert FleetManager.placement_refusal(ns, id) == {:run, :flow}
      assert FleetManager.factory_run(ns, id) == {:error, {:run, :flow}}
    end

    for {attribute, established, advertised} <- [
          {:each, :once, :chained},
          {:on_failure, :undo_all, :keep_progress}
        ] do
      test "a runner announcing another #{attribute} is reported as a run conflict and never placed",
           %{namespace: ns} do
        attribute = unquote(attribute)
        id = factory("loop")
        forget(ns, [id])
        FleetStateHelper.seed(ns, %{"r1" => [], "r2" => []})

        steps = fn value ->
          %{"id" => id, "sdk_lang" => @rust, "run" => "steps", "each" => "once"}
          |> Map.put("on_failure", "undo_all")
          |> Map.put(Atom.to_string(attribute), Atom.to_string(value))
        end

        :ok = FleetStateHelper.announce(ns, "r1", [steps.(unquote(established))])
        assert ClassRegistry.established(ns, id, attribute) == unquote(established)
        event = [:flezha, :runner, :factory_run_conflict]
        ref = :telemetry_test.attach_event_handlers(self(), [event])

        log =
          ExUnit.CaptureLog.capture_log(fn ->
            :ok = FleetStateHelper.announce(ns, "r2", [steps.(unquote(advertised))])
          end)

        assert log =~ "[flezha_runner] factory_run_conflict advertised=#{unquote(advertised)}"
        assert log =~ "attribute=#{attribute}"
        assert log =~ "factory=#{id}"

        assert_received {^event, ^ref, %{count: 1},
                         %{
                           factory: ^id,
                           established: unquote(established),
                           advertised: unquote(advertised)
                         }}

        assert FleetManager.runners_for_factory(ns, id) == ["r1"]

        :ok = FleetStateHelper.sync(ns, "r1", [])

        assert FleetManager.placement_refusal(ns, id) ==
                 {:run, "steps (#{attribute}: #{unquote(established)})"}
      end
    end

    test "a runner announcing no run never places", %{namespace: ns} do
      id = factory("unkinded")
      forget(ns, [id])

      # The init frame of a runner built before the run was announced.
      FleetStateHelper.seed(ns, %{"old_runner" => []})

      GenServer.cast(
        FleetManager,
        {:sync_factories, ns, "old_runner", [%{"id" => id, "sdk_lang" => @rust}]}
      )

      {:ok, factories} = FleetManager.list_factories(ns)
      assert [%{id: ^id, run: nil}] = factories
      assert FleetManager.runners_for_factory(ns, id) == []
      assert FleetManager.placement_refusal(ns, id) == :no_run_declared
    end

    test "a runner announcing a retired kind literal is logged by name and never places",
         %{namespace: ns} do
      id = factory("kind_literal")
      forget(ns, [id])
      FleetStateHelper.seed(ns, %{"old_runner" => []})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          GenServer.cast(
            FleetManager,
            {:sync_factories, ns, "old_runner",
             [%{"id" => id, "sdk_lang" => @rust, "run" => "unbounded"}]}
          )

          {:ok, _} = FleetManager.list_factories(ns)
        end)

      assert log =~ ~s(run "unbounded")
      assert FleetManager.runners_for_factory(ns, id) == []
      assert FleetManager.placement_refusal(ns, id) == :no_run_declared
    end
  end
end

defmodule FlezhaTest.Runner.FleetManagerSyncFactoriesTest do
  @moduledoc """
  `{:sync_factories, ...}` cast handling (`runner-factory-sync`): a full-set
  replacement of one runner's declared factory list composed from the existing
  removal + store paths. Idempotent on redelivery, scoped to the sending
  runner, legal with an empty set, and warning — not blocking — when a dropped
  factory still has controller-known relays bound to that runner (the
  suspended-instance removal hole's tripwire).

  Drives the live `Flezha.Runner.FleetManager` with synthetic state, mirroring
  `FleetManagerGhostTest`.
  """

  alias Flezha.Runner.ControlChannel.Requests
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

  test "both announcement forms reach one reader, and a blank description never overwrites one" do
    on_exit(fn -> FleetStateHelper.forget_established(@namespace, ["told", "untold"]) end)
    seed(%{"r1" => [], "r2" => [], "r3" => []})

    # The connect frame's serde form of a `FactoryDecl`, and the decoded one.
    json = %{
      "id" => "told",
      "description" => "",
      "sdk_lang" => 1,
      "run" => "steps",
      "each" => "once",
      "on_failure" => "undo_all",
      "api" => [],
      "graph" => nil
    }

    decl = %RunnerCommands.FactoryDecl{
      id: "told",
      sdk_lang: 1,
      run: "steps",
      each: "once",
      on_failure: "undo_all"
    }

    :ok = FleetStateHelper.announce(@namespace, "r1", [%{json | "description" => "the first"}])
    :ok = FleetStateHelper.announce(@namespace, "r2", [json, %{json | "id" => "untold"}])

    Requests.update_factories(
      %{namespace: @namespace, runner_id: "r3"},
      %RunnerCommands.UpdateFactories{factories: [decl]}
    )

    {:ok, rows} = FleetManager.list_factories(@namespace)
    assert Enum.map(rows, &{&1.id, &1.description}) == [{"told", "the first"}, {"untold", nil}]

    adverts = :sys.get_state(FleetManager).namespace_factories[@namespace]["told"].adverts
    [from_json, from_decl] = Enum.map(["r2", "r3"], &Map.delete(adverts[&1], :announced_at))
    assert from_json == from_decl
    assert %{run: :steps, each: :once, on_failure: :undo_all} = from_decl
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

  test "a dropped factory with controller-known bound relays logs a warning" do
    seed(%{"r1" => ["a", "b"]})

    set_mimic_global_until_exit()

    Mimic.stub(HubManager, :relay_rows, fn @namespace ->
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

  test "a suspended (unbound) relay warns only when no other runner has the factory" do
    seed(%{"r1" => ["b"], "r2" => ["b"]})

    set_mimic_global_until_exit()

    Mimic.stub(HubManager, :relay_rows, fn @namespace ->
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

  test "dropping a factory with no controller-known relays is silent" do
    seed(%{"r1" => ["a", "b"]})

    log =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        :ok = sync("r1", ["a"])
      end)

    refute log =~ "dropped factory"
    assert factory_ids() == ["a"]
  end
end

defmodule FlezhaTest.Runner.FleetManagerKeepaliveClockTest do
  @moduledoc """
  The removal grace is measured on one clock, and that clock is monotonic.

  The two halves are pinned separately, and neither depends on how a wall clock
  happens to be spelled. The writer's stamp must land inside the monotonic
  window that brackets the write; the reaper must keep a row stamped on that
  clock a moment ago. Any wall clock puts a reader about 2.4e12 ms ahead of a
  monotonic stamp, so it reaps every row it is handed, and puts a writer's stamp
  that far outside the window. A wall-clock step cannot reach a comparison whose
  two sides are provably on the same monotonic clock.
  """
  # async: false: the live controller and its fleet; node-global tables.
  # `:env` owns the `:runner_removal_grace_ms` two of these tests set. The
  # FleetManager's own state is not the table's business, so it stays here — and
  # still goes back FIRST, because `on_exit` unwinds in reverse registration
  # order and this setup runs after the fixture's. That is the order the one
  # hand-rolled callback had.
  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Helper.FleetStateHelper

  @namespace "removal_grace_clock_#{System.unique_integer([:positive])}"
  @grace_ms 60_000

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)

    :ok
  end

  test "the removal grace is read from its own variable, never the former one" do
    read = &Flezha.Config.Environment.runtime_config(&1, :prod)[:runner_removal_grace_ms]

    assert read.(%{"FLEZHA_RUNNER_REMOVAL_GRACE_MS" => "0"}) == 0

    assert read.(%{"FLEZHA_RUNNER_KEEPALIVE" => "0"}) ==
             Flezha.Config.default(:runner_removal_grace_ms)
  end

  test "the disconnect stamps the roster row on the monotonic clock" do
    seed_active("lone_runner")

    # The production writer stamps the row: the last runner of a namespace
    # nobody else serves is held `:inactive` for the removal grace.
    before_ms = System.monotonic_time(:millisecond)
    :ok = FleetStateHelper.disconnected(@namespace, "lone_runner")
    after_ms = System.monotonic_time(:millisecond)

    assert {:inactive, stamp} = roster()["lone_runner"]

    assert stamp >= before_ms and stamp <= after_ms,
           "the stamp must land in the monotonic window #{before_ms}..#{after_ms} " <>
             "that brackets the write; it is #{stamp}, off by #{stamp - after_ms} ms"
  end

  test "the reaper keeps a row stamped a moment ago on the monotonic clock" do
    Application.put_env(:flezha, :runner_removal_grace_ms, @grace_ms)
    seed_inactive("kept_runner", System.monotonic_time(:millisecond))

    :ok = GenServer.call(FleetManager, :runner_cleanup)

    assert Map.has_key?(roster(), "kept_runner"),
           "a runner inactive for milliseconds was reaped inside its removal grace"
  end

  test "a runner inactive past the removal grace is still reaped" do
    Application.put_env(:flezha, :runner_removal_grace_ms, 0)
    seed_inactive("stale_runner", System.monotonic_time(:millisecond) - 1)

    :ok = GenServer.call(FleetManager, :runner_cleanup)

    refute Map.has_key?(roster(), "stale_runner"),
           "a removal grace of zero must still drop the inactive row"
  end

  defp seed_active(runner_id), do: seed(runner_id, {:active, System.monotonic_time(:millisecond)})

  defp seed_inactive(runner_id, stamp), do: seed(runner_id, {:inactive, stamp})

  defp seed(runner_id, row) do
    :sys.replace_state(FleetManager, fn state ->
      put_in(state, [:runner_namespaces, @namespace], Map.new([{runner_id, row}]))
    end)
  end

  defp roster, do: :sys.get_state(FleetManager).runner_namespaces[@namespace] || %{}
end

defmodule FlezhaTest.Runner.FleetManagerServedGroupsTest do
  @moduledoc """
  What this controller declares to a namespace hub as the groups it serves.

  The hub places a relay among the controllers serving its factory, so the
  fleet manager declares exactly the factories `placeable_runners/3` answers
  for — the same filter the runner choice reads. A factory becoming placeable
  is declared at once; one that stops being placeable is held for
  `group_withdraw_grace_ms`, unless the controller asked for the disconnect.
  """

  # async: false: the live controller and its fleet; a real namespace hub.

  use ExUnit.Case, async: false

  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :capture_log

  @grace_ms 200

  setup do
    namespace = "served_groups_#{System.unique_integer([:positive])}"
    original = :sys.get_state(FleetManager)
    previous_grace = Application.get_env(:flezha, :group_withdraw_grace_ms)

    Application.put_env(:flezha, :group_withdraw_grace_ms, @grace_ms)
    {:ok, _pid} = Flezha.Service.HubManager.start_hub(namespace)

    on_exit(fn ->
      :sys.replace_state(FleetManager, fn _ -> original end)
      Flezha.Service.HubManager.stop_hub(namespace)
      FleetStateHelper.forget_established(namespace, ["alpha", "beta"])

      case previous_grace do
        nil -> Application.delete_env(:flezha, :group_withdraw_grace_ms)
        value -> Application.put_env(:flezha, :group_withdraw_grace_ms, value)
      end
    end)

    {:ok, namespace: namespace, hub_id: Identificator.hub(namespace)}
  end

  test "the placeable factories are declared, and a wrong-language runner is not",
       %{namespace: ns, hub_id: hub_id} do
    FleetStateHelper.seed(ns, %{"r1" => ["alpha", "beta"]})
    FleetStateHelper.sync(ns, "r1", ["alpha", "beta"])

    assert declared(hub_id, ["alpha", "beta"])

    # `beta` is established at `r1`'s language, so a runner advertising it at
    # another one is no candidate and makes its controller no eligible host.
    FleetStateHelper.seed(ns, %{"r2" => ["alpha", {"beta", 2}]})
    FleetStateHelper.sync(ns, "r2", ["alpha", {"beta", 2}])

    assert declared(hub_id, ["alpha"])
  end

  test "a factory that stops being placeable is withdrawn after the grace",
       %{namespace: ns, hub_id: hub_id} do
    FleetStateHelper.seed(ns, %{"r1" => ["alpha", "beta"]})
    FleetStateHelper.sync(ns, "r1", ["alpha", "beta"])
    assert declared(hub_id, ["alpha", "beta"])

    FleetStateHelper.sync(ns, "r1", ["alpha"])

    # Still declared inside the grace: a runner back before it elapses moves
    # no relay at all.
    assert GroupedHashing.served_groups(hub_id) |> Enum.sort() == ["alpha", "beta"]
    assert declared(hub_id, ["alpha"])
  end

  test "a runner back inside the grace withdraws nothing", %{namespace: ns, hub_id: hub_id} do
    FleetStateHelper.seed(ns, %{"r1" => ["alpha", "beta"]})
    FleetStateHelper.sync(ns, "r1", ["alpha", "beta"])
    assert declared(hub_id, ["alpha", "beta"])

    FleetStateHelper.sync(ns, "r1", ["alpha"])
    FleetStateHelper.sync(ns, "r1", ["alpha", "beta"])

    Process.sleep(@grace_ms * 3)
    assert GroupedHashing.served_groups(hub_id) |> Enum.sort() == ["alpha", "beta"]
  end

  test "a retiring runner's factories are withdrawn without the grace",
       %{namespace: ns, hub_id: hub_id} do
    FleetStateHelper.seed(ns, %{"r1" => ["alpha", "beta"]})
    FleetStateHelper.sync(ns, "r1", ["alpha", "beta"])
    assert declared(hub_id, ["alpha", "beta"])

    Marks.mark_runner_retiring("r1")
    :ok = FleetStateHelper.disconnected(ns, "r1")

    assert declared(hub_id, [])
  end

  defp declared(hub_id, groups) do
    NodeTestHelper.wait_until(
      fn -> Enum.sort(GroupedHashing.served_groups(hub_id)) == Enum.sort(groups) end,
      2_000
    ) ||
      flunk(
        "declared #{inspect(GroupedHashing.served_groups(hub_id))}, wanted #{inspect(groups)}"
      )
  end
end

defmodule FlezhaTest.Runner.FleetManagerSessionSignalsTest do
  @moduledoc """
  A runner session opening and closing reaches the ring buffer as a
  `:flezha_runner` signal, which is how a consumer on another node hears it:
  the event bus that carries the same two facts reaches local processes only.
  """
  # async: false: the live controller and its fleet; the shared ring buffer.
  use ExUnit.Case, async: false

  alias Flezha.Observability.RingBufferSink
  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Helper.FleetStateHelper
  alias Flezha.Runner.FleetManager.Session
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "session_signals_#{System.unique_integer([:positive])}"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  test "a completed handshake publishes runner_connected, a disconnect runner_disconnected" do
    since = System.monotonic_time(:microsecond)
    token = "signal_runner_#{System.unique_integer([:positive])}"
    connect = %{namespace: @namespace, factories: [], credential: nil, stable_id: nil}
    pending = %{connect: connect, stream_ref: nil, owed: [:command_sockets_connect], waiters: []}

    :sys.replace_state(FleetManager, fn state ->
      state
      |> put_in([:pending_handshakes, token], pending)
      |> Session.handshake_answered(token, :command_sockets_connect, {:reply, {:ok, ""}})
    end)

    assert signal(:runner_connected, since).metadata == %{namespace: @namespace, runner: token}

    :ok = FleetStateHelper.disconnected(@namespace, token)

    assert signal(:runner_disconnected, since).metadata == %{namespace: @namespace, runner: token}
  end

  defp signal(name, since) do
    find = fn ->
      :flezha_runner
      |> RingBufferSink.query(since)
      |> Enum.find(&(&1.name == name and &1.metadata[:namespace] == @namespace))
    end

    assert NodeTestHelper.wait_until(fn -> find.() != nil end, 2_000),
           "no #{name} signal reached the ring buffer"

    find.()
  end
end

defmodule FlezhaTest.Runner.FleetManagerRunnerRowsTest do
  @moduledoc """
  A runner's listing row: the node holding it, when its session connected,
  how long its control channel has been silent and whether that silence is a
  missed keepalive. The facts read at connect live exactly as long as the session.
  """
  # async: false: the live controller and its fleet; node-global tables.
  use ExUnit.Case, async: false

  alias Flezha.Config.Durations
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Session
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.FleetStateHelper

  @namespace "runner_rows_#{System.unique_integer([:positive])}"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  test "a connected runner's row carries its node, connect time and silence" do
    token = connect()
    stamp_control(token, 1_000)

    # A session with no stream to ask has no OS pid to show.
    assert [
             %{
               id: ^token,
               node: node,
               connected_at: at,
               pid: nil,
               silent_ms: silent,
               late?: false
             }
           ] = FleetManager.Cluster.runner_rows(@namespace)

    assert node == node()
    assert_in_delta at, System.system_time(:second), 5
    assert silent >= 1_000 and silent < 10_000
  end

  test "late? flips once the control silence outlasts two keepalives" do
    token = connect()
    stamp_control(token, 2 * Durations.get(:control_keepalive_ms) + 1_000)

    assert [%{id: ^token, late?: true}] = FleetManager.Cluster.runner_rows(@namespace)
  end

  test "a runner with no control stamp is neither silent nor late" do
    token = connect()

    assert [%{id: ^token, silent_ms: nil, late?: false}] =
             FleetManager.Cluster.runner_rows(@namespace)
  end

  test "the connect time is forgotten when the session is dropped or evicted" do
    dropped = connect()
    evicted = connect()

    :ok = FleetStateHelper.disconnected(@namespace, dropped)
    :ok = FleetManager.evict_runner(evicted, proved_wedged: true)

    facts = :sys.get_state(FleetManager).session_facts
    refute Map.has_key?(facts, dropped)
    refute Map.has_key?(facts, evicted)
  end

  test "the listing and the drop read the pid recorded at connect, not the channel" do
    token = connect()
    :sys.replace_state(FleetManager, &put_in(&1, [:session_facts, token, :pid], 4_242))

    assert [%{id: ^token, pid: 4_242}] = FleetManager.Cluster.runner_rows(@namespace)
    assert %{pid: 4_242, node: node} = FleetManager.find_session(@namespace, token)
    assert node == node()
  end

  defp connect() do
    token = "rows_runner_#{System.unique_integer([:positive])}"
    connect = %{namespace: @namespace, factories: [], credential: nil, stable_id: nil}
    pending = %{connect: connect, stream_ref: nil, owed: [:command_sockets_connect], waiters: []}

    :sys.replace_state(FleetManager, fn state ->
      state
      |> put_in([:pending_handshakes, token], pending)
      |> Session.handshake_answered(token, :command_sockets_connect, {:reply, {:ok, ""}})
    end)

    token
  end

  # A control frame `ago_ms` before now, as the runner's control channel stamps it.
  defp stamp_control(token, ago_ms) do
    ref = LivenessCounters.register_channel(token, :control)
    now = System.monotonic_time(:millisecond)
    LivenessCounters.set_channel(ref, :last_control_frame_ms, now - ago_ms)
    on_exit(fn -> :ets.delete(:flezha_liveness_counters, {:channel, token, :control}) end)
  end
end

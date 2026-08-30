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
  alias Flezha.State.Registry
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
    Logger.put_module_level(FleetManager, :warning)

    on_exit(fn ->
      Logger.delete_module_level(FleetManager)
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

      assert entity in Registry.entities_of(operative), "fixture: #{base}"
      assert Restore.class_sdk_lang(operative) == @java
      assert Restore.class_sdk_lang("never_written") == nil

      # A Rust runner got there first and established the factory wrongly.
      ClassRegistry.put_sdk_lang(ns, id, @rust)

      :ok = ClassRegistry.establish_sdk_lang_from_state(ns, id, operative)

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

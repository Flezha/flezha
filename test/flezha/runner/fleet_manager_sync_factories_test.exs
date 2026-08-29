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
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.FleetStateHelper

  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.

  use ExUnit.Case, async: false
  use Mimic

  @namespace "sync_factories_ns_#{System.unique_integer([:positive])}"

  setup do
    original = :sys.get_state(FleetManager)

    # `config/test.exs` pins the global Logger level to `:error`; raise this
    # module's level so the dropped-factory warning reaches the capture.
    Logger.put_module_level(FleetManager, :warning)

    on_exit(fn ->
      Logger.delete_module_level(FleetManager)
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

    Mimic.set_mimic_global()

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

    Mimic.set_mimic_global()

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

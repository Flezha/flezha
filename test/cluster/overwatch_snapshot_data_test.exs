defmodule Flezha.Cluster.OverwatchSnapshotDataTest do
  @moduledoc """
  Exercises `Flezha.Relay.Overwatch`'s `:get_snapshot_data` handler
  against a real Overwatch GenServer with a real executor round-trip.

  Complements `test/cluster/snapshot_e2e_test.exs`, which uses a fake
  receiver in place of Overwatch — that test would not catch a regression
  in the actual handler's return shape (e.g. a renamed field, a missing
  key, or a change in how `Table.serializable_plugins/1` is plumbed
  through). This test boots a real relay via the test rust_executor
  fixture, drives one input + one snapshot, then calls `:get_snapshot_data`
  directly on the GenServer pid and asserts the returned map matches the
  contract that `Flezha.Cluster.Snapshot.local_state/0` depends on.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Relay.Builder
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RelayManager

  @namespace "default"
  @factory "store_input"

  setup do
    on_exit(fn -> RelayManager.stop_all_relays(@namespace) end)
    :ok
  end

  test "returns {namespace, builder, plugins, snapshot_binary} matching the spawned relay" do
    relay_id = "overwatch-snapshot-data-#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: relay_id,
      factory_id: @factory,
      init_values: %{},
      opts: [snapshot_interval: :disabled]
    }

    assert {:ok, _info} = RelayManager.start_relay(@namespace, builder)

    # Force the periodic snapshot path to run synchronously: this is the
    # same call site (`Overwatch.handle_call(:create_snapshot, ...)`) that
    # the periodic timer hits, and the result is cached in `state.snapshot`,
    # which `:get_snapshot_data` echoes back.
    assert {:ok, snapshot_binary} =
             RelayManager.create_snapshot(@namespace, relay_id)

    assert is_binary(snapshot_binary)
    assert byte_size(snapshot_binary) > 0

    # Attach a plugin so :get_snapshot_data's `plugins` field is non-empty
    # and we exercise Table.serializable_plugins/1.
    plugin = %Plugin{
      id: "overwatch-snap-plug",
      provider: StdOut,
      namespace: @namespace,
      relay_id: relay_id,
      opts: %{},
      subscriptions: [{:on_output, []}]
    }

    assert {:ok, _} = PluginManager.enable_plugin(plugin.id, plugin)

    {:ok, pid} = RelayManager.get_relay_pid(@namespace, relay_id)
    data = GenServer.call(pid, :get_snapshot_data)

    # Shape contract that `Flezha.Cluster.Snapshot.local_state/0` depends on.
    assert is_map(data)
    assert Map.keys(data) |> Enum.sort() == [:builder, :namespace, :plugins, :snapshot_binary]

    assert data.namespace == @namespace
    assert %Builder{} = data.builder
    assert data.builder.id == relay_id
    assert data.builder.factory_id == @factory

    # The binary echoed back must be the one from create_snapshot — that is
    # how the cluster-snapshot path captures per-relay state.
    assert data.snapshot_binary == snapshot_binary

    # `plugins` is the [{plugin_id, %Plugin{}}] tuple list shape that
    # `Snapshot.collect_plugins/1` pattern-matches on.
    assert [{"overwatch-snap-plug", %Plugin{} = returned_plugin}] = data.plugins
    assert returned_plugin.relay_id == relay_id
    assert returned_plugin.provider == StdOut
  end
end

defmodule Flezha.Cluster.Snapshot.E2ETest do
  @moduledoc """
  End-to-end snapshot → file → restore flow covered in-BEAM. Exercises the
  full Snapshot.local_state → Coordinator.snapshot (gather + encode +
  atomic write) → file → Coordinator.restore (read + decode + RPC fan-out)
  → Snapshot.apply_local_state path with `ProcessorManager`,
  `PluginManager`, and `HubManager` stubbed via Mimic.

  Multi-peer / `:peer.start_link` cross-node flow is covered by
  `test/system/cluster_snapshot_restore_test.exs`.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Cluster.Snapshot.Coordinator
  alias Flezha.Cluster.Snapshot.Encoding
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Processor.Builder

  setup :set_mimic_global

  setup do
    Mimic.copy(Flezha.Service.HubManager)

    tmp =
      Path.join(System.tmp_dir!(), "flz_snap_e2e_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp)

    on_exit(fn -> File.rm_rf!(tmp) end)

    {:ok, tmp: tmp}
  end

  test "snapshot → file → restore primes a processor with the captured state binary",
       %{tmp: tmp} do
    path = Path.join(tmp, "e2e.flz")
    captured_binary = <<0xCA, 0xFE, 0xBA, 0xBE>>

    # Phase 1 — set up a fake Overwatch GenServer that answers
    # `:get_snapshot_data` with a known builder + plugin + state binary.
    builder = %Builder{
      id: "e2e-proc",
      factory_id: "e2e-factory",
      init_values: %{symbol: "BTC"},
      opts: [snapshot_interval: 30_000]
    }

    plugin = %Plugin{
      id: "e2e-stdout",
      provider: StdOut,
      namespace: "default",
      processor_id: "e2e-proc",
      opts: %{},
      subscriptions: [{:on_output, []}]
    }

    snapshot_data = %{
      namespace: "default",
      builder: builder,
      plugins: [{plugin.id, plugin}],
      snapshot_binary: captured_binary
    }

    fake_overwatch = start_fake_overwatch(snapshot_data)

    Flezha.Service.HubManager
    |> Mimic.stub(:get_running_namespaces, fn -> ["default"] end)
    |> Mimic.stub(:get_all_processors, fn "default" ->
      [{"e2e-proc", fake_overwatch}]
    end)

    # Phase 2 — snapshot. Coordinator.snapshot runs gather → reconcile →
    # encode → atomic write.
    assert {:ok, summary} = Coordinator.snapshot(path, peer_timeout_ms: 1_000)
    assert summary.processors == 1
    assert summary.plugins == 1
    assert summary.bytes > 0
    assert File.exists?(path)
    refute File.exists?(path <> ".tmp")

    # The on-disk file decodes to the expected payload.
    assert {:ok, payload} = Encoding.decode(File.read!(path))
    assert payload.initiator_node == node()
    assert node() in payload.cluster.nodes

    [proc_entry] = payload.processors
    assert proc_entry.processor.id == "e2e-proc"
    assert proc_entry.processor.factory_id == "e2e-factory"
    assert proc_entry.namespace == "default"
    assert proc_entry.last_observed_node == node()

    [plugin_entry] = payload.plugins
    assert plugin_entry.plugin.id == "e2e-stdout"
    assert payload.captured_states["e2e-proc"] == captured_binary

    # Phase 3 — apply mocks see the captured state binary threaded into
    # builder.opts and the right plugin id.
    Process.exit(fake_overwatch, :kill)
    test_pid = self()

    Flezha.Service.ProcessorManager
    |> Mimic.expect(:start_processor, fn ns, %Builder{} = b ->
      send(test_pid, {:applied_processor, ns, b})
      {:ok, %{processor_id: b.id, compute_engine_id: "ce", node: node(), pid: test_pid}}
    end)

    Flezha.Service.PluginManager
    |> Mimic.expect(:enable_plugin, fn id, %Plugin{} = p ->
      send(test_pid, {:applied_plugin, id, p})
      {:ok, "plugin enabled"}
    end)

    assert {:ok, %{processors: 1, plugins: 1}} =
             Coordinator.restore(path, warn_if_older_than_ms: :infinity)

    assert_receive {:applied_processor, "default", applied_builder}, 1_000
    assert applied_builder.id == "e2e-proc"
    assert applied_builder.factory_id == "e2e-factory"
    assert applied_builder.init_values == %{symbol: "BTC"}
    assert Keyword.get(applied_builder.opts, :initial_state_binary) == captured_binary
    assert Keyword.get(applied_builder.opts, :snapshot_interval) == 30_000

    assert_receive {:applied_plugin, "e2e-stdout", applied_plugin}, 1_000
    assert applied_plugin.processor_id == "e2e-proc"
  end

  test "snapshot drops processor state binaries that exceed :state_cap_bytes",
       %{tmp: tmp} do
    path = Path.join(tmp, "size-cap.flz")

    # 200-byte cap + 1KB state binary → the binary must be dropped from
    # the snapshot's captured_states map. The processor entry itself stays.
    cap = 200
    oversized = :crypto.strong_rand_bytes(1024)

    builder = %Builder{
      id: "huge-proc",
      factory_id: "f",
      init_values: %{},
      opts: []
    }

    snapshot_data = %{
      namespace: "default",
      builder: builder,
      plugins: [],
      snapshot_binary: oversized
    }

    fake_overwatch = start_fake_overwatch(snapshot_data)

    Flezha.Service.HubManager
    |> Mimic.stub(:get_running_namespaces, fn -> ["default"] end)
    |> Mimic.stub(:get_all_processors, fn "default" ->
      [{"huge-proc", fake_overwatch}]
    end)

    assert {:ok, _summary} =
             Coordinator.snapshot(path, peer_timeout_ms: 1_000, state_cap_bytes: cap)

    assert {:ok, payload} = Encoding.decode(File.read!(path))
    [proc_entry] = payload.processors
    assert proc_entry.processor.id == "huge-proc"

    # The processor entry survives, but its state binary was dropped because
    # 1024 > cap=200. captured_states must not include the oversized entry.
    refute Map.has_key?(payload.captured_states, "huge-proc")
  end

  # Minimal process that mimics the `:get_snapshot_data` handler on
  # `Flezha.Processor.Overwatch`. Returns the seeded snapshot data on demand.
  defp start_fake_overwatch(snapshot_data) do
    spawn(fn -> receive_loop(snapshot_data) end)
  end

  defp receive_loop(snapshot_data) do
    receive do
      {:"$gen_call", from, :get_snapshot_data} ->
        GenServer.reply(from, snapshot_data)
        receive_loop(snapshot_data)

      :stop ->
        :ok

      _other ->
        receive_loop(snapshot_data)
    end
  end
end

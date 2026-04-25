defmodule Flezha.Cluster.Snapshot.E2ETest do
  @moduledoc """
  End-to-end snapshot → file → restore flow covered in-BEAM, modelled on
  `test/recovery/e2e_test.exs`. Exercises the full
  Snapshot.local_state → Coordinator.snapshot (gather + encode + atomic
  write) → file → Coordinator.restore (read + decode + SysMode notify +
  RPC fan-out) → Snapshot.apply_local_state path with `ProcessorManager`,
  `PluginManager`, and `HubManager` stubbed via Mimic.

  Multi-peer / `:peer.start_link` cross-node flow is covered by the
  cluster system test (Phase 5 in the change tasks, currently deferred to
  a separate change once the CST infrastructure is wired into CI).
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Cluster.Snapshot.Coordinator
  alias Flezha.Cluster.Snapshot.Encoding
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Processor.Builder
  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode

  setup :set_mimic_global

  setup do
    Mimic.copy(Flezha.Service.HubManager)

    tmp =
      Path.join(System.tmp_dir!(), "flz_snap_e2e_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp)

    prev = %{
      dir: Application.get_env(:flezha, :recovery_data_dir),
      enabled: Application.get_env(:flezha, :recovery_enabled),
      window: Application.get_env(:flezha, :recovery_window_ms),
      prefer: Application.get_env(:flezha, :prefer_explicit_restore?)
    }

    Application.put_env(:flezha, :recovery_data_dir, tmp)
    Application.put_env(:flezha, :recovery_enabled, true)
    # Long window so the bootstrap timer never fires during the test —
    # the explicit-restore cast is what flips SysMode to :normal.
    Application.put_env(:flezha, :recovery_window_ms, 60_000)
    Application.put_env(:flezha, :prefer_explicit_restore?, true)

    stop_if_running([SysMode, Store])

    on_exit(fn ->
      stop_if_running([SysMode, Store])
      restore(:recovery_data_dir, prev.dir)
      restore(:recovery_enabled, prev.enabled)
      restore(:recovery_window_ms, prev.window)
      restore(:prefer_explicit_restore?, prev.prefer)
      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "snapshot → file → restore primes a processor with captured state and notifies SysMode",
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

    # Phase 3 — tear down "before" state, restart fresh SysMode in :bootstrap.
    Process.exit(fake_overwatch, :kill)
    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])
    assert SysMode.get() == :bootstrap

    # Phase 4 — restore. Telemetry should fire and the apply mocks should
    # see the captured state binary threaded into builder.opts.
    handler_id = "snap-e2e-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach_many(
      handler_id,
      [
        [:flezha, :recovery, :explicit_restore_started],
        [:flezha, :recovery, :explicit_restore_completed],
        [:flezha, :recovery, :sysmode_changed]
      ],
      fn name, _m, meta, _cfg -> send(test_pid, {:telemetry, name, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

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

    # SysMode telemetry: started → sysmode_changed (bootstrap → normal) → completed.
    assert_receive {:telemetry, [:flezha, :recovery, :explicit_restore_started], started_meta},
                   1_000

    assert is_reference(started_meta.ref)
    assert started_meta.node == node()

    assert_receive {:telemetry, [:flezha, :recovery, :sysmode_changed],
                    %{from: :bootstrap, to: :normal, reason: :explicit_restore}},
                   1_000

    assert_receive {:telemetry, [:flezha, :recovery, :explicit_restore_completed],
                    %{ref: completed_ref, result: :ok}},
                   1_000

    assert completed_ref == started_meta.ref

    # Apply path: the builder arrives with :initial_state_binary set and
    # its pre-existing opts preserved.
    assert_receive {:applied_processor, "default", applied_builder}, 1_000
    assert applied_builder.id == "e2e-proc"
    assert applied_builder.factory_id == "e2e-factory"
    assert applied_builder.init_values == %{symbol: "BTC"}
    assert Keyword.get(applied_builder.opts, :initial_state_binary) == captured_binary
    assert Keyword.get(applied_builder.opts, :snapshot_interval) == 30_000

    assert_receive {:applied_plugin, "e2e-stdout", applied_plugin}, 1_000
    assert applied_plugin.processor_id == "e2e-proc"

    assert SysMode.get() == :normal
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

  # Minimal GenServer that mimics the `:get_snapshot_data` handler on
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

  defp stop_if_running(mods) when is_list(mods), do: Enum.each(mods, &stop_if_running/1)

  defp stop_if_running(mod) do
    case Process.whereis(mod) do
      nil -> :ok
      pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
    end
  catch
    :exit, _ -> :ok
  end

  defp restore(key, nil), do: Application.delete_env(:flezha, key)
  defp restore(key, value), do: Application.put_env(:flezha, key, value)
end

defmodule Flezha.Cluster.Snapshot.IntegrationTest do
  @moduledoc """
  Integration test for the `Flezha.Cluster.Snapshot` → `Flezha.Cluster.Snapshot.Coordinator`
  → `Flezha.Recovery.SysMode` wiring.

  Exercises the full snapshot → file → restore round-trip in a single BEAM
  with `ProcessorManager.start_processor/2` and `PluginManager.enable_plugin/2`
  stubbed via Mimic so we can verify the apply-side fan-out without driving
  the real compute-engine stack. The cross-node `:rpc.call` fan-out is
  exercised against `node()` itself (which is always reachable from
  `Node.list([:visible, :this])`'s `:this` arm).

  Multi-peer and BEAM-restart scenarios are covered by the deferred
  cluster-system test (Phase 5 in the change tasks).
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Cluster.Snapshot
  alias Flezha.Cluster.Snapshot.Coordinator
  alias Flezha.Cluster.Snapshot.Encoding
  alias Flezha.Plugin
  alias Flezha.Processor.Builder
  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode

  setup :set_mimic_global

  setup do
    tmp =
      Path.join(System.tmp_dir!(), "flz_snapshot_int_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp)

    prev = %{
      dir: Application.get_env(:flezha, :recovery_data_dir),
      enabled: Application.get_env(:flezha, :recovery_enabled),
      window: Application.get_env(:flezha, :recovery_window_ms),
      prefer: Application.get_env(:flezha, :prefer_explicit_restore?)
    }

    Application.put_env(:flezha, :recovery_data_dir, tmp)
    Application.put_env(:flezha, :recovery_enabled, true)
    Application.put_env(:flezha, :recovery_window_ms, 60_000)
    Application.put_env(:flezha, :prefer_explicit_restore?, true)

    stop_if_running([SysMode, Store])

    on_exit(fn ->
      stop_if_running([SysMode, Store])
      restore_env(:recovery_data_dir, prev.dir)
      restore_env(:recovery_enabled, prev.enabled)
      restore_env(:recovery_window_ms, prev.window)
      restore_env(:prefer_explicit_restore?, prev.prefer)
      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "snapshot writes an ETF file decodable by Encoding.decode/1", %{tmp: tmp} do
    path = Path.join(tmp, "empty.flz")

    assert {:ok, summary} = Coordinator.snapshot(path, peer_timeout_ms: 1_000)
    assert summary.bytes > 0
    assert File.exists?(path)
    refute File.exists?(path <> ".tmp")

    bytes = File.read!(path)
    assert {:ok, payload} = Encoding.decode(bytes)
    assert payload.initiator_node == node()
    assert node() in payload.cluster.nodes
    assert payload.strict_mode == false
    assert is_integer(payload.taken_at_ms)
  end

  test "restore notifies SysMode of explicit restore and emits telemetry",
       %{tmp: tmp} do
    path = Path.join(tmp, "round-trip.flz")

    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])
    assert SysMode.get() == :bootstrap

    handler_id = "snapshot-int-#{System.unique_integer([:positive])}"
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

    # Write an empty snapshot, then restore it.
    assert {:ok, _} = Coordinator.snapshot(path, peer_timeout_ms: 1_000)

    assert {:ok, %{processors: 0, plugins: 0}} =
             Coordinator.restore(path, warn_if_older_than_ms: :infinity)

    assert_receive {:telemetry, [:flezha, :recovery, :explicit_restore_started], meta},
                   500

    assert is_reference(meta.ref)
    assert meta.node == node()

    assert_receive {:telemetry, [:flezha, :recovery, :sysmode_changed],
                    %{from: :bootstrap, to: :normal, reason: :explicit_restore}},
                   500

    assert_receive {:telemetry, [:flezha, :recovery, :explicit_restore_completed],
                    %{ref: ^_ref, result: :ok}},
                   500

    assert SysMode.get() == :normal
  end

  test "restore fans out apply_local_state to the right target node",
       %{tmp: tmp} do
    path = Path.join(tmp, "with-entries.flz")

    # Hand-craft a payload representing a single processor + plugin owned
    # by this node, write it through the encoder, then restore. We can do
    # this by feeding the payload directly to Encoding.encode/1 and
    # File.write — bypassing the gather phase isolates the apply phase.
    builder = %Builder{
      id: "test-proc-1",
      factory_id: "test-factory",
      init_values: %{},
      opts: []
    }

    plugin = %Plugin{
      id: "test-plugin-1",
      provider: Flezha.Plugin.Provider.StdOut,
      namespace: "default",
      processor_id: "test-proc-1"
    }

    payload = %{
      taken_at_ms: System.system_time(:millisecond),
      cluster: %{nodes: [node()]},
      processors: [
        %{processor: builder, namespace: "default", last_observed_node: node()}
      ],
      plugins: [%{plugin: plugin, last_observed_node: node()}],
      captured_states: %{"test-proc-1" => <<1, 2, 3>>},
      strict_mode: false,
      initiator_node: node()
    }

    File.write!(path, Encoding.encode(payload))

    Flezha.Service.ProcessorManager
    |> Mimic.expect(:start_processor, fn ns, %Builder{} = b ->
      assert ns == "default"
      assert b.id == "test-proc-1"
      assert Keyword.get(b.opts, :initial_state_binary) == <<1, 2, 3>>
      {:ok, %{processor_id: b.id, compute_engine_id: "ce-1", node: node(), pid: self()}}
    end)

    Flezha.Service.PluginManager
    |> Mimic.expect(:enable_plugin, fn id, %Plugin{} = p ->
      assert id == "test-plugin-1"
      assert p.processor_id == "test-proc-1"
      {:ok, "plugin enabled"}
    end)

    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])

    assert {:ok, %{processors: 1, plugins: 1}} =
             Coordinator.restore(path, warn_if_older_than_ms: :infinity)
  end

  test "snapshot atomicity: the .tmp file is removed on success", %{tmp: tmp} do
    path = Path.join(tmp, "atomic.flz")

    assert {:ok, _} = Coordinator.snapshot(path, peer_timeout_ms: 1_000)

    assert File.exists?(path)
    refute File.exists?(path <> ".tmp")
  end

  test "restore rejects a future-version envelope with a hard error",
       %{tmp: tmp} do
    path = Path.join(tmp, "future.flz")

    bytes = :erlang.term_to_binary({:flezha_snapshot, 99, %{}}, [:compressed])
    File.write!(path, bytes)

    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])

    assert {:error, {:unsupported_version, 99, [1]}} =
             Coordinator.restore(path, warn_if_older_than_ms: :infinity)

    # SYSMODE must NOT have been notified — nothing was applied.
    assert SysMode.get() == :bootstrap
  end

  test "restore on a corrupted file returns decode_failed without applying state",
       %{tmp: tmp} do
    path = Path.join(tmp, "garbage.flz")
    File.write!(path, <<0, 1, 2, 3>>)

    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])

    assert {:error, {:decode_failed, _reason}} =
             Coordinator.restore(path, warn_if_older_than_ms: :infinity)

    assert SysMode.get() == :bootstrap
  end

  test "Snapshot.apply_local_state/1 threads :initial_state_binary into builder opts" do
    builder = %Builder{
      id: "p-apply",
      factory_id: "f-apply",
      init_values: %{},
      opts: [snapshot_interval: 30_000]
    }

    Flezha.Service.ProcessorManager
    |> Mimic.expect(:start_processor, fn ns, %Builder{} = b ->
      assert ns == "default"
      assert Keyword.get(b.opts, :initial_state_binary) == <<42, 43>>
      # Pre-existing opts are preserved.
      assert Keyword.get(b.opts, :snapshot_interval) == 30_000
      {:ok, %{processor_id: b.id, compute_engine_id: "ce", node: node(), pid: self()}}
    end)

    assert {:ok, %{processor_id: "p-apply"}} =
             Snapshot.apply_local_state({:processor, builder, "default", <<42, 43>>})
  end

  test "Snapshot.apply_local_state/1 with nil state binary skips :initial_state_binary" do
    builder = %Builder{id: "p-empty", factory_id: "f", init_values: %{}, opts: []}

    Flezha.Service.ProcessorManager
    |> Mimic.expect(:start_processor, fn _ns, %Builder{} = b ->
      refute Keyword.has_key?(b.opts, :initial_state_binary)
      {:ok, %{processor_id: b.id, compute_engine_id: "ce", node: node(), pid: self()}}
    end)

    assert {:ok, _} = Snapshot.apply_local_state({:processor, builder, "default", nil})
  end

  defp stop_if_running(modules) when is_list(modules) do
    Enum.each(modules, &stop_if_running/1)
  end

  defp stop_if_running(mod) do
    case Process.whereis(mod) do
      nil ->
        :ok

      pid ->
        if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
    end
  catch
    :exit, _ -> :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, val), do: Application.put_env(:flezha, key, val)
end

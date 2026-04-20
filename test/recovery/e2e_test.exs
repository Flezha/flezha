defmodule Flezha.Recovery.E2ETest do
  @moduledoc """
  End-to-end recovery flow covered in-BEAM, without spawning peer nodes or
  killing the live agent. Exercises the full Store → SysMode → Coordinator
  chain with `ProcessorManager.start_processor/2` mocked via Mimic so we can
  count calls without driving the compute-engine stack.

  Covers task 9.2's invariant (peer reports :bootstrap → A still recovers)
  and reinforces 9.3's invariant in the single-BEAM form. The full
  cross-node 9.1/9.2/9.3 shell verification lives at
  `scripts/recovery_e2e.sh`.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin.Driver.IOBindingPlugin
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Recovery.Coordinator
  alias Flezha.Recovery.PluginStore
  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode
  alias Flezha.Processor.Builder

  setup :set_mimic_global

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_e2e_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    prev = %{
      dir: Application.get_env(:flezha, :recovery_data_dir),
      enabled: Application.get_env(:flezha, :recovery_enabled),
      window: Application.get_env(:flezha, :recovery_window_ms)
    }

    Application.put_env(:flezha, :recovery_data_dir, tmp)
    Application.put_env(:flezha, :recovery_enabled, true)
    Application.put_env(:flezha, :recovery_window_ms, 150)

    stop_if_running([Coordinator, SysMode, PluginStore, Store])

    on_exit(fn ->
      stop_if_running([Coordinator, SysMode, PluginStore, Store])
      restore(:recovery_data_dir, prev.dir)
      restore(:recovery_enabled, prev.enabled)
      restore(:recovery_window_ms, prev.window)
      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "seed via hook handler, restart, recover on window elapse" do
    # --- phase 1: simulate an agent hosting 3 processors ---
    {:ok, _} = Store.start_link([])
    {:ok, _} = Coordinator.start_link([])

    payload = %{
      children:
        Enum.map(1..3, fn i ->
          id = "recov-proc-#{i}"

          %{
            child_id: id,
            pid: self(),
            result: :ok,
            nodes: [node()],
            child_spec: %{
              id: id,
              start:
                {Flezha.Processor.Overwatch, :start_link,
                 [{"ns", %Builder{id: id, factory_id: "f"}}]},
              restart: :transient
            },
            metadata: %{}
          }
        end)
    }

    assert :ok = Coordinator.on_post_children_start(payload)
    assert Store.count() == 3

    # Stop the supervision (graceful) — DETS file remains on disk.
    stop_if_running([Coordinator, Store])

    # --- phase 2: restart + mock ProcessorManager, verify recovery ---
    parent = self()

    Flezha.Service.ProcessorManager
    |> Mimic.stub(:start_processor, fn ns, proc ->
      send(parent, {:recovered, ns, proc.id})
      {:ok, %{processor_id: proc.id, compute_engine_id: "ce", node: node(), pid: parent}}
    end)

    telemetry_id = "e2e-done-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      telemetry_id,
      [:flezha, :recovery, :completed],
      fn _n, m, _meta, _cfg -> send(parent, {:completed, m}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)

    {:ok, _} = Store.start_link([])
    assert Store.count() == 3

    {:ok, _} = Coordinator.start_link([])
    {:ok, _} = SysMode.start_link([])

    assert SysMode.get() == :recover

    # Simulate a peer reporting :bootstrap during the window — must NOT short-circuit.
    GenServer.cast(SysMode, {:peer_sysmode, :"peerB@host", :bootstrap})

    # Window elapses, recovery fires.
    assert_receive {:completed, %{recovered: 3, failed: 0}}, 2_000

    # All three processors were started by the recovery loop.
    assert_receive {:recovered, "ns", "recov-proc-1"}, 500
    assert_receive {:recovered, "ns", "recov-proc-2"}, 500
    assert_receive {:recovered, "ns", "recov-proc-3"}, 500

    assert SysMode.get() == :normal
  end

  test "processors and their plugins round-trip through DETS + recovery" do
    # --- phase 1: simulate an agent hosting one processor with a stdout plugin ---
    {:ok, _} = Store.start_link([])
    {:ok, _} = PluginStore.start_link([])
    {:ok, _} = Coordinator.start_link([])

    # Persist the processor via the hook handler.
    payload = %{
      children: [
        %{
          child_id: "recov-with-plugin",
          pid: self(),
          result: :ok,
          nodes: [node()],
          child_spec: %{
            id: "recov-with-plugin",
            start:
              {Flezha.Processor.Overwatch, :start_link,
               [{"default", %Builder{id: "recov-with-plugin", factory_id: "f"}}]},
            restart: :transient
          },
          metadata: %{}
        }
      ]
    }

    assert :ok = Coordinator.on_post_children_start(payload)

    # Persist the plugin via the plugin hook handler.
    plugin = %IOBindingPlugin{
      namespace: "default",
      processor_id: "recov-with-plugin",
      trigger: :on_output,
      provider: %StdOut{}
    }

    assert :ok = Coordinator.on_plugin_enabled("stdout-plug", plugin)

    assert Store.count() == 1
    assert PluginStore.count() == 1

    # --- phase 2: restart + mock both managers, verify two-phase recovery ---
    stop_if_running([Coordinator, SysMode, PluginStore, Store])

    parent = self()

    Flezha.Service.ProcessorManager
    |> Mimic.stub(:start_processor, fn ns, proc ->
      send(parent, {:recovered_processor, ns, proc.id})
      {:ok, %{processor_id: proc.id, compute_engine_id: "ce", node: node(), pid: parent}}
    end)
    |> Mimic.stub(:get_processor_pid, fn "default", "recov-with-plugin" -> {:ok, parent} end)

    Flezha.Service.PluginManager
    |> Mimic.stub(:enable_plugin, fn plugin_id, _plugin ->
      send(parent, {:recovered_plugin, plugin_id})
      {:ok, "plugin enabled"}
    end)

    telemetry_id = "e2e-done-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      telemetry_id,
      [:flezha, :recovery, :completed],
      fn _n, m, meta, _ -> send(parent, {:completed, m, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)

    {:ok, _} = Store.start_link([])
    {:ok, _} = PluginStore.start_link([])
    {:ok, _} = Coordinator.start_link([])
    {:ok, _} = SysMode.start_link([])

    assert SysMode.get() == :recover

    assert_receive {:completed, measurements, _meta}, 2_000
    assert measurements.recovered == 1
    assert measurements.plugins_recovered == 1
    assert measurements.plugins_failed == 0
    assert measurements.plugins_skipped_missing_processor == 0

    assert_receive {:recovered_processor, "default", "recov-with-plugin"}, 500
    assert_receive {:recovered_plugin, "stdout-plug"}, 500

    assert SysMode.get() == :normal
  end

  test "peer :normal mid-window aborts recovery with zero processor starts" do
    {:ok, _} = Store.start_link([])
    {:ok, _} = PluginStore.start_link([])
    {:ok, _} = Coordinator.start_link([])

    # Persist two rows then simulate agent restart.
    for i <- 1..2 do
      id = "neg-proc-#{i}"
      :ok = Store.put(id, "ns", %Builder{id: id, factory_id: "f"})
    end

    # Very long window — ensures the abort comes from peer_normal, not timer.
    Application.put_env(:flezha, :recovery_window_ms, 60_000)

    parent = self()

    Flezha.Service.ProcessorManager
    |> Mimic.stub(:start_processor, fn _ns, proc ->
      send(parent, {:unexpected_start, proc.id})
      {:ok, %{processor_id: proc.id, compute_engine_id: "ce", node: node(), pid: parent}}
    end)

    {:ok, _} = SysMode.start_link([])
    assert SysMode.get() == :recover

    # Peer reports :normal — must abort without starting any processor.
    GenServer.cast(SysMode, {:peer_sysmode, :"peerB@host", :normal})

    Process.sleep(100)
    refute_received {:unexpected_start, _}
    assert SysMode.get() == :normal
  end

  defp stop_if_running(mods) when is_list(mods), do: Enum.each(mods, &stop_if_running/1)

  defp stop_if_running(mod) do
    try do
      case Process.whereis(mod) do
        nil -> :ok
        pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
      end
    catch
      :exit, _ -> :ok
    end
  end

  defp restore(key, nil), do: Application.delete_env(:flezha, key)
  defp restore(key, value), do: Application.put_env(:flezha, key, value)
end

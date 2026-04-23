defmodule Flezha.Recovery.PluginCoordinatorTest do
  @moduledoc """
  Focused tests for the plugin side of `Recovery.Coordinator` under the
  extension-system contract.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.BeamMsgForward
  alias Flezha.Plugin.Provider.Filter
  alias Flezha.Plugin.Provider.Kafka
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Recovery.Coordinator
  alias Flezha.Recovery.PluginStore
  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode

  setup :set_mimic_global

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_plugin_coord_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    Application.put_env(:flezha, :recovery_data_dir, tmp)

    stop_if_running([Coordinator, SysMode, PluginStore, Store])

    {:ok, _} = Store.start_link([])
    {:ok, _} = PluginStore.start_link([])
    {:ok, _} = Coordinator.start_link([])

    Mimic.copy(Flezha.Service.PluginManager)
    Mimic.copy(Flezha.Service.ProcessorManager)

    on_exit(fn ->
      stop_if_running([Coordinator, SysMode, PluginStore, Store])

      case prev_dir do
        nil -> Application.delete_env(:flezha, :recovery_data_dir)
        v -> Application.put_env(:flezha, :recovery_data_dir, v)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  defp build_plugin(id, provider, opts \\ %{}, subs \\ [{:on_output, []}]) do
    %Plugin{
      id: id,
      namespace: "default",
      processor_id: "proc-1",
      provider: provider,
      opts: opts,
      subscriptions: subs
    }
  end

  describe "on_plugin_enabled / on_plugin_disabled" do
    test "persists recoverable plugins and strips transient fields" do
      for {provider, opts} <- [
            {StdOut, %{}},
            {Filter, %{regex_pattern: "hello"}},
            {Kafka.Producer, %{hosts: [{"k", 9092}], topic: "t", conn: self()}},
            {Websocket, %{uri: "ws://x", conn: self()}}
          ] do
        plugin_id = "p-#{System.unique_integer([:positive])}"
        plugin = build_plugin(plugin_id, provider, opts)

        assert :ok = Coordinator.on_plugin_enabled(plugin_id, plugin)

        [{^plugin_id, %{plugin: stored}}] =
          PluginStore.all() |> Enum.filter(fn {id, _} -> id == plugin_id end)

        refute Map.has_key?(stored.opts, :conn)
        refute Map.has_key?(stored.opts, :binding_pid)
      end
    end

    test "skips non-recoverable plugins (BeamMsgForward, MonitoredIOForward)" do
      for provider <- [BeamMsgForward, MonitoredIOForward] do
        opts =
          case provider do
            MonitoredIOForward ->
              %{
                recv_pid: self(),
                plugin_id: "pp",
                processor_id: "proc-1",
                namespace: "default"
              }

            _ ->
              %{recv_pid: self()}
          end

        plugin = build_plugin("skip-#{System.unique_integer()}", provider, opts)

        before = PluginStore.count()
        assert :ok = Coordinator.on_plugin_enabled(plugin.id, plugin)
        assert PluginStore.count() == before
      end
    end

    test "on_plugin_disabled removes the persisted row" do
      plugin = build_plugin("kill-me", StdOut)

      :ok = Coordinator.on_plugin_enabled("kill-me", plugin)
      assert PluginStore.count() == 1

      :ok = Coordinator.on_plugin_disabled("kill-me")
      assert PluginStore.count() == 0
    end
  end

  describe "strip_transient_fields/1" do
    test "nils :conn and :binding_pid in plugin opts" do
      plugin =
        build_plugin("p", Kafka.Producer, %{
          hosts: [{"k", 9092}],
          topic: "t",
          conn: self(),
          binding_pid: self()
        })

      stripped = Coordinator.strip_transient_fields(plugin)

      refute Map.has_key?(stripped.opts, :conn)
      refute Map.has_key?(stripped.opts, :binding_pid)
      assert stripped.opts.hosts == [{"k", 9092}]
      assert stripped.opts.topic == "t"
    end
  end

  describe "phase-2 recovery loop" do
    test "re-enables plugins whose processor is alive" do
      proc_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> if Process.alive?(proc_pid), do: Process.exit(proc_pid, :kill) end)

      seed_plugin_row("p-live", "live-proc")

      Flezha.Service.ProcessorManager
      |> Mimic.stub(:get_processor_pid, fn _ns, "live-proc" -> {:ok, proc_pid} end)

      Flezha.Service.PluginManager
      |> Mimic.expect(:enable_plugin, fn "p-live", _plugin -> {:ok, "plugin enabled"} end)

      result = Coordinator.run_recovery()

      assert result.plugins_recovered == 1
      assert result.plugins_failed == 0
      assert result.plugins_skipped_missing_processor == 0
    end

    test "skips (and deletes) plugins whose processor isn't running" do
      seed_plugin_row("p-orphan", "gone-proc")

      Flezha.Service.ProcessorManager
      |> Mimic.stub(:get_processor_pid, fn _ns, "gone-proc" ->
        {:error, "processor not found"}
      end)

      Flezha.Service.PluginManager
      |> Mimic.stub(:enable_plugin, fn _id, _plugin ->
        flunk("enable_plugin should not be called for orphan row")
      end)

      result = Coordinator.run_recovery()

      assert result.plugins_skipped_missing_processor == 1
      assert result.plugins_recovered == 0

      refute Enum.any?(PluginStore.all(), fn {id, _} -> id == "p-orphan" end)
    end

    test "tolerates per-row enable failures and classifies permanent vs transient" do
      proc_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> if Process.alive?(proc_pid), do: Process.exit(proc_pid, :kill) end)

      seed_plugin_row("p-dup", "p1")
      seed_plugin_row("p-net", "p2")

      Flezha.Service.ProcessorManager
      |> Mimic.stub(:get_processor_pid, fn _ns, _id -> {:ok, proc_pid} end)

      Flezha.Service.PluginManager
      |> Mimic.stub(:enable_plugin, fn id, _plugin ->
        case id do
          "p-dup" -> {:error, "plugin already registered"}
          "p-net" -> {:error, :nxdomain}
        end
      end)

      result = Coordinator.run_recovery()

      assert result.plugins_recovered == 0
      assert result.plugins_failed == 2

      ids = PluginStore.all() |> Enum.map(&elem(&1, 0))
      refute "p-dup" in ids
      assert "p-net" in ids
    end

    test "abort flag halts phase 2 after current row" do
      proc_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> if Process.alive?(proc_pid), do: Process.exit(proc_pid, :kill) end)

      for i <- 1..3, do: seed_plugin_row("p-#{i}", "px")

      Flezha.Service.ProcessorManager
      |> Mimic.stub(:get_processor_pid, fn _ns, _id -> {:ok, proc_pid} end)

      parent = self()

      Flezha.Service.PluginManager
      |> Mimic.stub(:enable_plugin, fn id, _ ->
        send(parent, {:enable, id})
        Coordinator.set_abort_flag(true)
        {:ok, "plugin enabled"}
      end)

      result = Coordinator.run_recovery()

      assert result.aborted == true
      assert_received {:enable, _first_id}
      refute_received {:enable, _other}
      assert result.plugins_recovered == 1

      Coordinator.set_abort_flag(false)
    end
  end

  ## helpers

  defp seed_plugin_row(plugin_id, processor_id) do
    plugin = build_plugin(plugin_id, StdOut) |> Map.put(:processor_id, processor_id)

    row = %{
      plugin: plugin,
      version: 2
    }

    :dets.insert(:flz_recovery_plugins, {plugin_id, row})
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
end

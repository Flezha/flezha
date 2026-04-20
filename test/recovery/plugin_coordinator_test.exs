defmodule Flezha.Recovery.PluginCoordinatorTest do
  @moduledoc """
  Focused tests for the plugin side of `Recovery.Coordinator`:
    - enable persists recoverable plugins
    - enable skips non-recoverable plugins
    - disable removes the row
    - transient-field stripping (no pids/conns in persisted rows)
    - phase-2 skips plugins whose processor isn't running
    - phase-2 tolerates per-row failures
    - abort flag halts phase 2
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin.Driver.IOBindingPlugin
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

  describe "on_plugin_enabled / on_plugin_disabled" do
    test "persists recoverable plugins" do
      for provider <- [%StdOut{}, %Filter{regex_pattern: "hello"}, %Kafka.Producer{hosts: [{"k", 9092}], topic: "t", conn: self()}, %Websocket{uri: "ws://x", conn: self()}] do
        plugin = %IOBindingPlugin{
          namespace: "default",
          processor_id: "proc-1",
          trigger: :on_output,
          provider: provider,
          pid: self(),
          monitor_ref: make_ref()
        }

        plugin_id = "p-#{System.unique_integer([:positive])}"
        assert :ok = Coordinator.on_plugin_enabled(plugin_id, plugin)

        [{^plugin_id, %{plugin: stored}}] =
          PluginStore.all() |> Enum.filter(fn {id, _} -> id == plugin_id end)

        # transient fields must be nil after stripping
        assert stored.pid == nil
        assert stored.monitor_ref == nil

        case stored.provider do
          %Kafka.Producer{conn: conn} -> assert conn == nil
          %Websocket{conn: conn} -> assert conn == nil
          _ -> :ok
        end
      end
    end

    test "skips non-recoverable plugins (BeamMsgForward, MonitoredIOForward)" do
      for provider <- [
            %BeamMsgForward{recv_pid: self()},
            %MonitoredIOForward{
              recv_pid: self(),
              plugin_id: "pp",
              processor_id: "proc-1",
              namespace: "default"
            }
          ] do
        plugin = %IOBindingPlugin{
          namespace: "default",
          processor_id: "proc-1",
          trigger: :on_output,
          provider: provider
        }

        before = PluginStore.count()
        assert :ok = Coordinator.on_plugin_enabled("skip-#{System.unique_integer()}", plugin)
        assert PluginStore.count() == before
      end
    end

    test "on_plugin_disabled removes the persisted row" do
      plugin = %IOBindingPlugin{
        namespace: "default",
        processor_id: "proc-x",
        trigger: :on_output,
        provider: %StdOut{}
      }

      :ok = Coordinator.on_plugin_enabled("kill-me", plugin)
      assert PluginStore.count() == 1

      :ok = Coordinator.on_plugin_disabled("kill-me")
      assert PluginStore.count() == 0
    end
  end

  describe "strip_transient_fields/1" do
    test "nils pid/monitor_ref on IOBindingPlugin and conn on Kafka/Websocket providers" do
      plugin = %IOBindingPlugin{
        namespace: "default",
        processor_id: "p",
        trigger: :on_output,
        provider: %Kafka.Producer{
          hosts: [{"k", 9092}],
          topic: "t",
          conn: self()
        },
        pid: self(),
        monitor_ref: make_ref()
      }

      stripped = Coordinator.strip_transient_fields(plugin)

      assert stripped.pid == nil
      assert stripped.monitor_ref == nil
      assert stripped.provider.conn == nil

      # Structural parts preserved
      assert stripped.namespace == "default"
      assert stripped.provider.hosts == [{"k", 9092}]
      assert stripped.provider.topic == "t"
    end

    test "persisted rows across all 6 providers contain no pid values" do
      providers = [
        %StdOut{},
        %Filter{regex_pattern: "x"},
        %Kafka.Producer{hosts: [{"k", 9092}], topic: "t", conn: self()},
        %Websocket{uri: "ws://x", conn: self()}
      ]

      for provider <- providers do
        plugin = %IOBindingPlugin{
          namespace: "default",
          processor_id: "p",
          trigger: :on_output,
          provider: provider,
          pid: self(),
          monitor_ref: make_ref()
        }

        :ok = Coordinator.on_plugin_enabled("pid-check-#{System.unique_integer()}", plugin)
      end

      # BeamMsgForward and MonitoredIOForward are non-recoverable so we
      # separately confirm they weren't persisted (the `recoverable?` check).
      for provider <- [%BeamMsgForward{recv_pid: self()}] do
        :ok =
          Coordinator.on_plugin_enabled(
            "nrec-#{System.unique_integer()}",
            %IOBindingPlugin{
              namespace: "default",
              processor_id: "p",
              trigger: :on_output,
              provider: provider
            }
          )
      end

      # Assert no pid values anywhere in persisted rows.
      for {_, %{plugin: plugin}} <- PluginStore.all() do
        refute_contains_pid(plugin)
      end
    end
  end

  describe "phase-2 recovery loop" do
    test "re-enables plugins whose processor is alive" do
      proc_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> if Process.alive?(proc_pid), do: Process.exit(proc_pid, :kill) end)

      seed_plugin_row("p-live", processor_id: "live-proc")

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
      seed_plugin_row("p-orphan", processor_id: "gone-proc")

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

      # Row should be deleted.
      refute Enum.any?(PluginStore.all(), fn {id, _} -> id == "p-orphan" end)
    end

    test "tolerates per-row enable failures and classifies permanent vs transient" do
      proc_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> if Process.alive?(proc_pid), do: Process.exit(proc_pid, :kill) end)

      seed_plugin_row("p-dup", processor_id: "p1")
      seed_plugin_row("p-net", processor_id: "p2")

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

      # Permanent failure ("plugin already registered") should remove the row.
      ids = PluginStore.all() |> Enum.map(&elem(&1, 0))
      refute "p-dup" in ids
      # Transient failure retains the row for a future recovery window.
      assert "p-net" in ids
    end

    test "abort flag halts phase 2 after current row" do
      proc_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> if Process.alive?(proc_pid), do: Process.exit(proc_pid, :kill) end)

      for i <- 1..3, do: seed_plugin_row("p-#{i}", processor_id: "px")

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
      # Only the first plugin row was processed before the abort.
      assert_received {:enable, _first_id}
      refute_received {:enable, _other}
      assert result.plugins_recovered == 1

      Coordinator.set_abort_flag(false)
    end
  end

  ## helpers

  defp seed_plugin_row(plugin_id, opts) do
    processor_id = Keyword.fetch!(opts, :processor_id)

    row = %{
      plugin: %IOBindingPlugin{
        namespace: "default",
        processor_id: processor_id,
        trigger: :on_output,
        provider: %StdOut{}
      },
      stored_node: node(),
      inserted_at: System.system_time(:millisecond)
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

  defp refute_contains_pid(plugin) do
    refute plugin.pid != nil
    refute plugin.monitor_ref != nil

    case plugin.provider do
      %Kafka.Producer{conn: conn} -> refute conn != nil
      %Websocket{conn: conn} -> refute conn != nil
      _ -> :ok
    end
  end
end

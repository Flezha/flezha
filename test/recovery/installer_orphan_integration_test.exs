defmodule Flezha.Recovery.InstallerOrphanIntegrationTest do
  @moduledoc """
  Tests that `Flezha.Recovery.Coordinator` consults
  `Flezha.Plugin.Installer.installed_ids/0` and skips orphaned
  installer-owned plugin rows during plugin recovery.

  This is a unit-level integration: we drive `run_plugin_phase/0`
  indirectly through `run_recovery/0` with test rows in
  `Recovery.PluginStore`.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store
  alias Flezha.Recovery.Coordinator
  alias Flezha.Recovery.PluginStore, as: RecoveryPluginStore
  alias Flezha.Recovery.Store, as: ProcStore
  alias Flezha.Recovery.SysMode

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_orphan_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    prev_pi = Application.get_env(:flezha, :plugin_installer)
    Application.put_env(:flezha, :recovery_data_dir, tmp)

    Application.put_env(
      :flezha,
      :plugin_installer,
      Keyword.merge(prev_pi || [], plugin_root: Path.join(tmp, "plugins"))
    )

    for mod <- [Coordinator, ProcStore, RecoveryPluginStore, SysMode, Installer, Loader, Store] do
      stop_if_running(mod)
    end

    on_exit(fn ->
      for mod <- [Coordinator, ProcStore, RecoveryPluginStore, SysMode, Installer, Loader, Store] do
        stop_if_running(mod)
      end

      if prev_dir do
        Application.put_env(:flezha, :recovery_data_dir, prev_dir)
      else
        Application.delete_env(:flezha, :recovery_data_dir)
      end

      if prev_pi do
        Application.put_env(:flezha, :plugin_installer, prev_pi)
      else
        Application.delete_env(:flezha, :plugin_installer)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "orphaned installer-owned plugin row is skipped and telemetry fires" do
    {:ok, _} = ProcStore.start_link([])
    {:ok, _} = RecoveryPluginStore.start_link([])
    {:ok, _} = Store.start_link([])
    {:ok, _} = Loader.start_link([])
    {:ok, _} = Installer.start_link([])
    {:ok, _} = Coordinator.start_link([])

    # Write a recovery row whose plugin_id follows the installer's
    # "<pkg>:<provision>" shape but whose pkg is NOT in the installer
    # catalog — an orphan.
    orphan = %Plugin{
      id: "ghost:main",
      provider: Enum,
      namespace: "default",
      processor_id: "nonexistent",
      subscriptions: [{:on_input, []}]
    }

    RecoveryPluginStore.put("ghost:main", %{plugin: orphan, version: 2})

    handler = "orphan-tel-#{System.unique_integer([:positive])}"
    me = self()

    :telemetry.attach(
      handler,
      [:flezha, :recovery, :orphaned_plugin],
      fn _n, _m, meta, _ -> send(me, {:orphan_tel, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # Run the plugin phase via run_recovery/0.
    result = Coordinator.run_recovery()

    assert_receive {:orphan_tel, %{plugin_id: "ghost:main"}}, 1_000
    assert result.plugins_skipped_missing_processor >= 1 or result.plugins_failed == 0
    # The orphan row should have been deleted.
    assert RecoveryPluginStore.all() == []
  end

  test "legacy plugin rows (no colon) are not treated as orphans" do
    {:ok, _} = ProcStore.start_link([])
    {:ok, _} = RecoveryPluginStore.start_link([])
    {:ok, _} = Store.start_link([])
    {:ok, _} = Loader.start_link([])
    {:ok, _} = Installer.start_link([])
    {:ok, _} = Coordinator.start_link([])

    legacy = %Plugin{
      id: "legacy_plugin",
      provider: Enum,
      namespace: "default",
      processor_id: "some_proc",
      subscriptions: [{:on_input, []}]
    }

    RecoveryPluginStore.put("legacy_plugin", %{plugin: legacy, version: 2})

    handler = "no-orphan-tel-#{System.unique_integer([:positive])}"
    me = self()

    :telemetry.attach(
      handler,
      [:flezha, :recovery, :orphaned_plugin],
      fn _n, _m, meta, _ -> send(me, {:orphan_tel, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    _result = Coordinator.run_recovery()

    # No orphan telemetry for the legacy row.
    refute_receive {:orphan_tel, %{plugin_id: "legacy_plugin"}}, 500
  end

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

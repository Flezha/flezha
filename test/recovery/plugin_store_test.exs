defmodule Flezha.Recovery.PluginStoreTest do
  @moduledoc """
  Round-trip tests for `Flezha.Recovery.PluginStore`. Mirrors
  `Flezha.Recovery.StoreTest` but exercises the plugin table — different
  DETS filename under the same recovery_data_dir so both stores coexist.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Recovery.PluginStore
  alias Flezha.Recovery.Store

  setup do
    tmp =
      Path.join(System.tmp_dir!(), "flz_plugin_store_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    Application.put_env(:flezha, :recovery_data_dir, tmp)

    stop_if_running(PluginStore)
    stop_if_running(Store)

    on_exit(fn ->
      stop_if_running(PluginStore)
      stop_if_running(Store)

      if prev_dir do
        Application.put_env(:flezha, :recovery_data_dir, prev_dir)
      else
        Application.delete_env(:flezha, :recovery_data_dir)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "opens own DETS file distinct from processor store", %{tmp: tmp} do
    {:ok, pid} = PluginStore.start_link([])
    on_exit_stop(pid)

    assert PluginStore.count() == 0
    assert PluginStore.path() == Path.join(tmp, "recovery_plugins.dets")
    refute PluginStore.path() == Path.join(tmp, "recovery.dets")
  end

  test "writes, reads, deletes, and survives reopen", %{tmp: tmp} do
    {:ok, pid} = PluginStore.start_link([])
    on_exit_stop(pid)

    plugin = sample_plugin("p-1")
    assert :ok = PluginStore.put("p-1", plugin)
    assert :ok = PluginStore.put("p-2", sample_plugin("p-2"))
    assert PluginStore.count() == 2

    rows = PluginStore.all() |> Enum.sort_by(&elem(&1, 0))
    assert [{"p-1", r1}, {"p-2", _r2}] = rows
    assert r1.plugin.processor_id == "p-1"
    assert r1.plugin.provider == StdOut
    assert r1.stored_node == node()
    assert is_integer(r1.inserted_at)

    assert :ok = PluginStore.delete("p-1")
    assert PluginStore.count() == 1

    # Close + reopen, contents must survive.
    GenServer.stop(pid, :normal)
    assert File.exists?(Path.join(tmp, "recovery_plugins.dets"))

    {:ok, pid2} = PluginStore.start_link([])
    on_exit_stop(pid2)

    assert PluginStore.count() == 1
    [{"p-2", _}] = PluginStore.all()
  end

  test "delete is idempotent" do
    {:ok, pid} = PluginStore.start_link([])
    on_exit_stop(pid)

    :ok = PluginStore.put("only", sample_plugin("only"))
    :ok = PluginStore.delete("only")
    :ok = PluginStore.delete("only")
    :ok = PluginStore.delete("does-not-exist")
    assert PluginStore.count() == 0
  end

  test "put overwrites an existing plugin_id" do
    {:ok, pid} = PluginStore.start_link([])
    on_exit_stop(pid)

    :ok = PluginStore.put("dup", sample_plugin("a"))
    :ok = PluginStore.put("dup", sample_plugin("b"))

    assert PluginStore.count() == 1
    [{"dup", row}] = PluginStore.all()
    assert row.plugin.processor_id == "b"
  end

  test "init emits :plugin_store_opened telemetry", %{tmp: tmp} do
    handler = "plugin-store-opened-#{System.unique_integer([:positive])}"
    pid = self()

    :telemetry.attach(
      handler,
      [:flezha, :recovery, :plugin_store_opened],
      fn _n, m, meta, _ -> send(pid, {:telemetry, m, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, store_pid} = PluginStore.start_link([])
    on_exit_stop(store_pid)

    assert_receive {:telemetry, %{rows: 0}, %{path: path}}, 500
    assert path == Path.join(tmp, "recovery_plugins.dets")
  end

  test "coexists with Recovery.Store in the same data dir", %{tmp: tmp} do
    {:ok, store_pid} = Store.start_link([])
    on_exit_stop(store_pid)
    {:ok, plugin_pid} = PluginStore.start_link([])
    on_exit_stop(plugin_pid)

    :ok = Store.put("cid-1", "ns", %Flezha.Processor.Builder{id: "cid-1", factory_id: "f"})
    :ok = PluginStore.put("p-1", sample_plugin("cid-1"))

    assert Store.count() == 1
    assert PluginStore.count() == 1
    assert File.exists?(Path.join(tmp, "recovery.dets"))
    assert File.exists?(Path.join(tmp, "recovery_plugins.dets"))
  end

  defp sample_plugin(processor_id) do
    %Plugin{
      id: processor_id,
      namespace: "default",
      processor_id: processor_id,
      provider: StdOut,
      opts: %{},
      subscriptions: [{:on_output, []}]
    }
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

  defp on_exit_stop(pid) do
    on_exit(fn ->
      try do
        if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)
  end
end

defmodule Flezha.Recovery.DetsPersistenceRestartTest do
  @moduledoc """
  End-to-end durability proof for `Flezha.Recovery.Store` +
  `Flezha.Recovery.PluginStore`.

  Writes rows, stops the stores cleanly, starts fresh store processes against
  the same `recovery_data_dir`, and asserts the rows are still there. This is
  the upstream-level regression protection for the flezha_market bug where
  a missing persistent volume caused DETS to vanish on every
  `docker compose down && up`.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Processor.Builder
  alias Flezha.Recovery.PluginStore
  alias Flezha.Recovery.Store

  setup do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "flz_recovery_restart_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    Application.put_env(:flezha, :recovery_data_dir, tmp)

    stop_if_running(Store)
    stop_if_running(PluginStore)

    on_exit(fn ->
      stop_if_running(Store)
      stop_if_running(PluginStore)

      if prev_dir do
        Application.put_env(:flezha, :recovery_data_dir, prev_dir)
      else
        Application.delete_env(:flezha, :recovery_data_dir)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "processors and plugins survive a clean stop/start cycle", %{tmp: tmp} do
    # 1. First lifecycle: start, write, stop.
    {:ok, store1} = Store.start_link([])
    {:ok, pstore1} = PluginStore.start_link([])

    :ok = Store.put("btc_demo", "default", %Builder{id: "btc_demo", factory_id: "crypto_trades"})
    :ok = Store.put("eth_demo", "default", %Builder{id: "eth_demo", factory_id: "crypto_trades"})

    plugin = %Plugin{
      id: "init_state_from_java",
      namespace: "default",
      processor_id: "btc_demo",
      provider: StdOut,
      opts: %{},
      subscriptions: [{:on_output, []}]
    }

    :ok = PluginStore.put("init_state_from_java", plugin)

    assert Store.count() == 2
    assert PluginStore.count() == 1

    GenServer.stop(store1, :normal)
    GenServer.stop(pstore1, :normal)

    # 2. DETS files exist under the tmp dir — the "persistent volume".
    assert File.exists?(Path.join(tmp, "recovery.dets"))
    assert File.exists?(Path.join(tmp, "recovery_plugins.dets"))

    # 3. Fresh start — same dir, new processes. Simulates an agent restart.
    {:ok, store2} = Store.start_link([])
    {:ok, pstore2} = PluginStore.start_link([])
    on_exit_stop(store2)
    on_exit_stop(pstore2)

    assert Store.count() == 2
    assert PluginStore.count() == 1

    ids = Store.all() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    assert ids == ["btc_demo", "eth_demo"]

    [{"init_state_from_java", prow}] = PluginStore.all()
    assert prow.plugin.processor_id == "btc_demo"
  end

  defp stop_if_running(mod) do
    case Process.whereis(mod) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal)
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

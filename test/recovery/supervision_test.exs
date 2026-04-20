defmodule Flezha.Recovery.SupervisionTest do
  @moduledoc """
  Verifies startup ordering described in design §D8 — Store → SysMode →
  Coordinator come up before `Identificator`/`HubManager`, and a seeded DETS
  file causes SysMode to report `:recover` immediately after init.
  """

  use ExUnit.Case, async: false

  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode
  alias Flezha.Recovery.Coordinator
  alias Flezha.Processor.Builder

  test "seeded DETS file makes SysMode report :recover right after start" do
    tmp = Path.join(System.tmp_dir!(), "flz_sup_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    prev_enabled = Application.get_env(:flezha, :recovery_enabled)
    prev_window = Application.get_env(:flezha, :recovery_window_ms)

    Application.put_env(:flezha, :recovery_data_dir, tmp)
    Application.put_env(:flezha, :recovery_enabled, true)
    Application.put_env(:flezha, :recovery_window_ms, 60_000)

    on_exit(fn ->
      for mod <- [Coordinator, SysMode, Store], do: stop_safely(mod)

      restore(:recovery_data_dir, prev_dir)
      restore(:recovery_enabled, prev_enabled)
      restore(:recovery_window_ms, prev_window)

      File.rm_rf!(tmp)
    end)

    # Stop any existing instances.
    for mod <- [Coordinator, SysMode, Store], do: stop_safely(mod)

    # Pre-seed the DETS file as though the agent had been running.
    {:ok, table} =
      :dets.open_file(:flz_recovery, [
        {:type, :set},
        {:file, Path.join(tmp, "recovery.dets") |> String.to_charlist()}
      ])

    row = %{
      namespace: "ns",
      processor: %Builder{id: "p-seed", factory_id: "f"},
      stored_node: node(),
      inserted_at: System.system_time(:millisecond)
    }

    :ok = :dets.insert(table, {"p-seed", row})
    :ok = :dets.close(table)

    # Now start Store → SysMode → Coordinator in the order the application
    # does it.
    {:ok, _} = Store.start_link([])
    assert Store.count() == 1

    {:ok, _} = SysMode.start_link([])
    # Immediately after SysMode init (which is synchronous), mode is :recover.
    assert SysMode.get() == :recover

    {:ok, _} = Coordinator.start_link([])
    # Coordinator starting does not by itself change sysmode.
    assert SysMode.get() == :recover
  end

  test "application children list has recovery subtree before HubManager" do
    # Static-order check: the `recovery_children/0` helper must be spliced
    # into the Supervisor spec strictly before Identificator/HubManager so
    # hooks + DETS are ready by the time hubs start.
    {:ok, source} = File.read(Path.relative_to_cwd("lib/flezha/application.ex"))

    idx = fn needle ->
      case :binary.match(source, needle) do
        :nomatch -> flunk("expected to find #{inspect(needle)} in application.ex")
        {pos, _} -> pos
      end
    end

    assert idx.("recovery_children()") < idx.("Flezha.Service.Identificator")
    assert idx.("recovery_children()") < idx.("Flezha.Service.HubManager")
    assert idx.("recovery_children()") < idx.("Flezha.ComputeEngine.FleetSupervisor")

    # And the helper returns the four modules in the expected order:
    # Store → PluginStore → SysMode → Coordinator.
    assert source =~
             ~r/Flezha\.Recovery\.Store\s*,\s*Flezha\.Recovery\.PluginStore\s*,\s*Flezha\.Recovery\.SysMode\s*,\s*Flezha\.Recovery\.Coordinator/s
  end

  defp restore(key, nil), do: Application.delete_env(:flezha, key)
  defp restore(key, value), do: Application.put_env(:flezha, key, value)

  defp stop_safely(mod) do
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

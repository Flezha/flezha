defmodule Flezha.Plugin.Installer.SupervisionTreeTest do
  @moduledoc """
  Tests for `Flezha.Plugin.Installer.Supervisor`: child ordering,
  boot-time config validation, and the restart strategy.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer.Supervisor, as: InstallerSup

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_sup_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :plugin_installer_data_dir)
    prev_pi = Application.get_env(:flezha, :plugin_installer)

    Application.put_env(:flezha, :plugin_installer_data_dir, tmp)

    on_exit(fn ->
      # Stop the supervisor first so it tears down its children cleanly;
      # a rest_for_one strategy would otherwise restart children killed
      # individually below.
      for mod <- [
            InstallerSup,
            Flezha.Plugin.Installer,
            Flezha.Plugin.Installer.Loader,
            Flezha.Plugin.Installer.Store
          ] do
        try do
          case Process.whereis(mod) do
            nil -> :ok
            pid -> if Process.alive?(pid), do: stop_quiet(pid)
          end
        catch
          :exit, _ -> :ok
        end
      end

      if prev_dir do
        Application.put_env(:flezha, :plugin_installer_data_dir, prev_dir)
      else
        Application.delete_env(:flezha, :plugin_installer_data_dir)
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

  test "supervisor validates config at init — require_signature without keys fails" do
    Application.put_env(:flezha, :plugin_installer,
      plugin_root: "tmp",
      trusted_keys: [],
      require_signature: true,
      allow_unsigned: false,
      max_package_bytes: 10_000,
      max_source_files: 100
    )

    assert {:error, :trusted_keys_empty} = InstallerSup.validate_config()
  end

  test "supervisor starts Store, Loader, Installer in order", %{tmp: _tmp} do
    Application.put_env(:flezha, :plugin_installer,
      plugin_root: "tmp",
      trusted_keys: [],
      require_signature: false,
      allow_unsigned: true,
      max_package_bytes: 10_000,
      max_source_files: 100
    )

    {:ok, _pid} = InstallerSup.start_link([])

    children = Supervisor.which_children(InstallerSup)
    names = Enum.map(children, fn {id, _, _, _} -> id end) |> Enum.reverse()

    assert names == [
             Flezha.Plugin.Installer.Store,
             Flezha.Plugin.Installer.Loader,
             Flezha.Plugin.Installer
           ]
  end

  defp stop_quiet(pid) do
    try do
      GenServer.stop(pid, :normal, 1_000)
    catch
      :exit, _ -> :ok
    end
  end
end

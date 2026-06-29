defmodule Flezha.Plugin.Installer.InstallerTest do
  @moduledoc """
  End-to-end tests for `Flezha.Plugin.Installer`. Builds real `.fpz`
  fixtures in-test via `:erl_tar` so each test owns its archive.

  These tests exercise install → enable/disable → uninstall / upgrade
  paths against Store+Loader+PluginManager-free stubs. A dedicated
  `Flezha.Service.PluginManager` is required at the module level — tests
  start one in a supervisor within each test.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store

  setup do
    tmp =
      Path.join(System.tmp_dir!(), "flz_installer_svc_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp)

    prev_data_dir = Application.get_env(:flezha, :plugin_installer_data_dir)
    prev_pi = Application.get_env(:flezha, :plugin_installer)

    Application.put_env(:flezha, :plugin_installer_data_dir, tmp)

    Application.put_env(
      :flezha,
      :plugin_installer,
      Keyword.merge(prev_pi || [],
        plugin_root: Path.join(tmp, "plugins"),
        trusted_keys: [],
        require_signature: false,
        allow_unsigned: true,
        max_package_bytes: 50_000_000,
        max_source_files: 200
      )
    )

    stop_if_running(Installer)
    stop_if_running(Loader)
    stop_if_running(Store)

    on_exit(fn ->
      stop_if_running(Installer)
      stop_if_running(Loader)
      stop_if_running(Store)

      if prev_data_dir do
        Application.put_env(:flezha, :plugin_installer_data_dir, prev_data_dir)
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

    {:ok, store} = Store.start_link([])
    {:ok, loader} = Loader.start_link([])
    {:ok, installer} = Installer.start_link([])

    on_exit_stop(store)
    on_exit_stop(loader)
    on_exit_stop(installer)

    {:ok, tmp: tmp}
  end

  test "install / list / info / uninstall round-trip", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod_name = "FlzInstallTest.Valid#{unique}"
    fpz = build_fixture!(tmp, "valid", "1.0.0", [mod_name])

    assert {:ok, installed} = Installer.install(fpz)
    assert installed.id == "valid"
    assert installed.version == "1.0.0"
    assert installed.status == :disabled

    assert Enum.any?(Installer.list(), fn p -> p.id == "valid" end)
    assert {:ok, _} = Installer.info("valid")

    # Uninstall requires disabled state (which is our default after install).
    assert :ok = Installer.uninstall("valid")
    assert {:error, :not_found} = Installer.info("valid")
  end

  test "install fails when manifest missing", %{tmp: tmp} do
    fpz = Path.join(tmp, "bad.fpz")

    write_fpz!(fpz, [
      {"src/x.ex", "defmodule X do end"},
      {"CHECKSUMS.sha256", ""}
    ])

    assert {:error, {:invalid_layout, :manifest_missing}} = Installer.install(fpz)
  end

  test "install rejects already-installed id", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod_name = "FlzInstallTest.Again#{unique}"
    fpz = build_fixture!(tmp, "again", "1.0.0", [mod_name])

    assert {:ok, _} = Installer.install(fpz)
    assert {:error, {:already_installed, "again"}} = Installer.install(fpz)

    :ok = Installer.uninstall("again")
  end

  test "install rejects missing dependency", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod_name = "FlzInstallTest.WithDep#{unique}"
    fpz = build_fixture!(tmp, "dep_user", "1.0.0", [mod_name], deps: [{"common", ">= 1.0.0"}])

    assert {:error, {:dependency_missing, "common", ">= 1.0.0"}} =
             Installer.install(fpz)
  end

  ## Helpers

  defp build_fixture!(tmp, id, version, mods, opts \\ []) do
    deps = Keyword.get(opts, :deps, [])
    fpz = Path.join(tmp, "#{id}-#{version}.fpz")

    [first_mod | _] = mods
    src_filename = Macro.underscore(first_mod) <> ".ex"

    src_body = Enum.map_join(mods, "\n", fn m -> "defmodule #{m} do\n  def hi, do: :ok\nend" end)

    manifest = """
    %{
      id: "#{id}",
      version: "#{version}",
      description: "test",
      author: "tester",
      modules: [#{Enum.join(mods, ", ")}],
      provides: [
        %{
          id: "main",
          provider: #{first_mod},
          subscriptions: [{:on_input, []}],
          default_opts: %{},
          scope: :operative
        }
      ],
      dependencies: #{inspect(deps)}
    }
    """

    manifest_hash = sha256(manifest)
    src_hash = sha256(src_body)

    checksums = "#{manifest_hash}  manifest.exs\n#{src_hash}  src/#{src_filename}\n"

    write_fpz!(fpz, [
      {"manifest.exs", manifest},
      {"src/#{src_filename}", src_body},
      {"CHECKSUMS.sha256", checksums}
    ])

    fpz
  end

  defp write_fpz!(fpz_path, entries) do
    :ok = File.mkdir_p!(Path.dirname(fpz_path))

    tar_entries =
      Enum.map(entries, fn {name, content} ->
        {String.to_charlist(name), content}
      end)

    :ok =
      :erl_tar.create(
        String.to_charlist(fpz_path),
        tar_entries,
        [:compressed]
      )
  end

  defp sha256(binary), do: :crypto.hash(:sha256, binary) |> Base.encode16(case: :lower)

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

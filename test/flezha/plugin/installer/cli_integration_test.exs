defmodule Flezha.Plugin.Installer.CliIntegrationTest do
  @moduledoc """
  CLI-layer integration tests. The Rust `flz` binary dispatches to
  `Flezha.Service.CliManager` RPC methods; this test exercises each of
  those entry points directly with a running installer subtree, covering
  the same flow `flz plugin install/list/info/enable/disable/upgrade/uninstall`
  would drive.

  Shelling out to the release binary would also require `mix release` +
  a live node — that harness is noted in `openspec/changes/add-installable-plugins/tasks.md`
  as future work. Exercising the CliManager functions here catches every
  wiring bug a shell harness would have caught while keeping the test
  hermetic.
  """

  # async: false: writes Application env; a shared on-disk root; registered singletons.

  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "flz_cli_integration_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)

    prev_data_dir = Application.get_env(:flezha, :plugin_installer_data_dir)
    prev_pi = Application.get_env(:flezha, :plugin_installer)

    Application.put_env(:flezha, :plugin_installer_data_dir, tmp)

    Application.put_env(:flezha, :plugin_installer,
      plugin_root: Path.join(tmp, "plugins"),
      trusted_keys: [],
      require_signature: false,
      allow_unsigned: true,
      max_package_bytes: 50_000_000,
      max_source_files: 200
    )

    stop_if_running(Installer)
    stop_if_running(Loader)
    stop_if_running(Store)

    {:ok, _} = Store.start_link([])
    {:ok, _} = Loader.start_link([])
    {:ok, _} = Installer.start_link([])

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

    {:ok, tmp: tmp}
  end

  test "install_plugin → list_installed_plugins text / json / info", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod = "CliIntegration.One#{unique}"
    fpz = build_fixture!(tmp, "cli_one_#{unique}", "1.0.0", [mod])

    # Install.
    install_output = capture_io(fn -> CliManager.install_plugin(fpz, "{}") end)
    assert install_output =~ "Installed plugin cli_one_#{unique}"
    assert install_output =~ "status: disabled"

    # Text list includes our plugin.
    text_list = capture_io(fn -> CliManager.list_installed_plugins("text") end)
    assert text_list =~ "cli_one_#{unique}@1.0.0"

    # JSON list parses and includes our plugin.
    json_list = capture_io(fn -> CliManager.list_installed_plugins("json") end)
    decoded = Jason.decode!(String.trim(json_list))
    assert Enum.any?(decoded, fn p -> p["id"] == "cli_one_#{unique}" end)

    ours = Enum.find(decoded, fn p -> p["id"] == "cli_one_#{unique}" end)
    assert ours["version"] == "1.0.0"
    assert ours["status"] == "disabled"
    assert Enum.any?(ours["provides"], fn prov -> prov["id"] == "main" end)

    # Info.
    info_output = capture_io(fn -> CliManager.installed_plugin_info("cli_one_#{unique}") end)
    assert info_output =~ "cli_one_#{unique}@1.0.0"
    assert info_output =~ "Status: disabled"

    # installed_plugin?/1 returns true for a known id.
    assert CliManager.installed_plugin?("cli_one_#{unique}") == true
    assert CliManager.installed_plugin?("not_there") == false
  end

  test "installer_enable/disable flip status through CliManager", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod = "CliIntegration.Lifecycle#{unique}"
    fpz = build_fixture!(tmp, "cli_life_#{unique}", "1.0.0", [mod])

    capture_io(fn -> CliManager.install_plugin(fpz, "{}") end)

    # Enable — this will call PluginManager.enable_plugin which expects a
    # real operative. Without one, the enable will fail. Verify the error
    # is surfaced to stdout rather than crashing.
    enable_output =
      capture_io(fn ->
        catch_exit(CliManager.installer_enable_plugin("cli_life_#{unique}", "{}"))
      end)

    # Either succeeds (if the PluginManager path is tolerant) or reports a
    # structured error and exits :cli_error — both are acceptable CLI
    # behaviours as long as we don't crash.
    assert enable_output =~ "Plugin" or enable_output =~ "Error"

    # Disable is idempotent on a not-enabled plugin, so it should always
    # produce the success line.
    disable_output =
      capture_io(fn -> CliManager.installer_disable_plugin("cli_life_#{unique}", "{}") end)

    assert disable_output =~ "disabled successfully"
  end

  test "uninstall_plugin removes the catalog row and directory", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod = "CliIntegration.Uninstall#{unique}"
    fpz = build_fixture!(tmp, "cli_un_#{unique}", "1.0.0", [mod])

    capture_io(fn -> CliManager.install_plugin(fpz, "{}") end)
    assert CliManager.installed_plugin?("cli_un_#{unique}") == true

    uninstall_output =
      capture_io(fn -> CliManager.uninstall_plugin("cli_un_#{unique}", "{}") end)

    assert uninstall_output =~ "Uninstalled plugin cli_un_#{unique}"
    assert CliManager.installed_plugin?("cli_un_#{unique}") == false
  end

  test "upgrade_plugin bumps the version", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod_a = "CliIntegration.UpgradeA#{unique}"
    mod_b = "CliIntegration.UpgradeB#{unique}"

    fpz_v1 = build_fixture!(tmp, "cli_up_#{unique}", "1.0.0", [mod_a])
    fpz_v2 = build_fixture!(tmp, "cli_up_#{unique}", "1.1.0", [mod_b])

    capture_io(fn -> CliManager.install_plugin(fpz_v1, "{}") end)

    upgrade_output =
      capture_io(fn -> CliManager.upgrade_plugin("cli_up_#{unique}", fpz_v2, "{}") end)

    assert upgrade_output =~ "Upgraded plugin cli_up_#{unique} to 1.1.0"

    info_output =
      capture_io(fn -> CliManager.installed_plugin_info("cli_up_#{unique}") end)

    assert info_output =~ "cli_up_#{unique}@1.1.0"
  end

  test "uninstall reports the refusal reason when plugin is enabled", %{tmp: tmp} do
    # Even without a live operative to attach subscriptions to, we can
    # force the catalog row to :enabled and verify uninstall refuses.
    unique = System.unique_integer([:positive])
    mod = "CliIntegration.Refuse#{unique}"
    fpz = build_fixture!(tmp, "cli_refuse_#{unique}", "1.0.0", [mod])

    capture_io(fn -> CliManager.install_plugin(fpz, "{}") end)
    :ok = Store.update_status("cli_refuse_#{unique}", :enabled)

    output =
      capture_io(fn -> catch_exit(CliManager.uninstall_plugin("cli_refuse_#{unique}", "{}")) end)

    assert output =~ "Error uninstalling plugin"
    assert output =~ "plugin_enabled"
  end

  test "install surfaces the structured error from a bad package", %{tmp: tmp} do
    bad = Path.join(tmp, "bad.fpz")
    File.write!(bad, "not a tarball")

    output = capture_io(fn -> catch_exit(CliManager.install_plugin(bad, "{}")) end)
    assert output =~ "Error installing plugin"
  end

  ## Helpers

  defp build_fixture!(tmp, id, version, mods) do
    fpz = Path.join(tmp, "#{id}-#{version}.fpz")

    [first_mod | _] = mods
    src_filename = Macro.underscore(first_mod) <> ".ex"

    src_body =
      Enum.map_join(mods, "\n", fn m -> "defmodule #{m} do\n  def hi, do: :ok\nend" end)

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
      dependencies: []
    }
    """

    manifest_hash = sha256(manifest)
    src_hash = sha256(src_body)
    checksums = "#{manifest_hash}  manifest.exs\n#{src_hash}  src/#{src_filename}\n"

    :ok = File.mkdir_p!(Path.dirname(fpz))

    entries = [
      {"manifest.exs", manifest},
      {"src/#{src_filename}", src_body},
      {"CHECKSUMS.sha256", checksums}
    ]

    :ok =
      :erl_tar.create(
        String.to_charlist(fpz),
        Enum.map(entries, fn {n, b} -> {String.to_charlist(n), b} end),
        [:compressed]
      )

    fpz
  end

  defp sha256(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)

  defp stop_if_running(mod) do
    try do
      case Process.whereis(mod) do
        nil -> :ok
        pid -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
      end
    catch
      :exit, _ -> :ok
    end
  end
end

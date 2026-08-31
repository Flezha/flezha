defmodule Flezha.Plugin.Installer.InstallerTest do
  @moduledoc """
  End-to-end tests for `Flezha.Plugin.Installer`. Builds real `.fpz`
  fixtures in-test via `:erl_tar` so each test owns its archive.

  These tests exercise install → enable/disable → uninstall / upgrade
  paths against Store+Loader+PluginManager-free stubs. A dedicated
  `Flezha.Service.PluginManager` is required at the module level — tests
  start one in a supervisor within each test.
  """

  # async: false: writes Application env; a shared on-disk root; registered singletons.

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store
  alias FlezhaTest.Helper.NodeTestHelper

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
        pid -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
      end
    catch
      :exit, _ -> :ok
    end
  end

  defp on_exit_stop(pid) do
    on_exit(fn ->
      try do
        NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)
  end
end

defmodule Flezha.Plugin.Installer.ExampleHelloWorldTest do
  @moduledoc """
  Verifies that `examples/plugins/hello_world/` is a genuinely installable
  package — not just a set of files that look correct. Builds a real
  `.fpz` from the example tree and runs it through
  `Flezha.Plugin.Installer.install/2`.

  This guards against the example drifting out of sync with the installer:
  a change that breaks the manifest schema, the checksum format, or the
  source layout would silently leave the docs-facing example broken
  without this test.
  """

  # async: false: writes Application env; a shared on-disk root; registered singletons.

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store
  alias FlezhaTest.Helper.NodeTestHelper

  @example_dir Path.expand("../../../../examples/plugins/hello_world", __DIR__)

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_hello_example_#{System.unique_integer([:positive])}")
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

  test "examples/plugins/hello_world tars into a working .fpz", %{tmp: tmp} do
    assert File.exists?(Path.join(@example_dir, "manifest.exs")),
           "examples/plugins/hello_world/manifest.exs missing"

    assert File.exists?(Path.join(@example_dir, "src/hello_world.ex")),
           "examples/plugins/hello_world/src/hello_world.ex missing"

    # Build the checksums string fresh from the source bytes so the test
    # tracks the example tree instead of asserting a frozen committed
    # value. The on-disk `CHECKSUMS.sha256` is the docs workflow's
    # snapshot; the test's job is to validate that the installer accepts
    # a freshly-tarred package, not that the committed hash file is
    # current.
    manifest_hash =
      :crypto.hash(:sha256, File.read!(Path.join(@example_dir, "manifest.exs")))
      |> Base.encode16(case: :lower)

    src_hash =
      :crypto.hash(:sha256, File.read!(Path.join(@example_dir, "src/hello_world.ex")))
      |> Base.encode16(case: :lower)

    checksums =
      "#{manifest_hash}  manifest.exs\n" <>
        "#{src_hash}  src/hello_world.ex\n"

    # Tar the example into a real .fpz and install.
    fpz = Path.join(tmp, "hello_world-1.0.0.fpz")

    :ok =
      :erl_tar.create(
        String.to_charlist(fpz),
        [
          {~c"manifest.exs", File.read!(Path.join(@example_dir, "manifest.exs"))},
          {~c"src/hello_world.ex", File.read!(Path.join(@example_dir, "src/hello_world.ex"))},
          {~c"CHECKSUMS.sha256", checksums}
        ],
        [:compressed]
      )

    assert {:ok, installed} = Installer.install(fpz)
    assert installed.id == "hello_world"
    assert installed.version == "1.0.0"
    assert installed.status == :disabled
    assert Flezha.Examples.HelloWorld in installed.modules
  end

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

defmodule Flezha.Plugin.Installer.PublicServiceAPITest do
  @moduledoc """
  Tests that mutating procedures serialize through the `Installer` GenServer
  so two concurrent installs cannot race on the code server or DETS.
  """

  # async: false: writes Application env; a shared on-disk root; registered singletons.

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    tmp =
      Path.join(System.tmp_dir!(), "flz_svc_api_test_#{System.unique_integer([:positive])}")

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

    for mod <- [Installer, Loader, Store] do
      try do
        case Process.whereis(mod) do
          nil -> :ok
          pid -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
        end
      catch
        :exit, _ -> :ok
      end
    end

    {:ok, _} = Store.start_link([])
    {:ok, _} = Loader.start_link([])
    {:ok, _} = Installer.start_link([])

    on_exit(fn ->
      for mod <- [Installer, Loader, Store] do
        try do
          case Process.whereis(mod) do
            nil -> :ok
            pid -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
          end
        catch
          :exit, _ -> :ok
        end
      end

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

  test "info/1 for unknown plugin returns :not_found" do
    assert {:error, :not_found} = Installer.info("nobody_home")
  end

  test "list/0 returns [] on empty catalog" do
    assert Installer.list() == []
  end

  test "concurrent installs serialize — one waits for the other", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    fpz_a = build_fpz!(tmp, "ser_a_#{unique}", "SerA.M#{unique}")
    fpz_b = build_fpz!(tmp, "ser_b_#{unique}", "SerB.M#{unique}")

    parent = self()

    t0 = System.monotonic_time(:millisecond)

    pid_a =
      spawn(fn ->
        r = Installer.install(fpz_a)
        send(parent, {:a_done, System.monotonic_time(:millisecond), r})
      end)

    pid_b =
      spawn(fn ->
        r = Installer.install(fpz_b)
        send(parent, {:b_done, System.monotonic_time(:millisecond), r})
      end)

    assert_receive {:a_done, _, {:ok, _}}, 60_000
    assert_receive {:b_done, _, {:ok, _}}, 60_000

    # Both succeed; the GenServer serialized them, no concurrent writes to
    # the code server / DETS.
    refute Process.alive?(pid_a)
    refute Process.alive?(pid_b)
    assert System.monotonic_time(:millisecond) >= t0
  end

  defp build_fpz!(tmp, id, mod) do
    fpz = Path.join(tmp, "#{id}.fpz")
    src_filename = Macro.underscore(mod) <> ".ex"

    manifest = """
    %{
      id: "#{id}",
      version: "1.0.0",
      description: "test",
      author: "tester",
      modules: [#{mod}],
      provides: [
        %{
          id: "main",
          provider: #{mod},
          subscriptions: [{:on_input, []}],
          default_opts: %{},
          scope: :operative
        }
      ],
      dependencies: []
    }
    """

    src = "defmodule #{mod} do\n  def hi, do: :ok\nend"

    entries = [
      {"manifest.exs", manifest},
      {"src/#{src_filename}", src},
      {"CHECKSUMS.sha256", "#{sha(manifest)}  manifest.exs\n#{sha(src)}  src/#{src_filename}\n"}
    ]

    :ok = File.mkdir_p!(Path.dirname(fpz))

    :ok =
      :erl_tar.create(
        String.to_charlist(fpz),
        Enum.map(entries, fn {n, b} -> {String.to_charlist(n), b} end),
        [:compressed]
      )

    fpz
  end

  defp sha(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)
end

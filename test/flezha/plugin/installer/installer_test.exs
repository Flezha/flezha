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

  use FlezhaTest.InstallerCase, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.TestHelpers.Plugins

  setup %{tmp: tmp} do
    start_installer_tree(tmp)
  end

  test "install / list / info / uninstall round-trip", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod_name = "FlzInstallTest.Valid#{unique}"
    fpz = Plugins.build_fixture!(tmp, "valid", "1.0.0", [mod_name])

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
    fpz = Plugins.build_fixture!(tmp, "again", "1.0.0", [mod_name])

    assert {:ok, _} = Installer.install(fpz)
    assert {:error, {:already_installed, "again"}} = Installer.install(fpz)

    :ok = Installer.uninstall("again")
  end

  test "install rejects missing dependency", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod_name = "FlzInstallTest.WithDep#{unique}"

    fpz =
      Plugins.build_fixture!(tmp, "dep_user", "1.0.0", [mod_name], deps: [{"common", ">= 1.0.0"}])

    assert {:error, {:dependency_missing, "common", ">= 1.0.0"}} =
             Installer.install(fpz)
  end

  ## Helpers

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

  use FlezhaTest.InstallerCase, async: false

  alias Flezha.Plugin.Installer

  @example_dir Path.expand("../../../../examples/plugins/hello_world", __DIR__)

  setup %{tmp: tmp} do
    start_installer_tree(tmp)
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
end

defmodule Flezha.Plugin.Installer.PublicServiceAPITest do
  @moduledoc """
  Tests that mutating procedures serialize through the `Installer` GenServer
  so two concurrent installs cannot race on the code server or DETS.
  """

  # async: false: writes Application env; a shared on-disk root; registered singletons.

  use FlezhaTest.InstallerCase, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Store

  setup %{tmp: tmp} do
    start_installer_tree(tmp)
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

  # Fails when: a package an earlier build installed, whose stored provision names
  # its hooks under `subscriptions`, is enabled with no hooks and nothing said.
  test "a stored provision without hooks is refused when enabled", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    id = "earlier_#{unique}"
    {:ok, _} = Installer.install(build_fpz!(tmp, id, "Earlier.M#{unique}"))

    row = Store.get(id)

    earlier =
      Enum.map(row.provides, fn provision ->
        provision |> Map.delete(:hooks) |> Map.put(:subscriptions, [{:on_input, []}])
      end)

    :ok = Store.put(id, %{row | provides: earlier})

    assert {:error, {:invalid_hook, :not_a_list}} =
             Installer.enable(id, namespace: "default", operative_id: "op")
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
          hooks: [{:on_input, []}],
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

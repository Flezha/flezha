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

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store

  @example_dir Path.expand("../../../examples/plugins/hello_world", __DIR__)

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
        pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
      end
    catch
      :exit, _ -> :ok
    end
  end
end

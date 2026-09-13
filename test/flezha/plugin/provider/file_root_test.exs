defmodule Flezha.Plugin.Provider.FileRootTest do
  @moduledoc """
  The one folder a file plugin may name: every path resolves inside
  `FLEZHA_FILE_PLUGIN_ROOT` with symlinks followed, or the enable is refused
  naming what stopped it.
  """

  # async: false: writes Application env.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.Config
  alias Flezha.Plugin.Provider.File, as: FilePlugin
  alias FlezhaTest.Helper.ProviderPrepare

  # The root is a folder inside the temporary directory, so there is always an
  # outside to climb into.
  setup %{tmp: tmp} do
    root = Path.join(tmp, "files")
    File.mkdir_p!(root)
    Application.put_env(:flezha, :file_plugin_root, root)
    %{root: root, outside: tmp}
  end

  test "an unset root is the default folder, created by the first enable" do
    Application.delete_env(:flezha, :file_plugin_root)
    default = Path.expand(Config.default(:file_plugin_root))
    existed? = File.dir?(default)
    # rmdir, never rm_rf: a checkout's default folder may hold someone's files.
    on_exit(fn -> if not existed?, do: File.rmdir(default) end)

    assert {:ok, %{path: path}} =
             ProviderPrepare.prepare(FilePlugin.Sink, %{path: "events.ndjson"})

    assert path == Path.join(default, "events.ndjson")
  end

  test "a configured root that does not exist is refused, and not created", %{outside: outside} do
    missing = Path.join(outside, "unmounted")
    Application.put_env(:flezha, :file_plugin_root, missing)

    for {provider, opts} <- [
          {FilePlugin.Source, %{path: "events.ndjson", channel_in: "events"}},
          {FilePlugin.Sink, %{path: "events.ndjson"}}
        ] do
      assert {:error, message} = ProviderPrepare.prepare(provider, opts)
      assert message =~ missing
    end

    refute File.exists?(missing)
  end

  test "a root that does not exist is refused naming it", %{outside: outside} do
    missing = Path.join(outside, "absent")

    assert {:error, message} = FilePlugin.resolve_within_root("events.ndjson", missing)
    assert message =~ missing
  end

  test "a relative path lands under the root", %{root: root} do
    assert FilePlugin.resolve_within_root("events.ndjson", root) ==
             {:ok, Path.join(root, "events.ndjson")}
  end

  test "a path that climbs out is refused naming the root", %{root: root, outside: outside} do
    File.write!(Path.join(outside, "controller.key"), "")

    assert {:error, message} = FilePlugin.resolve_within_root("../controller.key", root)
    assert message =~ "outside the root folder #{root}"
  end

  test "an absolute path outside the root is refused naming the root", %{root: root} do
    assert {:error, message} = FilePlugin.resolve_within_root("/etc/passwd", root)
    assert message =~ "outside the root folder #{root}"
  end

  test "a symlink pointing out of the root is refused, because the path lands outside it",
       %{root: root, outside: outside} do
    File.mkdir_p!(Path.join(outside, "etc"))
    File.write!(Path.join(outside, "etc/hosts"), "")
    File.ln_s!(Path.join(outside, "etc"), Path.join(root, "out"))
    File.ln_s!(Path.join(outside, "etc/hosts"), Path.join(root, "hosts"))

    # Through a linked directory, through a linked file, and through a link to a
    # file that does not exist yet.
    for path <- ["out/hosts", "hosts", "out/not-yet"] do
      assert {:error, message} = FilePlugin.resolve_within_root(path, root)
      assert message =~ "outside the root folder #{root}"
    end
  end

  test "a symlink pointing inside the root is followed to where it lands", %{root: root} do
    File.mkdir_p!(Path.join(root, "2026-09"))
    File.ln_s!("2026-09", Path.join(root, "current"))

    assert FilePlugin.resolve_within_root("current/events.ndjson", root) ==
             {:ok, Path.join(root, "2026-09/events.ndjson")}
  end

  test "a missing parent is refused naming the missing directory", %{root: root} do
    assert {:error, message} = FilePlugin.resolve_within_root("nope/events.ndjson", root)
    assert message =~ "the directory #{Path.join(root, "nope")} does not exist"
  end

  test "a missing directory outside the root is not named", %{root: root} do
    assert {:error, message} = FilePlugin.resolve_within_root("../nope/events.ndjson", root)
    assert message =~ "outside the root folder #{root}"
    refute message =~ "does not exist"
  end

  test "a link loop is refused rather than walked for ever", %{root: root} do
    File.ln_s!("b", Path.join(root, "a"))
    File.ln_s!("a", Path.join(root, "b"))

    assert {:error, message} = FilePlugin.resolve_within_root("a/events.ndjson", root)
    assert message =~ "more than 40 symbolic links"
  end

  # A plugin is prepared again from the options it already holds on a plugin
  # restart, so the resolved path must resolve to itself.
  test "the resolved path replaces the one written, and resolves to itself again",
       %{root: root} do
    File.mkdir_p!(Path.join(root, "2026-09"))
    File.ln_s!("2026-09", Path.join(root, "current"))

    assert {:ok, opts} = ProviderPrepare.prepare(FilePlugin.Sink, %{path: "current/out.ndjson"})
    assert opts.path == Path.join(root, "2026-09/out.ndjson")

    assert {:ok, again} = ProviderPrepare.prepare(FilePlugin.Sink, %{path: opts.path})
    assert again.path == opts.path
  end
end

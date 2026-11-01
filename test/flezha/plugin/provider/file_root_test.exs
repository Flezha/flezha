defmodule Flezha.Plugin.Provider.FileRootTest do
  @moduledoc """
  The one folder a file plugin may name: every path resolves inside its
  namespace's own folder under `FLEZHA_FILE_PLUGIN_ROOT` with symlinks
  followed, or the enable is refused naming what stopped it.
  """

  # async: false: writes Application env.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.Config
  alias Flezha.Plugin.Provider.File, as: FilePlugin
  alias Flezha.Plugin.Provider.File.Storage.Local
  alias FlezhaTest.Helper.Minio
  alias FlezhaTest.Helper.ProviderPrepare

  # The root is a folder inside the temporary directory, so there is always an
  # outside to climb into.
  setup %{tmp: tmp} do
    root = Path.join(tmp, "files")
    File.mkdir_p!(root)
    Application.put_env(:flezha, :file_plugin_root, root)
    %{root: root, outside: tmp}
  end

  test "an unset root is the default folder, created with the namespace's by the first enable" do
    Application.delete_env(:flezha, :file_plugin_root)
    default = Path.expand(Config.default(:file_plugin_root))
    namespace = "root-test-#{System.unique_integer([:positive])}"
    existed? = File.dir?(default)

    # rmdir, never rm_rf: a checkout's default folder may hold someone's files.
    on_exit(fn ->
      File.rmdir(Path.join(default, namespace))
      if not existed?, do: File.rmdir(default)
    end)

    assert {:ok, %{path: path}} = prepare_in(namespace, %{path: "events.ndjson"})
    assert path == Path.join([default, namespace, "events.ndjson"])
  end

  test "a configured root that does not exist is refused, and not created", %{outside: outside} do
    missing = Path.join(outside, "unmounted")
    Application.put_env(:flezha, :file_plugin_root, missing)

    for {provider, opts} <- [
          {FilePlugin.Source, %{path: "events.ndjson", channel_in: "events"}},
          {FilePlugin.Sink, %{path: "events.ndjson"}}
        ] do
      assert {:error, {:file_root_missing, ^missing}} = ProviderPrepare.prepare(provider, opts)
    end

    refute File.exists?(missing)
  end

  test "a root that does not exist is refused naming it", %{outside: outside} do
    missing = Path.join(outside, "absent")

    assert {:error, {:file_root_missing, ^missing}} = Local.resolve("events.ndjson", missing)
  end

  test "a relative path lands under the root", %{root: root} do
    assert Local.resolve("events.ndjson", root) ==
             {:ok, Path.join(root, "events.ndjson")}
  end

  test "a path that climbs out is refused naming the root", %{root: root, outside: outside} do
    File.write!(Path.join(outside, "controller.key"), "")

    assert {:error, {:file_path_outside_root, "../controller.key", ^root}} =
             Local.resolve("../controller.key", root)
  end

  test "an absolute path outside the root is refused naming the root", %{root: root} do
    assert {:error, {:file_path_outside_root, "/etc/passwd", ^root}} =
             Local.resolve("/etc/passwd", root)
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
      assert {:error, {:file_path_outside_root, ^path, ^root}} = Local.resolve(path, root)
    end
  end

  test "a symlink pointing inside the root is followed to where it lands", %{root: root} do
    File.mkdir_p!(Path.join(root, "2026-09"))
    File.ln_s!("2026-09", Path.join(root, "current"))

    assert Local.resolve("current/events.ndjson", root) ==
             {:ok, Path.join(root, "2026-09/events.ndjson")}
  end

  test "a missing parent is refused naming the missing directory", %{root: root} do
    missing = Path.join(root, "nope")

    assert {:error, {:file_directory_missing, "nope/events.ndjson", ^missing}} =
             Local.resolve("nope/events.ndjson", root)
  end

  test "a missing directory outside the root is not named", %{root: root} do
    assert {:error, {:file_path_outside_root, "../nope/events.ndjson", ^root}} =
             Local.resolve("../nope/events.ndjson", root)
  end

  test "a link loop is refused rather than walked for ever", %{root: root} do
    File.ln_s!("b", Path.join(root, "a"))
    File.ln_s!("a", Path.join(root, "b"))

    assert {:error, {:file_path_too_many_links, "a/events.ndjson", 40}} =
             Local.resolve("a/events.ndjson", root)
  end

  describe "each namespace's own folder" do
    test "a relative path lands in the namespace's folder, made readable by its owner only",
         %{root: root} do
      assert {:ok, %{path: path}} = prepare_in("orders", %{path: "events.ndjson"})
      assert path == Path.join([root, "orders", "events.ndjson"])
      assert %File.Stat{mode: mode} = File.stat!(Path.join(root, "orders"))
      assert Bitwise.band(mode, 0o777) == 0o700
    end

    test "a folder that exists keeps its mode", %{root: root} do
      File.mkdir_p!(Path.join(root, "orders"))
      File.chmod!(Path.join(root, "orders"), 0o750)

      assert {:ok, _opts} = prepare_in("orders", %{path: "events.ndjson"})
      assert Bitwise.band(File.stat!(Path.join(root, "orders")).mode, 0o777) == 0o750
    end

    test "a path that climbs out, or is absolute outside the root, is refused naming the folder",
         %{root: root, outside: outside} do
      folder = Path.join(root, "orders")
      File.write!(Path.join(outside, "controller.key"), "")

      for path <- ["../../controller.key", "../certs/controller.key", "/etc/passwd"] do
        assert {:error, {:file_path_outside_root, ^path, ^folder}} =
                 prepare_in("orders", %{path: path}),
               path
      end
    end

    test "a symlink out of the folder is refused, one inside it is followed", %{root: root} do
      folder = Path.join(root, "orders")
      File.mkdir_p!(Path.join(folder, "2026-09"))
      File.ln_s!("/etc", Path.join(folder, "out"))
      File.ln_s!(Path.join(folder, "2026-09"), Path.join(folder, "current"))

      assert {:error, {:file_path_outside_root, "out/hosts", ^folder}} =
               prepare_in("orders", %{path: "out/hosts"})

      assert {:ok, %{path: path}} = prepare_in("orders", %{path: "current/events.ndjson"})
      assert path == Path.join(folder, "2026-09/events.ndjson")
    end

    test "a missing parent is refused naming the missing directory", %{root: root} do
      missing = Path.join([root, "orders", "nope"])

      assert {:error, {:file_directory_missing, "nope/events.ndjson", ^missing}} =
               prepare_in("orders", %{path: "nope/events.ndjson"})
    end

    test "another namespace's folder is refused naming this namespace's", %{root: root} do
      File.mkdir_p!(Path.join(root, "orders/in"))
      folder = Path.join(root, "payments")
      absolute = Path.join(root, "orders/in/trades.jsonl")

      assert {:error, {:file_path_outside_root, "../orders/in/trades.jsonl", ^folder}} =
               prepare_in("payments", %{path: "../orders/in/trades.jsonl"})

      assert {:error, {:file_path_outside_namespace, ^absolute, ^folder, _moved}} =
               prepare_in("payments", %{path: absolute})
    end

    test "a symlink into another namespace's folder is refused", %{root: root} do
      File.mkdir_p!(Path.join(root, "orders"))
      File.mkdir_p!(Path.join(root, "payments"))
      File.ln_s!(Path.join(root, "orders"), Path.join(root, "payments/peek"))
      folder = Path.join(root, "payments")

      assert {:error, {:file_path_outside_root, "peek/events.ndjson", ^folder}} =
               prepare_in("payments", %{path: "peek/events.ndjson"})
    end

    test "a path stored under the shared folder is refused naming where it is now",
         %{root: root} do
      stored = Path.join(root, "out.ndjson")
      folder = Path.join(root, "orders")
      moved = Path.join(folder, "out.ndjson")

      assert {:error, {:file_path_outside_namespace, ^stored, ^folder, ^moved}} =
               prepare_in("orders", %{path: stored})

      refute File.exists?(stored)
      refute File.exists?(moved)
    end
  end

  # Nothing here reaches a network: a root is resolved without a request.
  describe "an S3 root" do
    setup do
      Minio.file_plugin_root("s3://events/flezha")
    end

    test "a relative path becomes a key under the namespace's prefix, and resolves to itself again" do
      assert {:ok, %{path: "s3://events/flezha/orders/out"}} = prepare_in_bucket(%{path: "out"})

      assert {:ok, %{path: "s3://events/flezha/orders/out/echo"} = opts} =
               prepare_in_bucket(%{path: "./out//echo"})

      assert {:ok, %{path: "s3://events/flezha/orders/out/echo"}} = prepare_in_bucket(opts)
    end

    test "a path that climbs out, another bucket and a filesystem path are refused naming the namespace's prefix" do
      for path <- [
            "../other/echo",
            "out/../../other",
            "",
            "s3://audit/flezha/echo",
            "s3://events/flezhax/echo",
            "/etc/passwd"
          ] do
        assert {:error, {:file_path_outside_root, ^path, "s3://events/flezha/orders"}} =
                 prepare_in_bucket(%{path: path}),
               inspect(path)
      end
    end

    test "another namespace's prefix is refused naming this namespace's" do
      path = "s3://events/flezha/orders/out/echo"

      assert {:error,
              {:file_path_outside_namespace, ^path, "s3://events/flezha/payments",
               "s3://events/flezha/payments/orders/out/echo"}} =
               ProviderPrepare.prepare(FilePlugin.Sink, %{
                 path: path,
                 commit: "rename",
                 source_namespace: "payments"
               })
    end

    # The folder walk refuses `s3://events/flezha` as a folder that does not
    # exist, so a prepare that succeeds is one that never looked for it.
    test "no folder is looked for or created" do
      assert {:ok, _opts} = prepare_in_bucket(%{path: "echo"})

      assert {:ok, _opts} =
               ProviderPrepare.prepare(FilePlugin.Source, %{path: "echo", channel_in: "events"})

      refute File.exists?("s3:")
    end

    test "an unset connection setting is refused naming its variable" do
      settings = Application.get_env(:flezha, :file_plugin_s3)

      for {key, variable} <- [
            endpoint: "FLEZHA_FILE_PLUGIN_S3_ENDPOINT",
            access_key_id: "FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID",
            secret_access_key_env: "FLEZHA_FILE_PLUGIN_S3_SECRET_ENV"
          ],
          {provider, opts} <- [
            {FilePlugin.Source, %{path: "echo", channel_in: "events"}},
            {FilePlugin.Sink, %{path: "echo", commit: "rename"}}
          ] do
        Application.put_env(:flezha, :file_plugin_s3, Keyword.delete(settings, key))

        assert {:error, {:file_root_setting_missing, "s3://events/flezha", ^variable}} =
                 ProviderPrepare.prepare(provider, opts)
      end
    end

    test "a root overlapping the durability tier's objects is refused naming both prefixes" do
      Minio.file_plugin_root("s3://events/flezha/files")
      endpoint = Application.get_env(:flezha, :file_plugin_s3)[:endpoint]
      tier = [endpoint: endpoint, bucket: "events", prefix: "flezha"]
      Application.put_env(:flezha, :durability_tier, backend: :s3, backend_opts: tier)

      assert {:error,
              {:file_root_overlaps_durability, "s3://events/flezha/files", "events", "flezha",
               "flezha/files"}} = prepare_in_bucket(%{path: "echo"})

      for apart <- [[prefix: "archive"], [bucket: "audit"], [endpoint: "http://elsewhere:9000"]] do
        backend_opts = Keyword.merge(tier, apart)
        Application.put_env(:flezha, :durability_tier, backend: :s3, backend_opts: backend_opts)
        assert {:ok, _opts} = prepare_in_bucket(%{path: "echo"}), inspect(apart)
      end
    end
  end

  defp prepare_in_bucket(opts), do: prepare_in("orders", Map.merge(%{commit: "rename"}, opts))

  defp prepare_in(namespace, opts),
    do: ProviderPrepare.prepare(FilePlugin.Sink, Map.put(opts, :source_namespace, namespace))

  # A plugin is prepared again from the options it already holds on a plugin
  # restart, so the resolved path must resolve to itself.
  test "the resolved path replaces the one written, and resolves to itself again",
       %{root: root} do
    File.mkdir_p!(Path.join(root, "orders/2026-09"))
    File.ln_s!("2026-09", Path.join(root, "orders/current"))

    assert {:ok, opts} = prepare_in("orders", %{path: "current/out.ndjson"})
    assert opts.path == Path.join(root, "orders/2026-09/out.ndjson")

    assert {:ok, again} = prepare_in("orders", %{path: opts.path})
    assert again.path == opts.path
  end
end

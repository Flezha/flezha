defmodule Flezha.Plugin.Installer.LoaderTest do
  @moduledoc """
  Tests for `Flezha.Plugin.Installer.Loader`. Each test uses a tmp dir for
  `plugin_root` and `recovery_data_dir` so Store rows and beam files stay
  isolated. Modules use unique names per test so the shared code server
  doesn't collide.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_loader_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    prev_data_dir = Application.get_env(:flezha, :recovery_data_dir)
    prev_pi = Application.get_env(:flezha, :plugin_installer)

    Application.put_env(:flezha, :recovery_data_dir, tmp)

    Application.put_env(
      :flezha,
      :plugin_installer,
      Keyword.merge(prev_pi || [], plugin_root: Path.join(tmp, "plugins"))
    )

    stop_if_running(Loader)
    stop_if_running(Store)

    on_exit(fn ->
      stop_if_running(Loader)
      stop_if_running(Store)

      if prev_data_dir do
        Application.put_env(:flezha, :recovery_data_dir, prev_data_dir)
      else
        Application.delete_env(:flezha, :recovery_data_dir)
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

  test "load_beams writes files, loads modules, records ownership", %{tmp: tmp} do
    {:ok, store_pid} = Store.start_link([])
    on_exit_stop(store_pid)
    {:ok, loader_pid} = Loader.start_link([])
    on_exit_stop(loader_pid)

    unique = System.unique_integer([:positive])
    mod = :"Elixir.FlzLoaderTest.Basic#{unique}"
    {:module, ^mod, bin, _} =
      defmodule_unique(unique, :basic, "def hello, do: :from_load")

    ebin = Path.join([tmp, "plugins", "installed", "basic", "1.0.0", "ebin"])
    :ok = Loader.load_beams("basic", "1.0.0", ebin, [{mod, bin}])

    assert File.exists?(Path.join(ebin, "#{mod}.beam"))
    assert Loader.modules_of("basic") == [mod]
    assert apply(mod, :hello, []) == :from_load
  end

  test "purge_modules drops modules and registry row", %{tmp: tmp} do
    {:ok, store_pid} = Store.start_link([])
    on_exit_stop(store_pid)
    {:ok, loader_pid} = Loader.start_link([])
    on_exit_stop(loader_pid)

    unique = System.unique_integer([:positive])
    mod = :"Elixir.FlzLoaderTest.Purge#{unique}"
    {:module, ^mod, bin, _} = defmodule_unique(unique, :purge, "def x, do: :ok")

    ebin = Path.join([tmp, "plugins", "installed", "purge", "1.0.0", "ebin"])
    :ok = Loader.load_beams("purge", "1.0.0", ebin, [{mod, bin}])
    assert function_exported?(mod, :x, 0) or :code.is_loaded(mod) != false

    :ok = Loader.purge_modules("purge")

    # After purge_modules, the module should not be loaded.
    refute :code.is_loaded(mod)
    assert Loader.modules_of("purge") == []
  end

  test "purge_modules is idempotent" do
    {:ok, store_pid} = Store.start_link([])
    on_exit_stop(store_pid)
    {:ok, loader_pid} = Loader.start_link([])
    on_exit_stop(loader_pid)

    assert :ok = Loader.purge_modules("never_installed")
    assert :ok = Loader.purge_modules("never_installed")
  end

  test "installed_ids reflects registry" do
    {:ok, store_pid} = Store.start_link([])
    on_exit_stop(store_pid)
    {:ok, loader_pid} = Loader.start_link([])
    on_exit_stop(loader_pid)

    assert MapSet.size(Loader.installed_ids()) == 0
  end

  test "module collision with already-loaded module rejected" do
    {:ok, store_pid} = Store.start_link([])
    on_exit_stop(store_pid)
    {:ok, loader_pid} = Loader.start_link([])
    on_exit_stop(loader_pid)

    # Try to install a plugin whose module is Enum (loaded from stdlib).
    unique = System.unique_integer([:positive])
    # We can't actually generate a beam *named* Enum, so instead pick a
    # module that is loaded from the release but construct a fake beam bin.
    # Use :lists instead — it's always loaded.
    fake_bin = :crypto.strong_rand_bytes(100)

    ebin =
      Path.join([System.tmp_dir!(), "flz_loader_collision_#{unique}", "ebin"])

    assert {:error, {:module_collision, :lists}} =
             Loader.load_beams("x", "1.0.0", ebin, [{:lists, fake_bin}])
  end

  test "hydrate_from_store reloads every row", %{tmp: tmp} do
    {:ok, store_pid} = Store.start_link([])
    on_exit_stop(store_pid)
    {:ok, loader_pid} = Loader.start_link([])
    on_exit_stop(loader_pid)

    unique = System.unique_integer([:positive])
    mod = :"Elixir.FlzLoaderTest.Hydrate#{unique}"
    {:module, ^mod, bin, _} = defmodule_unique(unique, :hydrate, "def h, do: :hydrated")

    ebin =
      Path.join([tmp, "plugins", "installed", "hydrate", "1.0.0", "ebin"])

    :ok = Loader.load_beams("hydrate", "1.0.0", ebin, [{mod, bin}])

    # Write a store row for hydration.
    :ok =
      Store.put("hydrate", %{
        version: "1.0.0",
        status: :enabled,
        path: Path.join([tmp, "plugins", "installed", "hydrate", "1.0.0"]),
        manifest_digest: String.duplicate("a", 64),
        modules: [mod],
        installed_at: System.monotonic_time(),
        source_checksums: %{}
      })

    # Now purge and re-hydrate.
    :ok = Loader.purge_modules("hydrate")
    assert :ok = Loader.hydrate_from_store()
    assert apply(mod, :h, []) == :hydrated
    assert Loader.modules_of("hydrate") == [mod]
  end

  test "hydrate_from_store marks row load_failed when ebin missing", %{tmp: tmp} do
    {:ok, store_pid} = Store.start_link([])
    on_exit_stop(store_pid)

    # Write a store row whose ebin does not exist.
    :ok =
      Store.put("orphan", %{
        version: "1.0.0",
        status: :enabled,
        path: Path.join([tmp, "plugins", "installed", "orphan", "1.0.0"]),
        manifest_digest: String.duplicate("a", 64),
        modules: [:"Elixir.FlzLoaderTest.Orphan#{System.unique_integer([:positive])}"],
        installed_at: System.monotonic_time(),
        source_checksums: %{}
      })

    handler = "load-failed-#{System.unique_integer([:positive])}"
    me = self()

    :telemetry.attach(
      handler,
      [:flezha, :plugin_installer, :load_failed],
      fn _n, _m, meta, _ -> send(me, {:telemetry, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, loader_pid} = Loader.start_link([])
    on_exit_stop(loader_pid)

    assert_receive {:telemetry, %{plugin_id: "orphan"}}, 500
    assert Store.get("orphan").status == :load_failed
  end

  ## Helpers

  # Build a module at runtime and capture its beam binary by compiling an
  # in-memory source string, not loading it into the VM.
  defp defmodule_unique(unique, label, body) do
    mod_name = :"Elixir.FlzLoaderTest.#{String.capitalize(to_string(label))}#{unique}"

    src = """
    defmodule FlzLoaderTest.#{String.capitalize(to_string(label))}#{unique} do
      #{body}
    end
    """

    [{mod, bin} | _] = Code.compile_string(src)
    # Purge whatever compile_string loaded so tests start from "not loaded".
    _ = :code.purge(mod)
    _ = :code.delete(mod)
    {:module, mod_name, bin, nil}
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

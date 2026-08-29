defmodule Flezha.Plugin.Installer.StoreTest do
  @moduledoc """
  Round-trip tests for `Flezha.Plugin.Installer.Store`. Exercises every
  mutator plus corrupt-file recovery. Runs synchronously because it uses
  the singleton DETS table.
  """

  # async: false: writes Application env; a shared on-disk root; the primary Logger.

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog, only: [capture_log: 1]

  alias Flezha.Plugin.Installer.Store

  setup do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "flz_installer_store_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :plugin_installer_data_dir)
    Application.put_env(:flezha, :plugin_installer_data_dir, tmp)

    stop_if_running(Store)

    on_exit(fn ->
      stop_if_running(Store)

      if prev_dir do
        Application.put_env(:flezha, :plugin_installer_data_dir, prev_dir)
      else
        Application.delete_env(:flezha, :plugin_installer_data_dir)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "opens its own DETS file", %{tmp: tmp} do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    assert Store.count() == 0
    assert Store.path() == Path.join(tmp, "installed_plugins.dets")
  end

  test "put / get / delete round-trip" do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    row = sample_row("1.0.0", :disabled)
    :ok = Store.put("kafka_ingest", row)

    assert Store.count() == 1
    assert Store.get("kafka_ingest") == row
    assert [{"kafka_ingest", ^row}] = Store.all()

    :ok = Store.delete("kafka_ingest")
    assert Store.count() == 0
    assert Store.get("kafka_ingest") == nil
  end

  test "put overwrites" do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    :ok = Store.put("x", sample_row("1.0.0", :disabled))
    :ok = Store.put("x", sample_row("1.0.1", :enabled))

    row = Store.get("x")
    assert row.version == "1.0.1"
    assert row.status == :enabled
  end

  test "update_status only touches the status field" do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    row = sample_row("1.0.0", :disabled)
    :ok = Store.put("x", row)

    :ok = Store.update_status("x", :enabled)

    updated = Store.get("x")
    assert updated.status == :enabled
    assert updated.version == row.version
    assert updated.path == row.path
    assert updated.modules == row.modules
  end

  test "update_status returns :not_found for absent row" do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    assert {:error, :not_found} = Store.update_status("missing", :enabled)
  end

  test "delete is idempotent" do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    :ok = Store.put("x", sample_row("1.0.0", :disabled))
    :ok = Store.delete("x")
    :ok = Store.delete("x")
    :ok = Store.delete("never_existed")
    assert Store.count() == 0
  end

  test "rows survive reopen", %{tmp: _tmp} do
    {:ok, pid} = Store.start_link([])
    :ok = Store.put("x", sample_row("1.0.0", :enabled))

    GenServer.stop(pid, :normal)

    {:ok, pid2} = Store.start_link([])
    on_exit_stop(pid2)

    assert Store.get("x").status == :enabled
  end

  test "emits store_opened telemetry with row count", %{tmp: tmp} do
    # Pre-populate so the second open sees a non-zero count.
    {:ok, pid} = Store.start_link([])
    :ok = Store.put("a", sample_row("1.0.0", :disabled))
    :ok = Store.put("b", sample_row("2.0.0", :enabled))
    GenServer.stop(pid, :normal)

    handler = "installer-store-opened-#{System.unique_integer([:positive])}"
    me = self()

    :telemetry.attach(
      handler,
      [:flezha, :plugin_installer, :store_opened],
      fn _n, m, meta, _ -> send(me, {:telemetry, m, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, pid2} = Store.start_link([])
    on_exit_stop(pid2)

    assert_receive {:telemetry, %{rows: 2}, %{path: p}}, 500
    assert p == Path.join(tmp, "installed_plugins.dets")
  end

  test "corrupt DETS file rotates and starts fresh", %{tmp: tmp} do
    file = Path.join(tmp, "installed_plugins.dets")
    File.write!(file, "not a dets file")

    log =
      capture_log(fn ->
        {:ok, pid} = Store.start_link([])
        on_exit_stop(pid)

        # Store is alive and empty.
        assert Store.count() == 0
      end)

    # The Store logs the rotation; we capture it so the intentional
    # corruption injection doesn't pollute the suite output.
    assert log =~ "corrupt DETS"

    rotated =
      File.ls!(tmp) |> Enum.filter(&String.starts_with?(&1, "installed_plugins.dets.corrupt-"))

    assert length(rotated) == 1
  end

  defp sample_row(version, status) do
    %{
      version: version,
      status: status,
      path: "/tmp/installed/x/#{version}",
      manifest_digest: String.duplicate("a", 64),
      modules: [MyOrg.X],
      installed_at: System.monotonic_time(),
      source_checksums: %{
        "manifest.exs" => String.duplicate("b", 64),
        "src/x.ex" => String.duplicate("c", 64)
      },
      description: "test",
      author: "tester",
      provides: [],
      dependencies: []
    }
  end

  defp stop_if_running(mod) do
    try do
      case Process.whereis(mod) do
        nil -> :ok
        pid -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
      end
    catch
      :exit, _ -> :ok
    end
  end

  defp on_exit_stop(pid) do
    on_exit(fn ->
      try do
        FlezhaTest.Helper.NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)
  end
end

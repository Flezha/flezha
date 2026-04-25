defmodule Flezha.Recovery.StoreTest do
  @moduledoc """
  Exercises the DETS store in isolation using a `@tmp_dir` fixture.

  Because the store registers itself under `Flezha.Recovery.Store` and the
  main application also starts one in test env, each test here stops the
  application-started Store (if any) and starts a fresh, isolated one
  pointing at the tmp dir.
  """

  use ExUnit.Case, async: false

  alias Flezha.Recovery.Store
  alias Flezha.Processor.Builder

  @table :flz_recovery

  setup do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "flz_recovery_store_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    Application.put_env(:flezha, :recovery_data_dir, tmp)

    # If the app-level store is running, stop it so we can own the name.
    stop_if_running()

    on_exit(fn ->
      stop_if_running()

      if prev_dir do
        Application.put_env(:flezha, :recovery_data_dir, prev_dir)
      else
        Application.delete_env(:flezha, :recovery_data_dir)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "opens DETS file, writes and reads rows, survives reopen", %{tmp: tmp} do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    # Start empty.
    assert Store.count() == 0
    assert Store.all() == []
    assert Store.path() == Path.join(tmp, "recovery.dets")

    assert :ok = Store.put("cid-1", "ns", %Builder{id: "cid-1", factory_id: "f1"})
    assert :ok = Store.put("cid-2", "ns2", %Builder{id: "cid-2", factory_id: "f2"})

    assert Store.count() == 2

    rows = Store.all() |> Enum.sort_by(&elem(&1, 0))
    assert [{"cid-1", r1}, {"cid-2", r2}] = rows
    assert r1.namespace == "ns"
    assert r1.processor.factory_id == "f1"
    assert r1.stored_node == node()
    assert is_integer(r1.inserted_at)
    assert r2.namespace == "ns2"

    # Close and reopen — contents must survive.
    GenServer.stop(pid, :normal)
    assert File.exists?(Path.join(tmp, "recovery.dets"))

    {:ok, pid2} = Store.start_link([])
    on_exit_stop(pid2)

    assert Store.count() == 2
    reopened = Store.all() |> Enum.sort_by(&elem(&1, 0)) |> Enum.map(&elem(&1, 0))
    assert reopened == ["cid-1", "cid-2"]
  end

  test "delete removes a row and is idempotent" do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    assert :ok = Store.put("a", "ns", %Builder{id: "a", factory_id: "f"})
    assert Store.count() == 1
    assert :ok = Store.delete("a")
    assert Store.count() == 0
    # second delete is a no-op
    assert :ok = Store.delete("a")
    assert :ok = Store.delete("missing")
  end

  test "put overwrites existing row for same child_id" do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    :ok = Store.put("x", "ns1", %Builder{id: "x", factory_id: "f-old"})
    :ok = Store.put("x", "ns2", %Builder{id: "x", factory_id: "f-new"})

    assert Store.count() == 1
    [{"x", row}] = Store.all()
    assert row.namespace == "ns2"
    assert row.processor.factory_id == "f-new"
  end

  test "init emits :store_opened telemetry with path and row count", %{tmp: tmp} do
    handler_id = "store-opened-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      handler_id,
      [:flezha, :recovery, :store_opened],
      fn _name, measurements, metadata, _cfg ->
        send(test_pid, {:telemetry, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    assert_receive {:telemetry, %{rows: 0}, %{path: path}}, 500
    assert path == Path.join(tmp, "recovery.dets")
  end

  test "terminate closes DETS cleanly so file can be reopened", %{tmp: tmp} do
    {:ok, pid} = Store.start_link([])
    :ok = Store.put("k", "ns", %Builder{id: "k", factory_id: "f"})
    path = Path.join(tmp, "recovery.dets")

    GenServer.stop(pid, :normal)

    # Opening via :dets again should succeed (file not locked).
    assert {:ok, @table} =
             :dets.open_file(@table, [
               {:type, :set},
               {:file, String.to_charlist(path)}
             ])

    assert :dets.info(@table, :size) == 1
    :dets.close(@table)
  end

  defp stop_if_running do
    case Process.whereis(Store) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal)
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

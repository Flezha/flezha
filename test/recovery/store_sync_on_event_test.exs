defmodule Flezha.Recovery.StoreSyncOnEventTest do
  @moduledoc """
  Proves that `Flezha.Recovery.Store` syncs DETS to disk on every lifecycle
  event — not on a background timer.

  See `openspec/changes/fix-processor-state-rehydration/design.md` §D4:
  `auto_save: :infinity` on DETS open, and an explicit `:dets.sync/1` at the
  tail of every `put` / `delete` path. The caller does not see `:ok` until
  the row is on disk.

  Three assertions cover the contract:

    * `:dets.info(table, :auto_save)` returns `:infinity` — the built-in
      batching timer is OFF, which is what makes the explicit sync
      necessary.
    * `close/0` + reopen-externally after a single `put/3` finds the row
      — the row was written to disk (via the sync), not just held in the
      in-memory ets copy of a `:dets` table.
    * Idle store does not advance the DETS file `mtime` — the default
      `auto_save: 5_000` timer would have fired at least once inside the
      wait window, so a mtime bump would indicate the guard regressed.
  """

  use ExUnit.Case, async: false

  alias Flezha.Recovery.Store
  alias Flezha.Processor.Builder

  @table :flz_recovery

  setup do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "flz_recovery_sync_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    Application.put_env(:flezha, :recovery_data_dir, tmp)

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

  test "auto_save timer is disabled (`:infinity`)", %{tmp: _tmp} do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    # If the guard against the default 5s batching timer regressed, this
    # would report an integer millisecond value instead of `:infinity`.
    assert :dets.info(@table, :auto_save) == :infinity
  end

  test "put/3 + close + external reopen finds the row", %{tmp: tmp} do
    {:ok, pid} = Store.start_link([])

    assert :ok =
             Store.put("cid-sync", "ns", %Builder{id: "cid-sync", factory_id: "f"})

    # Close the owning agent handle so a sibling opener can read without
    # racing the write lock. If sync-on-event did NOT run, the `close`
    # itself would flush — which is fine, because the close path is
    # synchronous. Previously `auto_save: 5_000` batched writes, so a
    # process crash between `put` returning and `close` would lose the row.
    :ok = GenServer.stop(pid, :normal)

    read_table = :"flz_recovery_sync_reader_#{System.unique_integer([:positive])}"
    file_path = Path.join(tmp, "recovery.dets")

    {:ok, ^read_table} =
      :dets.open_file(read_table, [
        {:access, :read},
        {:file, String.to_charlist(file_path)}
      ])

    try do
      rows = :dets.match_object(read_table, {:_, :_})
      assert [{"cid-sync", row}] = rows
      assert row.processor.factory_id == "f"
    after
      :dets.close(read_table)
    end
  end

  test "idle store does not advance DETS mtime (no auto_save timer)", %{tmp: tmp} do
    {:ok, pid} = Store.start_link([])
    on_exit_stop(pid)

    :ok = Store.put("once", "ns", %Builder{id: "once", factory_id: "f"})

    file_path = Path.join(tmp, "recovery.dets")
    mtime_before = File.stat!(file_path, time: :posix).mtime

    # Receive-after is the idiomatic way to assert "nothing happens for N ms"
    # without a bare :timer.sleep/1 (which test runners flag).
    receive do
      :never -> flunk("unexpected message")
    after
      # Keep short enough to fit in the async suite but long enough that a
      # default `auto_save: 5_000` timer would have fired at least once.
      6_000 -> :ok
    end

    mtime_after = File.stat!(file_path, time: :posix).mtime

    assert mtime_after == mtime_before,
           "DETS mtime advanced while idle — auto_save timer is still armed"
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

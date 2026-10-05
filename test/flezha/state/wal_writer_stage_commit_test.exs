defmodule Flezha.State.WalWriterStageCommitTest do
  @moduledoc """
  A durable steps relay Cell's writer: its effect records' outputs are released
  at the stage marker that names them, not at their fsync; a record no marker
  will name is dropped rather than released; the commit worker can wait on a
  record's durability; and a writer that boots, or an instance that binds,
  re-derives what is committed from the journal.
  """
  use FlezhaTest.OutboxCase, async: false

  alias Flezha.State.TaskJournal

  setup %{rc: rc} do
    :ok = ClassRegistry.put(rc, :steps)
    :ok
  end

  defp stage_writer(rc, cell_id) do
    pid = armed_writer(rc, cell_id)
    assert :sys.get_state(pid).commit_unit == :stage
    pid
  end

  defp status(pid), do: Map.take(WalWriter.outbox_status(pid), [:pending, :held, :awaiting])

  test "a stream Cell's writer commits at its record", %{cell_id: {_rc, sh, pk}} do
    stream_rc = "wo_flow_" <> TestHelpers.uniq()
    pid = armed_writer(stream_rc, {stream_rc, sh, pk})
    assert :sys.get_state(pid).commit_unit == :record
  end

  test "a record's output waits for the stage marker, then leaves with its identity", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = stage_writer(rc, cell_id)

    append_effect(pid, 0, [output("k", "a-out", [{"trace", "t1"}])], nil)
    refute_dispatched()
    assert status(pid) == %{pending: 0, held: 1, awaiting: 0}

    :ok = WalWriter.commit_records(pid, 0, 0)

    assert {"a-out", md, _} = next_output()
    assert md["flz.cell"] == "#{rc}/task/k1"
    assert md["flz.seq"] == "0.0"
    assert md["flz.epoch"] == "0"
    assert md["flz.key"] == "k"
    assert md["trace"] == "t1"
    assert md["channel"] == "out"

    settle(md)
    :ok = WalWriter.fsync(pid)
    assert Outbox.read(cell_id).outbox_flushed_seq == 0
  end

  test "a held record below a committed range never committed and is dropped", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = stage_writer(rc, cell_id)

    # An attempt that failed, then the attempt that committed.
    append_effect(pid, 0, [output("k", "failed")], nil)
    append_effect(pid, 1, [output("k", "kept")], nil)
    :ok = WalWriter.commit_records(pid, 1, 1)

    assert {"kept", md, _} = next_output()
    refute_dispatched()
    assert status(pid) == %{pending: 0, held: 0, awaiting: 1}

    # The cursor passes the dropped record with the one that was confirmed.
    settle(md)
    :ok = WalWriter.fsync(pid)
    assert Outbox.read(cell_id).outbox_flushed_seq == 1
  end

  test "a wait on a record answers once it is durable, and at once when it already is", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = stage_writer(rc, cell_id)

    :ok = :sys.suspend(FsyncBatcher)
    reqids = WalWriter.await_durable_async(pid, 0, :gen_server.reqids_new(), :first)
    stage_effect(pid, 0, [output("k", "v")], nil)
    assert :timeout = :gen_server.wait_response(reqids, 100, false)
    :ok = :sys.resume(FsyncBatcher)

    :ok = WalWriter.fsync(pid)
    assert {{:reply, {:ok, 0}}, :first, _} = :gen_server.receive_response(reqids, 1_000, true)

    reqids = WalWriter.await_durable_async(pid, 0, :gen_server.reqids_new(), :again)
    assert {{:reply, {:ok, 0}}, :again, _} = :gen_server.receive_response(reqids, 1_000, true)
  end

  test "a prefix cut's rewrite answers a wait on a record it made durable", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = stage_writer(rc, cell_id)

    :ok = :sys.suspend(FsyncBatcher)
    on_exit(fn -> if Process.whereis(FsyncBatcher), do: :sys.resume(FsyncBatcher) end)
    stage_effect(pid, 0, [output("k", "v0")], nil)
    stage_effect(pid, 1, [output("k", "v1")], nil)
    reqids = WalWriter.await_durable_async(pid, 1, :gen_server.reqids_new(), :cut)
    assert :timeout = :gen_server.wait_response(reqids, 100, false)

    :ok = WalWriter.truncate_up_to(pid, 0)
    assert {{:reply, {:ok, 1}}, :cut, _} = :gen_server.receive_response(reqids, 1_000, true)
  end

  test "a writer whose file refused a record refuses the wait", %{rc: rc, cell_id: cell_id} do
    pid = stage_writer(rc, cell_id)
    refusal = {:crc_mismatch, "test.wal", 0}
    :sys.replace_state(pid, &%{&1 | refusal: refusal})

    reqids = WalWriter.await_durable_async(pid, 0, :gen_server.reqids_new(), :refused)

    assert {{:reply, {:error, ^refusal}}, :refused, _} =
             :gen_server.receive_response(reqids, 1_000, true)
  end

  test "a booting writer releases what the journal names and drops what it passed over", %{
    rc: rc,
    cell_id: {_, sh, pk} = cell_id
  } do
    pid = stage_writer(rc, cell_id)

    for {seq, payload} <- [{0, "interrupted"}, {1, "committed"}, {2, "in flight"}],
        do: append_effect(pid, seq, [output("k", payload)], nil)

    refute_dispatched()
    close_writer(pid, cell_id)

    record = %{cell_id: Wire.encode_cell_id(rc, sh, pk), first_seq: 1, seq: 1, channels: ["out"]}
    :ok = TaskJournal.append_task_committed(rc, 0, 0, [record], [])

    pid = start_writer(cell_id)
    assert {"committed", _md, _} = next_output()
    refute_dispatched()
    # Above the last range: a stage that may still commit, so it stays held.
    assert status(pid) == %{pending: 0, held: 1, awaiting: 1}

    # The bind knows no such stage is running any more.
    :ok = WalWriter.seed_commits(pid, TaskJournal.committed_records(rc))
    assert status(pid) == %{pending: 0, held: 0, awaiting: 1}
    refute_dispatched()
  end
end

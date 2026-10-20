defmodule Flezha.State.LineageTest do
  @moduledoc """
  A Cell's dedup and lineage files: the dedup file keeps the layout an older
  build reads, and the read of the two serves the higher epoch whatever order
  they were left in on disk.
  """
  # async: false: a shared on-disk root and the writer supervisor.
  use FlezhaTest.StateCase, async: false

  import ExUnit.CaptureLog

  alias Flezha.State.{Lineage, Paths, Supervisor, TestHelpers, WalWriter}

  setup do
    %{cell_id: TestHelpers.sample_cell_id("lineage_" <> uniq())}
  end

  defp write_records(cell_id, seqs) do
    {:ok, srv} = Supervisor.find_or_start(cell_id)
    header = TestHelpers.stub_header()
    for n <- seqs, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
    TestHelpers.close_and_await(srv, cell_id)
  end

  defp stored(cell_id), do: Lineage.read(cell_id).record

  # fails when: the dedup file's layout changes, so a build from before the
  # lineage file reads it wrong after a rollback (the pattern is that build's).
  test "a Cell written through this build keeps the dedup file an older build reads", ctx do
    write_records(ctx.cell_id, 0..3)

    assert <<seq::64-big, flushed::64-big-signed, epoch::64-big, folded::64-big-signed>> =
             File.read!(Paths.outbox_dedup_path(ctx.cell_id))

    assert {seq, flushed, epoch, folded} == {3, -1, 0, -1}
  end

  # fails when: the epoch is read from the lineage file alone, so a dedup file
  # a rollback build advanced past it is ignored and a void cut is served.
  test "a dedup file ahead of the lineage record wins, and the record catches up", ctx do
    write_records(ctx.cell_id, 0..3)
    record = %{stored(ctx.cell_id) | epoch: 6, folded_offset: 3, cut: {:above, 1}}
    :ok = Lineage.put(ctx.cell_id, record)
    progress = %{last_persisted_seq: 3, outbox_flushed_seq: -1, epoch: 7, folded_offset: 5}
    :ok = Lineage.put_dedup(ctx.cell_id, progress)

    assert %{progress: %{epoch: 7, folded_offset: 5}, cut: nil} = Lineage.read(ctx.cell_id)

    write_records(ctx.cell_id, [4])

    assert %{epoch: 7, folded_offset: 5, cut: nil} = stored(ctx.cell_id)
    assert length(TestHelpers.record_offsets(Paths.wal_path(ctx.cell_id))) == 5
  end

  test "a pending cut counts only while the dedup file is behind the lineage record", ctx do
    write_records(ctx.cell_id, 0..3)
    :ok = Lineage.put(ctx.cell_id, %{stored(ctx.cell_id) | epoch: 1, folded_offset: 2, cut: :all})

    assert %{progress: %{epoch: 1, folded_offset: 2}, cut: :all} = Lineage.read(ctx.cell_id)
  end

  # fails when: a Cell from before the lineage file never gets one, or reads
  # anything but its dedup file's epoch and folded offset until then.
  test "a Cell with no lineage file serves its dedup file and gets one at its next write",
       ctx do
    write_records(ctx.cell_id, 0..3)
    File.rm!(Paths.lineage_path(ctx.cell_id))
    progress = %{last_persisted_seq: 3, outbox_flushed_seq: -1, epoch: 4, folded_offset: 8}
    :ok = Lineage.put_dedup(ctx.cell_id, progress)

    assert %{record: %{epoch: nil}, progress: %{epoch: 4, folded_offset: 8}} =
             Lineage.read(ctx.cell_id)

    write_records(ctx.cell_id, [4])

    assert %{epoch: 4, folded_offset: 8, cut: nil} = stored(ctx.cell_id)
  end

  test "an unreadable lineage file reads as none, with a warning", ctx do
    write_records(ctx.cell_id, 0..3)

    for bytes <- ["FLZL" <> <<1, 0::32>> <> "bad crc", "not a lineage file"] do
      File.write!(Paths.lineage_path(ctx.cell_id), bytes)

      log =
        capture_log(fn ->
          assert %{record: %{epoch: nil}, progress: %{epoch: 0}} = Lineage.read(ctx.cell_id)
        end)

      assert log =~ "unreadable"
    end
  end

  # fails when: the dedup file is written though the lineage install meant to
  # precede it failed, so the dedup file holds a fact the lineage file lacks.
  test "a failed lineage install writes no dedup file", ctx do
    write_records(ctx.cell_id, 0..3)
    dedup = File.read!(Paths.outbox_dedup_path(ctx.cell_id))
    File.rm!(Paths.lineage_path(ctx.cell_id))
    File.mkdir_p!(Paths.lineage_path(ctx.cell_id))

    write_records(ctx.cell_id, [4])

    assert File.read!(Paths.outbox_dedup_path(ctx.cell_id)) == dedup
  end
end

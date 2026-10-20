defmodule Flezha.State.LineageTest do
  @moduledoc """
  A Cell's dedup and lineage files: the dedup file keeps the layout an older
  build reads, and the read of the two serves the higher epoch whatever order
  they were left in on disk.
  """
  # async: false: a shared on-disk root and the writer supervisor.
  use FlezhaTest.StateCase, async: false

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.State.{InputLog, Lineage, Paths, Snapshot, Supervisor, TestHelpers, WalWriter}
  alias Flezha.State.WalWriter.Epoch
  alias FlezhaTest.OutboxCase

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

  defp append(pid, seq), do: WalWriter.append(pid, seq, TestHelpers.stub_header(), "x#{seq}")

  # A Cell of its own relay class (no Overwatch, no other reporter), its
  # records 0..`last` each consuming input `n` and owing one output.
  defp effect_cell(last) do
    cell_id = {"lineage_" <> uniq(), "task", "k1"}
    {:ok, srv} = Supervisor.find_or_start(cell_id)

    for n <- 0..last do
      body = OutboxCase.effect_body([OutboxCase.output("k", "v#{n}")], n)
      :ok = WalWriter.append(srv, n, OutboxCase.effect_header(), body)
    end

    TestHelpers.close_and_await(srv, cell_id)
    cell_id
  end

  defp snapshot(cell_id, seq),
    do: {:ok, _} = Snapshot.write_snapshot(cell_id, seq, <<0::unit(8)-size(16)>>, <<seq>>)

  defp records(cell_id), do: length(TestHelpers.record_offsets(Paths.wal_path(cell_id)))

  defp boot(cell_id) do
    {:ok, pid} = Supervisor.find_or_start(cell_id)
    {pid, WalWriter.outbox_status(pid)}
  end

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

  # fails when: a boot leaves a realign its lineage write committed half done,
  # so the discarded records' outputs are re-driven under the new epoch.
  test "a realign interrupted after its lineage write is completed at boot, re-driving nothing" do
    for empty_wal? <- [false, true] do
      cell_id = effect_cell(7)
      for seq <- [2, 5], do: snapshot(cell_id, seq)
      if empty_wal?, do: File.write!(Paths.wal_path(cell_id), "")
      :ok = Lineage.put(cell_id, %{stored(cell_id) | epoch: 5, folded_offset: -1, cut: :all})
      :ok = Lineage.put_dedup(cell_id, %{Lineage.dedup(cell_id) | epoch: 4})

      {pid, status} = boot(cell_id)

      assert %{pending: 0, held: 0, epoch: 5, outbox_flushed_seq: -1} = status
      assert WalWriter.last_persisted_seq(pid) == -1
      assert records(cell_id) == 0
      assert Paths.snapshot_paths(cell_id) == []
      assert %{epoch: 5, folded_offset: -1} = Lineage.dedup(cell_id)
      assert %{epoch: 5, cut: nil} = stored(cell_id)
      TestHelpers.close_and_await(pid, cell_id)
    end
  end

  # fails when: a boot leaves a rewind its lineage write committed half done:
  # the records above the boundary survive and the watermark stays high.
  test "a rewind interrupted after its lineage write is completed at boot, and its re-run is a no-op" do
    cell_id = effect_cell(7)
    rc = elem(cell_id, 0)
    for seq <- [2, 5], do: snapshot(cell_id, seq)
    :ok = Lineage.put(cell_id, %{stored(cell_id) | epoch: 6, folded_offset: -1, cut: {:above, 3}})

    {pid, status} = boot(cell_id)
    on_disk = fn -> {File.read!(Paths.wal_path(cell_id)), Paths.snapshot_paths(cell_id)} end
    after_boot = {on_disk.(), Lineage.read(cell_id)}

    assert %{epoch: 6, last_persisted_seq: 3} = status
    assert records(cell_id) == 4
    assert [{2, _}] = Paths.snapshot_paths(cell_id)
    assert InputLog.watermark(rc) == 3
    assert %{epoch: 6} = Lineage.dedup(cell_id)
    assert %{epoch: 6, cut: nil} = stored(cell_id)

    :ok = WalWriter.rewind_to(pid, 3, 6)

    assert {on_disk.(), Lineage.read(cell_id)} == after_boot
    assert InputLog.watermark(rc) == 3
    TestHelpers.close_and_await(pid, cell_id)
  end

  # fails when: a folded offset that landed before its cut moves the floor,
  # which is a maximum over what the records still on disk consumed.
  test "a prefix cut's folded offset over records still in the WAL leaves the floor as it was" do
    cell_id = effect_cell(7)
    floor = Epoch.floor(cell_id, :latest)

    :ok = Lineage.put(cell_id, %{stored(cell_id) | folded_offset: 3})

    assert floor == 7
    assert Epoch.floor(cell_id, :latest) == floor
  end

  # fails when: the dedup file a cut left behind narrows the re-drive, which may
  # only widen when it lags the WAL.
  test "a cut WAL with the dedup file from before the cut re-drives only above its cursor" do
    cell_id = effect_cell(7)
    :ok = Lineage.put_dedup(cell_id, %{Lineage.dedup(cell_id) | outbox_flushed_seq: 5})
    old_dedup = File.read!(Paths.outbox_dedup_path(cell_id))
    {pid, _status} = boot(cell_id)
    :ok = WalWriter.truncate_up_to(pid, 3)
    TestHelpers.close_and_await(pid, cell_id)
    File.write!(Paths.outbox_dedup_path(cell_id), old_dedup)

    {pid, status} = boot(cell_id)

    assert %{pending: 2, outbox_flushed_seq: 5} = status
    assert records(cell_id) == 4
    assert %{folded_offset: 3} = stored(cell_id)
    TestHelpers.close_and_await(pid, cell_id)
  end

  describe "the mint" do
    # fails when: the boot no longer marks a Cell without a lineage id for a mint.
    test "a new Cell mints its lineage id at its first record, not at its writer's open", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.cell_id)
      assert WalWriter.last_persisted_seq(pid) == -1
      refute File.exists?(Paths.lineage_path(ctx.cell_id))

      :ok = append(pid, 0)

      assert %{id: <<_::128>>, trail: [], landed: false} = stored(ctx.cell_id)
      TestHelpers.close_and_await(pid, ctx.cell_id)
    end

    test "a landed Cell mints once, at its first record, naming the landed id and its fork",
         ctx do
      write_records(ctx.cell_id, 0..3)
      %{id: landed_id} = landed = stored(ctx.cell_id)
      :ok = Lineage.put(ctx.cell_id, %{landed | landed: true})
      {:ok, pid} = Supervisor.find_or_start(ctx.cell_id)

      :ok = append(pid, 4)
      assert %{id: id, trail: [{^landed_id, 3}], landed: false} = stored(ctx.cell_id)
      refute id == landed_id

      :ok = WalWriter.fsync(pid)
      :ok = append(pid, 5)
      assert %{id: ^id, trail: [{^landed_id, 3}]} = stored(ctx.cell_id)
      TestHelpers.close_and_await(pid, ctx.cell_id)
    end

    test "a writer restarted on the same node mints nothing", ctx do
      write_records(ctx.cell_id, 0..3)
      %{id: id, trail: trail} = stored(ctx.cell_id)

      write_records(ctx.cell_id, [4])

      assert %{id: ^id, trail: ^trail} = stored(ctx.cell_id)
    end

    # E2 row "mint": the new id installed, its record not appended.
    test "a boot after a mint whose record was lost keeps the minted id for the resent record",
         ctx do
      write_records(ctx.cell_id, 0..3)
      minted = Lineage.mint(stored(ctx.cell_id), 3)
      :ok = Lineage.put(ctx.cell_id, minted)

      write_records(ctx.cell_id, [4])

      assert %{id: id, trail: trail} = stored(ctx.cell_id)
      assert {id, trail} == {minted.id, minted.trail}
    end

    # E2 row "mint fails": the append fails with no record written, and the
    # restarted writer mints at the resent record.
    test "a failed mint fails the append and the restarted writer mints again", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.cell_id)
      assert WalWriter.last_persisted_seq(pid) == -1
      File.mkdir_p!(Paths.lineage_path(ctx.cell_id))
      ref = Process.monitor(pid)

      log =
        capture_log(fn ->
          assert {:error, _} = append(pid, 0)
          assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
        end)

      assert log =~ "MatchError"
      assert records(ctx.cell_id) == 0

      File.rmdir!(Paths.lineage_path(ctx.cell_id))

      assert wait_until(
               fn -> match?({:ok, p} when p != pid, Supervisor.lookup(ctx.cell_id)) end,
               2_000
             )

      {:ok, restarted} = Supervisor.lookup(ctx.cell_id)
      :ok = append(restarted, 0)

      assert %{id: <<_::128>>} = stored(ctx.cell_id)
      assert records(ctx.cell_id) == 1
      TestHelpers.close_and_await(restarted, ctx.cell_id)
    end
  end
end

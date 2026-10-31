defmodule Flezha.State.WalWriterProgressTest do
  @moduledoc """
  The Cell's durable cursors as one value, and the ordering they hold.

  The invariant was written down twice and enforced nowhere
  (`fix-outbox-watermark-overwrite`). It lives at the value now — one accessor,
  one predicate — and this is what enforces it: the cursors are read through
  `progress/1` after every step a real writer takes, never field by field.
  """
  use FlezhaTest.OutboxCase, async: false

  import ExUnit.CaptureLog

  alias Flezha.State.WalWriter.Settlement
  alias Flezha.State.WalWriter.State
  alias FlezhaTest.Helper.NodeTestHelper

  defp progress(pid), do: WalWriter.outbox_status(pid)

  defp assert_ordered(pid, step) do
    cursors = progress(pid)
    assert State.ordered?(cursors), "#{step}: #{inspect(cursors)} is out of order"
    cursors
  end

  defp assert_advanced(later, earlier, step) do
    for key <- [:last_persisted_seq, :outbox_durable_seq, :outbox_flushed_seq] do
      assert Map.fetch!(later, key) >= Map.fetch!(earlier, key),
             "#{step}: #{key} moved backwards without an epoch bump"
    end

    assert later.epoch == earlier.epoch, "#{step}: the epoch moved with no bump"
    later
  end

  describe "the ordering the cursors assume" do
    test "holds at every step a real writer takes", %{rc: rc, cell_id: cell_id} do
      pid = armed_writer(rc, cell_id)
      boot = assert_ordered(pid, "at boot")

      :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("out", "p1")], 0))
      appended = assert_ordered(pid, "after append") |> assert_advanced(boot, "after append")

      :ok = WalWriter.fsync(pid)
      synced = assert_ordered(pid, "after fsync") |> assert_advanced(appended, "after fsync")

      # The record is durable but its output is not confirmed, so the flush
      # cursor is still behind the durable one. That gap is the invariant.
      assert synced.outbox_flushed_seq < synced.outbox_durable_seq
      assert synced.outbox_durable_seq == synced.last_persisted_seq

      {_payload, metadata, _} = next_output()
      settle(metadata)

      confirmed =
        wait_for_flush(pid, synced.outbox_flushed_seq)
        |> assert_advanced(synced, "after confirmation")

      assert State.ordered?(confirmed)
      assert confirmed.outbox_flushed_seq == confirmed.outbox_durable_seq
    end

    test "an out-of-order record is refused rather than persisted", %{
      rc: rc,
      cell_id: cell_id
    } do
      # The predicate is not decoration: the writer's own dedup write asks it
      # and keeps the older consistent record when the answer is no. A flush
      # cursor persisted ahead of what is durable would drop the outputs behind
      # it for good, so the safe direction is to leave the file lagging.
      pid = armed_writer(rc, cell_id)
      :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("out", "p1")], 0))
      :ok = WalWriter.fsync(pid)

      before = Lineage.progress(cell_id)
      writer = :sys.get_state(pid)
      ahead = %{writer | outbox_flushed_seq: writer.outbox_durable_seq + 99}

      log = capture_log(fn -> Settlement.persist_dedup(ahead) end)

      assert log =~ "refusing to persist an out-of-order cursor record"
      assert Lineage.progress(cell_id) == before
    end

    test "is one predicate over the record, not a rule each reader restates" do
      # The pure half, driven directly: the predicate has to be able to say no.
      ordered = %{
        last_persisted_seq: 7,
        outbox_durable_seq: 5,
        outbox_flushed_seq: 3,
        epoch: 0
      }

      assert State.ordered?(ordered)
      assert State.ordered?(%{ordered | outbox_durable_seq: 7, outbox_flushed_seq: 7})
      refute State.ordered?(%{ordered | outbox_durable_seq: 9})
      refute State.ordered?(%{ordered | outbox_flushed_seq: 6})
    end

    test "a writer with nothing in it starts ordered at the empty-log floor", %{
      rc: rc,
      cell_id: cell_id
    } do
      pid = armed_writer(rc, cell_id)
      cursors = progress(pid)

      assert State.ordered?(cursors)
      assert cursors.last_persisted_seq == -1
      assert cursors.outbox_flushed_seq == -1
      assert cursors.epoch == 0
    end
  end

  defp wait_for_flush(pid, was) do
    advanced = fn ->
      cursors = progress(pid)
      cursors.outbox_flushed_seq > was && cursors
    end

    NodeTestHelper.wait_until(advanced, 500, 10) ||
      flunk("the flush cursor never advanced past #{was}")
  end
end

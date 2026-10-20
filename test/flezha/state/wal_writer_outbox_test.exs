defmodule Flezha.State.WalWriterOutboxTest do
  @moduledoc """
  What the writer owes after a crash: fsync-gated `:on_output` dispatch with
  durable identity `(flz.cell, outbox_seq, epoch)`, restart re-drive above the
  `outbox_flushed_seq` cursor, the dedup file advancing at fsync-batch cadence
  rather than per record, dead-letter parks landing before any watermark moves,
  and the all-or-nothing crash-tear property of the atomic effect record.
  """
  use FlezhaTest.OutboxCase, async: false

  test "outputs are dispatched only after fsync, with durable identity", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    body =
      effect_body(
        [output("BTC/1m", "candle-1", [{"a", "b"}]), output("ETH/1m", "candle-2")],
        4711
      )

    # Freeze the shared batcher so the durable point is ours to trigger:
    # between append and fsync the outputs must not reach any plugin.
    :ok = :sys.suspend(FsyncBatcher)
    :ok = WalWriter.append(pid, 0, effect_header(), body)
    refute_dispatched()
    :ok = :sys.resume(FsyncBatcher)

    :ok = WalWriter.fsync(pid)

    assert {"candle-1", md1, _} = next_output()
    assert md1["channel"] == "out"
    assert md1["flz.cell"] == "#{rc}/task/k1"
    assert md1["flz.seq"] == "0.0"
    assert md1["flz.epoch"] == "0"
    assert md1["flz.key"] == "BTC/1m"
    assert md1["a"] == "b"

    assert {"candle-2", md2, _} = next_output()
    assert md2["flz.seq"] == "0.1"

    # The durable consumed-offset advanced the input-log watermark.
    assert InputLog.watermark(rc) == 4711

    # The cursor does NOT move on the hand-off — only once the chain has run
    # both outputs and no sink is holding either.
    assert Lineage.progress(cell_id).outbox_flushed_seq == -1

    settle(md1)
    settle(md2)
    :ok = WalWriter.fsync(pid)

    assert Lineage.progress(cell_id).outbox_flushed_seq == 0
  end

  test "checkpoint granularity: a mid-interval record flushes outputs without acking", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    # Mid-interval: the invocation emitted, but the state it emitted from is
    # not durable yet, so the record acknowledges nothing.
    append_effect(pid, 0, [output("k1", "v1")], nil)

    assert {"v1", md, _} = next_output()
    assert md["flz.key"] == "k1"

    refute InputLog.watermark(rc),
           "a record with no consumed-offset must not advance the watermark"

    # The checkpoint closes the interval: state and offset in one record.
    append_effect(pid, 1, [output("k2", "v2")], 4710)

    assert {"v2", _, _} = next_output()

    assert InputLog.watermark(rc) == 4710,
           "the checkpoint must advance the watermark to the interval's closing offset"
  end

  test "N appends produce no dedup-file writes until the fsync batch", %{
    cell_id: cell_id
  } do
    # The app-tree FsyncBatcher ticks every 5 ms, so without suppressing the
    # dirty mark the very batch this test is named for can fire mid-append and
    # answer for the append path.
    set_mimic_global_until_exit()
    stub(FsyncBatcher, :mark_dirty, fn _ -> :ok end)

    pid = start_writer(cell_id)

    for seq <- 0..9 do
      :ok = WalWriter.append(pid, seq, plain_header(), "change-#{seq}")
    end

    refute File.exists?(Paths.outbox_dedup_path(cell_id)),
           "append path must not write the dedup file per record"

    :ok = WalWriter.fsync(pid)

    assert Lineage.progress(cell_id).last_persisted_seq == 9
  end

  test "a stale dedup file is repaired from the WAL scan on reopen", %{
    cell_id: cell_id
  } do
    pid = start_writer(cell_id)
    for seq <- 0..4, do: :ok = WalWriter.append(pid, seq, plain_header(), "c#{seq}")
    :ok = WalWriter.fsync(pid)
    close_writer(pid, cell_id)

    # Crash-outran-the-cache simulation: the file claims an old watermark.
    :ok = Lineage.put_dedup(cell_id, %{last_persisted_seq: 0, outbox_flushed_seq: -1, epoch: 0})

    pid = start_writer(cell_id)
    assert WalWriter.last_persisted_seq(pid) == 4, "WAL scan is authoritative"
    assert :duplicate = WalWriter.append(pid, 3, plain_header(), "stale")
    :ok = WalWriter.append(pid, 5, plain_header(), "next")
  end

  test "crash before flush: restart re-drives the outputs exactly once", %{
    rc: rc,
    cell_id: cell_id
  } do
    # No Overwatch bound: the record becomes durable but cannot dispatch.
    pid = start_writer(cell_id)
    append_effect(pid, 0, [output("k", "payload-x")], 7)
    refute_dispatched()
    close_writer(pid, cell_id)

    assert Lineage.progress(cell_id).outbox_flushed_seq == -1

    # Restart with the relay bound: the re-drive dispatches with the
    # original durable identity.
    pid = armed_writer(rc, cell_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert {"payload-x", md, _} = next_output(2_000)
    assert md["flz.seq"] == "0.0"
    assert md["flz.epoch"] == "0"
    refute_dispatched()
  end

  test "crash after flush but before cursor write: identical re-drive", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "payload-y")], 1)
    assert {"payload-y", md_first, _} = next_output()
    settle(md_first)
    :ok = WalWriter.fsync(pid)
    close_writer(pid, cell_id)

    # The cursor write never landed (crash window): reset it to stale.
    _pid = reopen_with_stale_cursor(cell_id)

    assert {"payload-y", md_again, _} = next_output(2_000)
    assert md_again["flz.seq"] == md_first["flz.seq"]
    assert md_again["flz.epoch"] == md_first["flz.epoch"]
    assert md_again["flz.cell"] == md_first["flz.cell"]
  end

  test "re-drive waits for the relay to bind (retry path)", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = start_writer(cell_id)
    append_effect(pid, 0, [output("k", "late")], 2)
    refute_dispatched()

    # Bind after the fact: the 1s retry timer picks it up.
    ClassRegistry.put_overwatch(rc, self(), nil)
    assert {"late", _, _} = next_output(3_000)
  end

  test "reopen repopulates the cell's consumed-offset watermark", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)
    stage_effect(pid, 0, [output("k", "a")], 10)
    append_effect(pid, 1, [output("k", "b")], 17)
    assert {"a", _, _} = next_output()
    assert {"b", _, _} = next_output()
    close_writer(pid, cell_id)

    # Crash simulation: the rebuildable watermark cache is gone.
    InputLog.forget_cell(rc, cell_id)
    assert InputLog.watermark(rc) == nil

    pid = start_writer(cell_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert InputLog.watermark(rc) == 17,
           "open must repopulate the durable consumed-offset from the WAL scan"
  end

  test "a compaction that folded every offset-bearing record keeps the consumed offset", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "a")], 17)
    assert {"a", md, _} = next_output()
    settle(md)
    :ok = WalWriter.fsync(pid)
    assert InputLog.watermark(rc) == 17

    # Compaction: the snapshot covers seq 0 and the WAL is cut through it.
    {:ok, _} = Flezha.State.Snapshot.write_snapshot(cell_id, 0, TestHelpers.stub_header(), "s")
    close_writer(pid, cell_id)

    assert WalWriter.consumed_offset_on_disk(cell_id) == 17,
           "a sleeper's seed must still see the consumed offset"

    # Crash simulation: the rebuildable watermark cache is gone.
    InputLog.forget_cell(rc, cell_id)
    pid = start_writer(cell_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert InputLog.watermark(rc) == 17,
           "open must report the consumed offset the snapshot folded away"

    # A rewind whose surviving records carry no offset falls back to the folded one.
    append_effect(pid, 1, [], nil)
    assert WalWriter.consumed_offset_at(pid, 1) == 17
    append_effect(pid, 2, [], 20)
    assert InputLog.watermark(rc) == 20
    :ok = WalWriter.rewind_to(pid, 1, 1)

    assert InputLog.watermark(rc) == 17,
           "a rewind must not drop the row the folded records reported"
  end

  test "flz state outbox renders live and offline status", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "p")], 3)
    assert {_, md, _} = next_output()
    settle(md)
    :ok = WalWriter.fsync(pid)

    live =
      ExUnit.CaptureIO.capture_io(fn ->
        Cli.call("Steps.outbox", Jason.encode!(["#{rc}/task/k1"]), [])
      end)

    assert live =~ "outbox_flushed_seq 0"
    assert live =~ "outbox_lag         0"
    assert live =~ "input_watermark    3"

    :ok = WalWriter.close(pid)

    offline =
      ExUnit.CaptureIO.capture_io(fn ->
        Cli.call("Steps.outbox", Jason.encode!(["#{rc}/task/k1"]), [])
      end)

    assert offline =~ "unknown (writer not running)"
    assert offline =~ "epoch              0"
  end

  test "tier-on park: DLQ entry materializes after fsync and the watermark resumes", %{
    rc: rc,
    cell_id: cell_id,
    base: base
  } do
    redirect_dlq_dir(base)
    pid = armed_writer(rc, cell_id)

    # A healthy invocation at offset 10, then the poison payload's park
    # record at offset 11: {no outputs, dlq entry, consumed offset}.
    stage_effect(pid, 0, [output("k", "good")], 10)

    :ok =
      WalWriter.append(
        pid,
        1,
        effect_header(),
        effect_body([], 11, %{channel: "in", error: "kaboom", attempts: 3, payload: "poison"})
      )

    # Between append and fsync nothing is durable: no DLQ entry yet.
    assert DlqStore.list(rc) == []

    :ok = WalWriter.fsync(pid)
    assert {"good", _, _} = next_output()

    assert [entry] = DlqStore.list(rc)
    assert entry["channel"] == "in"
    assert entry["error"] == "kaboom"
    assert entry["attempts"] == 3
    assert entry["origin_offset"] == 11
    assert entry["cell_key"] == "k1"

    assert {:ok, _entry, "poison"} = DlqStore.get("#{rc}/task/#{entry["id"]}")

    # Parking un-froze the min-watermark: the cell's consumed offset
    # advanced PAST the poison payload.
    assert InputLog.watermark(rc) == 11
  end

  # fails when: a tier park stops telling the Overwatch, once its entry is
  # durable, that the input it parked is dead-lettered, with the call waiting
  # on it if there is one.
  test "tier-on park of an input is announced once the entry is durable", %{
    rc: rc,
    cell_id: cell_id,
    base: base
  } do
    redirect_dlq_dir(base)
    {:ok, 0} = InputLog.append(rc, "poison", %{"flz.reply_to" => "w:caller"})
    {:ok, 1} = InputLog.append(rc, "poison", %{})
    pid = armed_writer(rc, cell_id)
    dlq = %{channel: "in", error: "kaboom", attempts: 3, payload: "poison"}

    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([], 0, dlq))
    refute_received {:"$gen_cast", {:dead_lettered, _}}

    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast",
                    {:dead_lettered,
                     %{token: "w:caller", entry: ref, error: "kaboom", channel: "in", offset: 0}}},
                   2_000

    assert [entry] = DlqStore.list(rc)
    assert ref == "#{rc}/task/#{entry["id"]}"

    # A parked input no caller waits on is announced to the listeners alone.
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([], 1, dlq))
    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast",
                    {:dead_lettered, %{token: nil, offset: 1, tasklet: "task", attempts: 3}}},
                   2_000
  end

  # fails when: a restart that re-drives an effect record whose dead letter is
  # already durable tells the listeners again, or stops answering its caller.
  test "a restart re-drive of a tier-on park answers its caller but tells no listener", %{
    rc: rc,
    cell_id: cell_id,
    base: base
  } do
    redirect_dlq_dir(base)
    {:ok, 0} = InputLog.append(rc, "good", %{})
    {:ok, 1} = InputLog.append(rc, "poison", %{"flz.reply_to" => "w:caller"})
    pid = armed_writer(rc, cell_id)
    dlq = %{channel: "in", error: "kaboom", attempts: 3, payload: "poison"}

    # An output no sink has settled holds the cursor below the park, as a crash
    # between the park's announce and the cursor write would.
    stage_effect(pid, 0, [output("k", "good")], 0)
    append_effect(pid, 1, [], 1, dlq)
    assert {"good", _, _} = next_output()
    assert_receive {:"$gen_cast", {:dead_lettered, %{offset: 1} = first}}, 2_000
    close_writer(pid, cell_id)
    assert Lineage.progress(cell_id).outbox_flushed_seq == -1

    pid = armed_writer(rc, cell_id)
    _ = WalWriter.last_persisted_seq(pid)
    assert {"good", _, _} = next_output(2_000)

    assert_receive {:"$gen_cast", {:dead_lettered, %{offset: 1, token: "w:caller"} = again}},
                   2_000

    assert {first[:new], again[:new]} == {true, false}
    assert [_entry] = DlqStore.list(rc)
  end

  # KI-1: the outer window's own furthest offset must not overwrite the higher
  # one a window nested inside its dispatch already reported.
  test "a nested window's advance survives the outer window closing", %{
    rc: rc,
    cell_id: cell_id,
    base: base
  } do
    redirect_dlq_dir(base)
    pid = armed_writer(rc, cell_id)

    # One fsync batch, deliberately: the park record's entry carries no output,
    # so it completes inside the very window that dispatched it and
    # `complete_entry({:cursor, _})` re-enters dispatch while that window is
    # still open. The nested window then carries the next record — durable in
    # the same batch — and reports the furthest offset either window saw.
    in_one_window(fn ->
      :ok =
        WalWriter.append(
          pid,
          0,
          effect_header(),
          effect_body([], 1, %{channel: "in", error: "poison", attempts: 3, payload: "kp:poison"})
        )

      stage_effect(pid, 1, [output("k2", "b")], 2)
    end)

    :ok = WalWriter.fsync(pid)

    assert {"b", _, _} = next_output()

    assert InputLog.watermark(rc) == 2,
           "the outer window's furthest offset of 1 overwrote the nested window's 2"
  end

  # KI-41: a zero-output entry is what a park produces, and it completes inside
  # `await_entry/4`, so `complete_entry/2` re-enters dispatch while the window
  # is still open. The park's own DLQ record must be durable before that nested
  # window advances the watermark past the input the park owns.
  test "a park is stored before any watermark advance", %{
    rc: rc,
    cell_id: cell_id,
    base: base
  } do
    redirect_dlq_dir(base)
    pid = armed_writer(rc, cell_id)
    trace_park_and_watermark(pid)

    in_one_window(fn ->
      :ok =
        WalWriter.append(
          pid,
          0,
          effect_header(),
          effect_body([], 1, %{channel: "in", error: "poison", attempts: 3, payload: "kp:poison"})
        )

      stage_effect(pid, 1, [output("k2", "b")], 2)
    end)

    :ok = WalWriter.fsync(pid)
    assert {"b", _, _} = next_output()

    order = traced_order()

    assert {:park, 1} in order, "the park was never stored: #{inspect(order)}"
    assert Enum.any?(order, &match?({:consumed, _}, &1)), "nothing consumed: #{inspect(order)}"

    assert Enum.find_index(order, &(&1 == {:park, 1})) <
             Enum.find_index(order, &match?({:consumed, _}, &1)),
           "the watermark advanced before the park was stored: #{inspect(order)}"

    assert InputLog.watermark(rc) == 2
  end

  test "re-drive of a park record is idempotent: no duplicate DLQ entry", %{
    rc: rc,
    cell_id: cell_id,
    base: base
  } do
    redirect_dlq_dir(base)
    pid = armed_writer(rc, cell_id)

    :ok =
      WalWriter.append(
        pid,
        0,
        effect_header(),
        effect_body([], 5, %{channel: "in", error: "kaboom", attempts: 3, payload: "poison"})
      )

    # A marker record AFTER the park: in-order dispatch means receiving its
    # output proves the park record was (re-)processed.
    append_effect(pid, 1, [output("k", "marker")], 6)
    assert {"marker", _, _} = next_output()
    assert [_entry] = DlqStore.list(rc)
    close_writer(pid, cell_id)

    # Crash window: the park materialized but the outbox cursor write never
    # landed. The reopen re-drives both records; the park is idempotent on
    # origin identity, so exactly one entry survives.
    _pid = reopen_with_stale_cursor(cell_id)

    assert {"marker", _, _} = next_output(2_000)
    assert [_exactly_one] = DlqStore.list(rc)
  end

  test "crash-tear: a torn park record loses neither offset nor park, together", %{
    rc: rc,
    cell_id: cell_id,
    base: base
  } do
    redirect_dlq_dir(base)
    ClassRegistry.put_overwatch(rc, self(), nil)

    body = effect_body([], 9, %{channel: "in", error: "e", attempts: 1, payload: "p"})
    effect = Frame.encode_record(0, effect_header(), body)
    wal_path = Paths.wal_path(cell_id)
    File.mkdir_p!(Path.dirname(wal_path))

    # Torn mid-record: the record is discarded entirely — the consumed
    # offset cannot advance while the park is lost (they are one record).
    File.write!(wal_path, binary_part(effect, 0, byte_size(effect) - 3))
    pid = start_writer(cell_id)
    _ = WalWriter.last_persisted_seq(pid)
    assert DlqStore.list(rc) == []
    assert InputLog.watermark(rc) == nil
    :ok = WalWriter.close(pid)
    File.rm!(wal_path)
    File.rm(Paths.outbox_dedup_path(cell_id))

    # Complete record: park AND offset recover together.
    File.write!(wal_path, effect)
    pid = start_writer(cell_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert wait_until(fn -> DlqStore.list(rc) != [] end, 2_000),
           "park never materialized from the re-drive"

    assert InputLog.watermark(rc) == 9
    :ok = WalWriter.close(pid)
  end

  test "crash-tear: a torn effect record recovers all-or-nothing", %{
    rc: rc,
    cell_id: cell_id
  } do
    ClassRegistry.put_overwatch(rc, self(), nil)

    # Build the on-disk bytes of record 0 (plain) + record 1 (effect).
    plain = Frame.encode_record(0, plain_header(), "base")
    body = effect_body([output("k", "torn?")], 9)
    effect = Frame.encode_record(1, effect_header(), body)

    wal_path = Paths.wal_path(cell_id)
    File.mkdir_p!(Path.dirname(wal_path))

    # Kill the controller at every byte boundary inside the effect append.
    for cut <- 1..(byte_size(effect) - 1) do
      File.write!(wal_path, plain <> binary_part(effect, 0, cut))

      pid = start_writer(cell_id)

      assert WalWriter.last_persisted_seq(pid) == 0,
             "cut=#{cut}: torn effect record must be discarded entirely"

      refute_dispatched(10)
      :ok = WalWriter.close(pid)
      # The writer truncated the torn tail; reset the fixture for the next cut.
      File.rm!(wal_path)
      File.rm(Paths.outbox_dedup_path(cell_id))
    end

    # The complete append recovers change + outputs + offset together.
    File.write!(wal_path, plain <> effect)
    pid = start_writer(cell_id)
    assert WalWriter.last_persisted_seq(pid) == 1

    assert {"torn?", md, _} = next_output(2_000)
    assert md["flz.seq"] == "1.0"
    assert InputLog.watermark(rc) == 9
    :ok = WalWriter.close(pid)
  end
end

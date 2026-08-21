defmodule Flezha.State.WalWriterOutboxTest do
  @moduledoc """
  Effectively-once outbox at the WalWriter: fsync-gated `:on_output`
  dispatch with durable identity `(flz.entity, outbox_seq, epoch)`,
  restart re-drive above the `outbox_flushed_seq` cursor (crash-before-flush
  and crash-after-flush-before-cursor), dedup-file advance at fsync-batch
  cadence instead of per record (§4), and the crash-tear all-or-nothing
  property of the atomic effect record (§3.5).
  """
  use ExUnit.Case, async: false
  use Mimic

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Operative.ClassRegistry

  alias Flezha.State.{
    FsyncBatcher,
    InputLog,
    Outbox,
    Registry,
    Supervisor,
    TestHelpers,
    WalWriter,
    Wire
  }

  setup do
    base = TestHelpers.tmp_app_data_dir()
    ClassRegistry.ensure_table()

    TestHelpers.start_state_tree()

    rc = "wo_" <> Integer.to_string(System.unique_integer([:positive]))
    entity_id = {rc, "proc", "k1"}

    on_exit(fn ->
      # Stop the writer first so a late persist cannot race the rm_rf.
      try do
        case Supervisor.lookup(entity_id) do
          {:ok, pid} -> WalWriter.close(pid)
          :error -> :ok
        end
      catch
        # A test that closed its own writer may still see the registry entry
        # until the sweep lands; stopping it twice is not a failure.
        :exit, _ -> :ok
      end

      Application.delete_env(:flezha, :state_root)
      _ = File.rm_rf(base)
    end)

    %{rc: rc, entity_id: entity_id, base: base}
  end

  # Header byte 9 is the format id: 2 marks an effect record.
  defp effect_header(), do: <<1, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0>>
  defp plain_header(), do: <<1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0>>

  defp effect_body(outputs, offset, dlq \\ nil) do
    Wire.encode_effect_body(%{
      changes: [<<1>>],
      outputs: outputs,
      consumed_offset: offset,
      dlq: dlq
    })
  end

  # Point the global DLQ store at a per-test tmp dir.
  defp redirect_dlq_dir(base) do
    original = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.put(original, :dir, Path.join(base, "dlq")))
    on_exit(fn -> Application.put_env(:flezha, :dlq, original) end)
  end

  # This test process stands in for the Overwatch: the plugin chain run is what
  # settles a dispatched output, so a test expecting the cursor to move reports
  # it the same way the real dispatcher does.
  defp settle(metadata), do: :ok = Outbox.settled(metadata)

  defp output(key, payload, metadata \\ []) do
    %{channel: "out", key: key, metadata: metadata, payload: payload}
  end

  defp start_writer(entity_id) do
    {:ok, pid} = Supervisor.find_or_start(entity_id)
    pid
  end

  # Close and wait for the registry sweep, so the next find_or_start starts
  # a fresh writer instead of racing the dead entry.
  defp close_writer(pid, entity_id) do
    :ok = WalWriter.close(pid)

    assert wait_until(fn -> Supervisor.lookup(entity_id) == :error end, 2_000),
           "writer registry entry never cleared"
  end

  test "outputs are dispatched only after fsync, with durable identity", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    body =
      effect_body(
        [output("BTC/1m", "candle-1", [{"a", "b"}]), output("ETH/1m", "candle-2")],
        4711
      )

    # Freeze the shared batcher so the durable point is ours to trigger:
    # between append and fsync the outputs must not reach any plugin.
    :ok = :sys.suspend(FsyncBatcher)
    :ok = WalWriter.append(pid, 0, effect_header(), body)
    refute_receive {:"$gen_cast", {:io_out, _, _, _}}, 50
    :ok = :sys.resume(FsyncBatcher)

    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "candle-1", md1, _}}
    assert md1["channel"] == "out"
    assert md1["flz.entity"] == "#{rc}/proc/k1"
    assert md1["flz.seq"] == "0.0"
    assert md1["flz.epoch"] == "0"
    assert md1["flz.key"] == "BTC/1m"
    assert md1["a"] == "b"

    assert_receive {:"$gen_cast", {:io_out, "candle-2", md2, _}}
    assert md2["flz.seq"] == "0.1"

    # The durable consumed-offset advanced the input-log watermark.
    assert InputLog.watermark(rc) == 4711

    # The cursor does NOT move on the hand-off — only once the chain has run
    # both outputs and no sink is holding either.
    assert Outbox.read(entity_id).outbox_flushed_seq == -1

    settle(md1)
    settle(md2)
    :ok = WalWriter.fsync(pid)

    assert Outbox.read(entity_id).outbox_flushed_seq == 0
  end

  test "checkpoint granularity: a mid-interval record flushes outputs without acking", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    # Mid-interval: the invocation emitted, but the state it emitted from is
    # not durable yet, so the record acknowledges nothing.
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k1", "v1")], nil))
    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "v1", md, _}}
    assert md["flz.key"] == "k1"

    refute InputLog.watermark(rc),
           "a record with no consumed-offset must not advance the watermark"

    # The checkpoint closes the interval: state and offset in one record.
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k2", "v2")], 4710))
    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "v2", _, _}}

    assert InputLog.watermark(rc) == 4710,
           "the checkpoint must advance the watermark to the interval's closing offset"
  end

  test "N appends produce no dedup-file writes until the fsync batch", %{
    entity_id: entity_id
  } do
    # The app-tree FsyncBatcher ticks every 5 ms, so without suppressing the
    # dirty mark the very batch this test is named for can fire mid-append and
    # answer for the append path.
    set_mimic_global()
    stub(FsyncBatcher, :mark_dirty, fn _ -> :ok end)

    pid = start_writer(entity_id)

    for seq <- 0..9 do
      :ok = WalWriter.append(pid, seq, plain_header(), "change-#{seq}")
    end

    refute File.exists?(Registry.outbox_dedup_path(entity_id)),
           "append path must not write the dedup file per record"

    :ok = WalWriter.fsync(pid)

    assert Outbox.read(entity_id).last_persisted_seq == 9
  end

  test "a stale dedup file is repaired from the WAL scan on reopen", %{
    entity_id: entity_id
  } do
    pid = start_writer(entity_id)
    for seq <- 0..4, do: :ok = WalWriter.append(pid, seq, plain_header(), "c#{seq}")
    :ok = WalWriter.fsync(pid)
    close_writer(pid, entity_id)

    # Crash-outran-the-cache simulation: the file claims an old watermark.
    :ok = Outbox.write(entity_id, %{last_persisted_seq: 0, outbox_flushed_seq: -1, epoch: 0})

    pid = start_writer(entity_id)
    assert WalWriter.last_persisted_seq(pid) == 4, "WAL scan is authoritative"
    assert :duplicate = WalWriter.append(pid, 3, plain_header(), "stale")
    :ok = WalWriter.append(pid, 5, plain_header(), "next")
  end

  test "crash before flush: restart re-drives the outputs exactly once", %{
    rc: rc,
    entity_id: entity_id
  } do
    # No Overwatch bound: the record becomes durable but cannot dispatch.
    pid = start_writer(entity_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "payload-x")], 7))
    :ok = WalWriter.fsync(pid)
    refute_receive {:"$gen_cast", {:io_out, _, _, _}}, 50
    close_writer(pid, entity_id)

    assert Outbox.read(entity_id).outbox_flushed_seq == -1

    # Restart with the operative bound: the re-drive dispatches with the
    # original durable identity.
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert_receive {:"$gen_cast", {:io_out, "payload-x", md, _}}, 2_000
    assert md["flz.seq"] == "0.0"
    assert md["flz.epoch"] == "0"
    refute_receive {:"$gen_cast", {:io_out, _, _, _}}, 50
  end

  test "crash after flush but before cursor write: identical re-drive", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "payload-y")], 1))
    :ok = WalWriter.fsync(pid)
    assert_receive {:"$gen_cast", {:io_out, "payload-y", md_first, _}}
    settle(md_first)
    :ok = WalWriter.fsync(pid)
    close_writer(pid, entity_id)

    # The cursor write never landed (crash window): reset it to stale.
    dedup = Outbox.read(entity_id)
    :ok = Outbox.write(entity_id, %{dedup | outbox_flushed_seq: -1})

    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert_receive {:"$gen_cast", {:io_out, "payload-y", md_again, _}}, 2_000
    assert md_again["flz.seq"] == md_first["flz.seq"]
    assert md_again["flz.epoch"] == md_first["flz.epoch"]
    assert md_again["flz.entity"] == md_first["flz.entity"]
  end

  test "re-drive waits for the operative to bind (retry path)", %{
    rc: rc,
    entity_id: entity_id
  } do
    pid = start_writer(entity_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "late")], 2))
    :ok = WalWriter.fsync(pid)
    refute_receive {:"$gen_cast", {:io_out, _, _, _}}, 50

    # Bind after the fact: the 1s retry timer picks it up.
    ClassRegistry.put_overwatch(rc, self())
    assert_receive {:"$gen_cast", {:io_out, "late", _, _}}, 3_000
  end

  test "reopen repopulates the entity's consumed-offset watermark", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "a")], 10))
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "b")], 17))
    :ok = WalWriter.fsync(pid)
    assert_receive {:"$gen_cast", {:io_out, "b", _, _}}
    close_writer(pid, entity_id)

    # Crash simulation: the rebuildable watermark cache is gone.
    InputLog.forget_entity(rc, entity_id)
    assert InputLog.watermark(rc) == nil

    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert InputLog.watermark(rc) == 17,
           "open must repopulate the durable consumed-offset from the WAL scan"
  end

  test "flz state outbox renders live and offline status", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "p")], 3))
    :ok = WalWriter.fsync(pid)
    assert_receive {:"$gen_cast", {:io_out, _, md, _}}
    settle(md)
    :ok = WalWriter.fsync(pid)

    live =
      ExUnit.CaptureIO.capture_io(fn ->
        Flezha.Service.CliManager.state_outbox("#{rc}/proc/k1")
      end)

    assert live =~ "outbox_flushed_seq 0"
    assert live =~ "outbox_lag         0"
    assert live =~ "input_watermark    3"

    :ok = WalWriter.close(pid)

    offline =
      ExUnit.CaptureIO.capture_io(fn ->
        Flezha.Service.CliManager.state_outbox("#{rc}/proc/k1")
      end)

    assert offline =~ "unknown (writer not running)"
    assert offline =~ "epoch              0"
  end

  test "tier-on park: DLQ entry materializes after fsync and the watermark resumes", %{
    rc: rc,
    entity_id: entity_id,
    base: base
  } do
    redirect_dlq_dir(base)
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    # A healthy invocation at offset 10, then the poison payload's park
    # record at offset 11: {no outputs, dlq entry, consumed offset}.
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "good")], 10))

    :ok =
      WalWriter.append(
        pid,
        1,
        effect_header(),
        effect_body([], 11, %{channel: "in", error: "kaboom", attempts: 3, payload: "poison"})
      )

    # Between append and fsync nothing is durable: no DLQ entry yet.
    assert Flezha.Dlq.Store.list(rc) == []

    :ok = WalWriter.fsync(pid)
    assert_receive {:"$gen_cast", {:io_out, "good", _, _}}

    assert [entry] = Flezha.Dlq.Store.list(rc)
    assert entry["channel"] == "in"
    assert entry["error"] == "kaboom"
    assert entry["attempts"] == 3
    assert entry["origin_offset"] == 11
    assert entry["entity_key"] == "k1"

    assert {:ok, _entry, "poison"} = Flezha.Dlq.Store.get("#{rc}/proc/#{entry["id"]}")

    # Parking un-froze the min-watermark: the entity's consumed offset
    # advanced PAST the poison payload.
    assert InputLog.watermark(rc) == 11
  end

  test "re-drive of a park record is idempotent: no duplicate DLQ entry", %{
    rc: rc,
    entity_id: entity_id,
    base: base
  } do
    redirect_dlq_dir(base)
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok =
      WalWriter.append(
        pid,
        0,
        effect_header(),
        effect_body([], 5, %{channel: "in", error: "kaboom", attempts: 3, payload: "poison"})
      )

    # A marker record AFTER the park: in-order dispatch means receiving its
    # output proves the park record was (re-)processed.
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "marker")], 6))

    :ok = WalWriter.fsync(pid)
    assert_receive {:"$gen_cast", {:io_out, "marker", _, _}}
    assert [_entry] = Flezha.Dlq.Store.list(rc)
    close_writer(pid, entity_id)

    # Crash window: the park materialized but the outbox cursor write never
    # landed. The reopen re-drives both records; the park is idempotent on
    # origin identity, so exactly one entry survives.
    dedup = Outbox.read(entity_id)
    :ok = Outbox.write(entity_id, %{dedup | outbox_flushed_seq: -1})

    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert_receive {:"$gen_cast", {:io_out, "marker", _, _}}, 2_000
    assert [_exactly_one] = Flezha.Dlq.Store.list(rc)
  end

  test "crash-tear: a torn park record loses neither offset nor park, together", %{
    rc: rc,
    entity_id: entity_id,
    base: base
  } do
    redirect_dlq_dir(base)
    ClassRegistry.put_overwatch(rc, self())

    body = effect_body([], 9, %{channel: "in", error: "e", attempts: 1, payload: "p"})
    effect = WalWriter.encode_record(0, effect_header(), body)
    wal_path = Registry.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(wal_path))

    # Torn mid-record: the record is discarded entirely — the consumed
    # offset cannot advance while the park is lost (they are one record).
    File.write!(wal_path, binary_part(effect, 0, byte_size(effect) - 3))
    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)
    assert Flezha.Dlq.Store.list(rc) == []
    assert InputLog.watermark(rc) == nil
    :ok = WalWriter.close(pid)
    File.rm!(wal_path)
    File.rm(Registry.outbox_dedup_path(entity_id))

    # Complete record: park AND offset recover together.
    File.write!(wal_path, effect)
    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert wait_until(fn -> Flezha.Dlq.Store.list(rc) != [] end, 2_000),
           "park never materialized from the re-drive"

    assert InputLog.watermark(rc) == 9
    :ok = WalWriter.close(pid)
  end

  test "crash-tear: a torn effect record recovers all-or-nothing", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())

    # Build the on-disk bytes of record 0 (plain) + record 1 (effect).
    plain = WalWriter.encode_record(0, plain_header(), "base")
    body = effect_body([output("k", "torn?")], 9)
    effect = WalWriter.encode_record(1, effect_header(), body)

    wal_path = Registry.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(wal_path))

    # Kill the controller at every byte boundary inside the effect append.
    for cut <- 1..(byte_size(effect) - 1) do
      File.write!(wal_path, plain <> binary_part(effect, 0, cut))

      pid = start_writer(entity_id)

      assert WalWriter.last_persisted_seq(pid) == 0,
             "cut=#{cut}: torn effect record must be discarded entirely"

      refute_receive {:"$gen_cast", {:io_out, _, _, _}}, 10
      :ok = WalWriter.close(pid)
      # The writer truncated the torn tail; reset the fixture for the next cut.
      File.rm!(wal_path)
      File.rm(Registry.outbox_dedup_path(entity_id))
    end

    # The complete append recovers change + outputs + offset together.
    File.write!(wal_path, plain <> effect)
    pid = start_writer(entity_id)
    assert WalWriter.last_persisted_seq(pid) == 1

    assert_receive {:"$gen_cast", {:io_out, "torn?", md, _}}, 2_000
    assert md["flz.seq"] == "1.0"
    assert InputLog.watermark(rc) == 9
    :ok = WalWriter.close(pid)
  end

  test "stall_status reports the outbox cursors producing-liveness compares", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    assert %{out_pending_seq: -1, outbox_flushed_seq: -1} = WalWriter.stall_status(pid)

    # An effect record carrying NO outputs must not move out_pending_seq. This is
    # the whole reason it exists rather than reusing outbox_durable_seq, which
    # advances on every fsynced record: an Entity that persists state but emits
    # nothing would otherwise read as an outbox frozen forever.
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([], 100))
    :ok = WalWriter.fsync(pid)

    assert %{out_pending_seq: -1} = WalWriter.stall_status(pid)

    # An output-bearing record does move it — and the flush cursor follows it.
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "v")], 101))
    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "v", md, _}}, 2_000
    settle(md)

    assert %{out_pending_seq: 1, outbox_flushed_seq: 1, outbox_unconfirmed_ms: 0} =
             WalWriter.stall_status(pid)

    :ok = WalWriter.close(pid)
  end

  test "stall_status cursors survive a restart re-drive without a phantom jump", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())

    # Two output-bearing records land durably, but the flush cursor never advances
    # (crash before flush): the re-drive owes both.
    plain = WalWriter.encode_record(0, effect_header(), effect_body([output("k", "a")], 1))
    second = WalWriter.encode_record(1, effect_header(), effect_body([output("k", "b")], 2))

    wal_path = Registry.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(wal_path))
    File.write!(wal_path, plain <> second)

    pid = start_writer(entity_id)

    # Seeded from what the re-drive owes, NOT from -1 — otherwise the first tick
    # after a restart would read as an outbox that suddenly grew from nothing.
    assert %{out_pending_seq: 1} = WalWriter.stall_status(pid)

    :ok = WalWriter.close(pid)
  end

  # ---------- delivery confirmation ----------

  test "a held output holds the cursor until the sink confirms", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "held")], 1))
    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "held", md, _}}

    # A delivery-guaranteed sink takes ownership before the chain settles.
    :ok = Outbox.hold(md)
    settle(md)
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: -1, awaiting: 1} = WalWriter.outbox_status(pid)

    :ok = Outbox.confirm(md)
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: 0, awaiting: 0} = WalWriter.outbox_status(pid)
    assert Outbox.read(entity_id).outbox_flushed_seq == 0
  end

  test "an unconfirmed entry blocks the cursor behind a later confirmed one", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "first")], 1))
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "second")], 2))
    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "first", md_first, _}}
    assert_receive {:"$gen_cast", {:io_out, "second", md_second, _}}

    for md <- [md_first, md_second] do
      :ok = Outbox.hold(md)
      settle(md)
    end

    # The later entry confirms first: the cursor may not pass the earlier one.
    :ok = Outbox.confirm(md_second)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: -1} = WalWriter.outbox_status(pid)

    :ok = Outbox.confirm(md_first)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 1, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a permanently failed output parks and advances exactly its range", %{
    rc: rc,
    entity_id: entity_id,
    base: base
  } do
    redirect_dlq_dir(base)
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "doomed")], 1))
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "fine")], 2))
    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "doomed", md_doomed, _}}
    assert_receive {:"$gen_cast", {:io_out, "fine", md_fine, _}}

    for md <- [md_doomed, md_fine] do
      :ok = Outbox.hold(md)
      settle(md)
    end

    :ok = Outbox.park(md_doomed, "doomed", :broker_gone, 6)
    :ok = Outbox.confirm(md_fine)
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: 1, awaiting: 0} = WalWriter.outbox_status(pid)

    assert [entry] = Flezha.Dlq.Store.list(rc)
    assert entry["entity_key"] == "k1"
    assert entry["channel"] == "out"
    assert entry["attempts"] == 6
    assert entry["error"] =~ "broker_gone"
    assert {:ok, _entry, "doomed"} = Flezha.Dlq.Store.get("#{rc}/proc/#{entry["id"]}")
  end

  test "the in-flight window back-pressures the flush instead of dropping", %{
    rc: rc,
    entity_id: entity_id
  } do
    Application.put_env(:flezha, :outbox_inflight_max, 1)
    on_exit(fn -> Application.delete_env(:flezha, :outbox_inflight_max) end)

    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "one")], 1))
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "two")], 2))
    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "one", md_one, _}}
    refute_receive {:"$gen_cast", {:io_out, "two", _, _}}, 100

    assert %{pending: 1} = WalWriter.outbox_status(pid)

    :ok = Outbox.hold(md_one)
    settle(md_one)
    :ok = Outbox.confirm(md_one)

    # The confirmation itself refills the window — no fsync and no retry tick
    # (1 s) in between, or a burst's tail would drain one window per tick.
    assert_receive {:"$gen_cast", {:io_out, "two", _, _}}, 500
  end

  test "a chain run that never reports back is re-dispatched, not wedged", %{
    rc: rc,
    entity_id: entity_id
  } do
    Application.put_env(:flezha, :outbox_resettle_ms, 50)
    on_exit(fn -> Application.delete_env(:flezha, :outbox_resettle_ms) end)

    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "lost")], 1))
    :ok = WalWriter.fsync(pid)

    # The Overwatch died mid-chain: the output was dispatched and nothing ever
    # settled it. The retry tick re-dispatches it with identical identity.
    assert_receive {:"$gen_cast", {:io_out, "lost", md, _}}
    assert_receive {:"$gen_cast", {:io_out, "lost", md_again, _}}, 3_000
    assert md_again["flz.seq"] == md["flz.seq"]
    assert md_again["flz.epoch"] == md["flz.epoch"]

    settle(md_again)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0} = WalWriter.outbox_status(pid)
  end
end

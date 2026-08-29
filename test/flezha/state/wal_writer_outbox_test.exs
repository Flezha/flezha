defmodule Flezha.State.WalWriterOutboxTest do
  @moduledoc """
  Effectively-once outbox at the WalWriter: fsync-gated `:on_output`
  dispatch with durable identity `(flz.entity, outbox_seq, epoch)`,
  restart re-drive above the `outbox_flushed_seq` cursor (crash-before-flush
  and crash-after-flush-before-cursor), dedup-file advance at fsync-batch
  cadence instead of per record (§4), and the crash-tear all-or-nothing
  property of the atomic effect record (§3.5).
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  import ExUnit.CaptureLog

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  import Flezha.State.TestHelpers,
    only: [
      assert_batch: 1,
      in_one_window: 1,
      metadata_of: 1,
      next_output: 0,
      next_output: 1,
      refute_dispatched: 0,
      refute_dispatched: 1,
      run_batch: 1,
      run_batch: 2
    ]

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
    refute_dispatched()
    :ok = :sys.resume(FsyncBatcher)

    :ok = WalWriter.fsync(pid)

    assert {"candle-1", md1, _} = next_output()
    assert md1["channel"] == "out"
    assert md1["flz.entity"] == "#{rc}/proc/k1"
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

    assert {"v1", md, _} = next_output()
    assert md["flz.key"] == "k1"

    refute InputLog.watermark(rc),
           "a record with no consumed-offset must not advance the watermark"

    # The checkpoint closes the interval: state and offset in one record.
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k2", "v2")], 4710))
    :ok = WalWriter.fsync(pid)

    assert {"v2", _, _} = next_output()

    assert InputLog.watermark(rc) == 4710,
           "the checkpoint must advance the watermark to the interval's closing offset"
  end

  test "N appends produce no dedup-file writes until the fsync batch", %{
    entity_id: entity_id
  } do
    # The app-tree FsyncBatcher ticks every 5 ms, so without suppressing the
    # dirty mark the very batch this test is named for can fire mid-append and
    # answer for the append path.
    set_mimic_global_until_exit()
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
    refute_dispatched()
    close_writer(pid, entity_id)

    assert Outbox.read(entity_id).outbox_flushed_seq == -1

    # Restart with the operative bound: the re-drive dispatches with the
    # original durable identity.
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert {"payload-x", md, _} = next_output(2_000)
    assert md["flz.seq"] == "0.0"
    assert md["flz.epoch"] == "0"
    refute_dispatched()
  end

  test "crash after flush but before cursor write: identical re-drive", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "payload-y")], 1))
    :ok = WalWriter.fsync(pid)
    assert {"payload-y", md_first, _} = next_output()
    settle(md_first)
    :ok = WalWriter.fsync(pid)
    close_writer(pid, entity_id)

    # The cursor write never landed (crash window): reset it to stale.
    dedup = Outbox.read(entity_id)
    :ok = Outbox.write(entity_id, %{dedup | outbox_flushed_seq: -1})

    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert {"payload-y", md_again, _} = next_output(2_000)
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
    refute_dispatched()

    # Bind after the fact: the 1s retry timer picks it up.
    ClassRegistry.put_overwatch(rc, self())
    assert {"late", _, _} = next_output(3_000)
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
    assert {"a", _, _} = next_output()
    assert {"b", _, _} = next_output()
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
    assert {_, md, _} = next_output()
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
    assert {"good", _, _} = next_output()

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
    assert {"marker", _, _} = next_output()
    assert [_entry] = Flezha.Dlq.Store.list(rc)
    close_writer(pid, entity_id)

    # Crash window: the park materialized but the outbox cursor write never
    # landed. The reopen re-drives both records; the park is idempotent on
    # origin identity, so exactly one entry survives.
    dedup = Outbox.read(entity_id)
    :ok = Outbox.write(entity_id, %{dedup | outbox_flushed_seq: -1})

    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert {"marker", _, _} = next_output(2_000)
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

      refute_dispatched(10)
      :ok = WalWriter.close(pid)
      # The writer truncated the torn tail; reset the fixture for the next cut.
      File.rm!(wal_path)
      File.rm(Registry.outbox_dedup_path(entity_id))
    end

    # The complete append recovers change + outputs + offset together.
    File.write!(wal_path, plain <> effect)
    pid = start_writer(entity_id)
    assert WalWriter.last_persisted_seq(pid) == 1

    assert {"torn?", md, _} = next_output(2_000)
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

    assert {"v", md, _} = next_output(2_000)
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

    assert {"held", md, _} = next_output()

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

    assert {"first", md_first, _} = next_output()
    assert {"second", md_second, _} = next_output()

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

    assert {"doomed", md_doomed, _} = next_output()
    assert {"fine", md_fine, _} = next_output()

    for md <- [md_doomed, md_fine] do
      :ok = Outbox.hold(md)
      settle(md)
    end

    # Parking an undeliverable output logs at :error by design — assert that
    # operator-facing line instead of letting it reach the console unasserted.
    log =
      capture_log(fn ->
        :ok = Outbox.park(md_doomed, "doomed", :broker_gone, 6)
        :ok = Outbox.confirm(md_fine)
        :ok = WalWriter.fsync(pid)
      end)

    assert log =~ "parked undeliverable output"
    assert log =~ "broker_gone"

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

    assert {"one", md_one, _} = next_output()
    refute_dispatched(100)

    assert %{pending: 1} = WalWriter.outbox_status(pid)

    :ok = Outbox.hold(md_one)
    settle(md_one)
    :ok = Outbox.confirm(md_one)

    # The confirmation itself refills the window — no fsync and no retry tick
    # (1 s) in between, or a burst's tail would drain one window per tick.
    assert {"two", _, _} = next_output(500)
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
    assert {"lost", md, _} = next_output()
    assert {"lost", md_again, _} = next_output(3_000)
    assert md_again["flz.seq"] == md["flz.seq"]
    assert md_again["flz.epoch"] == md["flz.epoch"]

    settle(md_again)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0} = WalWriter.outbox_status(pid)
  end

  # ---------- batched reports ----------

  test "one settlement and one confirmation report complete a whole batch", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    in_one_window(fn ->
      for seq <- 0..9 do
        :ok =
          WalWriter.append(
            pid,
            seq,
            effect_header(),
            effect_body([output("k", "p#{seq}")], seq + 1)
          )
      end
    end)

    :ok = WalWriter.fsync(pid)

    outputs = assert_batch(2_000)
    assert length(outputs) == 10, "one dispatch pass must reach the chain as one batch"

    # One pass advances the watermark once, to the furthest offset it carried.
    assert InputLog.watermark(rc) == 10

    # The chain runs as the dispatcher does: every output held, the whole batch
    # settled in ONE report. Nothing confirmed yet, so no cursor may move.
    run_batch(outputs, fn _payload, metadata -> Outbox.hold(metadata) end)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: -1, awaiting: 10} = WalWriter.outbox_status(pid)

    # And ONE report from the sink completes all ten entries.
    :ok = Outbox.confirm_many(Enum.map(outputs, &metadata_of/1))
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: 9, awaiting: 0} = WalWriter.outbox_status(pid)
    assert Outbox.read(entity_id).outbox_flushed_seq == 9
  end

  test "an out-of-order batch report cannot carry the cursor past an unconfirmed entry", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "early")], 1))
    :ok = WalWriter.fsync(pid)
    early = assert_batch(2_000)

    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "late")], 2))
    :ok = WalWriter.fsync(pid)
    late = assert_batch(2_000)

    for batch <- [early, late],
        do: run_batch(batch, fn _payload, metadata -> Outbox.hold(metadata) end)

    :ok = Outbox.confirm_many(Enum.map(late, &metadata_of/1))
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: -1} = WalWriter.outbox_status(pid),
           "the later batch's report must not move the cursor past the earlier entry"

    :ok = Outbox.confirm_many(Enum.map(early, &metadata_of/1))
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 1, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a crashed batch run settles none of its entries and the resettle re-drives them", %{
    rc: rc,
    entity_id: entity_id
  } do
    Application.put_env(:flezha, :outbox_resettle_ms, 50)
    on_exit(fn -> Application.delete_env(:flezha, :outbox_resettle_ms) end)

    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    in_one_window(fn ->
      :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "a")], 1))
      :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "b")], 2))
    end)

    :ok = WalWriter.fsync(pid)
    outputs = assert_batch(2_000)
    assert length(outputs) == 2

    # The chain crashes partway through the batch. `run/2` reports only after
    # the whole batch has run, so neither entry is settled and the hold the
    # dead run took is never answered.
    capture_log(fn ->
      {:ok, runner} =
        Task.start(fn ->
          Outbox.run(outputs, fn payload, metadata ->
            :ok = Outbox.hold(metadata)
            if payload == "b", do: raise("chain crashed")
          end)
        end)

      ref = Process.monitor(runner)
      assert_receive {:DOWN, ^ref, :process, ^runner, _}, 1_000
    end)

    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: -1, awaiting: 2} = WalWriter.outbox_status(pid)

    # The resettle re-dispatches both under identical identity.
    redriven = assert_batch(3_000)

    assert [{"a", %{"flz.seq" => "0.0"}, _}, {"b", %{"flz.seq" => "1.0"}, _}] =
             Enum.sort_by(redriven, fn {payload, _metadata, _ack} -> payload end)

    run_batch(redriven)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 1, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a checkpoint record's outputs flush as one batch with sequential ordinals", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    # What an `outputs: :at_checkpoint` interval ships: ONE record carrying the
    # whole interval's outputs and the offset of the input that closed it.
    outputs = for n <- 0..299, do: output("k#{n}", "v#{n}")
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body(outputs, 4_711))
    :ok = WalWriter.fsync(pid)

    batch = assert_batch(2_000)

    assert length(batch) == 300,
           "the interval's outputs reach the chain as one batch, not 300 casts"

    assert Enum.map(batch, fn {_payload, md, _ack} -> md["flz.seq"] end) ==
             Enum.map(0..299, &"0.#{&1}"),
           "identities are `<record seq>.<ordinal>`, sequential across the batch"

    assert InputLog.watermark(rc) == 4_711,
           "the closing record acknowledges the interval's last input"

    # One settlement report and one confirmation complete the whole interval.
    run_batch(batch, fn _payload, metadata -> Outbox.hold(metadata) end)
    :ok = Outbox.confirm_many(Enum.map(batch, &metadata_of/1))
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: 0, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a re-drive re-sends a checkpoint record's outputs under identical identity", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok =
      WalWriter.append(
        pid,
        0,
        effect_header(),
        effect_body([output("k", "a"), output("k", "b")], 9)
      )

    :ok = WalWriter.fsync(pid)

    first = assert_batch(2_000)
    run_batch(first)
    close_writer(pid, entity_id)

    # Crash before the cursor write: the re-drive owes the whole record again.
    dedup = Outbox.read(entity_id)
    :ok = Outbox.write(entity_id, %{dedup | outbox_flushed_seq: -1})

    pid = start_writer(entity_id)
    _ = WalWriter.last_persisted_seq(pid)

    again = assert_batch(3_000)

    assert Enum.map(again, fn {payload, md, _} -> {payload, md["flz.seq"], md["flz.epoch"]} end) ==
             Enum.map(first, fn {payload, md, _} -> {payload, md["flz.seq"], md["flz.epoch"]} end),
           "a re-driven record carries byte-identical identity, which is what sink dedup absorbs"

    :ok = WalWriter.close(pid)
  end

  test "a batch report naming entries the writer no longer owes changes nothing", %{
    rc: rc,
    entity_id: entity_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(entity_id)

    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "done")], 1))
    :ok = WalWriter.fsync(pid)

    outputs = assert_batch(2_000)
    run_batch(outputs, fn _payload, metadata -> Outbox.hold(metadata) end)
    :ok = Outbox.confirm_many(Enum.map(outputs, &metadata_of/1))
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0, awaiting: 0} = WalWriter.outbox_status(pid)

    # A superseded run answering late: every key it names is gone from the
    # ledger, so both batch reports are dropped per key and nothing moves.
    :ok = Outbox.confirm_many(Enum.map(outputs, &metadata_of/1))

    GenServer.cast(
      pid,
      {:outbox_settled_many, Enum.map(outputs, fn {_payload, _md, %{key: key}} -> {key, 1} end)}
    )

    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0, awaiting: 0} = WalWriter.outbox_status(pid)
  end
end

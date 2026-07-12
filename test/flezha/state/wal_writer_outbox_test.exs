defmodule Flezha.State.WalWriterOutboxTest do
  @moduledoc """
  Effectively-once outbox at the WalWriter: fsync-gated `:on_output`
  dispatch with durable identity `(flz.segment, outbox_seq, epoch)`,
  restart re-drive above the `outbox_flushed_seq` cursor (crash-before-flush
  and crash-after-flush-before-cursor), dedup-file advance at fsync-batch
  cadence instead of per record (§4), and the crash-tear all-or-nothing
  property of the atomic effect record (§3.5).
  """
  use ExUnit.Case, async: false

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

    case start_supervised({Elixir.Registry, keys: :unique, name: Supervisor.registry_name()}) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(Supervisor) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(FsyncBatcher) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    rc = "wo_" <> Integer.to_string(System.unique_integer([:positive]))
    segment_id = {rc, "op", "k1"}

    on_exit(fn ->
      # Stop the writer first so a late persist cannot race the rm_rf.
      case Supervisor.lookup(segment_id) do
        {:ok, pid} -> WalWriter.close(pid)
        :error -> :ok
      end

      Application.delete_env(:flezha, :state_root)
      _ = File.rm_rf(base)
    end)

    %{rc: rc, segment_id: segment_id, base: base}
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

  defp output(key, payload, metadata \\ []) do
    %{channel: "out", key: key, metadata: metadata, payload: payload}
  end

  defp start_writer(segment_id) do
    {:ok, pid} = Supervisor.find_or_start(segment_id)
    pid
  end

  # Close and wait for the registry sweep, so the next find_or_start starts
  # a fresh writer instead of racing the dead entry.
  defp close_writer(pid, segment_id) do
    :ok = WalWriter.close(pid)

    assert wait_until(fn -> Supervisor.lookup(segment_id) == :error end, 2_000),
           "writer registry entry never cleared"
  end

  test "outputs are dispatched only after fsync, with durable identity", %{
    rc: rc,
    segment_id: segment_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(segment_id)

    body =
      effect_body(
        [output("BTC/1m", "candle-1", [{"a", "b"}]), output("ETH/1m", "candle-2")],
        4711
      )

    # Freeze the shared batcher so the durable point is ours to trigger:
    # between append and fsync the outputs must not reach any plugin.
    :ok = :sys.suspend(FsyncBatcher)
    :ok = WalWriter.append(pid, 0, effect_header(), body)
    refute_receive {:"$gen_cast", {:io_out, _, _}}, 50
    :ok = :sys.resume(FsyncBatcher)

    :ok = WalWriter.fsync(pid)

    assert_receive {:"$gen_cast", {:io_out, "candle-1", md1}}
    assert md1["channel"] == "out"
    assert md1["flz.segment"] == "#{rc}/op/k1"
    assert md1["flz.seq"] == "0.0"
    assert md1["flz.epoch"] == "0"
    assert md1["flz.key"] == "BTC/1m"
    assert md1["a"] == "b"

    assert_receive {:"$gen_cast", {:io_out, "candle-2", md2}}
    assert md2["flz.seq"] == "0.1"

    # The durable consumed-offset advanced the input-log watermark.
    assert InputLog.watermark(rc) == 4711

    # And the cursor advanced (at batch cadence) to the dispatched seq.
    assert Outbox.read(segment_id).outbox_flushed_seq == 0
  end

  test "N appends produce no dedup-file writes until the fsync batch", %{
    segment_id: segment_id
  } do
    pid = start_writer(segment_id)

    for seq <- 0..9 do
      :ok = WalWriter.append(pid, seq, plain_header(), "change-#{seq}")
    end

    refute File.exists?(Registry.outbox_dedup_path(segment_id)),
           "append path must not write the dedup file per record"

    :ok = WalWriter.fsync(pid)

    assert Outbox.read(segment_id).last_persisted_seq == 9
  end

  test "a stale dedup file is repaired from the WAL scan on reopen", %{
    segment_id: segment_id
  } do
    pid = start_writer(segment_id)
    for seq <- 0..4, do: :ok = WalWriter.append(pid, seq, plain_header(), "c#{seq}")
    :ok = WalWriter.fsync(pid)
    close_writer(pid, segment_id)

    # Crash-outran-the-cache simulation: the file claims an old watermark.
    :ok = Outbox.write(segment_id, %{last_persisted_seq: 0, outbox_flushed_seq: -1, epoch: 0})

    pid = start_writer(segment_id)
    assert WalWriter.last_persisted_seq(pid) == 4, "WAL scan is authoritative"
    assert :duplicate = WalWriter.append(pid, 3, plain_header(), "stale")
    :ok = WalWriter.append(pid, 5, plain_header(), "next")
  end

  test "crash before flush: restart re-drives the outputs exactly once", %{
    rc: rc,
    segment_id: segment_id
  } do
    # No Overwatch bound: the record becomes durable but cannot dispatch.
    pid = start_writer(segment_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "payload-x")], 7))
    :ok = WalWriter.fsync(pid)
    refute_receive {:"$gen_cast", {:io_out, _, _}}, 50
    close_writer(pid, segment_id)

    assert Outbox.read(segment_id).outbox_flushed_seq == -1

    # Restart with the operative bound: the re-drive dispatches with the
    # original durable identity.
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(segment_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert_receive {:"$gen_cast", {:io_out, "payload-x", md}}, 2_000
    assert md["flz.seq"] == "0.0"
    assert md["flz.epoch"] == "0"
    refute_receive {:"$gen_cast", {:io_out, _, _}}, 50
  end

  test "crash after flush but before cursor write: identical re-drive", %{
    rc: rc,
    segment_id: segment_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(segment_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "payload-y")], 1))
    :ok = WalWriter.fsync(pid)
    assert_receive {:"$gen_cast", {:io_out, "payload-y", md_first}}
    close_writer(pid, segment_id)

    # The cursor write never landed (crash window): reset it to stale.
    dedup = Outbox.read(segment_id)
    :ok = Outbox.write(segment_id, %{dedup | outbox_flushed_seq: -1})

    pid = start_writer(segment_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert_receive {:"$gen_cast", {:io_out, "payload-y", md_again}}, 2_000
    assert md_again["flz.seq"] == md_first["flz.seq"]
    assert md_again["flz.epoch"] == md_first["flz.epoch"]
    assert md_again["flz.segment"] == md_first["flz.segment"]
  end

  test "re-drive waits for the operative to bind (retry path)", %{
    rc: rc,
    segment_id: segment_id
  } do
    pid = start_writer(segment_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "late")], 2))
    :ok = WalWriter.fsync(pid)
    refute_receive {:"$gen_cast", {:io_out, _, _}}, 50

    # Bind after the fact: the 1s retry timer picks it up.
    ClassRegistry.put_overwatch(rc, self())
    assert_receive {:"$gen_cast", {:io_out, "late", _}}, 3_000
  end

  test "reopen repopulates the segment's consumed-offset watermark", %{
    rc: rc,
    segment_id: segment_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(segment_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "a")], 10))
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k", "b")], 17))
    :ok = WalWriter.fsync(pid)
    assert_receive {:"$gen_cast", {:io_out, "b", _}}
    close_writer(pid, segment_id)

    # Crash simulation: the rebuildable watermark cache is gone.
    InputLog.forget_segment(rc, segment_id)
    assert InputLog.watermark(rc) == nil

    pid = start_writer(segment_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert InputLog.watermark(rc) == 17,
           "open must repopulate the durable consumed-offset from the WAL scan"
  end

  test "flz state outbox renders live and offline status", %{
    rc: rc,
    segment_id: segment_id
  } do
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(segment_id)
    :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "p")], 3))
    :ok = WalWriter.fsync(pid)
    assert_receive {:"$gen_cast", {:io_out, _, _}}

    live =
      ExUnit.CaptureIO.capture_io(fn ->
        Flezha.Service.CliManager.state_outbox("#{rc}/op/k1")
      end)

    assert live =~ "outbox_flushed_seq 0"
    assert live =~ "outbox_lag         0"
    assert live =~ "input_watermark    3"

    :ok = WalWriter.close(pid)

    offline =
      ExUnit.CaptureIO.capture_io(fn ->
        Flezha.Service.CliManager.state_outbox("#{rc}/op/k1")
      end)

    assert offline =~ "unknown (writer not running)"
    assert offline =~ "epoch              0"
  end

  test "tier-on park: DLQ entry materializes after fsync and the watermark resumes", %{
    rc: rc,
    segment_id: segment_id,
    base: base
  } do
    redirect_dlq_dir(base)
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(segment_id)

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
    assert_receive {:"$gen_cast", {:io_out, "good", _}}

    assert [entry] = Flezha.Dlq.Store.list(rc)
    assert entry["channel"] == "in"
    assert entry["error"] == "kaboom"
    assert entry["attempts"] == 3
    assert entry["origin_offset"] == 11
    assert entry["segment_key"] == "k1"

    assert {:ok, _entry, "poison"} = Flezha.Dlq.Store.get("#{rc}/op/#{entry["id"]}")

    # Parking un-froze the min-watermark: the segment's consumed offset
    # advanced PAST the poison payload.
    assert InputLog.watermark(rc) == 11
  end

  test "re-drive of a park record is idempotent: no duplicate DLQ entry", %{
    rc: rc,
    segment_id: segment_id,
    base: base
  } do
    redirect_dlq_dir(base)
    ClassRegistry.put_overwatch(rc, self())
    pid = start_writer(segment_id)

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
    assert_receive {:"$gen_cast", {:io_out, "marker", _}}
    assert [_entry] = Flezha.Dlq.Store.list(rc)
    close_writer(pid, segment_id)

    # Crash window: the park materialized but the outbox cursor write never
    # landed. The reopen re-drives both records; the park is idempotent on
    # origin identity, so exactly one entry survives.
    dedup = Outbox.read(segment_id)
    :ok = Outbox.write(segment_id, %{dedup | outbox_flushed_seq: -1})

    pid = start_writer(segment_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert_receive {:"$gen_cast", {:io_out, "marker", _}}, 2_000
    assert [_exactly_one] = Flezha.Dlq.Store.list(rc)
  end

  test "crash-tear: a torn park record loses neither offset nor park, together", %{
    rc: rc,
    segment_id: segment_id,
    base: base
  } do
    redirect_dlq_dir(base)
    ClassRegistry.put_overwatch(rc, self())

    body = effect_body([], 9, %{channel: "in", error: "e", attempts: 1, payload: "p"})
    effect = WalWriter.encode_record(0, effect_header(), body)
    wal_path = Registry.wal_path(segment_id)
    File.mkdir_p!(Path.dirname(wal_path))

    # Torn mid-record: the record is discarded entirely — the consumed
    # offset cannot advance while the park is lost (they are one record).
    File.write!(wal_path, binary_part(effect, 0, byte_size(effect) - 3))
    pid = start_writer(segment_id)
    _ = WalWriter.last_persisted_seq(pid)
    assert Flezha.Dlq.Store.list(rc) == []
    assert InputLog.watermark(rc) == nil
    :ok = WalWriter.close(pid)
    File.rm!(wal_path)
    File.rm(Registry.outbox_dedup_path(segment_id))

    # Complete record: park AND offset recover together.
    File.write!(wal_path, effect)
    pid = start_writer(segment_id)
    _ = WalWriter.last_persisted_seq(pid)

    assert wait_until(fn -> Flezha.Dlq.Store.list(rc) != [] end, 2_000),
           "park never materialized from the re-drive"

    assert InputLog.watermark(rc) == 9
    :ok = WalWriter.close(pid)
  end

  test "crash-tear: a torn effect record recovers all-or-nothing", %{
    rc: rc,
    segment_id: segment_id
  } do
    ClassRegistry.put_overwatch(rc, self())

    # Build the on-disk bytes of record 0 (plain) + record 1 (effect).
    plain = WalWriter.encode_record(0, plain_header(), "base")
    body = effect_body([output("k", "torn?")], 9)
    effect = WalWriter.encode_record(1, effect_header(), body)

    wal_path = Registry.wal_path(segment_id)
    File.mkdir_p!(Path.dirname(wal_path))

    # Kill the controller at every byte boundary inside the effect append.
    for cut <- 1..(byte_size(effect) - 1) do
      File.write!(wal_path, plain <> binary_part(effect, 0, cut))

      pid = start_writer(segment_id)

      assert WalWriter.last_persisted_seq(pid) == 0,
             "cut=#{cut}: torn effect record must be discarded entirely"

      refute_receive {:"$gen_cast", {:io_out, _, _}}, 10
      :ok = WalWriter.close(pid)
      # The writer truncated the torn tail; reset the fixture for the next cut.
      File.rm!(wal_path)
      File.rm(Registry.outbox_dedup_path(segment_id))
    end

    # The complete append recovers change + outputs + offset together.
    File.write!(wal_path, plain <> effect)
    pid = start_writer(segment_id)
    assert WalWriter.last_persisted_seq(pid) == 1

    assert_receive {:"$gen_cast", {:io_out, "torn?", md}}, 2_000
    assert md["flz.seq"] == "1.0"
    assert InputLog.watermark(rc) == 9
    :ok = WalWriter.close(pid)
  end
end

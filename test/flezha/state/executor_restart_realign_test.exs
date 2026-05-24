defmodule Flezha.State.ExecutorRestartRealignTest do
  @moduledoc """
  §16 — Per-Partition WAL realignment on executor restart.

  Reproduces the production durability defect: after an executor pod restarts
  and rebuilds its state from Kafka, it reconnects under a NEW executor id and
  resumes emitting Changes from a fresh (lower) seq base. The controller's
  `WalWriter` — still alive across the restart, with `last_persisted_seq` at the
  pre-restart high-water mark — drops every incoming Change as a duplicate
  (`seq <= last_persisted_seq`), so the WAL freezes forever while Kafka keeps
  producing: `dedup_drops_total` climbs, no `seq_gap` fires (the seq regressed,
  it did not jump), and the Watchdog's `output_stall` resync cannot help because
  it would push the controller's stale snapshot back onto the freshly-rebuilt
  executor.

  The fix (`Flezha.State.WalWriter`): a Change from a *different* executor than
  the one that wrote the current epoch realigns the WAL to the new executor's
  seq base — the rebuilt executor is the live source of truth — instead of
  freezing. A same-executor low seq stays a legitimate dedup.
  """

  use ExUnit.Case, async: false

  alias Flezha.Executor.StreamChannel
  alias Flezha.State.{Metrics, Snapshot, Supervisor, TestHelpers, WalWriter, Wire}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    for spec <- [
          {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
          Supervisor,
          Flezha.State.FsyncBatcher
        ] do
      case start_supervised(spec) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    partition_id = TestHelpers.sample_partition_id("realign_" <> uniq())
    {:ok, _} = Supervisor.find_or_start(partition_id)
    {:ok, writer} = Supervisor.lookup(partition_id)
    header = TestHelpers.stub_header()

    parent = self()
    gap_handler = attach([:flezha, :state, :seq_gap], parent, :seq_gap)
    realign_handler = attach([:flezha, :state, :wal_realign], parent, :wal_realign)

    on_exit(fn ->
      :telemetry.detach(gap_handler)
      :telemetry.detach(realign_handler)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{partition_id: partition_id, writer: writer, header: header, parent: parent}
  end

  test "an executor restart realigns the frozen WAL to the new executor's seq base", ctx do
    # The pre-restart executor advances the WAL to seq 5.
    old = start_channel("exec_old")
    feed(old, ctx, 1..5)
    assert WalWriter.last_persisted_seq(ctx.writer) == 5

    # A pre-restart snapshot exists; realignment must discard it so a later
    # RestoreInitState ships only the new epoch (not stale snapshot + new WAL).
    {:ok, _} = Snapshot.write_snapshot(ctx.partition_id, 3, ctx.header, "old-snap")
    assert Snapshot.latest_snapshot(ctx.partition_id) != nil

    # The executor pod restarts: a NEW executor id reconnects and, after its
    # Kafka rebuild, resumes from a fresh low seq base (1, 2, 3...).
    new = start_channel("exec_new")
    feed(new, ctx, 1..3)

    # Realignment fires once on the first regressed Change, adopting the new
    # executor's epoch as the WAL's source of truth.
    assert_receive {:wal_realign, %{from_seq: 5, to_seq: 1}, meta}, 1_000
    assert meta.relay == "TestRelay"
    assert meta.executor == "exec_new"

    # The WAL is no longer frozen at 5: it follows the new executor down to 3,
    # then keeps advancing on the new epoch.
    assert WalWriter.last_persisted_seq(ctx.writer) == 3
    feed(new, ctx, 4..6)
    assert WalWriter.last_persisted_seq(ctx.writer) == 6

    # The stale pre-restart snapshot was discarded by the realignment.
    assert Snapshot.latest_snapshot(ctx.partition_id) == nil

    # The regression was healed by realignment — not mistaken for a forward gap.
    refute_received {:seq_gap, _, _}
  end

  test "a same-executor duplicate is still deduped (no spurious realign)", ctx do
    same = start_channel("exec_same")
    feed(same, ctx, 1..5)

    # The SAME executor resends an already-persisted seq: a legitimate
    # retransmit, deduped — not a restart, so the WAL must not realign.
    feed(same, ctx, [3])

    refute_received {:wal_realign, _, _}
    assert WalWriter.last_persisted_seq(ctx.writer) == 5
    assert Metrics.get(Metrics.for_partition(ctx.partition_id), :dedup_drops_total) >= 1
  end

  # ---------- helpers ----------

  defp feed(channel, ctx, seqs) do
    Enum.each(seqs, fn seq ->
      send(
        channel,
        {:flz_data, 0, "partition_state_change",
         frame(ctx.partition_id, seq, ctx.header, "s#{seq}"), %{}}
      )
    end)

    # Barrier: a synchronous call drains the channel mailbox (FIFO), so every
    # frame's synchronous WalWriter.append has completed before we assert.
    _ = GenServer.call(channel, :stream_ref_status)
    :ok
  end

  defp start_channel(executor_id) do
    name = :"sc_#{uniq()}"
    {:ok, pid} = StreamChannel.start_link({executor_id, name, :primary})
    pid
  end

  defp frame({rc, sh, pk}, seq, header, body) do
    pid_bytes = Wire.encode_partition_id(rc, sh, pk)

    <<byte_size(pid_bytes)::16-big, pid_bytes::binary, seq::64-big, header::binary,
      byte_size(body)::32-big, body::binary, :erlang.crc32(body)::32-big>>
  end

  defp attach(event, parent, tag) do
    id = {__MODULE__, tag, System.unique_integer([:positive])}
    :telemetry.attach(id, event, fn _e, meas, meta, _ -> send(parent, {tag, meas, meta}) end, nil)
    id
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end

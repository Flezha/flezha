defmodule Flezha.State.ExecutorRestartRealignTest do
  @moduledoc """
  §16 — Per-Segment WAL realignment on executor restart.

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

  Owner fencing (`wal-owner-fencing`): a realign is honoured only from the
  Segment's current owner binding. A session superseded by a newest-wins
  takeover is fenced at its StreamChannel (unbind-old before bind-new) and,
  for late in-flight residue, refused at the WalWriter — never realigned on.
  """

  use ExUnit.Case, async: false

  alias Flezha.Executor.{FleetManager, StreamChannel}
  alias Flezha.State.{Metrics, Snapshot, Supervisor, TestHelpers, WalWriter, Wire}
  alias Flezha.Transport.LivenessCounters

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

    segment_id = TestHelpers.sample_segment_id("realign_" <> uniq())
    {:ok, _} = Supervisor.find_or_start(segment_id)
    {:ok, writer} = Supervisor.lookup(segment_id)
    header = TestHelpers.stub_header()

    parent = self()
    gap_handler = attach([:flezha, :state, :seq_gap], parent, :seq_gap)
    realign_handler = attach([:flezha, :state, :wal_realign], parent, :wal_realign)
    stale_handler = attach([:flezha, :state, :stale_writer_refused], parent, :stale_refused)

    on_exit(fn ->
      :telemetry.detach(gap_handler)
      :telemetry.detach(realign_handler)
      :telemetry.detach(stale_handler)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{segment_id: segment_id, writer: writer, header: header, parent: parent}
  end

  test "an executor restart realigns the frozen WAL to the new executor's seq base", ctx do
    # The pre-restart executor advances the WAL to seq 5.
    old = start_channel("exec_old")
    feed(old, ctx, 0..4)
    assert WalWriter.last_persisted_seq(ctx.writer) == 4

    # A pre-restart snapshot exists; realignment must discard it so a later
    # RestoreInitState ships only the new epoch (not stale snapshot + new WAL).
    {:ok, _} = Snapshot.write_snapshot(ctx.segment_id, 3, ctx.header, "old-snap")
    assert Snapshot.latest_snapshot(ctx.segment_id) != nil

    # The executor pod restarts: a NEW executor id reconnects and, after its
    # Kafka rebuild, resumes from a fresh low seq base (1, 2, 3...).
    new = start_channel("exec_new")
    feed(new, ctx, 0..2)

    # Realignment fires once on the first regressed Change, adopting the new
    # executor's epoch as the WAL's source of truth.
    assert_receive {:wal_realign, %{from_seq: 4, to_seq: 0}, meta}, 1_000
    assert meta.operative == "TestOperative"
    assert meta.executor == "exec_new"

    # The WAL is no longer frozen at 5: it follows the new executor down to 3,
    # then keeps advancing on the new epoch.
    assert WalWriter.last_persisted_seq(ctx.writer) == 2
    feed(new, ctx, 3..5)
    assert WalWriter.last_persisted_seq(ctx.writer) == 5

    # The stale pre-restart snapshot was discarded by the realignment.
    assert Snapshot.latest_snapshot(ctx.segment_id) == nil

    # The regression was healed by realignment — not mistaken for a forward gap.
    refute_received {:seq_gap, _, _}
  end

  test "a same-executor duplicate is still deduped (no spurious realign)", ctx do
    same = start_channel("exec_same")
    feed(same, ctx, 0..4)

    # The SAME executor resends an already-persisted seq: a legitimate
    # retransmit, deduped — not a restart, so the WAL must not realign.
    feed(same, ctx, [3])

    refute_received {:wal_realign, _, _}
    assert WalWriter.last_persisted_seq(ctx.writer) == 4
    assert Metrics.get(Metrics.for_segment(ctx.segment_id), :dedup_drops_total) >= 1
  end

  test "a superseded session's divergent frames refuse at the writer instead of realigning",
       ctx do
    owner_id = "exec_owner_" <> uniq()
    stale_id = supersede("exec_stale_" <> uniq())

    owner = start_channel(owner_id)
    feed(owner, ctx, 0..4)
    assert WalWriter.last_persisted_seq(ctx.writer) == 4

    # The wedged-channel case: the superseded session's frames still reach the
    # writer (this channel was never flipped), so the writer-side gate must
    # refuse the divergent seq base rather than adopt the stale lineage.
    stale = start_channel(stale_id)
    feed(stale, ctx, 0..2)

    assert_receive {:stale_refused, %{count: 1}, meta}, 1_000
    assert meta.session == stale_id
    refute_received {:wal_realign, _, _}
    assert WalWriter.last_persisted_seq(ctx.writer) == 4

    # The owning session's path is untouched by the fence — it keeps advancing
    # with no added per-record work (its frames never consult the fence).
    feed(owner, ctx, 5..6)
    assert WalWriter.last_persisted_seq(ctx.writer) == 6
    refute_received {:seq_gap, _, _}
  end

  test "stale refusal is episode-bounded: one event, the counter tracks every refusal", ctx do
    owner = start_channel("exec_owner_" <> uniq())
    feed(owner, ctx, 0..2)

    stale_id = supersede("exec_stale_" <> uniq())
    stale = start_channel(stale_id)
    feed(stale, ctx, 0..3)
    assert WalWriter.last_persisted_seq(ctx.writer) == 2

    assert_receive {:stale_refused, %{count: 1}, %{session: ^stale_id}}, 1_000
    refute_receive {:stale_refused, _, _}, 200
    assert Metrics.get(Metrics.for_segment(ctx.segment_id), :stale_writer_refused_total) == 4
  end

  test "a fenced channel refuses every durable-write forward at the binding", ctx do
    owner_id = "exec_owner_" <> uniq()
    owner = start_channel(owner_id)
    feed(owner, ctx, 0..4)

    # Fencing OTHER sessions adds nothing to the owning session's path.
    _ = supersede("exec_unrelated_" <> uniq())
    feed(owner, ctx, 5..6)
    assert WalWriter.last_persisted_seq(ctx.writer) == 6
    refute_received {:stale_refused, _, _}

    old_id = supersede("exec_old_" <> uniq())
    old = start_channel(old_id)
    assert :ok = GenServer.call(old, :fence_write_path)

    # Changes die at the binding — the writer never sees them.
    feed(old, ctx, 7..8)
    assert WalWriter.last_persisted_seq(ctx.writer) == 6
    assert_receive {:stale_refused, %{count: 1}, %{session: ^old_id}}, 1_000

    # Snapshot and delete forwards are refused too: no snap file appears and
    # the Segment's writer/WAL survive.
    send(
      old,
      {:flz_data, 0, "segment_state_snapshot", frame(ctx.segment_id, 6, ctx.header, "snap"), %{}}
    )

    send(old, {:flz_data, 0, "delete_segment_state", encode_segment_id(ctx.segment_id), %{}})
    _ = GenServer.call(old, :stream_ref_status)

    assert Snapshot.latest_snapshot(ctx.segment_id) == nil
    assert Process.alive?(ctx.writer)

    # An undecodable frame is still counted, with no episode event.
    send(old, {:flz_data, 0, "segment_state_change", <<1, 2, 3>>, %{}})
    _ = GenServer.call(old, :stream_ref_status)
    assert channel_refused_count(old_id) == 5
    refute_receive {:stale_refused, _, _}, 100
  end

  test "takeover race: in-flight old-session Changes never land past the unbind; the new owner realigns cleanly",
       ctx do
    old_id = "exec_old_" <> uniq()
    new_id = "exec_new_" <> uniq()

    old = start_channel(old_id)
    feed(old, ctx, 0..4)

    # Frames forwarded before the unbind are legitimate owner-era Changes;
    # the synchronous read also barriers the writer past them.
    feed(old, ctx, 5..6)
    assert WalWriter.last_persisted_seq(ctx.writer) == 6

    # The takeover fence in `resolve_stable_identity` order: mark superseded,
    # flip the channel — unbind-old strictly before bind-new.
    supersede(old_id)
    assert :ok = GenServer.call(old, :fence_write_path)

    # In-flight frames crossing the swap: refused at the binding.
    feed(old, ctx, 7..9)
    assert WalWriter.last_persisted_seq(ctx.writer) == 6
    assert_receive {:stale_refused, %{count: 1}, %{session: ^old_id}}, 1_000

    # Bind-new: the owner's divergent seq base realigns exactly as before.
    new = start_channel(new_id)
    feed(new, ctx, 0..2)
    assert_receive {:wal_realign, %{from_seq: 6, to_seq: 0}, %{executor: ^new_id}}, 1_000
    assert WalWriter.last_persisted_seq(ctx.writer) == 2

    # Wedged-channel residue hitting the writer directly after the swap must
    # not drag the WAL back onto the superseded lineage.
    WalWriter.append_async(ctx.writer, 42, ctx.header, "zombie", old_id)
    assert WalWriter.last_persisted_seq(ctx.writer) == 2
    assert_receive {:stale_refused, %{count: 1}, %{session: ^old_id}}, 1_000

    # Exactly one realign for the whole episode, and the new owner advances.
    feed(new, ctx, 3..4)
    assert WalWriter.last_persisted_seq(ctx.writer) == 4
    refute_received {:wal_realign, _, _}
  end

  # ---------- helpers ----------

  # Mark + register cleanup — the table is owned by the app's FleetManager and
  # outlives this test otherwise.
  defp supersede(session_id) do
    FleetManager.mark_session_superseded(session_id)

    on_exit(fn ->
      if :ets.whereis(:flezha_superseded_sessions) != :undefined do
        :ets.delete(:flezha_superseded_sessions, session_id)
      end
    end)

    session_id
  end

  defp channel_refused_count(executor_id) do
    LivenessCounters.snapshot_all().channels
    |> Enum.find_value(0, fn
      %{executor_id: ^executor_id, stale_writer_refused: n} -> n
      _ -> nil
    end)
  end

  defp encode_segment_id({rc, sh, pk}), do: Wire.encode_segment_id(rc, sh, pk)

  defp feed(channel, ctx, seqs) do
    Enum.each(seqs, fn seq ->
      send(
        channel,
        {:flz_data, 0, "segment_state_change", frame(ctx.segment_id, seq, ctx.header, "s#{seq}"),
         %{}}
      )
    end)

    # Barrier: a synchronous call drains the channel mailbox (FIFO), so every
    # frame has been handed to the WalWriter; the later synchronous WalWriter
    # read is itself queued behind those async appends (writer FIFO).
    _ = GenServer.call(channel, :stream_ref_status)
    :ok
  end

  defp start_channel(executor_id) do
    name = :"sc_#{uniq()}"
    {:ok, pid} = StreamChannel.start_link({executor_id, name, :primary})
    pid
  end

  defp frame({rc, sh, pk}, seq, header, body) do
    pid_bytes = Wire.encode_segment_id(rc, sh, pk)

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

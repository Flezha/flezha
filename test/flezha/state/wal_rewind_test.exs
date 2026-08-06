defmodule Flezha.State.WalRewindTest do
  @moduledoc """
  The non-destructive rewind primitive (`unbounded-late-input-correction` G2)
  and the event-time baseline it rewinds to.

  The distinction from `realign_epoch/4` is the whole point: a realign is a
  cold-takeover wipe (WAL truncated to zero, every snapshot deleted), while a
  rewind keeps the history at or below a boundary the controller means to
  rebuild. These tests pin that difference, plus the two properties a correction
  rests on — the epoch is explicit (so a re-run cannot double-bump) and the
  input-log watermark falls back with the records that reported it.
  """
  use ExUnit.Case, async: false

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{FsyncBatcher, InputLog, Registry, Supervisor, TestHelpers, WalWriter, Wire}

  setup do
    base = TestHelpers.tmp_app_data_dir()
    ClassRegistry.ensure_table()

    for child <- [
          {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
          Supervisor,
          FsyncBatcher
        ] do
      case start_supervised(child) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    rc = "rw_" <> Integer.to_string(System.unique_integer([:positive]))
    entity_id = {rc, "proc", "k1"}

    on_exit(fn ->
      case Supervisor.lookup(entity_id) do
        {:ok, pid} -> WalWriter.close(pid)
        :error -> :ok
      end

      Application.delete_env(:flezha, :state_root)
      _ = File.rm_rf(base)
    end)

    %{rc: rc, entity_id: entity_id, base: base}
  end

  defp effect_header(), do: <<1, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0>>

  # One invocation: an input consumed at `offset`, whose event time is `event_time`.
  defp append_effect(pid, seq, offset, event_time) do
    body =
      Wire.encode_effect_body(%{
        changes: [<<1>>],
        outputs: [],
        consumed_offset: offset,
        dlq: nil,
        event_time: event_time
      })

    :ok = WalWriter.append(pid, seq, effect_header(), body)
  end

  # A candle stream: seq N consumes input offset N and carries event time N*100.
  defp seed(pid, seqs) do
    for seq <- seqs, do: append_effect(pid, seq, seq, seq * 100)
    WalWriter.fsync(pid)
  end

  defp wal_seqs(entity_id) do
    path = Registry.wal_path(entity_id)
    {:ok, device} = :file.open(path, [:raw, :read, :binary])

    try do
      collect(device, 0, [])
    after
      :file.close(device)
    end
  end

  defp collect(device, position, acc) do
    case WalWriter.decode_record(device, position) do
      {:ok, seq, _h, _b, consumed} -> collect(device, position + consumed, [seq | acc])
      {:error, _} -> Enum.reverse(acc)
    end
  end

  describe "rewind_to/3" do
    test "keeps the records at or below the boundary and drops the rest", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      seed(pid, 0..5)

      :ok = WalWriter.rewind_to(pid, 3, 1)

      assert wal_seqs(ctx.entity_id) == [0, 1, 2, 3]
      assert WalWriter.last_persisted_seq(pid) == 3
    end

    test "drops only the snapshots above the boundary — a realign drops them all", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      seed(pid, 0..5)

      # Two baselines: one below the boundary, one above it.
      for seq <- [2, 5] do
        {:ok, _path} =
          Flezha.State.Snapshot.write_snapshot(
            ctx.entity_id,
            seq,
            <<0::unit(8)-size(16)>>,
            <<seq>>
          )
      end

      :ok = WalWriter.rewind_to(pid, 3, 1)

      # The snapshot at 2 is precisely what a restore to seq 3 needs. Deleting it
      # (as a realign would) would leave the Entity with no baseline to rebuild
      # from — a wipe wearing a rewind's name.
      assert [{2, _}] = Registry.snapshot_paths(ctx.entity_id)
    end

    test "bumps to the epoch it is given, so a re-run cannot double-bump", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      seed(pid, 0..5)
      assert WalWriter.outbox_epoch(pid) == 0

      :ok = WalWriter.rewind_to(pid, 3, 1)
      assert WalWriter.outbox_epoch(pid) == 1

      # A correction that crashed after the rewind and re-runs asserts the SAME
      # epoch. A second bump would strand the first attempt's rows at an epoch
      # nothing ever overwrites.
      :ok = WalWriter.rewind_to(pid, 3, 1)
      assert WalWriter.outbox_epoch(pid) == 1
    end

    test "lowers the input-log watermark with the records that reported it", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      seed(pid, 0..5)

      # What the fsync path reports once the effect records are durable.
      InputLog.record_consumed(ctx.rc, ctx.entity_id, 5)
      assert InputLog.watermark(ctx.rc) == 5

      :ok = WalWriter.rewind_to(pid, 3, 1)

      # Offsets 4 and 5 are no longer consumed by anyone — the records that
      # claimed them are gone. Left high, the next retention sweep would truncate
      # away the very inputs the correction is about to replay.
      assert InputLog.watermark(ctx.rc) == 3
    end
  end

  describe "correction_baseline/2" do
    test "rewinds to the record before the first one at or after the target", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      seed(pid, 0..5)

      # A trade at event time 250 belongs between seq 2 (200) and seq 3 (300).
      assert {:ok, %{seq: 2, consumed_offset: 2}} = WalWriter.correction_baseline(pid, 250)
    end

    test "folds an out-of-order straggler into the replay", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      # Event times 100, 200, 300, then a beyond-window straggler at 150 that was
      # processed as an arrival, then 400.
      append_effect(pid, 0, 0, 100)
      append_effect(pid, 1, 1, 200)
      append_effect(pid, 2, 2, 300)
      append_effect(pid, 3, 3, 150)
      append_effect(pid, 4, 4, 400)
      WalWriter.fsync(pid)

      # Target 250. Scanning BACKWARDS for the first earlier event time would stop
      # at seq 3 (150) and leave seq 2 (300) — the record that overtook the late
      # input — in place, correcting nothing. Scanning forward stops before the
      # first record at or after 250, which is seq 2, so the baseline is seq 1 and
      # the straggler gets re-ordered along with everything else.
      assert {:ok, %{seq: 1, consumed_offset: 1}} = WalWriter.correction_baseline(pid, 250)
    end

    test "refuses when nothing is at or after the target", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      seed(pid, 0..5)

      # Later than every record: the input is not actually late for this Entity.
      assert {:error, :nothing_to_correct} = WalWriter.correction_baseline(pid, 10_000)
    end

    test "refuses when the target precedes the whole history", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      seed(pid, 0..5)

      # Every record is at or after the target, so there is no baseline to keep —
      # the rewind would have to go below the retained history.
      assert {:error, :nothing_to_correct} = WalWriter.correction_baseline(pid, 0)
    end
  end

  describe "consumed_offset_at/2" do
    test "is the input-log offset an operator replay re-forwards from", ctx do
      {:ok, pid} = Supervisor.find_or_start(ctx.entity_id)
      seed(pid, 0..5)

      assert WalWriter.consumed_offset_at(pid, 3) == 3
      assert WalWriter.consumed_offset_at(pid, 0) == 0
    end
  end
end

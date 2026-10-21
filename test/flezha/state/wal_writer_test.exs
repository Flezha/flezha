defmodule Flezha.State.WalWriterTest do
  # async: false: writes Application env; a shared on-disk root; the primary Logger.
  use FlezhaTest.StateCase, async: false

  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.State.Frame
  alias FlezhaTest.Helper.NodeTestHelper

  import ExUnit.CaptureLog

  alias Flezha.State.{Lineage, Metrics, Paths, RecordFrame, SeqIndex, Supervisor, TestHelpers}
  alias Flezha.State.WriterRegistry
  alias Flezha.State.WalWriter
  alias Flezha.State.Snapshot, as: StateSnapshot
  alias Flezha.State.WalWriter.State, as: WriterState

  setup do
    %{cell_id: TestHelpers.sample_cell_id("p_" <> uniq())}
  end

  describe "encode/decode round-trip" do
    test "encode_record produces a byte-identical, crc-valid record", %{} do
      header = TestHelpers.stub_header()
      bytes = Frame.encode_record(7, header, "hello")

      assert <<7::64-big, len::32-big, ^header::binary, "hello", crc::32-big>> = bytes
      assert len == 16 + byte_size("hello")
      assert crc == :erlang.crc32(<<header::binary, "hello">>)
    end

    test "decode_record/2 reads the same record back", %{base: base} do
      path = Path.join(base, "round_trip.bin")
      header = TestHelpers.stub_header()
      bytes = Frame.encode_record(42, header, "payload-bytes")
      File.write!(path, bytes)
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])

      assert {:ok, 42, ^header, "payload-bytes", consumed} = RecordFrame.decode_record(dev, 0)
      assert consumed == byte_size(bytes)

      :file.close(dev)
    end

    test "decode_record/2 detects crc mismatch", %{base: base} do
      path = Path.join(base, "corrupt.bin")
      header = TestHelpers.stub_header()
      record = Frame.encode_record(1, header, "x")
      prefix_size = byte_size(record) - 1
      <<prefix::binary-size(^prefix_size), _last>> = record

      File.write!(path, prefix <> <<0>>)
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])

      # The framed extent rides the error: it is what decides whether the
      # failure is a torn tail or a record with history behind it.
      assert {:error, :crc_mismatch, extent} = RecordFrame.decode_record(dev, 0)
      assert extent == byte_size(record)
      :file.close(dev)
    end

    test "the truncation proof answers only what it can prove", %{} do
      # Design §3, as a table. `remaining` is the byte count from the failure
      # point to the end of the file.
      assert Frame.repairable?(:short_read, nil, 900)
      assert Frame.repairable?(:crc_mismatch, 36, 36)
      refute Frame.repairable?(:crc_mismatch, 36, 72)
      assert Frame.repairable?(:bad_len, nil, 31)
      refute Frame.repairable?(:bad_len, nil, 32)

      # An I/O error is not a proof of anything, so it is never a repair.
      refute Frame.repairable?(:ebadf, nil, 0)
    end

    test "a reader with no channel for a refusal raises instead of reading short",
         %{base: base} do
      path = Path.join(base, "no_channel.bin")
      header = TestHelpers.stub_header()

      File.write!(
        path,
        Frame.encode_record(0, header, "a") <>
          Frame.encode_record(1, header, "b") <>
          Frame.encode_record(2, header, "c")
      )

      # The middle record: a corrupt LAST record is the torn tail the fold is
      # allowed to stop at, and would not raise.
      [_first, second, _third] = TestHelpers.record_offsets(path)
      TestHelpers.corrupt_record_body(path, second)

      capture_log(fn ->
        assert_raise RuntimeError, ~r/durable record refused \(crc_mismatch\)/, fn ->
          RecordFrame.fold_path!(path, [], fn {seq, _h, _b}, acc -> {:cont, [seq | acc]} end)
        end
      end)
    end

    test "decode_record/2 detects short read", %{base: base} do
      path = Path.join(base, "short.bin")
      File.write!(path, <<1::64-big, 100::32-big>>)
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])

      assert {:error, :short_read, nil} = RecordFrame.decode_record(dev, 0)
      :file.close(dev)
    end
  end

  describe "append" do
    test "appends sequential records and updates metrics", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      # Per-Cell seq is 0-based (the SDK's change_seq starts at 0).
      assert :ok = WalWriter.append(srv, 0, header, "a")
      assert :ok = WalWriter.append(srv, 1, header, "bb")
      assert :ok = WalWriter.append(srv, 2, header, "ccc")
      assert WalWriter.last_persisted_seq(srv) == 2

      m = Metrics.to_map(Metrics.for_cell(pid))
      assert m.persisted_changes_total == 3
      assert m.wal_seq == 2
      assert m.persisted_bytes_total > 0
    end

    test "rejects duplicate seq with :duplicate", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()

      assert :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "x")
      assert :duplicate = WalWriter.append(srv, 0, TestHelpers.stub_header(), "x")
      assert Metrics.get(Metrics.for_cell(pid), :dedup_drops_total) == 1
    end

    test "detects gaps", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      assert :ok = WalWriter.append(srv, 0, header, "a")
      assert {:error, {:gap, expected: 1, got: 5}} = WalWriter.append(srv, 5, header, "z")
    end
  end

  describe "awaiting baseline (empty-WAL gap)" do
    setup do
      # The loop plans the pull; here the test stands in for it and reads what
      # the writer declares.
      become_loop()
      :ok
    end

    test "a gap flood sheds every frame and declares one gap per episode", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      for seq <- 5001..5050 do
        assert :awaiting_baseline = WalWriter.append(srv, seq, header, "d", "exec-cold")
      end

      assert_receive {:declare, :seq_gap, ^pid,
                      %{trigger: :baseline_seed, runner_id: "exec-cold"}},
                     1_000

      refute_receive {:declare, :seq_gap, ^pid, _}, 50

      assert WalWriter.last_persisted_seq(srv) == -1
      assert Metrics.get(Metrics.for_cell(pid), :wal_shed_total) == 50
    end

    test "the pulled snapshot establishes the baseline, closes the episode, and appends resume",
         %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      assert :awaiting_baseline = WalWriter.append(srv, 5001, header, "d1", "exec-cold")
      assert_receive {:declare, :seq_gap, ^pid, _}, 1_000

      # The CompactCell round-trip lands as a snapshot at the runner's seq.
      assert {:ok, _path} = StateSnapshot.write_snapshot(pid, 5005, header, "full-state")

      assert WalWriter.last_persisted_seq(srv) == 5005
      assert_receive {:declare, :seq_gap_closed, ^pid, %{}}, 1_000

      assert :ok = WalWriter.append(srv, 5006, header, "d3", "exec-cold")
      assert WalWriter.last_persisted_seq(srv) == 5006

      # A healed Cell declares nothing more.
      assert :ok = WalWriter.append(srv, 5007, header, "d4", "exec-cold")
      refute_receive {:declare, :seq_gap, ^pid, _}, 50
    end
  end

  describe "a snapshot ahead of the WAL" do
    setup do
      become_loop()
      :ok
    end

    # Fails as: a restore ships the snapshot's seq, the runner resumes past it,
    # and the writer refuses every Change as a gap for good.
    test "a writer reopened over a newer snapshot resumes from the snapshot's seq",
         %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      :ok = WalWriter.append(srv, 0, header, "a")
      {:ok, _} = StateSnapshot.write_snapshot(pid, 50, header, "full-state")
      # Closed and booted again from disk, as a controller restart leaves it:
      # nothing yet names its owning runner.
      TestHelpers.close_and_await(srv, pid)
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()

      assert WalWriter.last_persisted_seq(srv) == 50
      assert :ok = WalWriter.append(srv, 51, header, "b")
    end

    test "a snapshot past an open gap closes it and appends resume", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      assert :ok = WalWriter.append(srv, 0, header, "a", "exec-1")
      WalWriter.append_async(srv, 7, header, "z", "exec-1")
      assert_receive {:declare, :seq_gap, ^pid, _}, 1_000

      assert {:ok, _} = StateSnapshot.write_snapshot(pid, 30, header, "full-state")
      assert_receive {:declare, :seq_gap_closed, ^pid, %{}}, 1_000
      assert WalWriter.last_persisted_seq(srv) == 30
      assert :ok = WalWriter.append(srv, 31, header, "b", "exec-1")
    end
  end

  describe "scan_for_last_valid_seq" do
    test "recovers last seq across a re-open", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 0, header, "a")
      :ok = WalWriter.append(srv, 1, header, "b")
      :ok = WalWriter.append(srv, 2, header, "c")

      TestHelpers.close_and_await(srv, pid)
      # Drop the dedup file so the writer must rebuild from the WAL.
      File.rm(Paths.outbox_dedup_path(pid))

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv2} = Supervisor.lookup(pid) |> ok!()
      assert WalWriter.last_persisted_seq(srv2) == 2
    end

    test "a corrupt record mid-WAL is refused, and the records after it stay on disk",
         %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      for n <- 0..3, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      TestHelpers.close_and_await(srv, pid)

      path = Paths.wal_path(pid)
      # The third record: the last seq that verifies before it is 1, which is
      # what a scan that stopped there would have reported as the WAL's seq.
      [_zero, _one, corrupt, after_corrupt] = TestHelpers.record_offsets(path)
      size = RecordFrame.file_size(path)
      TestHelpers.corrupt_record_body(path, corrupt)

      # A value no scan produces: it survives only if the refused open reported
      # no seq at all. The size assertion below is on the same counters ref, so
      # it is the proof this one is reading the ref the writer writes.
      metrics = Metrics.for_cell(pid)
      Metrics.put(metrics, :wal_seq, -1)

      handler =
        :telemetry_test.attach_event_handlers(self(), [
          [:flezha, :state, :record_frame, :refused]
        ])

      log =
        capture_log(fn ->
          {:ok, _} = Supervisor.find_or_start(pid)
          {:ok, srv2} = Supervisor.lookup(pid) |> ok!()

          # The writer is alive and refusing, not restarting into the same file.
          assert Process.alive?(srv2)

          assert {:error, {:crc_mismatch, 1, ^corrupt}} =
                   WalWriter.append(srv2, 4, header, "x4")
        end)

      assert_receive {[:flezha, :state, :record_frame, :refused], ^handler, %{count: 1},
                      %{path: ^path, reason: :crc_mismatch, last_seq: 1, offset: ^corrupt}}

      :telemetry.detach(handler)

      assert log =~ ~r/record_frame_refused .*reason=crc_mismatch/
      assert log =~ path

      # Not one byte gone, and the record after the corrupt one still decodes.
      assert RecordFrame.file_size(path) == size
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])
      assert {:ok, 3, ^header, "x3", _} = RecordFrame.decode_record(dev, after_corrupt)
      :file.close(dev)

      assert Metrics.get(metrics, :wal_size_bytes) == size
      assert Metrics.get(metrics, :wal_seq) == -1
    end

    # fails when: a writer whose open refused the file never read the Cell's
    # dedup and lineage files, yet writes its defaults over them at close, so
    # the Cell loses its epoch and folded offset (epoch 0 written).
    test "a refused writer's close leaves the dedup and lineage files byte-identical",
         %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      for n <- 0..3, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      TestHelpers.close_and_await(srv, pid)

      progress = %{last_persisted_seq: 3, outbox_flushed_seq: 2, epoch: 5, folded_offset: 9}
      :ok = Lineage.put_dedup(pid, progress)
      dedup = File.read!(Paths.outbox_dedup_path(pid))
      lineage = File.read(Paths.lineage_path(pid))

      path = Paths.wal_path(pid)
      [_zero, _one, corrupt, _after_corrupt] = TestHelpers.record_offsets(path)
      TestHelpers.corrupt_record_body(path, corrupt)

      capture_log(fn ->
        {:ok, _} = Supervisor.find_or_start(pid)
        {:ok, srv2} = Supervisor.lookup(pid) |> ok!()
        assert {:error, {:crc_mismatch, 1, ^corrupt}} = WalWriter.append(srv2, 4, header, "x4")
        TestHelpers.close_and_await(srv2, pid)
      end)

      assert File.read!(Paths.outbox_dedup_path(pid)) == dedup
      assert File.read(Paths.lineage_path(pid)) == lineage
    end

    # fails when: a writer opens a Cell's files while a landing still writes
    # them, so it scans a half-landed WAL and loads no lineage record.
    test "a writer started while a landing holds the Cell's files opens after it",
         %{cell_id: pid} do
      test_pid = self()

      spawn_link(fn ->
        WriterRegistry.with_files(pid, fn ->
          send(test_pid, {:holding, self()})
          receive do: (:release -> :ok)
        end)
      end)

      assert_receive {:holding, holder}
      {:ok, srv} = Supervisor.find_or_start(pid)
      assert wait_until(fn -> awaiting_files?(srv) end, 2_000)
      refute File.exists?(Paths.wal_path(pid))

      send(holder, :release)
      _ = :sys.get_state(srv)
      assert File.exists?(Paths.wal_path(pid))
      TestHelpers.close_and_await(srv, pid)
    end

    # fails when: a booting writer does not wait for the Cell's file lock, so it
    # answers an append while a cut still holds the Cell's files.
    test "an append waits for a killed lock holder to exit, then lands in the WAL",
         %{cell_id: pid} do
      test_pid = self()

      spawn(fn ->
        WriterRegistry.with_files(pid, fn ->
          send(test_pid, :holding)
          receive do: (:release -> :ok)
        end)
      end)

      assert_receive :holding
      {:ok, holder} = WriterRegistry.lookup({:cell_files, pid})
      on_exit(fn -> Process.exit(holder, :kill) end)
      {:ok, srv} = Supervisor.find_or_start(pid)
      append = Task.async(fn -> WalWriter.append(srv, 0, TestHelpers.stub_header(), "a") end)
      assert wait_until(fn -> awaiting_files?(srv) end, 2_000)
      assert Task.yield(append, 0) == nil

      Process.exit(holder, :kill)
      assert :ok = Task.await(append)

      seqs = RecordFrame.fold_path!(Paths.wal_path(pid), [], &{:cont, [elem(&1, 0) | &2]})
      assert seqs == [0]
      TestHelpers.close_and_await(srv, pid)
    end

    test "a rewind refuses rather than cutting at a corrupt record below its boundary",
         %{cell_id: pid} do
      {:ok, srv} = Supervisor.find_or_start(pid)
      header = TestHelpers.stub_header()
      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

      path = Paths.wal_path(pid)
      [_first, second | _] = TestHelpers.record_offsets(path)
      size = RecordFrame.file_size(path)

      # The bit flips under a live writer, below the seq the rewind cuts at:
      # `cut_above/4` keeps `[0, bound)`, so a boundary taken at the corrupt
      # record would delete the valid records between it and seq 3.
      TestHelpers.corrupt_record_body(path, second)

      assert capture_log(fn ->
               assert {:error, {:crc_mismatch, 0, ^second}} = WalWriter.rewind_to(srv, 3, 1)
             end) =~ "[flezha_state] record_frame_refused"

      assert RecordFrame.file_size(path) == size
    end

    test "a prefix cut refuses rather than cutting past a corrupt record",
         %{cell_id: pid} do
      {:ok, srv} = Supervisor.find_or_start(pid)
      header = TestHelpers.stub_header()
      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

      path = Paths.wal_path(pid)
      [_first, second | _] = TestHelpers.record_offsets(path)
      size = RecordFrame.file_size(path)
      TestHelpers.corrupt_record_body(path, second)

      # `cut_below/5`'s `@spec` has always promised an error its caller never
      # matched; a refused boundary is the second way it answers one.
      assert capture_log(fn ->
               assert {:error, {:crc_mismatch, 0, ^second}} = WalWriter.truncate_up_to(srv, 3)
             end) =~ "[flezha_state] record_frame_refused"

      assert RecordFrame.file_size(path) == size

      # The refusal stopped the writer serving, so the next append is refused
      # without walking the file again.
      assert {:error, {:crc_mismatch, 0, ^second}} = WalWriter.append(srv, 6, header, "x6")
    end

    test "a cut whose file went away answers the error its @spec always promised",
         %{cell_id: pid} do
      {:ok, srv} = Supervisor.find_or_start(pid)
      header = TestHelpers.stub_header()
      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

      # A cut queued behind its directory's removal: `cut_below/5` has always
      # documented this answer and its caller matched only `{:ok, …}`/`:none`.
      File.rm!(Paths.wal_path(pid))

      assert {:error, :enoent} = WalWriter.truncate_up_to(srv, 3)

      # A missing file is not a refused record: the writer keeps serving.
      assert :ok = WalWriter.append(srv, 6, header, "x6")
    end

    test "a repaired torn tail is reported with the byte count it discarded",
         %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 0, header, "a")
      TestHelpers.close_and_await(srv, pid)

      path = Paths.wal_path(pid)
      kept = RecordFrame.file_size(path)
      File.write!(path, <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>, [:append])

      handler =
        :telemetry_test.attach_event_handlers(self(), [
          [:flezha, :state, :record_frame, :repaired]
        ])

      # A repair is a warning, and the suite's primary logger level is `:error`.
      restore_level = NodeTestHelper.override_log_level(:warning)
      on_exit(restore_level)

      log =
        capture_log(fn ->
          {:ok, srv2} = Supervisor.find_or_start(pid)
          assert WalWriter.last_persisted_seq(srv2) == 0
        end)

      assert_receive {[:flezha, :state, :record_frame, :repaired], ^handler, %{bytes: 5},
                      %{path: ^path, reason: :short_read, last_seq: 0, offset: ^kept}}

      :telemetry.detach(handler)
      assert log =~ ~r/record_frame_repaired .*reason=short_read.* bytes=5/
      assert RecordFrame.file_size(path) == kept
    end

    test "truncates partial-write tail past last valid record", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 0, header, "a")
      :ok = WalWriter.append(srv, 1, header, "bb")
      TestHelpers.close_and_await(srv, pid)

      # Append 5 garbage bytes that look like a partial record.
      File.write!(Paths.wal_path(pid), <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>, [:append])

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv2} = Supervisor.lookup(pid) |> ok!()
      assert WalWriter.last_persisted_seq(srv2) == 1

      # File should be truncated back to the end of the last valid record.
      {:ok, info} = :file.read_file_info(Paths.wal_path(pid))
      r1 = Frame.encode_record(0, header, "a")
      r2 = Frame.encode_record(1, header, "bb")
      assert elem(info, 1) == byte_size(r1) + byte_size(r2)
    end
  end

  describe "truncate_up_to" do
    test "drops records with seq <= target", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

      :ok = WalWriter.truncate_up_to(srv, 3)

      # File now contains records 4 and 5 only.
      {:ok, dev} = :file.open(Paths.wal_path(pid), [:raw, :read, :binary])
      assert {:ok, 4, _, "x4", c1} = RecordFrame.decode_record(dev, 0)
      assert {:ok, 5, _, "x5", _} = RecordFrame.decode_record(dev, c1)
      :file.close(dev)
    end

    # fails when: an index whose positions drifted from the file makes a
    # compaction cut mid-record, and the Cell's next restore is refused.
    test "a stale index never cuts mid-record", %{cell_id: pid} do
      {:ok, srv} = Supervisor.find_or_start(pid)
      header = TestHelpers.stub_header()
      for n <- 0..299, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      TestHelpers.close_and_await(srv, pid)

      path = Paths.wal_path(pid)
      offsets = TestHelpers.record_offsets(path)
      ends = tl(offsets) ++ [RecordFrame.file_size(path)]

      # Every sample one byte past its record.
      stale =
        offsets
        |> Enum.zip(ends)
        |> Enum.with_index()
        |> Enum.reduce(SeqIndex.new(1), fn {{from, to}, seq}, index ->
          SeqIndex.note(index, seq, to - from)
        end)

      {:ok, device} = :file.open(path, [:raw, :read, :write, :binary])
      assert {:ok, _cut, file, _index} = RecordFrame.cut_below(path, device, stale, 261)
      :file.close(file)

      assert TestHelpers.record_seqs(path) == {:ok, Enum.to_list(261..299)}
    end
  end

  describe "non-blocking init" do
    test "init/1 returns instantly with the disk scan deferred to handle_continue",
         %{cell_id: pid} do
      # Run in a child so init's `trap_exit` flag doesn't leak onto the test process.
      assert {:ok, %WriterState{file_handle: nil}, {:continue, :open}} =
               Task.async(fn -> WalWriter.init(pid) end) |> Task.await()
    end

    test "an append issued immediately after start lands after the deferred scan",
         %{cell_id: pid} do
      header = TestHelpers.stub_header()

      # Pre-seed a WAL up to seq 2 so the deferred scan has records to recover.
      wal_path = Paths.wal_path(pid)
      File.mkdir_p!(Path.dirname(wal_path))

      File.write!(
        wal_path,
        Frame.encode_record(0, header, "a") <>
          Frame.encode_record(1, header, "b") <>
          Frame.encode_record(2, header, "c")
      )

      {:ok, srv} = Supervisor.find_or_start(pid)
      # Cast the next contiguous change before handle_continue can have run.
      :ok = WalWriter.append_async(srv, 3, header, "d")

      # The call queues behind the continue and the cast, proving ordering: the
      # scan recovered seq 2 and the queued append advanced it to 3 with no loss.
      assert WalWriter.last_persisted_seq(srv) == 3

      # And it is durable: a fresh writer recovers seq 3 from the WAL on disk.
      TestHelpers.close_and_await(srv, pid)
      File.rm(Paths.outbox_dedup_path(pid))
      {:ok, srv2} = Supervisor.find_or_start(pid)
      assert WalWriter.last_persisted_seq(srv2) == 3
    end

    test "writers for distinct cells are independent processes", %{cell_id: pid} do
      other = TestHelpers.sample_cell_id("p_" <> uniq())

      {:ok, a} = Supervisor.find_or_start(pid)
      {:ok, b} = Supervisor.find_or_start(other)

      assert a != b
      assert Process.alive?(a) and Process.alive?(b)
      assert {:ok, ^a} = Supervisor.lookup(pid)
      assert {:ok, ^b} = Supervisor.lookup(other)

      header = TestHelpers.stub_header()
      assert :ok = WalWriter.append(a, 0, header, "a")
      assert :ok = WalWriter.append(b, 0, header, "b")
    end
  end

  describe "hard in-memory bound (shed)" do
    # R2c: a WalWriter that cannot drain must not let its mailbox / un-fsynced
    # bytes grow without bound. Past the hard bound, async appends are shed
    # (dropped + counted) and the resulting seq gap drives resync.
    test "sheds appends past the hard bound, declaring the gap that drives the resync",
         %{cell_id: pid} do
      cap = 50
      Application.put_env(:flezha, :wal_pending_msg_cap, cap)
      become_loop()

      on_exit(fn -> Application.delete_env(:flezha, :wal_pending_msg_cap) end)

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      # Baseline at seq 0 (synchronous, so handle_continue has run).
      assert :ok = WalWriter.append(srv, 0, header, "a")

      # Freeze the writer, then flood its mailbox so the cast handler — once it
      # resumes — sees a backlog far past the cap.
      :sys.suspend(srv)
      k = 500
      for n <- 1..k, do: WalWriter.append_async(srv, n, header, "x")

      {:message_queue_len, qlen} = Process.info(srv, :message_queue_len)
      assert qlen >= k

      :sys.resume(srv)

      # A sync call drains behind every queued cast.
      last = WalWriter.last_persisted_seq(srv)

      shed = Metrics.get(Metrics.for_cell(pid), :wal_shed_total)
      assert shed > 0
      # The WAL never advanced to k — shed Changes were dropped, not written.
      assert last < k
      # The shed gap is declared once for the episode; the loop plans the
      # resync (the SDK is authoritative for cell state).
      assert_receive {:declare, :seq_gap, ^pid, %{trigger: :shed}}, 1_000
    end
  end

  describe "batched append" do
    # The writer marks the application's batcher dirty, so every case freezes it
    # (`in_one_window`) to hold the records un-synced for as long as it needs.
    test "appends landing in one window share one fsync and answer only after it",
         %{cell_id: id} do
      {:ok, srv} = Supervisor.find_or_start(id)
      header = TestHelpers.stub_header()
      before = Metrics.get(Metrics.for_cell(id), :fsync_total)

      reqids =
        TestHelpers.in_one_window(fn ->
          reqids =
            Enum.reduce(0..9, :gen_server.reqids_new(), fn i, reqids ->
              WalWriter.append_next_async(srv, header, "r#{i}", [sync: :batched], reqids, i)
            end)

          # Written, and nobody is answered before the sync.
          assert WalWriter.last_persisted_seq(srv) == 9
          assert :timeout = :gen_server.wait_response(reqids, 50, true)
          reqids
        end)

      assert Enum.sort(collect(reqids)) == for(i <- 0..9, do: {i, {:ok, i}})
      assert Metrics.get(Metrics.for_cell(id), :fsync_total) == before + 1
    end

    test "a prefix cut answers the appends it made durable", %{cell_id: id} do
      {:ok, srv} = Supervisor.find_or_start(id)
      header = TestHelpers.stub_header()

      TestHelpers.in_one_window(fn ->
        reqids =
          WalWriter.append_next_async(
            srv,
            header,
            "a",
            [sync: :batched],
            :gen_server.reqids_new(),
            :a
          )

        assert :timeout = :gen_server.wait_response(reqids, 20, true)
        :ok = WalWriter.truncate_up_to(srv, 0)
        assert {{:reply, {:ok, 0}}, :a, _} = :gen_server.wait_response(reqids, 1_000, true)
      end)
    end

    test "a clean close answers the appends it holds", %{cell_id: id} do
      {:ok, srv} = Supervisor.find_or_start(id)
      header = TestHelpers.stub_header()

      TestHelpers.in_one_window(fn ->
        reqids =
          WalWriter.append_next_async(
            srv,
            header,
            "a",
            [sync: :batched],
            :gen_server.reqids_new(),
            :a
          )

        assert :timeout = :gen_server.wait_response(reqids, 20, true)
        :ok = WalWriter.close(srv)
        assert {{:reply, {:ok, 0}}, :a, _} = :gen_server.wait_response(reqids, 1_000, true)
      end)
    end

    # The kill leaves a supervisor crash report; captured, and the transient
    # restart it announces is awaited so the report lands inside the capture.
    @tag capture_log: true
    test "a writer that dies before the sync fails the waiter", %{cell_id: id} do
      {:ok, srv} = Supervisor.find_or_start(id)
      header = TestHelpers.stub_header()

      TestHelpers.in_one_window(fn ->
        reqids =
          WalWriter.append_next_async(
            srv,
            header,
            "a",
            [sync: :batched],
            :gen_server.reqids_new(),
            :a
          )

        assert :timeout = :gen_server.wait_response(reqids, 20, true)
        Process.exit(srv, :kill)
        assert {{:error, {:killed, ^srv}}, :a, _} = :gen_server.wait_response(reqids, 1_000, true)
      end)

      assert wait_until(
               fn -> match?({:ok, pid} when pid != srv, Supervisor.lookup(id)) end,
               1_000
             ),
             "a crashed writer is restarted"
    end

    defp collect(reqids) do
      case :gen_server.wait_response(reqids, 2_000, true) do
        :no_request -> []
        {{:reply, reply}, label, reqids} -> [{label, reply} | collect(reqids)]
      end
    end
  end

  describe "header dedup" do
    test "a keyed append refuses a header still in the file, until a cut releases it",
         %{cell_id: id} do
      {:ok, srv} = Supervisor.find_or_start(id)
      h1 = <<1::128>>
      h2 = <<2::128>>

      assert {:ok, 0} = WalWriter.append_next(srv, h1, "a", dedup: true)
      assert {:duplicate, 0} = WalWriter.append_next(srv, h1, "a again", dedup: true)
      assert {:ok, 1} = WalWriter.append_next(srv, h2, "b", dedup: true)
      assert WalWriter.last_persisted_seq(srv) == 1

      # The index is rebuilt from the file by a reopened writer.
      TestHelpers.close_and_await(srv, id)
      {:ok, srv} = Supervisor.find_or_start(id)
      assert {:duplicate, 1} = WalWriter.append_next(srv, h2, "b again", dedup: true)

      # The file is the window: a cut releases the header.
      :ok = WalWriter.truncate_up_to(srv, 0)
      assert {:ok, 2} = WalWriter.append_next(srv, h1, "a anew", dedup: true)
      assert {:duplicate, 1} = WalWriter.append_next(srv, h2, "b still", dedup: true)
    end
  end

  defp ok!({:ok, x}), do: {:ok, x}

  # Module-agnostic: once a test stubs WriterRegistry, Mimic runs its code
  # under a renamed module for the rest of the run.
  defp awaiting_files?(srv) do
    match?({:current_function, {_, :await_files, 1}}, Process.info(srv, :current_function))
  end

  describe "close_writers/1" do
    test "stops every live writer of the class and leaves its files", %{cell_id: cell_id} do
      {class, _, _} = cell_id
      {:ok, pid} = Supervisor.find_or_start(cell_id)

      :ok =
        WalWriter.append_next(pid, TestHelpers.stub_header(), "one")
        |> then(fn {:ok, _} -> :ok end)

      assert pid in Supervisor.live_writers(class)

      :ok = Supervisor.close_writers(class)

      refute Process.alive?(pid)
      # The writer is registered in an Elixir `Registry`, which unregisters a
      # dead process on its own `:DOWN` — so it leaves the list after it dies,
      # not with it.
      assert wait_until(fn -> Supervisor.live_writers(class) == [] end, 2_000)
      assert File.exists?(Paths.wal_path(cell_id))
    end

    test "a writer another closer already stopped counts as closed", %{cell_id: cell_id} do
      {:ok, pid} = Supervisor.find_or_start(cell_id)
      :ok = WalWriter.close(pid)

      assert WalWriter.close(pid) == :ok
    end
  end
end

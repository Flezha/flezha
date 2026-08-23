defmodule Flezha.State.WalWriterTest do
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.State.{Metrics, Registry, Supervisor, TestHelpers, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    # Bring up the registry + dyn-supervisor that `WalWriter` uses for naming.
    TestHelpers.start_state_tree()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base, entity_id: TestHelpers.sample_entity_id("p_" <> uniq())}
  end

  describe "encode/decode round-trip" do
    test "encode_record produces a byte-identical, crc-valid record", %{} do
      header = TestHelpers.stub_header()
      bytes = WalWriter.encode_record(7, header, "hello")

      assert <<7::64-big, len::32-big, ^header::binary, "hello", crc::32-big>> = bytes
      assert len == 16 + byte_size("hello")
      assert crc == :erlang.crc32(<<header::binary, "hello">>)
    end

    test "decode_record/2 reads the same record back", %{base: base} do
      path = Path.join(base, "round_trip.bin")
      header = TestHelpers.stub_header()
      bytes = WalWriter.encode_record(42, header, "payload-bytes")
      File.write!(path, bytes)
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])

      assert {:ok, 42, ^header, "payload-bytes", consumed} = WalWriter.decode_record(dev, 0)
      assert consumed == byte_size(bytes)

      :file.close(dev)
    end

    test "decode_record/2 detects crc mismatch", %{base: base} do
      path = Path.join(base, "corrupt.bin")
      header = TestHelpers.stub_header()
      record = WalWriter.encode_record(1, header, "x")
      prefix_size = byte_size(record) - 1
      <<prefix::binary-size(^prefix_size), _last>> = record

      File.write!(path, prefix <> <<0>>)
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])

      assert {:error, :crc_mismatch} = WalWriter.decode_record(dev, 0)
      :file.close(dev)
    end

    test "decode_record/2 detects short read", %{base: base} do
      path = Path.join(base, "short.bin")
      File.write!(path, <<1::64-big, 100::32-big>>)
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])

      assert {:error, :short_read} = WalWriter.decode_record(dev, 0)
      :file.close(dev)
    end
  end

  describe "append" do
    test "appends sequential records and updates metrics", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      # Per-Entity seq is 0-based (the SDK's change_seq starts at 0).
      assert :ok = WalWriter.append(srv, 0, header, "a")
      assert :ok = WalWriter.append(srv, 1, header, "bb")
      assert :ok = WalWriter.append(srv, 2, header, "ccc")
      assert WalWriter.last_persisted_seq(srv) == 2

      m = Metrics.to_map(Metrics.for_entity(pid))
      assert m.persisted_changes_total == 3
      assert m.wal_seq == 2
      assert m.persisted_bytes_total > 0
    end

    test "rejects duplicate seq with :duplicate", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()

      assert :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "x")
      assert :duplicate = WalWriter.append(srv, 0, TestHelpers.stub_header(), "x")
      assert Metrics.get(Metrics.for_entity(pid), :dedup_drops_total) == 1
    end

    test "detects gaps", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      assert :ok = WalWriter.append(srv, 0, header, "a")
      assert {:error, {:gap, expected: 1, got: 5}} = WalWriter.append(srv, 5, header, "z")
    end
  end

  describe "awaiting baseline (empty-WAL gap)" do
    @retry_interval_ms 200

    setup do
      parent = self()
      Application.put_env(:flezha, :wal_resync_retry_interval_ms, @retry_interval_ms)

      # The pull fails to dispatch, as a torn control channel would make it.
      Application.put_env(:flezha, :state_resync_fun, fn entity_id, opts ->
        send(parent, {:resync_called, entity_id, opts})
        {:error, :no_control_channel}
      end)

      on_exit(fn ->
        Application.delete_env(:flezha, :state_resync_fun)
        Application.delete_env(:flezha, :wal_resync_retry_interval_ms)
      end)

      :ok
    end

    test "a gap flood sheds every frame but pulls at most once per interval", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      for seq <- 5001..5050 do
        assert :awaiting_baseline = WalWriter.append(srv, seq, header, "d", "exec-cold")
      end

      assert_receive {:resync_called, ^pid, opts}, 1_000
      assert opts[:reason] == :baseline_seed
      assert opts[:runner_id] == "exec-cold"
      refute_receive {:resync_called, _, _}, 50

      assert WalWriter.last_persisted_seq(srv) == -1
      assert Metrics.get(Metrics.for_entity(pid), :wal_shed_total) == 50
    end

    test "a pull that failed to dispatch is retried once the interval elapses", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      assert :awaiting_baseline = WalWriter.append(srv, 5001, header, "d1", "exec-cold")
      assert_receive {:resync_called, ^pid, _}, 1_000

      # Nothing but the pulled snapshot clears the wait, so the next gap frame
      # after the interval must drive another pull rather than shed forever.
      Process.sleep(@retry_interval_ms + 20)
      assert :awaiting_baseline = WalWriter.append(srv, 5002, header, "d2", "exec-cold")
      assert_receive {:resync_called, ^pid, opts}, 1_000
      assert opts[:reason] == :baseline_seed
    end

    test "the pulled snapshot establishes the baseline and appends resume", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      assert :awaiting_baseline = WalWriter.append(srv, 5001, header, "d1", "exec-cold")
      assert_receive {:resync_called, ^pid, _}, 1_000

      # The CompactEntity round-trip lands as a snapshot at the runner's seq.
      assert {:ok, _path} = Flezha.State.Snapshot.write_snapshot(pid, 5005, header, "full-state")

      assert WalWriter.last_persisted_seq(srv) == 5005
      assert :ok = WalWriter.append(srv, 5006, header, "d3", "exec-cold")
      assert WalWriter.last_persisted_seq(srv) == 5006

      # A healed Entity stops pulling.
      Process.sleep(@retry_interval_ms + 20)
      assert :ok = WalWriter.append(srv, 5007, header, "d4", "exec-cold")
      refute_receive {:resync_called, _, _}, 50
    end
  end

  describe "scan_for_last_valid_seq" do
    test "recovers last seq across a re-open", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 0, header, "a")
      :ok = WalWriter.append(srv, 1, header, "b")
      :ok = WalWriter.append(srv, 2, header, "c")

      TestHelpers.close_and_await(srv, pid)
      # Drop the dedup file so the writer must rebuild from the WAL.
      File.rm(Registry.outbox_dedup_path(pid))

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv2} = Supervisor.lookup(pid) |> ok!()
      assert WalWriter.last_persisted_seq(srv2) == 2
    end

    test "truncates partial-write tail past last valid record", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 0, header, "a")
      :ok = WalWriter.append(srv, 1, header, "bb")
      TestHelpers.close_and_await(srv, pid)

      # Append 5 garbage bytes that look like a partial record.
      File.write!(Registry.wal_path(pid), <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>, [:append])

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv2} = Supervisor.lookup(pid) |> ok!()
      assert WalWriter.last_persisted_seq(srv2) == 1

      # File should be truncated back to the end of the last valid record.
      {:ok, info} = :file.read_file_info(Registry.wal_path(pid))
      r1 = WalWriter.encode_record(0, header, "a")
      r2 = WalWriter.encode_record(1, header, "bb")
      assert elem(info, 1) == byte_size(r1) + byte_size(r2)
    end
  end

  describe "truncate_up_to" do
    test "drops records with seq <= target", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

      :ok = WalWriter.truncate_up_to(srv, 3)

      # File now contains records 4 and 5 only.
      {:ok, dev} = :file.open(Registry.wal_path(pid), [:raw, :read, :binary])
      assert {:ok, 4, _, "x4", c1} = WalWriter.decode_record(dev, 0)
      assert {:ok, 5, _, "x5", _} = WalWriter.decode_record(dev, c1)
      :file.close(dev)
    end
  end

  describe "non-blocking init" do
    test "init/1 returns instantly with the disk scan deferred to handle_continue",
         %{entity_id: pid} do
      # Run in a child so init's `trap_exit` flag doesn't leak onto the test process.
      assert {:ok, %WalWriter{file_handle: nil}, {:continue, :open}} =
               Task.async(fn -> WalWriter.init(pid) end) |> Task.await()
    end

    test "an append issued immediately after start lands after the deferred scan",
         %{entity_id: pid} do
      header = TestHelpers.stub_header()

      # Pre-seed a WAL up to seq 2 so the deferred scan has records to recover.
      wal_path = Registry.wal_path(pid)
      File.mkdir_p!(Path.dirname(wal_path))

      File.write!(
        wal_path,
        WalWriter.encode_record(0, header, "a") <>
          WalWriter.encode_record(1, header, "b") <>
          WalWriter.encode_record(2, header, "c")
      )

      {:ok, srv} = Supervisor.find_or_start(pid)
      # Cast the next contiguous change before handle_continue can have run.
      :ok = WalWriter.append_async(srv, 3, header, "d")

      # The call queues behind the continue and the cast, proving ordering: the
      # scan recovered seq 2 and the queued append advanced it to 3 with no loss.
      assert WalWriter.last_persisted_seq(srv) == 3

      # And it is durable: a fresh writer recovers seq 3 from the WAL on disk.
      TestHelpers.close_and_await(srv, pid)
      File.rm(Registry.outbox_dedup_path(pid))
      {:ok, srv2} = Supervisor.find_or_start(pid)
      assert WalWriter.last_persisted_seq(srv2) == 3
    end

    test "writers for distinct entities are independent processes", %{entity_id: pid} do
      other = TestHelpers.sample_entity_id("p_" <> uniq())

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
    test "sheds appends past the hard bound, leaving a gap that drives resync",
         %{entity_id: pid} do
      cap = 50
      Application.put_env(:flezha, :wal_pending_msg_cap, cap)
      parent = self()

      Application.put_env(:flezha, :state_resync_fun, fn entity_id, opts ->
        send(parent, {:resync_called, entity_id, opts})
        {:ok, make_ref()}
      end)

      on_exit(fn ->
        Application.delete_env(:flezha, :wal_pending_msg_cap)
        Application.delete_env(:flezha, :state_resync_fun)
      end)

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

      shed = Metrics.get(Metrics.for_entity(pid), :wal_shed_total)
      assert shed > 0
      # The WAL never advanced to k — shed Changes were dropped, not written.
      assert last < k
      # The shed gap drove a resync (the SDK is authoritative for entity state).
      assert_receive {:resync_called, ^pid, _opts}, 1_000
    end
  end

  describe "batched append" do
    # The writer marks the application's batcher dirty, so every case freezes it
    # (`in_one_window`) to hold the records un-synced for as long as it needs.
    test "appends landing in one window share one fsync and answer only after it",
         %{entity_id: id} do
      {:ok, srv} = Supervisor.find_or_start(id)
      header = TestHelpers.stub_header()
      before = Metrics.get(Metrics.for_entity(id), :fsync_total)

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
      assert Metrics.get(Metrics.for_entity(id), :fsync_total) == before + 1
    end

    test "a prefix cut answers the appends it made durable", %{entity_id: id} do
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

    test "a clean close answers the appends it holds", %{entity_id: id} do
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
    test "a writer that dies before the sync fails the waiter", %{entity_id: id} do
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
         %{entity_id: id} do
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

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))

  describe "close_writers/1" do
    test "stops every live writer of the class and leaves its files", %{entity_id: entity_id} do
      {class, _, _} = entity_id
      {:ok, pid} = Supervisor.find_or_start(entity_id)

      :ok =
        WalWriter.append_next(pid, TestHelpers.stub_header(), "one")
        |> then(fn {:ok, _} -> :ok end)

      assert pid in Supervisor.live_writers(class)

      :ok = Supervisor.close_writers(class)

      refute Process.alive?(pid)
      assert Supervisor.live_writers(class) == []
      assert File.exists?(Registry.wal_path(entity_id))
    end
  end
end

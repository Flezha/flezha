defmodule Flezha.State.WalWriterTest do
  use ExUnit.Case, async: false

  alias Flezha.State.{Metrics, Registry, Supervisor, TestHelpers, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    # Bring up the registry + dyn-supervisor that `WalWriter` uses for naming.
    case start_supervised({Elixir.Registry, keys: :unique, name: Supervisor.registry_name()}) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(Supervisor) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(Flezha.State.FsyncBatcher) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base, partition_id: TestHelpers.sample_partition_id("p_" <> uniq())}
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
      <<prefix::binary-size(prefix_size), _last>> = record

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
    test "appends sequential records and updates metrics", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      # Per-Partition seq is 0-based (the SDK's change_seq starts at 0).
      assert :ok = WalWriter.append(srv, 0, header, "a")
      assert :ok = WalWriter.append(srv, 1, header, "bb")
      assert :ok = WalWriter.append(srv, 2, header, "ccc")
      assert WalWriter.last_persisted_seq(srv) == 2

      m = Metrics.to_map(Metrics.for_partition(pid))
      assert m.persisted_changes_total == 3
      assert m.wal_seq == 2
      assert m.persisted_bytes_total > 0
    end

    test "rejects duplicate seq with :duplicate", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()

      assert :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "x")
      assert :duplicate = WalWriter.append(srv, 0, TestHelpers.stub_header(), "x")
      assert Metrics.get(Metrics.for_partition(pid), :dedup_drops_total) == 1
    end

    test "detects gaps", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      assert :ok = WalWriter.append(srv, 0, header, "a")
      assert {:error, {:gap, expected: 1, got: 5}} = WalWriter.append(srv, 5, header, "z")
    end
  end

  describe "scan_for_last_valid_seq" do
    test "recovers last seq across a re-open", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 0, header, "a")
      :ok = WalWriter.append(srv, 1, header, "b")
      :ok = WalWriter.append(srv, 2, header, "c")

      WalWriter.close(srv)
      # Drop the dedup file so the writer must rebuild from the WAL.
      File.rm(Registry.outbox_dedup_path(pid))

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv2} = Supervisor.lookup(pid) |> ok!()
      assert WalWriter.last_persisted_seq(srv2) == 2
    end

    test "truncates partial-write tail past last valid record", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 0, header, "a")
      :ok = WalWriter.append(srv, 1, header, "bb")
      WalWriter.close(srv)

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
    test "drops records with seq <= target", %{partition_id: pid} do
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
         %{partition_id: pid} do
      # Run in a child so init's `trap_exit` flag doesn't leak onto the test process.
      assert {:ok, %WalWriter{file_handle: nil}, {:continue, :open}} =
               Task.async(fn -> WalWriter.init(pid) end) |> Task.await()
    end

    test "an append issued immediately after start lands after the deferred scan",
         %{partition_id: pid} do
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
      WalWriter.close(srv)
      File.rm(Registry.outbox_dedup_path(pid))
      {:ok, srv2} = Supervisor.find_or_start(pid)
      assert WalWriter.last_persisted_seq(srv2) == 3
    end

    test "writers for distinct partitions are independent processes", %{partition_id: pid} do
      other = TestHelpers.sample_partition_id("p_" <> uniq())

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
         %{partition_id: pid} do
      cap = 50
      Application.put_env(:flezha, :wal_pending_msg_cap, cap)
      parent = self()

      Application.put_env(:flezha, :state_resync_fun, fn partition_id, opts ->
        send(parent, {:resync_called, partition_id, opts})
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

      shed = Metrics.get(Metrics.for_partition(pid), :wal_shed_total)
      assert shed > 0
      # The WAL never advanced to k — shed Changes were dropped, not written.
      assert last < k
      # The shed gap drove a resync (the SDK is authoritative for partition state).
      assert_receive {:resync_called, ^pid, _opts}, 1_000
    end
  end

  defp ok!({:ok, x}), do: {:ok, x}

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end

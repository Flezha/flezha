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

      assert :ok = WalWriter.append(srv, 1, header, "a")
      assert :ok = WalWriter.append(srv, 2, header, "bb")
      assert :ok = WalWriter.append(srv, 3, header, "ccc")
      assert WalWriter.last_persisted_seq(srv) == 3

      m = Metrics.to_map(Metrics.for_partition(pid))
      assert m.persisted_changes_total == 3
      assert m.wal_seq == 3
      assert m.persisted_bytes_total > 0
    end

    test "rejects duplicate seq with :duplicate", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()

      assert :ok = WalWriter.append(srv, 1, TestHelpers.stub_header(), "x")
      assert :duplicate = WalWriter.append(srv, 1, TestHelpers.stub_header(), "x")
      assert Metrics.get(Metrics.for_partition(pid), :dedup_drops_total) == 1
    end

    test "detects gaps", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      assert :ok = WalWriter.append(srv, 1, header, "a")
      assert {:error, {:gap, expected: 2, got: 5}} = WalWriter.append(srv, 5, header, "z")
    end
  end

  describe "scan_for_last_valid_seq" do
    test "recovers last seq across a re-open", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 1, header, "a")
      :ok = WalWriter.append(srv, 2, header, "b")
      :ok = WalWriter.append(srv, 3, header, "c")

      WalWriter.close(srv)
      # Drop the dedup file so the writer must rebuild from the WAL.
      File.rm(Registry.outbox_dedup_path(pid))

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv2} = Supervisor.lookup(pid) |> ok!()
      assert WalWriter.last_persisted_seq(srv2) == 3
    end

    test "truncates partial-write tail past last valid record", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()
      :ok = WalWriter.append(srv, 1, header, "a")
      :ok = WalWriter.append(srv, 2, header, "bb")
      WalWriter.close(srv)

      # Append 5 garbage bytes that look like a partial record.
      File.write!(Registry.wal_path(pid), <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>, [:append])

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv2} = Supervisor.lookup(pid) |> ok!()
      assert WalWriter.last_persisted_seq(srv2) == 2

      # File should be truncated back to the end of seq 2's record.
      {:ok, info} = :file.read_file_info(Registry.wal_path(pid))
      r1 = WalWriter.encode_record(1, header, "a")
      r2 = WalWriter.encode_record(2, header, "bb")
      assert elem(info, 1) == byte_size(r1) + byte_size(r2)
    end
  end

  describe "truncate_up_to" do
    test "drops records with seq <= target", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid) |> ok!()
      header = TestHelpers.stub_header()

      for n <- 1..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

      :ok = WalWriter.truncate_up_to(srv, 3)

      # File now contains records 4 and 5 only.
      {:ok, dev} = :file.open(Registry.wal_path(pid), [:raw, :read, :binary])
      assert {:ok, 4, _, "x4", c1} = WalWriter.decode_record(dev, 0)
      assert {:ok, 5, _, "x5", _} = WalWriter.decode_record(dev, c1)
      :file.close(dev)
    end
  end

  defp ok!({:ok, x}), do: {:ok, x}

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end

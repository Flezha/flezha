defmodule Flezha.Boot.WalTransferTest do
  use ExUnit.Case, async: false

  alias Flezha.Boot.WalTransfer
  alias Flezha.State.{TestHelpers, FsyncBatcher, Supervisor, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base}
  end

  test "query returns :none for an unknown partition" do
    assert :none = WalTransfer.query({"NoRelay", "shard-0", "p-x"})
  end

  test "query returns last_persisted_seq when WAL exists" do
    pid = TestHelpers.sample_partition_id("wal_q_" <> Integer.to_string(System.unique_integer([:positive])))
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.find_or_start(pid)

    :ok = WalWriter.append(srv, 1, TestHelpers.stub_header(), "x")
    :ok = WalWriter.append(srv, 2, TestHelpers.stub_header(), "y")
    FsyncBatcher.flush_now()

    assert {:ok, info} = WalTransfer.query(pid)
    assert info.last_persisted_seq >= 2
    assert is_binary(info.wal_path)
  end

  test "stream copies a file with fsync + atomic rename", %{base: base} do
    src = Path.join(base, "src.bin")
    dst = Path.join(base, "subdir/dst.bin")
    File.write!(src, :crypto.strong_rand_bytes(200_000))

    assert :ok = WalTransfer.stream(src, dst)
    assert File.exists?(dst)
    refute File.exists?(dst <> ".transferring")
    assert File.read!(src) == File.read!(dst)
  end

  test "stream cleans up .transferring on source error", %{base: base} do
    src = Path.join(base, "missing.bin")
    dst = Path.join(base, "dst.bin")

    assert {:error, _} = WalTransfer.stream(src, dst)
    refute File.exists?(dst)
    refute File.exists?(dst <> ".transferring")
  end

  test "stream over the empty file produces an empty destination", %{base: base} do
    src = Path.join(base, "empty.bin")
    dst = Path.join(base, "empty_dst.bin")
    File.write!(src, "")

    assert :ok = WalTransfer.stream(src, dst)
    assert File.read!(dst) == ""
  end
end

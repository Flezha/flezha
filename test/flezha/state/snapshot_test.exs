defmodule Flezha.State.SnapshotTest do
  # async: false: writes Application env; a shared on-disk root; node-global tables.
  use ExUnit.Case, async: false

  alias Flezha.State.{Paths, RecordFrame, Snapshot, Supervisor, TestHelpers, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    TestHelpers.start_state_tree()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base, entity_id: TestHelpers.sample_entity_id("snap_" <> uniq())}
  end

  test "write_snapshot round-trip", %{entity_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, path} = Snapshot.write_snapshot(pid, 100, header, "state-blob")
    assert File.exists?(path)
    assert {:ok, ^header, "state-blob"} = Snapshot.read_snapshot(path)
  end

  test "write_snapshot truncates the WAL up to the snapshot's seq", %{entity_id: pid} do
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.lookup(pid)
    header = TestHelpers.stub_header()

    for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

    {:ok, _} = Snapshot.write_snapshot(pid, 3, header, "snap@3")

    # WAL retains records 4, 5.
    {:ok, dev} = :file.open(Paths.wal_path(pid), [:raw, :read, :binary])
    assert {:ok, 4, _, "x4", c1} = RecordFrame.decode_record(dev, 0)
    assert {:ok, 5, _, "x5", c2} = RecordFrame.decode_record(dev, c1)
    assert {:error, _} = RecordFrame.decode_record(dev, c1 + c2)
    :file.close(dev)
  end

  test "latest_snapshot returns the highest-seq file", %{entity_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 10, header, "a")
    {:ok, _} = Snapshot.write_snapshot(pid, 100, header, "b")
    {:ok, _} = Snapshot.write_snapshot(pid, 50, header, "c")

    assert {100, path} = Snapshot.latest_snapshot(pid)
    assert String.ends_with?(path, ".snap.100")
  end

  test "atomic rename: stale .tmp does not appear as a snapshot", %{entity_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 7, header, "good")

    # Hand-craft a leftover .tmp like a crash mid-write would leave.
    dir = Paths.state_dir(pid)
    {_, _, key} = pid
    tmp = Path.join(dir, Paths.encode_entity_key(key) <> ".snap.99.tmp")
    File.write!(tmp, "garbage-not-a-snapshot")

    # Latest seq is still 7; the .tmp is filtered out.
    assert {7, _} = Snapshot.latest_snapshot(pid)
    assert %{snapshot_files: snaps} = Paths.scan_dir(pid)
    assert Enum.all?(snaps, fn {_, seq, _} -> seq == 7 end)
  end

  test "delete_snapshot removes only the named seq", %{entity_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 1, header, "a")
    {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "b")

    :ok = Snapshot.delete_snapshot(pid, 1)
    assert [{2, _}] = Paths.snapshot_paths(pid)
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end

defmodule Flezha.State.RestoreTest do
  use ExUnit.Case, async: false

  alias Flezha.State.{FsyncBatcher, Restore, Snapshot, Supervisor, TestHelpers, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

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

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base, partition_id: TestHelpers.sample_partition_id("rest_" <> uniq())}
  end

  test "cold partition: empty snapshot, empty change_log, terminal_seq=-1 (no records)", %{partition_id: pid} do
    assert {:ok, payload} = Restore.assemble_payload(pid)
    assert payload.snapshot_bytes == <<>>
    assert payload.change_log_bytes == <<>>
    assert payload.terminal_seq_hint == -1
    refute Restore.has_persisted_state?(pid)
  end

  test "snapshot only: change_log empty, terminal_seq matches snapshot seq", %{partition_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 50, header, "snap-bytes")

    assert {:ok, payload} = Restore.assemble_payload(pid)
    assert payload.snapshot_bytes == "snap-bytes"
    assert payload.change_log_bytes == <<>>
    assert payload.terminal_seq_hint == 50
    assert Restore.has_persisted_state?(pid)
  end

  test "snapshot + tail WAL: change_log carries seq > snapshot.seq only", %{partition_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.lookup(pid)

    for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

    {:ok, _} = Snapshot.write_snapshot(pid, 3, header, "snap@3")

    assert {:ok, payload} = Restore.assemble_payload(pid)
    assert payload.snapshot_bytes == "snap@3"
    assert payload.terminal_seq_hint == 5

    # change_log_bytes contains exactly seq 4 and seq 5 in WAL framing.
    expected =
      WalWriter.encode_record(4, header, "x4") <>
        WalWriter.encode_record(5, header, "x5")

    assert payload.change_log_bytes == expected
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end

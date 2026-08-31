defmodule Flezha.State.RestoreTest do
  # async: false: writes Application env; a shared on-disk root; node-global tables.
  use ExUnit.Case, async: false

  alias Flezha.State.{RecordFrame, Restore, Snapshot, Supervisor, TestHelpers, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    TestHelpers.start_state_tree()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base, entity_id: TestHelpers.sample_entity_id("rest_" <> uniq())}
  end

  test "cold entity: empty snapshot, empty change_log, terminal_seq=-1 (no records)", %{
    entity_id: pid
  } do
    assert {:ok, payload} = Restore.assemble_payload(pid)
    assert payload.snapshot_bytes == <<>>
    assert payload.snapshot_header == <<>>
    assert payload.change_log_bytes == <<>>
    assert payload.terminal_seq_hint == -1
    refute Restore.has_persisted_state?(pid)
  end

  test "snapshot only: change_log empty, terminal_seq matches snapshot seq", %{entity_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 50, header, "snap-bytes")

    assert {:ok, payload} = Restore.assemble_payload(pid)
    assert payload.snapshot_bytes == "snap-bytes"

    # The header travels with the body: it carries the schema_version the
    # snapshot was written at, which the SDK resolves before decoding.
    assert payload.snapshot_header == header
    assert byte_size(payload.snapshot_header) == 16
    assert payload.change_log_bytes == <<>>
    assert payload.terminal_seq_hint == 50
    assert Restore.has_persisted_state?(pid)
  end

  test "snapshot + tail WAL: change_log carries seq > snapshot.seq only", %{entity_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.lookup(pid)

    for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

    {:ok, _} = Snapshot.write_snapshot(pid, 3, header, "snap@3")

    assert {:ok, payload} = Restore.assemble_payload(pid)
    assert payload.snapshot_bytes == "snap@3"
    assert payload.snapshot_header == header
    assert payload.terminal_seq_hint == 5

    # change_log_bytes contains exactly seq 4 and seq 5 in WAL framing.
    expected =
      RecordFrame.encode_record(4, header, "x4") <>
        RecordFrame.encode_record(5, header, "x5")

    assert payload.change_log_bytes == expected
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end

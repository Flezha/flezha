defmodule FlezhaTest.State.CompensationMarkersTest do
  @moduledoc """
  §4.3 durable saga compensation records (controller half). A durable bounded
  operative ships `compensation_intent` / `compensation_done` frames; the stream
  channel persists them as per-Segment markers, and the bind-time restore path
  ships them back on `RestoreInitState` so a respawned driver finishes the
  remaining hooks and skips already-undone ones.
  """
  use ExUnit.Case, async: false

  import Mimic

  alias Flezha.Runner.{ControlChannel, StreamChannel}
  alias Flezha.Operative.Overwatch
  alias Flezha.State.{Registry, TestHelpers, Wire}

  setup :verify_on_exit!

  setup do
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    :ok
  end

  # Empty-body compensation wire frame (the record's information is the Segment
  # identity plus the payload-id), matching `Wire.decode_frame/1`: seq 0, a
  # zero header, no body (crc32 over an empty body is 0).
  defp comp_frame({rc, sh, pk}) do
    sid = Wire.encode_segment_id(rc, sh, pk)
    <<byte_size(sid)::16-big, sid::binary, 0::64-big, 0::128, 0::32-big, 0::32-big>>
  end

  defp eventually(fun, retries \\ 100) do
    cond do
      fun.() -> true
      retries == 0 -> false
      true -> Process.sleep(5) && eventually(fun, retries - 1)
    end
  end

  test "compensation_markers/1 reflects the persisted marker files" do
    seg = {"rc", "op", "seg"}
    assert Registry.compensation_markers(seg) == %{intent: false, done: false}

    File.mkdir_p!(Path.dirname(Registry.compensation_marker_path(seg, :intent)))
    File.write!(Registry.compensation_marker_path(seg, :intent), "")
    assert Registry.compensation_markers(seg) == %{intent: true, done: false}

    File.write!(Registry.compensation_marker_path(seg, :done), "")
    assert Registry.compensation_markers(seg) == %{intent: true, done: true}

    # delete_files/1 (the "remove every file for a Segment" contract) reclaims the
    # markers too, so a recreated Segment never sees a stale one.
    Registry.delete_files(seg)
    assert Registry.compensation_markers(seg) == %{intent: false, done: false}
  end

  test "the stream channel persists a marker for each compensation frame" do
    name = :"stream_comp_#{System.unique_integer([:positive])}"
    {:ok, pid} = StreamChannel.start_link({"exec-comp", name, :secondary})

    seg = {"comp_rc", "comp_op", "comp_seg"}
    frame = comp_frame(seg)

    send(pid, {:flz_data, 0, "compensation_intent", frame, %{}})
    intent_path = Registry.compensation_marker_path(seg, :intent)
    assert eventually(fn -> File.exists?(intent_path) end), "intent marker persisted"

    send(pid, {:flz_data, 0, "compensation_done", frame, %{}})
    done_path = Registry.compensation_marker_path(seg, :done)
    assert eventually(fn -> File.exists?(done_path) end), "done marker persisted"
  end

  test "bind-time restore ships the compensation markers back on RestoreInitState" do
    seg = {"ship_rc", "ship_op", "ship_seg"}
    # A durable Segment that completed forward and needs undo: WAL on disk plus an
    # intent marker (no done marker — its compensation had not run before the crash).
    wal = Registry.wal_path(seg)
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, TestHelpers.build_wal_record(1, <<>>))
    File.write!(Registry.compensation_marker_path(seg, :intent), "")

    test_pid = self()

    expect(ControlChannel, :send_control, fn "exec-ship", command ->
      send(test_pid, {:captured, command})
      {:ok, make_ref()}
    end)

    Overwatch.dispatch_restore_init_state("exec-ship", seg)

    assert_receive {:captured, {:restore_init_state, cmd}}
    assert cmd.has_compensation_intent, "intent marker rides RestoreInitState"
    refute cmd.has_compensation_done, "no done marker was persisted"
  end
end

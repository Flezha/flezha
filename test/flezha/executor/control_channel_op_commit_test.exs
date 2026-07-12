defmodule FlezhaTest.Executor.ControlChannelOpCommitTest do
  @moduledoc """
  SDK -> Controller `OpCommit` dispatch path (`bounded-op-journal`): the
  ControlChannel appends the `op_committed` entry to the instance's
  op-journal, fsyncs, and only then replies `OpCommitResponse` under the
  request's reference_id. A failed append rides back in `error`.

  Uses the 4-tuple ControlChannel constructor with a capturing `write_fun`
  (`Flezha.Transport.Native` is a NIF and cannot be Mimic-copied).
  """
  use ExUnit.Case, async: false

  alias Flezha.Executor.ControlChannel
  alias Flezha.State.{OpJournal, TestHelpers, Wire}

  @namespace "ctrl-chan-op-commit-test"

  setup do
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    :ok
  end

  test "OpCommit is journaled durably and acked with an empty error" do
    inst = "opc_inst_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("ok")

    seg = Wire.encode_segment_id(inst, "OpA", "s0")
    payload = build_op_commit_payload(inst, 1, [seg], [{4, "handoff"}], "ref-opc-1")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 1_000
    response = decode_reply(reply)
    assert response.reference_id == "ref-opc-1"

    assert {:op_commit_response, %ExecutorCommands.OpCommitResponse{error: "", op_idx: 1}} =
             response.command

    # The ack was emitted only after the entry landed in the journal.
    assert {:ok, [entry]} = OpJournal.read(inst)
    assert %{type: :op_committed, op_idx: 1, iter: 0} = entry
    assert entry.payload.segments_ran == [seg]
    assert entry.payload.handoff == [{4, "handoff"}]
  end

  test "a refused append (oversized handoff) rides back in the error field" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = "opc_fat_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("fat")

    fat = :binary.copy(<<0>>, OpJournal.max_entry_bytes() + 1)
    payload = build_op_commit_payload(inst, 0, [], [{1, fat}], "ref-opc-fat")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000
    response = decode_reply(reply)

    assert {:op_commit_response, %ExecutorCommands.OpCommitResponse{error: error}} =
             response.command

    assert error =~ "oversized_handoff"
    assert {:ok, []} = OpJournal.read(inst)
  end

  defp start_control_channel(suffix) do
    stream_ref = make_ref()
    executor_id = "#{@namespace}_#{suffix}_#{System.unique_integer([:positive])}"
    test_pid = self()

    write_fun = fn ref, payload ->
      send(test_pid, {:test_writer_called, ref, payload})
      :ok
    end

    {:ok, pid} = ControlChannel.start_link({@namespace, executor_id, stream_ref, write_fun})

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
    end)

    {pid, stream_ref}
  end

  defp build_op_commit_payload(operative_id, op_idx, segments_ran, handoff, reference_id) do
    request = %ExecutorCommands.CommandRequest{
      reference_id: reference_id,
      command:
        {:op_commit,
         %ExecutorCommands.OpCommit{
           operative_id: operative_id,
           op_idx: op_idx,
           iter: 0,
           segments_ran: segments_ran,
           handoff:
             Enum.map(handoff, fn {cid, bytes} ->
               %ExecutorCommands.OpHandoffItem{channel_id: cid, payload: bytes}
             end)
         }}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end

  defp decode_reply(framed_payload) do
    <<_payload_id::binary-size(32), _payload_size::32, message_size::32, rest::binary>> =
      framed_payload

    <<message::binary-size(^message_size), _metadata_size::16, _metadata::binary>> = rest

    {:ok, %ExecutorCommands.CommandRequest{} = req} =
      ExecutorCommands.CommandRequest.decode(message)

    req
  end
end

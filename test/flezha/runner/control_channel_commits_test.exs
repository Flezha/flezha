defmodule FlezhaTest.Runner.ControlChannelCommitsTest do
  @moduledoc """
  SDK -> Controller `TaskCommit` dispatch path (`workflow-task-journal` +
  `workflow-transactional-outbox`): the ControlChannel appends the
  `task_committed` entry — cells, handoff AND the tasklet's staged outbox
  in one record — fsyncs, replies `TaskCommitResponse` under the request's
  reference_id, and only then flushes the outbox to the bound operative. A
  failed append rides back in `error` and flushes nothing.

  Uses the 4-tuple ControlChannel constructor with a capturing `write_fun`
  (`Flezha.Transport.Native` is a NIF and cannot be Mimic-copied).
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.{ControlChannel, ControlUnit}
  alias Flezha.Runner.ControlChannel.Commits
  alias Flezha.SDK.Wire
  alias Flezha.State.DurabilityTier.Manifest
  alias Flezha.State.{Outbox, Paths, TaskInbox, TaskJournal, TestHelpers}
  alias Flezha.State.WalWriter
  alias FlezhaTest.Helper.ControlFrameTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  import Flezha.State.TestHelpers, only: [next_output: 1, refute_dispatched: 1]

  alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub

  @namespace "ctrl-chan-task-commit-test"

  setup do
    ClassRegistry.ensure_table()
    on_exit(&Stub.uninstall/0)

    :ok
  end

  test "TaskCommit is journaled durably and acked with an empty error" do
    inst = "opc_inst_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("ok")

    seg = Wire.encode_cell_id(inst, "TaskA", "s0")
    payload = build_task_commit_payload(inst, 1, [seg], [{4, "handoff"}], "ref-opc-1")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 1_000
    response = decode_reply(reply)
    assert response.reference_id == "ref-opc-1"

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: "", task_idx: 1}} =
             response.command

    # The ack was emitted only after the entry landed in the journal.
    assert {:ok, [entry]} = TaskJournal.read(inst)
    assert %{type: :task_committed, task_idx: 1, iter: 0} = entry
    assert entry.payload.cells_ran == [seg]
    assert entry.payload.handoff == [{4, "handoff"}]
  end

  test "a completing commit journals its result on both barriers, and a plain one none" do
    inst = "opc_done_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("done")

    for payload <- [
          build_task_commit_payload(inst, 0, [], [], "ref-done-p", completes_with: "3"),
          build_iter_commit_payload(inst, 0, 0, [], "ref-done-i", completes_with: "3"),
          build_task_commit_payload(inst, 1, [], [], "ref-plain-p")
        ] do
      send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})
      assert_receive {:test_writer_called, ^stream_ref, _reply}, 1_000
    end

    assert {:ok, [task, iter, plain]} = TaskJournal.read(inst)
    assert {task.type, task.payload.completion} == {:task_committed, "3"}
    assert {iter.type, iter.payload.completion} == {:iter_committed, "3"}
    assert plain.payload.completion == nil
  end

  test "a refused append (oversized handoff) rides back in the error field" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = "opc_fat_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("fat")

    fat = :binary.copy(<<0>>, TaskJournal.max_entry_bytes() + 1)
    payload = build_task_commit_payload(inst, 0, [], [{1, fat}], "ref-opc-fat")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000
    response = decode_reply(reply)

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: error}} =
             response.command

    assert error =~ "oversized_handoff"
    assert {:ok, []} = TaskJournal.read(inst)
  end

  test "a re-sent barrier under the same reference_id is acked again from the one entry" do
    inst = "opc_resend_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("resend")

    seg = Wire.encode_cell_id(inst, "TaskA", "s0")
    commit = build_task_commit_payload(inst, 1, [seg], [{4, "handoff"}], "ref-opc-resend")
    iter = build_iter_commit_payload(inst, 0, 0, [], "ref-iter-resend")

    # The SDK's wait expired and it re-sent both barriers; the controller had
    # journaled each the first time.
    for payload <- [commit, commit, iter, iter] do
      send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})
    end

    replies =
      for _ <- 1..4 do
        assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000
        response = decode_reply(reply)

        case response.command do
          {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: error}} ->
            {response.reference_id, error}

          {:iter_commit_response, %RunnerCommands.IterCommitResponse{error: error}} ->
            {response.reference_id, error}
        end
      end

    assert Enum.sort(replies) ==
             [
               {"ref-iter-resend", ""},
               {"ref-iter-resend", ""},
               {"ref-opc-resend", ""},
               {"ref-opc-resend", ""}
             ]

    assert {:ok, [%{type: :task_committed, task_idx: 1}, %{type: :iter_committed, iter: 0}]} =
             TaskJournal.read(inst)
  end

  test "a hundred barriers in flight do not delay a ping on the same channel" do
    %{channel: channel, stream_ref: stream_ref, runner_id: runner_id} = start_runner("storm")
    {:ok, unit} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> stop_quietly(unit) end)

    # With the batcher frozen every barrier below stays in flight, unacked.
    pong =
      TestHelpers.in_one_window(fn ->
        for i <- 1..100 do
          inst = "storm_#{i}_#{System.unique_integer([:positive])}"
          payload = build_task_commit_payload(inst, 0, [], [{1, "h"}], "ref-storm-#{i}")
          send(channel, {:flz_data, :stream_id, "control_command", payload, %{}})
        end

        ping = Task.async(fn -> ControlUnit.ping_runner(runner_id, timeout_ms: 2_000) end)
        answer_ping(channel, stream_ref)
        Task.await(ping, 3_000)
      end)

    assert pong == {:ok, :pong}

    # The hundred acks follow the sync.
    for _ <- 1..100 do
      assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000
      assert {:task_commit_response, %{error: ""}} = decode_reply(reply).command
    end
  end

  # Stand in for the SDK: answer the one ping the channel wrote.
  defp answer_ping(channel, stream_ref) do
    assert_receive {:test_writer_called, ^stream_ref, frame}, 1_000
    %{reference_id: ref, command: {:ping, %RunnerCommands.Ping{}}} = decode_reply(frame)

    {:ok, iodata, _} =
      Protox.encode(%RunnerCommands.CommandResponse{reference_id: ref, status: "success"})

    send(channel, {:flz_data, :stream_id, "control_command", IO.iodata_to_binary(iodata), %{}})
  end

  # The marker is appended by the instance's journal writer once the delivery is
  # confirmed, off the flushing process — so the journal is read after it lands.
  defp await_marker(inst) do
    assert wait_until(
             fn ->
               {:ok, entries} = TaskJournal.read(inst)
               Enum.any?(entries, &(&1.type == :outbox_flushed))
             end,
             2_000
           ),
           "outbox_flushed marker never landed"
  end

  test "the staged outbox is journaled with the commit and flushed only after the ack" do
    inst = "opc_outbox_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("outbox")

    # Stand in as the bound operative: the flush casts `:io_out_batch` at us.
    own(inst, control_channel)

    seg = Wire.encode_cell_id(inst, "TaskA", "s0")

    payload =
      build_task_commit_payload(inst, 0, [seg], [], "ref-opc-outbox",
        outbox: [
          %{cell_id: seg, seq_in_task: 0, channel: "out", payload: "one"},
          %{cell_id: seg, seq_in_task: 1, channel: "out", payload: "two"}
        ]
      )

    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # The ack comes first: it IS the durable point past which output may be seen.
    assert_receive {:test_writer_called, ^stream_ref, reply}, 1_000

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: ""}} =
             decode_reply(reply).command

    assert {"one", meta_one, _} = next_output(1_000)
    assert {"two", meta_two, _} = next_output(1_000)
    assert meta_one["flz.cell"] == "#{inst}/TaskA/s0"
    assert meta_one["flz.seq"] == "0.0"
    assert meta_one["flz.epoch"] == "0"
    assert meta_one["channel"] == "out"
    assert meta_two["flz.seq"] == "0.1"

    # Standing in for the Overwatch also means reporting the chain run: the
    # marker is what a confirmed delivery leaves behind, never the hand-off.
    :ok = Outbox.settled(meta_one)
    :ok = Outbox.settled(meta_two)
    await_marker(inst)

    # Outbox and progress are one record; the flush is recorded separately.
    assert {:ok, [committed, flushed]} = TaskJournal.read(inst)
    assert [%{payload: "one"}, %{payload: "two"}] = committed.payload.outbox
    assert %{type: :outbox_flushed, task_idx: 0, payload: %{for_type: :task_committed}} = flushed
    assert TaskJournal.pending_entries(inst) == []
  end

  describe "synchronous remote durability" do
    setup do
      Stub.install()
      {:ok, _pid} = start_supervised(Flezha.State.DurabilityTier.Shipper)
      :ok
    end

    test "the ack is withheld until the barrier is on the remote store" do
      inst = "opc_sync_#{System.unique_integer([:positive])}"
      ClassRegistry.put_sync_durability(inst, true)
      {control_channel, stream_ref} = start_control_channel("sync")

      seg = Wire.encode_cell_id(inst, "TaskA", "s0")
      payload = build_task_commit_payload(inst, 0, [seg], [{1, "handoff"}], "ref-sync-ok")
      send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

      assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000

      assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: ""}} =
               decode_reply(reply).command

      # The ack means remote-durable: the journal's manifest is in the store.
      assert {:ok, _blob} =
               Stub.get(Manifest.key({:cell, {inst, inst, Paths.journal_cell_key()}}), [])
    end

    test "a failed remote ship withholds the ack's success and re-runs the task on resume" do
      inst = "opc_sync_down_#{System.unique_integer([:positive])}"
      ClassRegistry.put_sync_durability(inst, true)
      {control_channel, stream_ref} = start_control_channel("syncdown")

      seg = Wire.encode_cell_id(inst, "TaskA", "s0")
      Stub.set_down(true)
      payload = build_task_commit_payload(inst, 0, [seg], [{1, "handoff"}], "ref-sync-down")
      send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

      assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000

      assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: error}} =
               decode_reply(reply).command

      assert error =~ "backend_down"

      # The local append DID land — that is the crash window the mode is about.
      # The SDK never saw a successful ack, so it re-runs the tasklet on
      # resume, and the journal it resumes against is the local one.
      assert {:ok, [%{type: :task_committed, task_idx: 0}]} = TaskJournal.read(inst)

      refute match?(
               {:ok, _},
               Stub.get(Manifest.key({:cell, {inst, inst, "_task_journal"}}), [])
             )
    end

    test "a class that declares nothing acks on local fsync exactly as before" do
      inst = "opc_async_#{System.unique_integer([:positive])}"
      {control_channel, stream_ref} = start_control_channel("async")

      seg = Wire.encode_cell_id(inst, "TaskA", "s0")
      Stub.set_down(true)
      payload = build_task_commit_payload(inst, 0, [seg], [{1, "handoff"}], "ref-async")
      send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

      assert_receive {:test_writer_called, ^stream_ref, reply}, 1_000

      assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: ""}} =
               decode_reply(reply).command
    end
  end

  test "a compensating commit journals a `compensated` entry with comp-marked keys" do
    inst = "opc_comp_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("comp")
    own(inst, control_channel)

    seg = Wire.encode_cell_id(inst, "TaskA", "s0")

    payload =
      build_task_commit_payload(inst, 2, [seg], [], "ref-opc-comp",
        compensating: true,
        outbox: [%{cell_id: seg, seq_in_task: 0, channel: "mid", payload: "undo"}]
      )

    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 1_000

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: ""}} =
             decode_reply(reply).command

    assert {"undo", meta, _} = next_output(1_000)
    # The comp marker keeps the undo key off the forward output it reverses.
    assert meta["flz.seq"] == "2.0.1"

    :ok = Outbox.settled(meta)
    await_marker(inst)

    assert {:ok, [compensated, flushed]} = TaskJournal.read(inst)
    assert %{type: :compensated, task_idx: 2} = compensated
    assert [%{payload: "undo", channel: "mid"}] = compensated.payload.outbox
    assert %{type: :outbox_flushed, payload: %{for_type: :compensated}} = flushed
  end

  test "a refused commit flushes nothing" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = "opc_norefuse_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("norefuse")
    own(inst, control_channel)

    seg = Wire.encode_cell_id(inst, "TaskA", "s0")
    fat = :binary.copy(<<0>>, TaskJournal.max_entry_bytes() + 1)

    payload =
      build_task_commit_payload(inst, 0, [seg], [], "ref-opc-norefuse",
        outbox: [%{cell_id: seg, seq_in_task: 0, channel: "out", payload: fat}]
      )

    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: error}} =
             decode_reply(reply).command

    assert error =~ "oversized_handoff"
    # Nothing durable ⇒ nothing delivered.
    refute_dispatched(300)
    assert {:ok, []} = TaskJournal.read(inst)
  end

  test "IterCommit journals the boundary, acks, then releases the consumed inbox" do
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))
    inst = "iter_inst_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("iter")
    own(inst, control_channel)

    # Two turns are waiting in the durable inbox; iteration 0 consumed the first.
    {:ok, 1} = TaskInbox.append(inst, "turn-1", %{})
    {:ok, 2} = TaskInbox.append(inst, "turn-2", %{})

    payload = build_iter_commit_payload(inst, 0, 1, [{4, "carry"}], "ref-iter-0")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 1_000
    response = decode_reply(reply)
    assert response.reference_id == "ref-iter-0"

    assert {:iter_commit_response, %RunnerCommands.IterCommitResponse{error: "", iter: 0}} =
             response.command

    # The ack was emitted only after the boundary record landed.
    assert {:ok, [entry]} = TaskJournal.read(inst)
    assert %{type: :iter_committed, iter: 0} = entry
    assert entry.payload.consumed_offset == 1
    assert entry.payload.handoff == [{4, "carry"}]
    assert TaskJournal.consumed_offset(inst) == 1

    # Post-ack cleanup runs on the commit worker after the ack.
    assert wait_until(fn -> Enum.map(TaskInbox.read(inst), & &1.offset) == [2] end, 2_000),
           "the consumed turn is released; the undelivered one is kept"

    assert Enum.map(TaskInbox.undelivered(inst), & &1.message) == ["turn-2"]
  end

  test "the iteration boundary requests a snapshot for the instance's cells" do
    inst = "iter_snap_#{System.unique_integer([:positive])}"

    # A persisted Cell of this instance — what the boundary compacts so a
    # rehydrate loads iteration-N state instead of replaying the WAL tail.
    seg_id = {inst, "TaskA", "s0"}
    {:ok, writer} = Flezha.State.Supervisor.find_or_start(seg_id)
    :ok = WalWriter.append(writer, 0, <<0::128>>, "change")

    test_pid = self()
    set_mimic_global_until_exit()

    Mimic.stub(ControlChannel, :send_control, fn _runner, command ->
      send(test_pid, {:control_command, command})
      {:ok, make_ref()}
    end)

    {control_channel, stream_ref} = start_control_channel("snap")

    payload = build_iter_commit_payload(inst, 0, 0, [], "ref-iter-snap")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, _reply}, 1_000

    expected = Wire.encode_cell_id(inst, "TaskA", "s0")

    assert_receive {:control_command,
                    {:compact_cell, %RunnerCommands.CompactCell{cell_id: ^expected}}},
                   1_000
  end

  test "IterPark journals the park record and suspends the instance" do
    inst = "park_inst_#{System.unique_integer([:positive])}"
    {control_channel, _stream_ref} = start_control_channel("park")

    # Stand in as the instance's Overwatch: the suspension is cast at us. The
    # park runs in a Task (the eviction's RemOperative reply returns on this very
    # control stream), so the stub must be visible outside the test process.
    test_pid = self()
    set_mimic_global_until_exit()
    Mimic.stub(Flezha.Service.HubManager, :get_pid, fn _ns, ^inst -> {:ok, test_pid} end)

    payload = build_iter_park_payload(inst, 3, 1_800_000_000_000, "ref-park")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:"$gen_cast", {:suspend, 3, 1_800_000_000_000}}, 1_000

    # The park is durable BEFORE the eviction, so a controller crash in between
    # still rediscovers the suspension.
    assert %{parked: true, wake_deadline_ms: 1_800_000_000_000} = TaskJournal.restore_state(inst)
  end

  defp start_control_channel(suffix) do
    %{channel: channel, stream_ref: stream_ref} = start_runner(suffix)
    {channel, stream_ref}
  end

  # Stand in as the bound operative, owned by the channel's own session.
  defp own(inst, control_channel),
    do: ClassRegistry.put_overwatch(inst, self(), :sys.get_state(control_channel).identity)

  # The channel and its commit worker, as the runner supervisor pairs them.
  defp start_runner(suffix) do
    stream_ref = make_ref()
    runner_id = "#{@namespace}_#{suffix}_#{System.unique_integer([:positive])}"
    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:test_writer_called, ref, message})
      :ok
    end

    {:ok, worker} = Commits.start_link({@namespace, runner_id})
    {:ok, pid} = ControlChannel.start_link({@namespace, runner_id, stream_ref, write_fun})

    on_exit(fn ->
      stop_quietly(pid)
      stop_quietly(worker)
    end)

    %{channel: pid, stream_ref: stream_ref, runner_id: runner_id}
  end

  # Linked to the (now exiting) test process, so it may already be on its way
  # down — stopping it is best-effort cleanup, not an assertion.
  defp stop_quietly(pid) do
    NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
  catch
    :exit, _ -> :ok
  end

  defp build_task_commit_payload(
         operative_id,
         task_idx,
         cells_ran,
         handoff,
         reference_id,
         opts \\ []
       ) do
    ControlFrameTestHelper.encode_request(
      reference_id,
      {:task_commit,
       %RunnerCommands.TaskCommit{
         operative_id: operative_id,
         task_idx: task_idx,
         iter: 0,
         cells_ran: cells_ran,
         handoff:
           Enum.map(handoff, fn {cid, bytes} ->
             %RunnerCommands.TaskHandoffItem{channel_id: cid, payload: bytes}
           end),
         outbox:
           Enum.map(Keyword.get(opts, :outbox, []), fn item ->
             struct!(RunnerCommands.TaskOutboxItem, item)
           end),
         compensating: Keyword.get(opts, :compensating, false),
         completes: Keyword.has_key?(opts, :completes_with),
         result: Keyword.get(opts, :completes_with, "")
       }}
    )
  end

  defp build_iter_commit_payload(
         operative_id,
         iter,
         consumed_offset,
         handoff,
         reference_id,
         opts \\ []
       ) do
    ControlFrameTestHelper.encode_request(reference_id, {
      :iter_commit,
      %RunnerCommands.IterCommit{
        operative_id: operative_id,
        iter: iter,
        consumed_offset: consumed_offset,
        handoff:
          Enum.map(handoff, fn {cid, bytes} ->
            %RunnerCommands.TaskHandoffItem{channel_id: cid, payload: bytes}
          end),
        completes: Keyword.has_key?(opts, :completes_with),
        result: Keyword.get(opts, :completes_with, "")
      }
    })
  end

  defp build_iter_park_payload(operative_id, iter, wake_deadline_ms, reference_id) do
    ControlFrameTestHelper.encode_request(reference_id, {
      :iter_park,
      %RunnerCommands.IterPark{
        operative_id: operative_id,
        iter: iter,
        wake_deadline_ms: wake_deadline_ms
      }
    })
  end

  defp decode_reply(framed_payload) do
    ControlFrameTestHelper.decode_request(framed_payload)
  end
end

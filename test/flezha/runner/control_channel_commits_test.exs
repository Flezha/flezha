defmodule FlezhaTest.Runner.ControlChannelCommitsTest do
  @moduledoc """
  SDK -> Controller `TaskCommit` dispatch path (`steps-task-journal`): the
  commit worker waits until every effect record the barrier names is durable
  on its Cell's writer, appends the `task_committed` entry naming them, fsyncs,
  replies `TaskCommitResponse` under the request's reference_id, and only then
  has the Cells' writers release those records' outputs. A failed append, or a
  named record that never becomes durable, rides back in `error` and releases
  nothing.

  Uses the 4-tuple ControlChannel constructor with a capturing `write_fun`
  (`Flezha.Transport.Native` is a NIF and cannot be Mimic-copied).
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.NodeTestHelper, only: [override_log_level: 1, wait_until: 2]

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.{ControlChannel, ControlUnit}
  alias Flezha.Runner.ControlChannel.Commits
  alias Flezha.State.Wire
  alias Flezha.State.DurabilityTier.Manifest
  alias Flezha.State.{InputLog, Paths, TaskJournal, TestHelpers}
  alias Flezha.State.InputLog.Sweep
  alias Flezha.State.WalWriter
  alias FlezhaTest.Helper.ControlFrameTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  import Flezha.State.TestHelpers, only: [next_output: 1, refute_dispatched: 1]

  alias Flezha.State.Frame

  alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub
  alias FlezhaTest.Helper.Telemetry
  alias FlezhaTest.Helper.WireEncoders

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

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: nil, task_idx: 1}} =
             response.command

    # The ack was emitted only after the entry landed in the journal.
    assert {:ok, [entry]} = TaskJournal.read(inst)
    assert %{type: :task_committed, task_idx: 1, iter: 0} = entry
    assert entry.payload.cells_ran == [%{cell_id: seg, first_seq: nil, seq: nil, channels: []}]
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

    assert error.code == "commit.oversized_handoff"
    assert {:ok, []} = TaskJournal.read(inst)
  end

  # Fails when: the exit of a journal writer closed under an append (its relay
  # stopping) reaches the runner as the reason the commit failed.
  test "an append whose journal writer is closed under it is answered as the writer gone" do
    on_exit(override_log_level(:warning))
    inst = "opc_closed_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("closed")
    {:ok, writer} = Flezha.State.Supervisor.find_or_start(Paths.journal_id(inst))
    :ok = :sys.suspend(writer)
    :erlang.trace(writer, true, [:receive])

    payload = build_task_commit_payload(inst, 0, [], [], "ref-opc-closed")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})
    assert_receive {:trace, ^writer, :receive, {:"$gen_call", _, {:append_next, _, _, _}}}, 1_000

    log =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        :ok = Flezha.State.Supervisor.close_writers(inst)
        assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000
        assert {:task_commit_response, %{error: error}} = decode_reply(reply).command
        assert error.code == "controller.process_gone"
      end)

    assert log =~ "task-journal commit relay=#{inspect(inst)} task_idx=0 failed"
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
               {"ref-iter-resend", nil},
               {"ref-iter-resend", nil},
               {"ref-opc-resend", nil},
               {"ref-opc-resend", nil}
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
      assert {:task_commit_response, %{error: nil}} = decode_reply(reply).command
    end
  end

  # Stand in for the SDK: answer the one ping the channel wrote.
  defp answer_ping(channel, stream_ref) do
    assert_receive {:test_writer_called, ^stream_ref, frame}, 1_000
    %{reference_id: ref, command: {:ping, %RunnerCommands.Ping{}}} = decode_reply(frame)

    answer = ControlFrameTestHelper.encode_response(ref, "success")
    send(channel, {:flz_data, :stream_id, "control_command", answer, %{}})
  end

  # A durable steps relay Cell of `inst` whose writer holds one effect record at
  # `seq` carrying one output keyed `k` with a metadata pair, on `channel`.
  defp steps_cell(inst, seq, payload, opts \\ []) do
    :ok = ClassRegistry.put(inst, :steps)
    cell = {inst, "TaskA", "s0"}
    {:ok, writer} = Flezha.State.Supervisor.find_or_start(cell)

    if Keyword.get(opts, :written?, true) do
      :ok = WalWriter.append(writer, seq, effect_header(), effect_body(payload, opts))
    end

    {cell, writer}
  end

  defp effect_header(),
    do: WireEncoders.encode_header(1, 1, 1, Frame.format_id(:effect_record), <<>>)

  defp effect_body(payload, opts) do
    WireEncoders.encode_effect_body(%{
      changes: [<<1>>],
      outputs: [
        %{
          channel: Keyword.get(opts, :channel, "out"),
          key: "k",
          metadata: [{"trace", "t1"}],
          payload: payload
        }
      ],
      consumed_offset: nil,
      dlq: nil
    })
  end

  test "the entry waits for the records it names, and their outputs leave after the ack" do
    inst = "opc_records_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("records")
    own(inst, control_channel)

    # The record is appended but its batch has not synced when the barrier lands.
    :ok = :sys.suspend(Flezha.State.FsyncBatcher)
    {_cell, writer} = steps_cell(inst, 0, "a-out")
    seg = Wire.encode_cell_id(inst, "TaskA", "s0")
    payload = build_task_commit_payload(inst, 0, [{seg, 0, 0, ["out"]}], [], "ref-records")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    refute_receive {:test_writer_called, ^stream_ref, _reply}, 200
    assert {:ok, []} = TaskJournal.read(inst)
    refute_dispatched(0)
    :ok = :sys.resume(Flezha.State.FsyncBatcher)
    :ok = WalWriter.fsync(writer)

    # The ack comes first: it IS the durable point past which output may be seen.
    assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: nil}} =
             decode_reply(reply).command

    assert {:ok, [entry]} = TaskJournal.read(inst)
    assert entry.payload.cells_ran == [%{cell_id: seg, first_seq: 0, seq: 0, channels: ["out"]}]

    assert {"a-out", meta, _} = next_output(1_000)
    assert meta["flz.cell"] == "#{inst}/TaskA/s0"
    assert meta["flz.seq"] == "0.0"
    assert meta["flz.epoch"] == "0"
    assert meta["flz.key"] == "k"
    assert meta["trace"] == "t1"
    assert meta["channel"] == "out"
  end

  test "a record the writer never gets fails the barrier, naming the Cell and the seq" do
    on_exit(TestHelpers.override_env(:sdk_commit_ack_ms, 200))
    inst = "opc_missing_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("missing")
    own(inst, control_channel)
    steps_cell(inst, 5, "never", written?: false)

    seg = Wire.encode_cell_id(inst, "TaskA", "s0")
    payload = build_task_commit_payload(inst, 0, [{seg, 5, 5, ["out"]}], [], "ref-missing")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: error}} =
             decode_reply(reply).command

    assert error.code == "commit.record_not_durable"
    assert Jason.decode!(error.details_json) == %{"cell" => "#{inst}/TaskA/s0", "seq" => 5}
    assert {:ok, []} = TaskJournal.read(inst)
  end

  test "a re-sent barrier whose first copy still waits on its records joins that wait" do
    inst = "opc_join_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("join")
    own(inst, control_channel)

    :ok = :sys.suspend(Flezha.State.FsyncBatcher)
    {_cell, writer} = steps_cell(inst, 0, "once")
    seg = Wire.encode_cell_id(inst, "TaskA", "s0")
    payload = build_task_commit_payload(inst, 0, [{seg, 0, 0, ["out"]}], [], "ref-join")

    for _copy <- 1..2,
        do: send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    refute_receive {:test_writer_called, ^stream_ref, _reply}, 200
    :ok = :sys.resume(Flezha.State.FsyncBatcher)
    :ok = WalWriter.fsync(writer)

    assert_receive {:test_writer_called, ^stream_ref, _reply}, 2_000
    refute_receive {:test_writer_called, ^stream_ref, _second}, 300
    assert {:ok, [_one_entry]} = TaskJournal.read(inst)
    assert {"once", _meta, _} = next_output(1_000)
    refute_dispatched(100)
  end

  describe "synchronous remote durability" do
    setup do
      Stub.install()
      {:ok, _pid} = start_supervised(Flezha.State.DurabilityTier.Shipper)
      :ok
    end

    test "the ack is withheld until the barrier is on the remote store" do
      inst = "opc_sync_#{System.unique_integer([:positive])}"
      ClassRegistry.put_remote(inst, true)
      {control_channel, stream_ref} = start_control_channel("sync")

      seg = Wire.encode_cell_id(inst, "TaskA", "s0")
      payload = build_task_commit_payload(inst, 0, [seg], [{1, "handoff"}], "ref-sync-ok")
      send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

      assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000

      assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: nil}} =
               decode_reply(reply).command

      # The ack means remote-durable: the journal's manifest is in the store.
      assert {:ok, _blob} =
               Stub.get(Manifest.key({:cell, {inst, inst, Paths.journal_cell_key()}}), [])
    end

    test "a failed remote ship withholds the ack's success and re-runs the task on resume" do
      inst = "opc_sync_down_#{System.unique_integer([:positive])}"
      ClassRegistry.put_remote(inst, true)
      {control_channel, stream_ref} = start_control_channel("syncdown")

      seg = Wire.encode_cell_id(inst, "TaskA", "s0")
      Stub.set_down(true)
      payload = build_task_commit_payload(inst, 0, [seg], [{1, "handoff"}], "ref-sync-down")
      send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

      assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000

      assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: error}} =
               decode_reply(reply).command

      assert error.code == "durability.sync_ship_failed"
      assert Jason.decode!(error.details_json)["reason"] =~ "backend_down"

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

      assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: nil}} =
               decode_reply(reply).command
    end
  end

  test "a compensating commit journals the undo record it names and releases its output" do
    inst = "opc_comp_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("comp")
    own(inst, control_channel)

    {_cell, writer} = steps_cell(inst, 0, "undo", channel: "mid")
    :ok = WalWriter.fsync(writer)
    seg = Wire.encode_cell_id(inst, "TaskA", "s0")

    payload =
      build_task_commit_payload(inst, 2, [{seg, 0, 0, ["mid"]}], [], "ref-opc-comp",
        compensating: true
      )

    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 1_000

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: nil}} =
             decode_reply(reply).command

    assert {"undo", meta, _} = next_output(1_000)
    assert meta["flz.seq"] == "0.0"
    assert meta["channel"] == "mid"

    assert {:ok, [compensated]} = TaskJournal.read(inst)
    assert %{type: :compensated, task_idx: 2} = compensated

    assert compensated.payload.cells_ran == [
             %{cell_id: seg, first_seq: 0, seq: 0, channels: ["mid"]}
           ]
  end

  test "a refused commit releases nothing" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = "opc_norefuse_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("norefuse")
    own(inst, control_channel)

    {_cell, writer} = steps_cell(inst, 0, "never")
    :ok = WalWriter.fsync(writer)
    seg = Wire.encode_cell_id(inst, "TaskA", "s0")
    fat = :binary.copy(<<0>>, TaskJournal.max_entry_bytes() + 1)

    payload =
      build_task_commit_payload(inst, 0, [{seg, 0, 0, ["out"]}], [{1, fat}], "ref-opc-norefuse")

    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000

    assert {:task_commit_response, %RunnerCommands.TaskCommitResponse{error: error}} =
             decode_reply(reply).command

    assert error.code == "commit.oversized_handoff"
    # Nothing journaled ⇒ nothing released.
    refute_dispatched(300)
    assert {:ok, []} = TaskJournal.read(inst)
  end

  test "IterCommit journals the boundary, acks, then releases the consumed input" do
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))
    inst = "iter_inst_#{System.unique_integer([:positive])}"
    {control_channel, stream_ref} = start_control_channel("iter")
    own(inst, control_channel)

    # Three turns are waiting in the input log; iteration 0 consumed the second.
    for {turn, n} <- Enum.with_index(["turn-0", "turn-1", "turn-2"]),
        do: {:ok, ^n} = InputLog.append(inst, turn, %{})

    payload = build_iter_commit_payload(inst, 0, 1, [{4, "carry"}], "ref-iter-0")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:test_writer_called, ^stream_ref, reply}, 1_000
    response = decode_reply(reply)
    assert response.reference_id == "ref-iter-0"

    assert {:iter_commit_response, %RunnerCommands.IterCommitResponse{error: nil, iter: 0}} =
             response.command

    # The ack was emitted only after the boundary record landed.
    assert {:ok, [entry]} = TaskJournal.read(inst)
    assert %{type: :iter_committed, iter: 0} = entry
    assert entry.payload.consumed_offset == 1
    assert entry.payload.handoff == [{4, "carry"}]
    assert TaskJournal.consumed_offset(inst) == 1

    # Post-ack, the journal reports the offset into the log's watermark; the
    # retention sweep reclaims below it, and only the turn above it is owed.
    assert wait_until(fn -> InputLog.watermark(inst) == 1 end, 2_000)
    assert [{2, "turn-2", _}] = InputLog.undelivered(inst)

    :ok = Sweep.run(inst)

    assert wait_until(
             fn -> InputLog.read_range(inst, 0, 2) |> Enum.map(&elem(&1, 0)) == [1, 2] end,
             2_000
           )

    # A pass that consumed nothing commits no offset and leaves the floor alone.
    payload = build_iter_commit_payload(inst, 1, nil, [{4, "carry"}], "ref-iter-1")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})
    assert_receive {:test_writer_called, ^stream_ref, _reply}, 1_000
    assert {:ok, [_, %{iter: 1, payload: %{consumed_offset: nil}}]} = TaskJournal.read(inst)
    assert InputLog.watermark(inst) == 1
  end

  test "the iteration boundary requests no snapshot: the Watchdog compacts a steps relay's Cells" do
    inst = "iter_snap_#{System.unique_integer([:positive])}"

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
    refute_receive {:control_command, {:compact_cell, _}}, 300
  end

  test "IterPark journals the unread release, then hands the park record to the instance's Overwatch" do
    inst = "park_inst_#{System.unique_integer([:positive])}"
    {control_channel, _stream_ref} = start_control_channel("park")

    # Stand in as the instance's Overwatch: the suspension is cast at us. The
    # park runs in a Task (the eviction's RemRelay reply returns on this very
    # control stream), so the stub must be visible outside the test process.
    test_pid = self()
    set_mimic_global_until_exit()
    Mimic.stub(Flezha.Service.HubManager, :get_pid, fn _ns, ^inst -> {:ok, test_pid} end)

    payload = build_iter_park_payload(inst, 3, 1_800_000_000_000, "ref-park", 0)
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # The Overwatch writes the record durably before it evicts the instance.
    assert_receive {:"$gen_cast", {:suspend, 3, %{wake_deadline_ms: 1_800_000_000_000} = park}},
                   1_000

    refute Map.has_key?(park, :consumed_offset)

    # The message the run parked unread (offset 0) is journaled as released
    # before the suspension asks the log whether input is owed.
    assert TaskJournal.released_offset(inst) == 0
    assert InputLog.watermark(inst) == 0
  end

  describe "a dead-lettered iteration commit" do
    setup do
      FlezhaTest.Helper.DlqFixtures.isolated_dir()
      on_exit(override_log_level(:warning))
      inst = "dl_inst_#{System.unique_integer([:positive])}"
      {control_channel, stream_ref} = start_control_channel("dl")
      own(inst, control_channel)
      %{inst: inst, channel: control_channel, stream_ref: stream_ref}
    end

    defp send_dead_letter(%{channel: channel, stream_ref: stream_ref}, inst, offset, ref) do
      payload = build_dead_letter_payload(inst, 2, offset, ref)
      send(channel, {:flz_data, :stream_id, "control_command", payload, %{}})
      assert_receive {:test_writer_called, ^stream_ref, reply}, 2_000
      assert {:iter_commit_response, response} = decode_reply(reply).command
      response
    end

    # Fails when: a controller term rides to the runner in the ack, or its
    # reason reaches the log other than once.
    test "a park the journal refuses is acked without the controller's term", ctx do
      set_mimic_global_until_exit()
      refused = {:error, {:shutdown, "vault key s3cr3t-7f3a"}}

      Mimic.stub(TaskJournal.Turns, :park_turn_async, fn _id, _turn, _reqids, _label ->
        refused
      end)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert %{error: error} = send_dead_letter(ctx, ctx.inst, 0, "ref-dl-refused")
          assert error.code == "commit.turn_not_parked"
          assert error.details_json == ~s({"reason":"the controller's log names the cause"})
        end)

      assert length(String.split(log, "s3cr3t-7f3a")) == 2
    end

    test "parks the message, journals the record naming its entry, acks, then releases", ctx do
      %{inst: inst} = ctx

      Telemetry.capture([:flezha, :steps, :turn_dead_lettered],
        as: :dead_lettered
      )

      for {turn, n} <- Enum.with_index(["hello", "boom", "again"]),
          do: {:ok, ^n} = InputLog.append(inst, turn, %{})

      log =
        ExUnit.CaptureLog.capture_log([level: :warning], fn ->
          assert %{error: nil, iter: 2} = send_dead_letter(ctx, inst, 1, "ref-dl-1")
          assert_receive {:dead_lettered, %{count: 1}, metadata}, 2_000
          assert %{iter: 2, consumed_offset: 1, tasklet: "answer", attempts: 3} = metadata
        end)

      assert [entry] = Flezha.Dlq.Store.list(inst)

      assert %{
               "tasklet_id" => "_turn",
               "cell_key" => "_turn",
               "channel" => "in",
               "origin_offset" => 1,
               "attempts" => 3,
               "error" => "tasklet `answer` failed after 3 attempt(s): model unavailable"
             } = entry

      ref = "#{inst}/_turn/#{entry["id"]}"
      assert {:ok, _entry, "boom"} = Flezha.Dlq.Store.get(ref)

      assert {:ok, [%{type: :dead_lettered, iter: 2, payload: payload}]} = TaskJournal.read(inst)
      assert %{consumed_offset: 1, tasklet: "answer", dlq_entry: ^ref, count: 1} = payload
      assert log =~ "dead-lettered as #{ref}"

      assert wait_until(fn -> InputLog.watermark(inst) == 1 end, 2_000)
      assert [{2, "again", _}] = InputLog.undelivered(inst)
    end

    test "a re-sent commit lands on the same entry", ctx do
      %{inst: inst} = ctx
      {:ok, 0} = InputLog.append(inst, "boom", %{})

      assert %{error: nil} = send_dead_letter(ctx, inst, 0, "ref-dl-a")
      assert %{error: nil} = send_dead_letter(ctx, inst, 0, "ref-dl-b")

      assert [_one] = Flezha.Dlq.Store.list(inst)
      assert {:ok, [%{type: :dead_lettered}]} = TaskJournal.read(inst)
    end

    test "a record no longer in the log parks without its bytes and says so", ctx do
      %{inst: inst} = ctx

      log =
        ExUnit.CaptureLog.capture_log([level: :warning], fn ->
          assert %{error: nil} = send_dead_letter(ctx, inst, 5, "ref-dl-gone")
        end)

      assert log =~ "no longer in the log; it is parked without its bytes"
      assert [%{"origin_offset" => 5, "payload_bytes" => nil}] = Flezha.Dlq.Store.list(inst)
      assert {:ok, [%{type: :dead_lettered}]} = TaskJournal.read(inst)
    end

    # fails when: a failed turn is announced (its caller answered, its
    # listeners told) before the turn's `dead_lettered` record is durable, or
    # not at all.
    test "a failed turn is announced once its record is durable", ctx do
      %{inst: inst} = ctx
      test_pid = self()
      {:ok, 0} = InputLog.append(inst, "boom", %{"flz.reply_to" => "w:caller"})
      set_mimic_global_until_exit()

      Mimic.stub(TaskJournal.Turns, :announce_turn, fn id, turn, entry_ref ->
        send(test_pid, {:journal_when_announced, TaskJournal.read(id)})
        Mimic.call_original(TaskJournal.Turns, :announce_turn, [id, turn, entry_ref])
      end)

      ExUnit.CaptureLog.capture_log(fn ->
        assert %{error: nil} = send_dead_letter(ctx, inst, 0, "ref-dl-called")
      end)

      assert_receive {:journal_when_announced, {:ok, [%{type: :dead_lettered}]}}, 2_000

      assert_receive {:"$gen_cast",
                      {:dead_lettered, %{token: "w:caller", entry: ref, error: error} = notice}},
                     2_000

      assert [%{"id" => id}] = Flezha.Dlq.Store.list(inst)
      assert ref == "#{inst}/_turn/#{id}"
      assert error == "tasklet `answer` failed after 3 attempt(s): model unavailable"
      assert %{offset: 0, tasklet: "answer", attempts: 3} = notice
    end

    test "a park the store refuses journals nothing and answers an error", ctx do
      %{inst: inst} = ctx
      {:ok, 0} = InputLog.append(inst, "boom", %{})
      # The store cannot make the entry's directory.
      blocker = Path.dirname(Flezha.Dlq.Store.task_dir(inst, "_turn"))
      File.mkdir_p!(Path.dirname(blocker))
      File.write!(blocker, "")

      ExUnit.CaptureLog.capture_log(fn ->
        assert %{error: %RunnerCommands.Error{code: "commit.turn_not_parked"}} =
                 send_dead_letter(ctx, inst, 0, "ref-dl-refused")
      end)

      assert {:ok, []} = TaskJournal.read(inst)
      assert InputLog.watermark(inst) in [-1, nil]
    end
  end

  defp start_control_channel(suffix) do
    %{channel: channel, stream_ref: stream_ref} = start_runner(suffix)
    {channel, stream_ref}
  end

  # Stand in as the bound relay, owned by the channel's own session.
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
         relay_id,
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
         relay_id: relay_id,
         task_idx: task_idx,
         iter: 0,
         cells_ran: Enum.map(cells_ran, &committed_cell/1),
         handoff:
           Enum.map(handoff, fn {cid, bytes} ->
             %RunnerCommands.TaskHandoffItem{channel_id: cid, payload: bytes}
           end),
         compensating: Keyword.get(opts, :compensating, false),
         completes: Keyword.has_key?(opts, :completes_with),
         result: Keyword.get(opts, :completes_with, "")
       }}
    )
  end

  # A Cell a stage ran: its identity alone, or with the records it wrote there
  # and the channels they wrote on.
  defp committed_cell({id, first, last, channels}),
    do: %RunnerCommands.CommittedCell{
      cell_id: id,
      first_seq: first,
      seq: last,
      channels: channels
    }

  defp committed_cell(id) when is_binary(id), do: %RunnerCommands.CommittedCell{cell_id: id}

  defp build_iter_commit_payload(
         relay_id,
         iter,
         consumed_offset,
         handoff,
         reference_id,
         opts \\ []
       ) do
    ControlFrameTestHelper.encode_request(reference_id, {
      :iter_commit,
      %RunnerCommands.IterCommit{
        relay_id: relay_id,
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

  defp build_dead_letter_payload(relay_id, iter, consumed_offset, reference_id) do
    ControlFrameTestHelper.encode_request(reference_id, {
      :iter_commit,
      %RunnerCommands.IterCommit{
        relay_id: relay_id,
        iter: iter,
        consumed_offset: consumed_offset,
        entry_channel: "in",
        dead_letter: %RunnerCommands.DeadLetter{
          tasklet: "answer",
          attempts: 3,
          error: "model unavailable"
        }
      }
    })
  end

  defp build_iter_park_payload(relay_id, iter, wake_deadline_ms, reference_id, consumed) do
    ControlFrameTestHelper.encode_request(reference_id, {
      :iter_park,
      %RunnerCommands.IterPark{
        relay_id: relay_id,
        iter: iter,
        wake_deadline_ms: wake_deadline_ms,
        consumed_offset: consumed
      }
    })
  end

  defp decode_reply(framed_payload) do
    ControlFrameTestHelper.decode_request(framed_payload)
  end
end

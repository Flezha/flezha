defmodule FlezhaTest.Runner.ScopeExitCommitTest do
  @moduledoc """
  A stepped scope's exit values ride the commit that makes them final: the
  commit worker journals the entry with them, then hands them to the outer
  instance, and acks after. A commit that is refused hands nothing over, and
  the values are named by the entry they rode, so a stage run again under a
  new barrier hands over the same names.
  """
  # async: false: writes Application env; Mimic global mode.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Operator.{ClassRegistry, Lookup}
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlChannel.Commits
  alias Flezha.SDK.Wire
  alias Flezha.State.{ScopePaths, TaskJournal, TestHelpers}
  alias FlezhaTest.Helper.{ControlFrameTestHelper, NodeTestHelper}

  @namespace "scope-exit-commit-test"

  setup do
    ClassRegistry.ensure_table()
    outer = "gw_#{System.unique_integer([:positive])}"
    test_pid = self()
    set_mimic_global_until_exit()
    Mimic.stub(Lookup, :operator, fn
      @namespace, ^outer -> {:ok, test_pid}
      namespace, id -> Mimic.call_original(Lookup, :operator, [namespace, id])
    end)
    %{outer: outer, scope: ScopePaths.scope_id(outer, "session", "alice")}
  end

  # fails when: the exit of a commit the controller refuses reaches the outer
  # instance, which then runs the answer of a pass the scope undoes.
  test "a refused commit hands its exit to nobody", ctx do
    on_exit(TestHelpers.override_env(:sdk_commit_ack_ms, 200))
    channel = start_runner(ctx.outer)
    {:ok, _writer} = Flezha.State.Supervisor.find_or_start({ctx.scope, "TaskA", "s0"})
    seg = Wire.encode_cell_id(ctx.scope, "TaskA", "s0")

    send_commit(channel, ctx.outer, "ref-refused", 1, cells_ran: [{seg, 5, 5}])

    assert_receive {:test_writer_called, _stream, reply}, 2_000
    assert %{error: %{code: "commit.record_not_durable"}} = response(reply)
    refute_received {:"$gen_call", _from, {:scope_exit, _, _, _}}
  end

  # fails when: an exit reaches the outer instance before its entry is in the
  # journal, or a stage run again under a new barrier names it anew, so the
  # outer instance takes it twice.
  test "an exit is handed over after its entry is journaled, by the entry's name", ctx do
    channel = start_runner(ctx.outer)

    keys =
      for reference_id <- ["ref-first", "ref-rerun"] do
        send_commit(channel, ctx.outer, reference_id, 1)

        assert_receive {:"$gen_call", from, {:scope_exit, {"session", "alice"}, key, exit}}, 2_000
        assert {:ok, [%{payload: %{exit: ^exit}} | _]} = TaskJournal.read(ctx.scope)
        refute_received {:test_writer_called, _stream, _reply}

        GenServer.reply(from, :ok)
        assert_receive {:test_writer_called, _stream, reply}, 2_000
        assert %{error: nil} = response(reply)
        key
      end

    assert keys == [{0, 1}, {0, 1}]
  end

  defp send_commit(channel, outer, reference_id, task_idx, opts \\ []) do
    commit = %RunnerCommands.TaskCommit{
      operator_id: outer,
      task_idx: task_idx,
      iter: 0,
      cells_ran: Enum.map(Keyword.get(opts, :cells_ran, []), &committed_cell/1),
      scope: %RunnerCommands.Scope{inner: "session", key: "alice"},
      exit: %RunnerCommands.ScopeExit{channel: "answer", values: ["alice#1:hi"]}
    }

    payload = ControlFrameTestHelper.encode_request(reference_id, {:task_commit, commit})
    send(channel, {:flz_data, :stream_id, "control_command", payload, %{}})
  end

  defp committed_cell({id, first, last}),
    do: %RunnerCommands.CommittedCell{cell_id: id, first_seq: first, seq: last}

  defp response(reply) do
    {:task_commit_response, response} = ControlFrameTestHelper.decode_request(reply).command
    response
  end

  # The channel and its commit worker, owned by this test as the outer
  # instance's Overwatch.
  defp start_runner(outer) do
    runner_id = "#{@namespace}_#{System.unique_integer([:positive])}"
    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:test_writer_called, ref, message})
      :ok
    end

    {:ok, worker} = Commits.start_link({@namespace, runner_id})
    {:ok, pid} = ControlChannel.start_link({@namespace, runner_id, make_ref(), write_fun})
    ClassRegistry.put_overwatch(outer, self(), :sys.get_state(pid).identity)

    on_exit(fn ->
      for p <- [pid, worker], do: NodeTestHelper.ensure_stopped(p, :normal, 1_000)
    end)

    pid
  end
end

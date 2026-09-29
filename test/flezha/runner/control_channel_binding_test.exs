defmodule FlezhaTest.Runner.ControlChannelBindingTest do
  @moduledoc """
  A runner's frame about an operator acts only on an operator bound to that
  runner. Two runner sessions share one namespace; the operator's ownership
  record names runner A, and runner B's session writes each frame naming it.

  The control frames go through real `ControlChannel`s (4-tuple constructor,
  capturing `write_fun`) and the data frame through a real `StreamChannel`.
  The operator's process is a stand-in holding a real `%Overwatch{}`, which
  applies a shutdown snapshot through the real `Overwatch` callback; a real
  Overwatch needs a hub and a runner bind, which is not what these frames
  exercise. Neither session presented a stable identity, so each is its own
  session token.
  """
  # async: false: writes Application env and a shared on-disk root; Mimic
  # global mode, because the channels refuse and act in processes of their own.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Operator.{ClassRegistry, Overwatch}
  alias Flezha.Runner.{ControlChannel, ControlUnit, StreamChannel, StreamPool}
  alias Flezha.Runner.ControlChannel.Commits
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.{Completion, Paths, TaskJournal}
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.ControlFrameTestHelper
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @ns "ctrl-chan-binding-test"

  defmodule BoundOverwatch do
    @moduledoc false
    use GenServer

    def start_link(overwatch, test_pid),
      do: GenServer.start_link(__MODULE__, {overwatch, test_pid})

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call(:snapshot, _from, {overwatch, _} = state),
      do: {:reply, overwatch.snapshot, state}

    @impl true
    def handle_cast({:apply_external_snapshot, _} = msg, {overwatch, test_pid}) do
      {:noreply, overwatch} = Overwatch.handle_cast(msg, overwatch)
      {:noreply, {overwatch, test_pid}}
    end

    def handle_cast({:suspend, iter, deadline}, {overwatch, test_pid} = state) do
      send(test_pid, {:suspend_cast, overwatch.id, iter, deadline})
      {:noreply, state}
    end

    def handle_cast({:operator_exited, runner_id, reason}, {overwatch, test_pid} = state) do
      send(test_pid, {:exited_cast, overwatch.id, runner_id, reason})
      {:noreply, state}
    end
  end

  setup :set_mimic_global_until_exit

  setup do
    unique = System.unique_integer([:positive])
    runner_a = "binding_runner_a_#{unique}"
    runner_b = "binding_runner_b_#{unique}"
    operator_id = "bound-to-a-#{unique}"
    test_pid = self()

    {:ok, overwatch} =
      BoundOverwatch.start_link(
        %Overwatch{
          id: operator_id,
          namespace: @ns,
          runner_id: runner_a,
          snapshot: "shipped-by-a"
        },
        test_pid
      )

    :ok = ClassRegistry.put_overwatch(operator_id, overwatch, runner_a)

    HubManager
    |> stub(:get_pid, fn
      @ns, ^operator_id -> {:ok, overwatch}
      ns, id -> {:error, {:operator_not_found, ns, id}}
    end)

    # A refusal tells the sending runner to remove its copy.
    ControlUnit
    |> stub(:stop_operator_async, fn runner_id, id ->
      send(test_pid, {:rem_operator, runner_id, id})
      :ok
    end)

    channel_b = start_channel(runner_b)

    on_exit(fn ->
      ClassRegistry.delete(operator_id)
      NodeTestHelper.ensure_stopped(overwatch, :normal, 1_000)
    end)

    %{
      operator_id: operator_id,
      overwatch: overwatch,
      runner_a: runner_a,
      runner_b: runner_b,
      channel_b: channel_b
    }
  end

  test "KB-209: runner B's BoundedCompleted stops nothing and records nothing for A's operator",
       ctx do
    test_pid = self()
    %{operator_id: op, runner_b: runner_b} = ctx

    OperatorManager
    |> stub(:stop_operator, fn @ns, id ->
      send(test_pid, {:stop_operator_called, id})
      :ok
    end)

    write_frame(
      ctx.channel_b,
      {:bounded_completed,
       %RunnerCommands.BoundedCompleted{operator_id: op, status: "ok", result: "forged-by-b"}}
    )

    # The refusal is the positive signal that the frame was judged; a completion
    # acted on would have stopped the operator from a task of its own.
    assert_receive {:rem_operator, ^runner_b, ^op}, 1_000

    refute_received {:stop_operator_called, ^op}
    assert Completion.read(op) == :error
  end

  test "KB-210: runner B's IterPark neither journals a park nor suspends A's instance", ctx do
    %{operator_id: op, runner_b: runner_b} = ctx

    write_frame(
      ctx.channel_b,
      {:iter_park, %RunnerCommands.IterPark{operator_id: op, iter: 0, wake_deadline_ms: 0}}
    )

    assert_receive {:rem_operator, ^runner_b, ^op}, 1_000

    refute_received {:suspend_cast, ^op, _, _}
    refute parked?(op)
  end

  test "KB-210: runner B's IterPark records no park for an instance another namespace claims",
       ctx do
    foreign = "claimed-elsewhere-#{System.unique_integer([:positive])}"
    :ok = Paths.claim_namespace(foreign, "team-other")

    # No ownership record names the instance here, so the park is bounded by
    # the namespace its durable state belongs to.
    assert ClassRegistry.put_parked(foreign, %{wake_deadline_ms: nil, consumed_offset: nil}, @ns) ==
             {:error, {:namespace_conflict, foreign, "team-other"}}

    write_frame(
      ctx.channel_b,
      {:iter_park, %RunnerCommands.IterPark{operator_id: foreign, iter: 3, wake_deadline_ms: 0}}
    )

    parked = NodeTestHelper.wait_until(fn -> parked?(foreign) end, 1_000)

    assert parked in [nil, false],
           "a park was journaled at #{Paths.journal_path(foreign)}"
  end

  test "KB-211: runner B's shutdown notification leaves A's operator's snapshot alone", ctx do
    %{operator_id: op, runner_b: runner_b} = ctx

    write_frame(
      ctx.channel_b,
      {:runner_shutdown_notification,
       %RunnerCommands.RunnerShutdownNotification{
         operator_id: op,
         serialized_state: "forged-by-b"
       }}
    )

    # The ack is still written, and after any cast the channel made, so the
    # snapshot read below is queued behind that cast.
    assert_receive {:written, ^runner_b, _ref, reply}, 1_000

    assert {:runner_shutdown_ack, %RunnerCommands.RunnerShutdownAck{operator_id: ^op}} =
             ControlFrameTestHelper.decode_request(reply).command

    assert GenServer.call(ctx.overwatch, :snapshot) == "shipped-by-a"
    assert_received {:rem_operator, ^runner_b, ^op}
  end

  test "KB-325: runner B's TaskCommit for A's instance is refused, not journaled", ctx do
    %{operator_id: op, runner_b: runner_b} = ctx

    write_frame(
      ctx.channel_b,
      {:task_commit,
       %RunnerCommands.TaskCommit{
         operator_id: op,
         task_idx: 0,
         iter: 0,
         cells_ran: [],
         handoff: [%RunnerCommands.TaskHandoffItem{channel_id: 1, payload: "forged-by-b"}],
         compensating: false
       }}
    )

    # Every barrier is answered by the commit worker, an accepted one only after
    # the journal answered, so the reply is the barrier for the journal read.
    assert_receive {:written, ^runner_b, _ref, reply}, 2_000

    %RunnerCommands.CommandRequest{command: {:task_commit_response, response}} =
      ControlFrameTestHelper.decode_request(reply)

    assert {response.error.code, journaled?(op)} == {"commit.not_owner", false}
    assert_received {:rem_operator, ^runner_b, ^op}
  end

  test "the owner's exit report reaches its Overwatch behind a flush of its stream channels",
       ctx do
    test_pid = self()
    %{operator_id: op, runner_a: runner_a} = ctx

    StreamPool
    |> stub(:flush_channels, fn runner_id ->
      send(test_pid, {:flushed, runner_id})
      :ok
    end)

    channel_a = start_channel(runner_a)

    write_frame(
      channel_a,
      {:operator_exited, %RunnerCommands.OperatorExited{operator_id: op, reason: ":killed"}}
    )

    assert_receive {:flushed, ^runner_a}, 1_000
    assert_receive {:exited_cast, ^op, ^runner_a, ":killed"}, 1_000
    refute_received {:rem_operator, _, _}
  end

  test "runner B's exit report for A's instance is refused, counted, and parks nothing", ctx do
    %{operator_id: op, runner_a: runner_a, runner_b: runner_b} = ctx
    MessageTestHelper.forward_telemetry([:flezha, :runner, :foreign_frame_refused], :refused)
    frame = {:operator_exited, %RunnerCommands.OperatorExited{operator_id: op, reason: ":killed"}}

    write_frame(ctx.channel_b, frame)
    write_frame(ctx.channel_b, frame)
    _ = :sys.get_state(ctx.channel_b)

    assert_received {:refused, %{count: 1},
                     %{
                       runner_id: ^runner_b,
                       owner: ^runner_a,
                       operator_id: ^op,
                       kind: :operator_exited
                     }}

    refute_received {:refused, _, _}
    assert_received {:rem_operator, ^runner_b, ^op}
    refute_received {:rem_operator, _, _}
    rows = LivenessCounters.by_runner(LivenessCounters.snapshot_all().channels)
    assert get_in(rows, [runner_b, :foreign_frame_refused]) == 2
    refute_receive {:exited_cast, ^op, _, _}, 200
  end

  # The data plane has the same door: a frame naming an operator the sending
  # runner's channel holds no route for is judged by the ownership record.
  test "KB-326: runner B's data frame for A's operator does not reach A's operator", ctx do
    %{operator_id: op, runner_b: runner_b} = ctx
    :ok = ClassRegistry.put_overwatch(op, self(), ctx.runner_a)

    {:ok, channel} = StreamChannel.start_link({runner_b, :"binding_sc_#{runner_b}", :secondary})
    on_exit(fn -> NodeTestHelper.ensure_stopped(channel, :normal, 1_000) end)

    send(channel, {:flz_data, 0, op, "forged-by-b", %{}})
    _ = GenServer.call(channel, :stream_ref_status)

    refute_received {:"$gen_cast", {:io_out, "forged-by-b", _}},
                    "the frame was cast to the operator's Overwatch as its own output"

    assert_received {:rem_operator, ^runner_b, ^op}
  end

  defp journaled?(instance_id), do: match?({:ok, [_ | _]}, TaskJournal.read(instance_id))

  defp parked?(instance_id), do: ClassRegistry.parked(instance_id) != nil

  defp start_channel(runner_id) do
    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:written, runner_id, ref, message})
      :ok
    end

    {:ok, worker} = Commits.start_link({@ns, runner_id})
    {:ok, pid} = ControlChannel.start_link({@ns, runner_id, make_ref(), write_fun})

    on_exit(fn ->
      NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
      NodeTestHelper.ensure_stopped(worker, :normal, 1_000)
    end)

    pid
  end

  defp write_frame(channel, command) do
    payload = ControlFrameTestHelper.encode_request("ref-binding", command)
    send(channel, {:flz_data, :stream_id, "control_command", payload, %{}})
  end
end

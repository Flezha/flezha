defmodule FlezhaTest.Runner.ControlChannelBindingTest do
  @moduledoc """
  A runner's frame about a relay acts only on a relay bound to that
  runner. Two runner sessions share one namespace; the relay's ownership
  record names runner A, and runner B's session writes each frame naming it.

  The control frames go through real `ControlChannel`s (4-tuple constructor,
  capturing `write_fun`) and the data frame through a real `StreamChannel`.
  The relay's process is a stand-in holding a real `%Overwatch{}`, which
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

  alias Flezha.Relay.{ClassRegistry, Overwatch, Suspend}
  alias Flezha.Runner.{ControlChannel, ControlUnit, StreamChannel}
  alias Flezha.Runner.ControlChannel.Commits
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.{Completion, Paths, TaskJournal}
  alias FlezhaTest.Helper.ControlFrameTestHelper
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
  end

  setup :set_mimic_global_until_exit

  setup do
    unique = System.unique_integer([:positive])
    runner_a = "binding_runner_a_#{unique}"
    runner_b = "binding_runner_b_#{unique}"
    relay_id = "bound-to-a-#{unique}"
    test_pid = self()

    {:ok, overwatch} =
      BoundOverwatch.start_link(
        %Overwatch{
          id: relay_id,
          namespace: @ns,
          runner_id: runner_a,
          snapshot: "shipped-by-a"
        },
        test_pid
      )

    :ok = ClassRegistry.put_overwatch(relay_id, overwatch, runner_a)

    HubManager
    |> stub(:get_pid, fn
      @ns, ^relay_id -> {:ok, overwatch}
      ns, id -> {:error, {:relay_not_found, ns, id}}
    end)

    # A refusal tells the sending runner to remove its copy.
    ControlUnit
    |> stub(:stop_relay_async, fn runner_id, id ->
      send(test_pid, {:rem_relay, runner_id, id})
      :ok
    end)

    channel_b = start_channel(runner_b)

    on_exit(fn ->
      ClassRegistry.delete(relay_id)
      NodeTestHelper.ensure_stopped(overwatch, :normal, 1_000)
    end)

    %{
      relay_id: relay_id,
      overwatch: overwatch,
      runner_a: runner_a,
      runner_b: runner_b,
      channel_b: channel_b
    }
  end

  test "KB-209: runner B's RelayCompleted stops nothing and records nothing for A's relay",
       ctx do
    test_pid = self()
    %{relay_id: op, runner_b: runner_b} = ctx

    RelayManager
    |> stub(:stop_relay, fn @ns, id, _opts ->
      send(test_pid, {:stop_relay_called, id})
      :ok
    end)

    write_frame(
      ctx.channel_b,
      {:relay_completed,
       %RunnerCommands.RelayCompleted{relay_id: op, status: "ok", result: "forged-by-b"}}
    )

    # The refusal is the positive signal that the frame was judged; a completion
    # acted on would have stopped the relay from a task of its own.
    assert_receive {:rem_relay, ^runner_b, ^op}, 1_000

    refute_received {:stop_relay_called, ^op}
    assert Completion.read(op) == :error
  end

  test "the owning runner's RelayCompleted writes no record and hands the outcome to the stop",
       ctx do
    test_pid = self()
    %{relay_id: op} = ctx

    RelayManager
    |> stub(:stop_relay, fn @ns, id, opts ->
      send(test_pid, {:stop_relay_called, id, opts})
      :ok
    end)

    before = System.system_time(:second)

    write_frame(
      start_channel(ctx.runner_a),
      {:relay_completed,
       %RunnerCommands.RelayCompleted{relay_id: op, status: "error", error: "boom", result: "r"}}
    )

    assert_receive {:stop_relay_called, ^op, opts}, 2_000
    assert opts[:cause] == :complete
    assert Completion.read(op) == :error
    refute File.exists?(Paths.class_dir(op))

    assert %{status: "error", error: "boom", result: "r", undo: "", finished_at: finished_at} =
             opts[:terminal]

    assert finished_at >= before
  end

  test "KB-210: runner B's IterPark neither journals a park nor suspends A's instance", ctx do
    %{relay_id: op, runner_b: runner_b} = ctx

    write_frame(
      ctx.channel_b,
      {:iter_park, %RunnerCommands.IterPark{relay_id: op, iter: 0, wake_deadline_ms: 0}}
    )

    assert_receive {:rem_relay, ^runner_b, ^op}, 1_000

    refute_received {:suspend_cast, ^op, _, _}
    refute parked?(op)
  end

  test "KB-210: runner B's IterPark records no park for an instance another namespace claims",
       ctx do
    foreign = "claimed-elsewhere-#{System.unique_integer([:positive])}"
    :ok = Paths.claim_namespace(foreign, "team-other")

    # No ownership record names the instance here, so the park is bounded by
    # the namespace its durable state belongs to.
    assert Suspend.put_sleep(foreign, %{wake_deadline_ms: nil, consumed_offset: nil}, @ns) ==
             {:error, {:namespace_conflict, foreign, "team-other"}}

    write_frame(
      ctx.channel_b,
      {:iter_park, %RunnerCommands.IterPark{relay_id: foreign, iter: 3, wake_deadline_ms: 0}}
    )

    _ = :sys.get_state(ctx.channel_b)
    NodeTestHelper.await_durable_tasks()

    refute parked?(foreign), "a park was journaled at #{Paths.journal_path(foreign)}"
    # Refused at the ownership check, before the park reached any path.
    runner_b = ctx.runner_b
    assert_received {:rem_relay, ^runner_b, ^foreign}
  end

  # Fails when: a control request's relay id, which no cell-id decode checks,
  # names a directory past its namespace and the park is written there.
  test "KB-877: an IterPark whose relay id climbs out of its namespace writes nothing" do
    runner = "binding_runner_sys_#{System.unique_integer([:positive])}"
    climbing = "../team-other/planted"

    write_frame(
      start_channel(runner, "_system"),
      {:iter_park, %RunnerCommands.IterPark{relay_id: climbing, iter: 0, wake_deadline_ms: 0}}
    )

    assert_receive {:rem_relay, ^runner, ^climbing}, 1_000
    NodeTestHelper.await_durable_tasks()
    refute File.exists?(Path.join([Paths.state_root(), "team-other", "planted"]))
  end

  # KB-632: during a hub merge's hand-over the hub still lists the source
  # node's departing Overwatch, so a frame from the bound runner goes to the
  # Overwatch its ownership record names, the one it was admitted against.
  test "KB-632: the owner's IterPark reaches the Overwatch its ownership record names, " <>
         "not a departing copy the hub still lists",
       ctx do
    %{relay_id: op, runner_a: runner_a} = ctx
    hub_lists_a_departing_copy(op)

    write_frame(
      start_channel(runner_a),
      {:iter_park, %RunnerCommands.IterPark{relay_id: op, iter: 0, wake_deadline_ms: 0}}
    )

    assert_receive {:suspend_cast, suspended, 0, _}, 1_000
    assert suspended == op, "the park went to #{inspect(suspended)}, not the bound Overwatch"
    refute_received {:rem_relay, _, _}
  end

  test "KB-632: the owner's shutdown snapshot reaches the Overwatch its ownership record names",
       ctx do
    %{relay_id: op, runner_a: runner_a} = ctx
    hub_lists_a_departing_copy(op)

    write_frame(
      start_channel(runner_a),
      {:runner_shutdown_notification,
       %RunnerCommands.RunnerShutdownNotification{relay_id: op, serialized_state: "final-from-a"}}
    )

    # The ack is written after the channel's cast, so the read below queues behind it.
    assert_receive {:written, ^runner_a, _ref, _ack}, 1_000
    assert GenServer.call(ctx.overwatch, :snapshot) == "final-from-a"
  end

  # KB-764: the ownership record outlives the Overwatch it names, and the hub
  # can still list that dead pid or a departing copy, so the owner's park is
  # recorded on disk for the next start to find instead.
  test "KB-764: the owner's IterPark is recorded when its recorded Overwatch died and the hub " <>
         "still lists that dead pid",
       ctx do
    %{relay_id: op, runner_a: runner_a, overwatch: overwatch} = ctx
    NodeTestHelper.ensure_stopped(overwatch, :normal, 1_000)
    assert {:ok, ^overwatch, ^runner_a} = ClassRegistry.owner(op)
    assert HubManager.get_pid(@ns, op) == {:ok, overwatch}

    write_frame(
      start_channel(runner_a),
      {:iter_park, %RunnerCommands.IterPark{relay_id: op, iter: 0, wake_deadline_ms: 0}}
    )

    assert NodeTestHelper.wait_until(fn -> parked?(op) end, 1_000) == true,
           "the park went to the dead pid the hub listed and was lost"
  end

  test "KB-764: the owner's IterPark is recorded, not sent to a departing copy the hub lists, " <>
         "when its recorded Overwatch died",
       ctx do
    %{relay_id: op, runner_a: runner_a, overwatch: overwatch} = ctx
    NodeTestHelper.ensure_stopped(overwatch, :normal, 1_000)
    hub_lists_a_departing_copy(op)

    write_frame(
      start_channel(runner_a),
      {:iter_park, %RunnerCommands.IterPark{relay_id: op, iter: 0, wake_deadline_ms: 0}}
    )

    # The cast and the record are the two ends of one decision, so once the
    # record is there a cast to the departing copy would already have landed.
    assert NodeTestHelper.wait_until(fn -> parked?(op) end, 1_000) == true,
           "the park went to the departing copy the hub listed"

    refute_received {:suspend_cast, "departing", 0, _}
  end

  test "KB-211: runner B's shutdown notification leaves A's relay's snapshot alone", ctx do
    %{relay_id: op, runner_b: runner_b} = ctx

    write_frame(
      ctx.channel_b,
      {:runner_shutdown_notification,
       %RunnerCommands.RunnerShutdownNotification{
         relay_id: op,
         serialized_state: "forged-by-b"
       }}
    )

    # The ack is still written, and after any cast the channel made, so the
    # snapshot read below is queued behind that cast.
    assert_receive {:written, ^runner_b, _ref, reply}, 1_000

    assert {:runner_shutdown_ack, %RunnerCommands.RunnerShutdownAck{relay_id: ^op}} =
             ControlFrameTestHelper.decode_request(reply).command

    assert GenServer.call(ctx.overwatch, :snapshot) == "shipped-by-a"
    assert_received {:rem_relay, ^runner_b, ^op}
  end

  test "KB-325: runner B's TaskCommit for A's instance is refused, not journaled", ctx do
    %{relay_id: op, runner_b: runner_b} = ctx

    write_frame(
      ctx.channel_b,
      {:task_commit,
       %RunnerCommands.TaskCommit{
         relay_id: op,
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
    assert_received {:rem_relay, ^runner_b, ^op}
  end

  # The data plane has the same door: a frame naming a relay the sending
  # runner's channel holds no route for is judged by the ownership record.
  test "KB-326: runner B's data frame for A's relay does not reach A's relay", ctx do
    %{relay_id: op, runner_b: runner_b} = ctx
    :ok = ClassRegistry.put_overwatch(op, self(), ctx.runner_a)

    {:ok, channel} =
      StreamChannel.start_link({@ns, runner_b, :"binding_sc_#{runner_b}", :secondary})

    on_exit(fn -> NodeTestHelper.ensure_stopped(channel, :normal, 1_000) end)

    send(channel, {:flz_data, 0, op, "forged-by-b", %{}})
    _ = GenServer.call(channel, :stream_ref_status)

    refute_received {:"$gen_cast", {:io_out, "forged-by-b", _}},
                    "the frame was cast to the relay's Overwatch as its own output"

    assert_received {:rem_relay, ^runner_b, ^op}
  end

  defp hub_lists_a_departing_copy(relay_id) do
    {:ok, departing} =
      BoundOverwatch.start_link(
        %Overwatch{id: "departing", namespace: @ns, runner_id: "on-the-source-node"},
        self()
      )

    on_exit(fn -> NodeTestHelper.ensure_stopped(departing, :normal, 1_000) end)

    HubManager
    |> stub(:get_pid, fn
      @ns, ^relay_id -> {:ok, departing}
      ns, id -> {:error, {:relay_not_found, ns, id}}
    end)
  end

  defp journaled?(instance_id), do: match?({:ok, [_ | _]}, TaskJournal.read(instance_id))

  defp parked?(instance_id), do: Suspend.sleep_record(instance_id) != nil

  defp start_channel(runner_id, namespace \\ @ns) do
    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:written, runner_id, ref, message})
      :ok
    end

    {:ok, worker} = Commits.start_link({namespace, runner_id})
    {:ok, pid} = ControlChannel.start_link({namespace, runner_id, make_ref(), write_fun})

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

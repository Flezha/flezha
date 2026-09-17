defmodule FlezhaTest.System.WireRefusalDrillTest do
  @moduledoc """
  A wire that has gone for good is detected once per runtime, and both facts that
  say so lead to the same exit.

  Standard input closing and a termination signal are the two ways a runner
  learns the wire will take nothing more, and neither is observed by the host's
  own loop — which is exactly the thread a compute-bound handler holds. The
  detector and the signal handler are the core's, on a thread of its own, so
  what this drill asserts per runtime is that the runner ends, promptly, and
  opens no new control connection on its way out (KI-131's shape).

      FLEZHA_E2E_RUNTIME=elixir mix test --include system \
        test/system/wire_refusal_drill_test.exs

  The guarantee class each frame carries — a durable frame held and replayed, a
  control frame refused, a heartbeat owed nothing, a payload id the table does
  not name refused at the call — is proved over real streams by
  `sdk/elixir/test/flezha/sdk/hold_class_test.exs`, `test/flezha/sdk/record_hold_test.exs`
  and `flz_core/src/state_sink.rs`'s own tests, one per runtime core.

  A data plane the controller severs is a wire that has gone for part of the
  session only, and it is repaired inside that session: the runner does not
  reconnect for it, and production resumes on the rebound streams.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.TierE2EHarness

  # Forwards its input to its output unchanged, which is what makes an output
  # attributable to the input that produced it.
  @factory "do_nothing"

  setup do
    namespace = "wire_refusal_#{System.unique_integer([:positive])}"

    on_exit(fn -> TierE2EHarness.stop_namespace(namespace) end)

    %{namespace: namespace}
  end

  @tag timeout: 300_000
  test "a closed standard input ends the runner, whatever its host language",
       %{namespace: namespace} do
    {port, runner_id} = boot(namespace)
    operative_id = "wr_eof_#{System.unique_integer([:positive])}"

    assert {:ok, %{runner_id: ^runner_id}} =
             OperativeManager.start_operative(namespace, builder(operative_id, self()))

    assert produced?(operative_id, namespace, "alive", 60_000)

    {:os_pid, os_pid} = :erlang.port_info(port, :os_pid)
    # The harness's own stop: it manages a runner as a Port and closes it,
    # which is this process's standard input and no signal at all.
    NodeTestHelper.ensure_port_dead(port)

    assert wait_until(fn -> not alive?(os_pid) end, 60_000),
           "a closed standard input did not end the runner"

    assert wait_until(fn -> runner_id not in FleetManager.active_runners(namespace) end, 60_000),
           "the runner stayed in the fleet after it exited"
  end

  @tag timeout: 300_000
  test "a termination signal ends the runner, and it opens no new control connection",
       %{namespace: namespace} do
    {port, runner_id} = boot(namespace)
    operative_id = "wr_exit_#{System.unique_integer([:positive])}"

    assert {:ok, %{runner_id: ^runner_id}} =
             OperativeManager.start_operative(namespace, builder(operative_id, self()))

    assert produced?(operative_id, namespace, "alive", 60_000)

    {:os_pid, os_pid} = :erlang.port_info(port, :os_pid)
    {_, 0} = System.cmd("kill", ["-TERM", Integer.to_string(os_pid)], stderr_to_stdout: true)

    assert wait_until(fn -> not alive?(os_pid) end, 60_000),
           "the runner did not exit on SIGTERM; the detector is the core's and reaches " <>
             "the exit on a thread of its own, so no host loop can hold it up"

    # KI-131's shape: a stopping runner that reached the exit through a path
    # that re-registers comes back under a fresh id and wins the next bind.
    assert wait_until(fn -> runner_id not in FleetManager.active_runners(namespace) end, 60_000),
           "the runner stayed in the fleet after it exited"

    assert FleetManager.active_runners(namespace) == [],
           "a stopping runner opened a new control connection: " <>
             inspect(FleetManager.active_runners(namespace))

    NodeTestHelper.ensure_port_dead(port)
  end

  @tag timeout: 300_000
  test "severed data streams are rebound inside the session, and nothing reconnects",
       %{namespace: namespace} do
    # A ten-minute heartbeat, so no ladder of any kind can stand in for the repair.
    {port, runner_id} = boot(namespace, [{"FLEZHA_DATA_HEARTBEAT_MS", "600000"}])
    operative_id = "wr_sever_#{System.unique_integer([:positive])}"

    assert {:ok, %{runner_id: ^runner_id}} =
             OperativeManager.start_operative(namespace, builder(operative_id, self()))

    assert produced?(operative_id, namespace, "before", 60_000)

    # A break on the controller's end, as its driver reports one: the owner is
    # told, then the stream is released.
    for name <- StreamPool.stream_ids(runner_id, :secondary),
        channel = GenServer.whereis(name),
        ref = :sys.get_state(channel).stream_ref do
      send(channel, {:flz_closed, Flezha.Transport.Stream.stream_id(ref), "severed"})
      Flezha.Transport.Stream.close(ref)
    end

    resumed? = produced?(operative_id, namespace, "after", 60_000)
    reconnects = count_lines(port, "reconnecting (attempt")

    assert resumed?, "production never resumed; the runner logged #{reconnects} reconnects"
    assert reconnects == 0, "the runner reconnected #{reconnects} times for a data-plane fault"
    assert runner_id in FleetManager.active_runners(namespace)
  end

  # ---------- helpers ----------

  defp boot(namespace, env \\ []) do
    {port, runner_id} =
      NodeTestHelper.start_runner_port(namespace,
        transport: :uds,
        env: env,
        port_opts: [:stderr_to_stdout]
      )

    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)
    {port, runner_id}
  end

  defp builder(operative_id, recv_pid) do
    sink = %Flezha.Plugin{
      id: "wire_refusal_sink",
      namespace: nil,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: recv_pid},
      hooks: [{:on_output, []}]
    }

    %Builder{id: operative_id, factory_id: @factory, init_values: %{}, opts: [plugins: [sink]]}
  end

  # Feeds one input until its own output comes back, so "the wire works" is
  # answered by the wire rather than by a sleep.
  defp produced?(operative_id, namespace, input, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    feed_until(operative_id, namespace, input, deadline)
  end

  defp feed_until(operative_id, namespace, input, deadline) do
    OperativeManager.send_io(operative_id, namespace, input, %{"input_type" => "2"})

    receive do
      {:message_forward, payload} ->
        to_string(payload) == input or feed_until(operative_id, namespace, input, deadline)
    after
      500 ->
        System.monotonic_time(:millisecond) < deadline and
          feed_until(operative_id, namespace, input, deadline)
    end
  end

  # The runner's own log, as the harness's port delivered it so far.
  defp count_lines(port, needle) do
    receive do
      {^port, {:data, data}} ->
        length(String.split(IO.iodata_to_binary(data), needle)) - 1 + count_lines(port, needle)
    after
      0 -> 0
    end
  end

  # A runner that exited but has not been reaped still answers `kill -0`, and
  # the harness closes the port before it can reap: the state is what says
  # whether the process is still running.
  defp alive?(os_pid) do
    case System.cmd("ps", ["-o", "state=", "-p", Integer.to_string(os_pid)],
           stderr_to_stdout: true
         ) do
      {out, 0} -> String.trim(out) not in ["", "Z", "X"]
      _ -> false
    end
  end
end

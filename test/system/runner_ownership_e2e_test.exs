defmodule FlezhaTest.System.RunnerOwnershipE2ETest do
  @moduledoc """
  A runner may act only on the relays the controller has handed it, and a
  copy it keeps after the controller moved the relay elsewhere is refused
  the moment it speaks.

  The fault is injected the way it happens in production: runner A is frozen
  (SIGSTOP), the controller judges it dead and moves its relay to runner B,
  then A resumes, reconnects under the same stable identity, and its copy of
  the relay fires the timer it armed before the freeze. The timer is
  durable, so B restores it and fires it too, at the same deadline and with the
  same output: the output cannot tell the two runners apart. What does is
  whether the controller admits the copy's frames, which it must refuse by
  name, never route and never let rewind the owner's WAL.

  Tagged `:system`; run with
  `mix test --include system test/system/runner_ownership_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  # Controller mechanics: which runner's frames the controller takes as the
  # owner's. Every SDK keeps its copies the same way; the reference proves it.
  @moduletag reference_runtime: true
  @moduletag timeout: 180_000

  import FlezhaTest.Helper.NodeTestHelper, only: [runner_event: 5, wait_until: 2]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [
      boot_runner: 3,
      os_pid: 1,
      produces?: 5,
      sigkill: 1,
      stop_namespace: 1,
      unique_id_path: 1
    ]

  alias Flezha.Convergence.Loop
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Relay.Builder
  alias FlezhaTest.Helper.FleetStateHelper
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.{HubManager, RelayManager}
  alias Flezha.State.{Paths, WalWriter}
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.RelayTestHelper
  alias FlezhaTest.Helper.Telemetry

  @tick_ms 1_000

  setup do
    namespace = "ownership_#{System.unique_integer([:positive])}"

    ConvergenceSnapshot.reset_recovery_states()
    collect([:flezha, :convergence, :tick], :tick)
    collect([:flezha, :runner, :foreign_frame_refused], :foreign_frame_refused)

    on_exit(fn -> stop_namespace(namespace) end)

    %{namespace: namespace}
  end

  @tag capability: :cell_timers, skip: FlezhaTest.SystemCase.deferral(:cell_timers)
  test "a runner evicted while alive keeps its copy, and the copy is refused when it speaks",
       %{namespace: namespace} do
    # A must notice, once resumed, that its session ended. The controller's end
    # of a stopped session's control socket closes only when the last reference
    # to the stream is collected, which can outlast the reconnect wait below, so
    # A's own control idle window, shortened here, bounds its reconnect instead.
    {port_a, runner_a} =
      boot_runner(namespace, unique_id_path("own_a"),
        env: [
          {"TIMER_TEST_TIMEOUT_MS", "4000"},
          {"FLEZHA_CONTROL_KEEPALIVE_MS", "1000"},
          {"FLEZHA_CONTROL_IDLE_TIMEOUT_MS", "6000"}
        ]
      )

    id = start_relay(namespace)
    assert bound_runner(namespace, id) == runner_a

    {_port_b, runner_b} =
      boot_runner(namespace, unique_id_path("own_b"), env: [{"TIMER_TEST_TIMEOUT_MS", "600000"}])

    start_loop()

    # A's copy records the touch and arms its 4 s timer.
    send_input(namespace, id, "t1")
    cell = await_cell(id)
    seq_on_a = await_seq_change(cell, -1)

    # The controller judges A dead while the process is alive, and moves the
    # relay to B.
    signal(os_pid(port_a), "-STOP")
    :ok = FleetStateHelper.disconnected(namespace, runner_a)

    assert wait_until(fn -> bound_runner(namespace, id) == runner_b end, 30_000),
           "the relay never moved to the surviving runner"

    # The owner advances the lineage by two records.
    send_input(namespace, id, "t2")
    seq_after_t2 = await_seq_change(cell, seq_on_a)
    send_input(namespace, id, "t3")
    owner_seq = await_seq_change(cell, seq_after_t2)

    # A resumes: it reconnects under a fresh session token and the same stable
    # identity, still holding its copy, whose timer fires.
    {_, runner_a2} =
      runner_event(:connected, namespace, nil, 30_000, fn ->
        signal(os_pid(port_a), "-CONT")
      end)

    runner_a2 || flunk("runner A never reconnected")
    assert runner_a2 != runner_a

    assert wait_until(fn -> refusals(runner_a2) != [] end, 30_000),
           "A's copy was never refused after reconnecting as #{runner_a2}"

    # Nothing of the copy was admitted: its session routed no output for the
    # relay, so none reached the relay's sinks, and the owner's lineage
    # was never rewound. B's own firing of the restored timer may advance it.
    assert routed_by(runner_a2, id) == [], "the copy's session routed outputs for #{id}"
    assert last_persisted_seq(cell) >= owner_seq, "the owner's WAL lineage was rewound"

    # The copy's frames are counted, and it is reported and told to stop once.
    assert refused_frames(runner_a2) >= 1
    assert [%{relay_id: ^id}] = refusals(runner_a2)

    # The owner keeps its lineage and keeps advancing it.
    persisted = last_persisted_seq(cell)
    send_input(namespace, id, "t4")
    assert await_seq_change(cell, persisted) > persisted
  end

  test "a reconnect under the same stable identity re-points the relay's Overwatch",
       %{namespace: namespace} do
    id_path = unique_id_path("own_restart")
    {_port_a, runner_a} = boot_runner(namespace, id_path, [])
    id = start_relay(namespace, "do_nothing")
    assert bound_runner(namespace, id) == runner_a
    start_loop()

    :ok = FlezhaTest.Helper.NodeTestHelper.stop_runner(namespace, runner_a)
    {_port_a2, runner_a2} = boot_runner(namespace, id_path, [])
    assert runner_a2 != runner_a

    assert wait_until(
             fn -> HubManager.relay_runner_tag(namespace, id) == runner_a2 end,
             10_000
           ),
           "the registry tag never moved to the new session"

    assert wait_until(fn -> bound_runner(namespace, id) == runner_a2 end, 1_000),
           "the tag names #{runner_a2} but the Overwatch still names #{bound_runner(namespace, id)}"
  end

  # A runner restarted on its identity runs none of the relays it held, so
  # its connect must bind them onto it afresh rather than only re-point them.
  @tag capability: :liveness, skip: FlezhaTest.SystemCase.deferral(:liveness)
  test "a stream on a runner restarted on its identity answers input again",
       %{namespace: namespace} do
    id_path = unique_id_path("own_restart_stream")
    {port_a, runner_a} = boot_runner(namespace, id_path, [])
    id = start_relay(namespace, "wedge_probe")
    assert produces?(id, namespace, "one", "ok:one", 30_000)

    sigkill(os_pid(port_a))
    {_port_a2, runner_a2} = boot_runner(namespace, id_path, [])
    assert runner_a2 != runner_a

    assert produces?(id, namespace, "two", "ok:two", 60_000),
           "the stream answered no input on the restarted runner"
  end

  # ---------- helpers ----------

  defp refusals(runner) do
    for {:foreign_frame_refused, %{runner_id: ^runner} = m} <- mailbox(), do: m
  end

  # The channels of `runner`'s session that hold a route to `id`.
  defp routed_by(runner, id) do
    for pid <- StreamPool.channel_pids(runner),
        Map.has_key?(:sys.get_state(pid).relay_mapping, id),
        do: pid
  end

  defp refused_frames(runner_id) do
    rows = LivenessCounters.by_runner(LivenessCounters.snapshot_all().channels)
    get_in(rows, [runner_id, :foreign_frame_refused]) || 0
  end

  defp mailbox() do
    {:messages, messages} = Process.info(self(), :messages)
    messages
  end

  defp start_relay(namespace, factory \\ "session_timeout") do
    id = "owned_#{System.unique_integer([:positive])}"

    sink = %Flezha.Plugin{
      id: "ownership_sink_#{id}",
      namespace: namespace,
      relay_id: id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    builder = %Builder{
      id: id,
      factory_id: factory,
      init_values: %{},
      opts: [bindings: RelayFixture.bindings(namespace, id, [sink])]
    }

    assert {:ok, _} = RelayManager.start_relay(namespace, builder)
    id
  end

  defp send_input(namespace, id, input),
    do: RelayTestHelper.cast_io(id, namespace, input, %{"input_type" => "2"})

  defp await_cell(id) do
    assert wait_until(fn -> Paths.cells_of(id) != [] end, 30_000),
           "the relay persisted no cell"

    [cell] = Paths.cells_of(id)
    cell
  end

  defp await_seq_change(cell, previous) do
    assert wait_until(fn -> last_persisted_seq(cell) not in [previous, nil] end, 30_000),
           "the WAL never advanced past #{previous}"

    last_persisted_seq(cell)
  end

  defp last_persisted_seq(cell) do
    case Flezha.State.Supervisor.lookup(cell) do
      {:ok, writer} -> WalWriter.last_persisted_seq(writer)
      :error -> nil
    end
  end

  defp bound_runner(namespace, id) do
    with {:ok, pid} <- RelayManager.get_relay_pid(namespace, id),
         %{runner_id: runner_id} <- :sys.get_state(pid) do
      runner_id
    else
      _ -> nil
    end
  end

  defp start_loop() do
    start_supervised!({Loop, tick_ms: @tick_ms, output_stall_grace_ms: 30_000})
    assert_receive {:tick, _}, 10_000
  end

  defp signal(os_pid, flag) do
    {_, 0} = System.cmd("kill", [flag, Integer.to_string(os_pid)], stderr_to_stdout: true)
  end

  defp collect(event, tag),
    do: Telemetry.capture(event, as: fn _e, meas, meta -> {tag, Map.merge(meas, meta)} end)
end

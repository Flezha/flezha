defmodule FlezhaTest.System.DataStreamLossDrillTest do
  @moduledoc """
  Drill: one data stream is lost while the runner and its control channel stay
  up. Does the platform get it back?

  The loss is real and is caused by the RUNNER's side of the wire. The runner
  boots with `FLEZHA_MAX_FRAME_BYTES` far below the frame the controller is
  about to send it; that frame's declared length is stream-fatal to
  `payloader::extract_payload`, so `run_data_reader` breaks out of its loop and
  the thread drops the primary socket. The controller's driver reads EOF and
  delivers `{:flz_closed, ...}` to the `StreamChannel`, which unbinds — the
  exact state `FleetManager.rebind_data_streams/1` documents itself as the
  remedy for, reached without touching the control channel.

      FLEZHA_DATA_STREAM_LOSS_DRILL=1 mix test --include system \
        test/system/data_stream_loss_drill_test.exs
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import ExUnit.CaptureLog, only: [capture_log: 1]
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness, only: [boot_runner: 3, unique_id_path: 1]

  alias Flezha.Convergence.Loop
  alias Flezha.Operative.Builder
  alias Flezha.Runner.{ControlUnit, FleetManager, StreamPool}
  alias Flezha.Service.OperativeManager

  # An instrument, not a suite member: its two windows measure rather than
  # assert, and they cost ~210 s. `--include system` therefore leaves it alone.
  @moduletag skip:
               FlezhaTest.SystemCase.deferral(:runner_lifecycle) ||
                 FlezhaTest.SystemCase.skip_unless(
                   System.get_env("FLEZHA_DATA_STREAM_LOSS_DRILL") == "1",
                   "run the data-stream-loss drill with FLEZHA_DATA_STREAM_LOSS_DRILL=1"
                 )

  @factory "do_nothing"
  # Below every control frame the handshake needs, above nothing the drill
  # sends by accident.
  @frame_cap 8192
  @tick_ms 1_500

  setup do
    namespace = "dsl_drill_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    %{namespace: namespace}
  end

  @tag timeout: 420_000
  test "a lost primary data stream on a live runner", %{namespace: namespace} do
    # The test env pins Logger at `:error`, and the acceptor's account of what
    # it did with each incoming stream is a warning.
    previous_level = Logger.level()
    Logger.configure(level: :warning)
    on_exit(fn -> Logger.configure(level: previous_level) end)

    parent = self()
    drops = collector(parent, [:flezha, :stream, :drop], :drop)
    heals = collector(parent, [:flezha, :convergence, :heal], :heal)
    stalls = collector(parent, [:flezha, :state, :output_stall], :stall)
    on_exit(fn -> Enum.each([drops, heals, stalls], &:telemetry.detach/1) end)

    {port, runner_id} =
      boot_runner(namespace, unique_id_path("dsl_drill"),
        env: [{"FLEZHA_MAX_FRAME_BYTES", Integer.to_string(@frame_cap)}],
        port_opts: [:stderr_to_stdout]
      )

    # Up BEFORE the loss: the Loop's own telemetry handler is what turns an
    # `output_stall` into a tick, and one attached afterwards never sees the
    # event the close emitted.
    start_supervised!({Loop, tick_ms: @tick_ms})

    operative_id = "dsl_#{System.unique_integer([:positive])}"

    assert {:ok, %{runner_id: ^runner_id}} =
             OperativeManager.start_operative(namespace, builder(operative_id, parent))

    # --- Phase 0: the pair works ------------------------------------------
    assert produces?(operative_id, namespace, "before", 60_000),
           "the operative never produced before the loss"

    primaries = StreamPool.stream_ids(runner_id, :primary)
    secondaries = StreamPool.stream_ids(runner_id, :secondary)

    assert bound(primaries) == length(primaries), "not every primary was bound before the drill"
    assert bound(secondaries) == length(secondaries), "not every secondary was bound"

    log(
      "phase 0: #{bound(primaries)}/#{length(primaries)} primaries and " <>
        "#{bound(secondaries)}/#{length(secondaries)} secondaries bound, operative producing"
    )

    # --- Phase 1: lose the primary ----------------------------------------
    OperativeManager.send_io(
      operative_id,
      namespace,
      :binary.copy("x", 10 * @frame_cap),
      %{"input_type" => "2"}
    )

    assert wait_until(fn -> bound(primaries) < length(primaries) end, 30_000),
           "no primary unbound: the oversized frame did not kill the runner's reader"

    lost = Enum.find(primaries, &(ref_of(&1) == nil))

    log(
      "phase 1: primary #{inspect(lost)} UNBOUND " <>
        "(#{bound(primaries)}/#{length(primaries)} primaries, " <>
        "#{bound(secondaries)}/#{length(secondaries)} secondaries still bound)"
    )

    # The rest of the runner is untouched: this is a data-stream loss, not a
    # runner loss.
    assert runner_id in FleetManager.active_runners(namespace),
           "the runner left the active set — the control channel died too"

    assert {:ok, :pong} = ControlUnit.ping_runner(runner_id, timeout_ms: 10_000),
           "the control channel did not answer a ping"

    log("phase 1: the control channel answered a ping — the runner is alive")

    _ = drain_port(port, [])

    # --- Phase 2: does anything bring it back? -----------------------------
    healed_by_itself? = produces?(operative_id, namespace, "after", 90_000)

    log("""
    phase 2 (self-heal window, 90 s, convergence Loop ticking at #{@tick_ms} ms):
      produced again      = #{healed_by_itself?}
      primaries bound     = #{bound(primaries)}/#{length(primaries)}
      secondaries bound   = #{bound(secondaries)}/#{length(secondaries)}
      lost one rebound    = #{ref_of(lost) != nil}
      runner still active = #{runner_id in FleetManager.active_runners(namespace)}
      bound runner        = #{inspect(bound_runner(namespace, operative_id))}
      heal ops seen       = #{inspect(seen(:heal))}
      output_stalls seen  = #{length(seen(:stall))}
      stream drops seen   = #{length(seen(:drop))}
      runner said         = #{inspect(drain_port(port, []), printable_limit: 4_000)}
    """)

    # --- Phase 3: the dead function, driven by hand ------------------------
    unless healed_by_itself? do
      {{manual, rebound?, healed_by_hand?}, phase3_log} =
        with_log(fn ->
          out = FleetManager.rebind_data_streams(runner_id)
          r = wait_until(fn -> ref_of(lost) != nil end, 30_000)
          {out, r, r and produces?(operative_id, namespace, "manual", 60_000)}
        end)

      log("""
      phase 3 (FleetManager.rebind_data_streams/1 called by hand):
        call answered       = #{inspect(manual)}
        lost one rebound    = #{rebound?}
        produced again      = #{healed_by_hand?}
        primaries bound     = #{bound(primaries)}/#{length(primaries)}
        secondaries bound   = #{bound(secondaries)}/#{length(secondaries)}
        runner still active = #{runner_id in FleetManager.active_runners(namespace)}
        heal ops seen       = #{inspect(seen(:heal))}
        primaries refused   = #{count(phase3_log, "No rebindable primary")}
        secondaries refused = #{count(phase3_log, "No rebindable secondary")}
        log lines captured  = #{count(phase3_log, "")}
        runner broken pipes = #{count(drain_port(port, []), "Broken pipe")}
      """)
    end
  end

  @tag timeout: 420_000
  test "the same loss with a second runner in the fleet", %{namespace: namespace} do
    parent = self()
    heals = collector(parent, [:flezha, :convergence, :heal], :heal)
    on_exit(fn -> :telemetry.detach(heals) end)

    start_supervised!({Loop, tick_ms: @tick_ms})

    {_port_a, runner_a} =
      boot_runner(namespace, unique_id_path("dsl_drill_a"),
        env: [{"FLEZHA_MAX_FRAME_BYTES", Integer.to_string(@frame_cap)}]
      )

    operative_id = "dsl2_#{System.unique_integer([:positive])}"

    assert {:ok, %{runner_id: ^runner_a}} =
             OperativeManager.start_operative(namespace, builder(operative_id, parent))

    assert produces?(operative_id, namespace, "before", 60_000)

    # The replacement a real fleet has. Up before the loss, so a rebind is not
    # timing the backoff ladder against an empty fleet.
    {_port_b, runner_b} = boot_runner(namespace, unique_id_path("dsl_drill_b"), [])

    primaries_a = StreamPool.stream_ids(runner_a, :primary)

    OperativeManager.send_io(
      operative_id,
      namespace,
      :binary.copy("x", 10 * @frame_cap),
      %{"input_type" => "2"}
    )

    assert wait_until(fn -> bound(primaries_a) < length(primaries_a) end, 30_000)

    healed? = produces?(operative_id, namespace, "after", 120_000)

    log("""
    two-runner arm:
      produced again      = #{healed?}
      bound runner        = #{inspect(bound_runner(namespace, operative_id))}
      runner_a            = #{runner_a}
      runner_b            = #{runner_b}
      runner_a primaries  = #{bound(primaries_a)}/#{length(primaries_a)}
      active runners      = #{inspect(FleetManager.active_runners(namespace))}
      heal ops seen       = #{inspect(seen(:heal))}
    """)
  end

  # ---------- helpers ----------

  # `capture_log/1` swallows the return value, so the two are threaded by hand.
  defp with_log(fun) do
    owner = self()
    log = capture_log(fn -> send(owner, {:with_log, fun.()}) end)

    receive do
      {:with_log, value} -> {value, log}
    after
      0 -> {nil, log}
    end
  end

  defp count(text, needle) do
    text |> String.split("\n") |> Enum.count(&(&1 =~ needle))
  end

  defp builder(operative_id, recv_pid) do
    sink = %Flezha.Plugin{
      id: "dsl_sink",
      namespace: nil,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: recv_pid},
      subscriptions: [{:on_output, []}]
    }

    %Builder{id: operative_id, factory_id: @factory, init_values: %{}, opts: [plugins: [sink]]}
  end

  defp bound(names), do: Enum.count(names, &(ref_of(&1) != nil))

  defp ref_of(name) do
    case GenServer.whereis(name) do
      nil -> nil
      pid -> GenServer.call(pid, :stream_ref_status, 5_000)
    end
  end

  defp bound_runner(namespace, operative_id) do
    with {:ok, pid} <- OperativeManager.get_operative_pid(namespace, operative_id),
         %{runner_id: runner_id} <- :sys.get_state(pid) do
      runner_id
    else
      _ -> nil
    end
  end

  defp produces?(operative_id, namespace, input, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    feed_until(operative_id, namespace, input, deadline)
  end

  defp feed_until(operative_id, namespace, input, deadline) do
    OperativeManager.send_io(operative_id, namespace, input, %{"input_type" => "2"})

    receive do
      {:message_forward, payload} ->
        if to_string(payload) == input,
          do: true,
          else: feed_until(operative_id, namespace, input, deadline)
    after
      500 ->
        System.monotonic_time(:millisecond) < deadline and
          feed_until(operative_id, namespace, input, deadline)
    end
  end

  defp collector(parent, event, tag) do
    id = "dsl-drill-#{tag}-#{System.unique_integer([:positive])}"
    :telemetry.attach(id, event, fn _e, _m, meta, _ -> send(parent, {tag, meta}) end, nil)
    id
  end

  defp seen(tag) do
    receive do
      {^tag, meta} -> [meta | seen(tag)]
    after
      0 -> []
    end
  end

  defp drain_port(port, acc) do
    receive do
      {^port, {:data, data}} -> drain_port(port, [data | acc])
    after
      0 -> acc |> Enum.reverse() |> Enum.join()
    end
  end

  defp log(line), do: IO.puts("\n[DRILL] " <> line)
end

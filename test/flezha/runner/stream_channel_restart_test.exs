defmodule FlezhaTest.Runner.StreamChannelRestartTest do
  @moduledoc """
  A data channel that dies is repaired as a closed stream is.

  The channel's process holds its stream's binding, and its supervisor brings
  it back unbound under the same name. Nothing else asks the runner for a new
  stream, so the runner stays listed as connected while every input sent
  through that channel reaches nothing. These drive the real pool
  (`Flezha.Runner.StreamSup`) beside a real `Flezha.Runner.ControlChannel`
  and a real stream (`FlezhaTest.Helper.StreamPair`); only the rebind's call
  into the runner is stubbed, which answers the test instead.
  """

  # async: false: writes Application env; registered runner channels; global mimic.

  use FlezhaTest.Case, async: false
  use Mimic

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.StreamChannel
  alias Flezha.Runner.StreamPool
  alias Flezha.Runner.StreamSup
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.StreamPair
  alias FlezhaTest.Helper.Telemetry

  @bound_ms 5_000

  setup :set_mimic_global_until_exit

  setup do
    FlezhaTest.Case.install([:env])
    # The session reports a channel's death as a warning, below the test
    # config's primary `:error` gate, which no capture handler sees past.
    on_exit(NodeTestHelper.override_log_level(:warning))
    runner_id = "restart_#{System.unique_integer([:positive])}"
    test_pid = self()

    stub(FleetManager, :rebind_data_streams, fn id ->
      send(test_pid, {:rebind, id})
      {:ok, ""}
    end)

    write_fun = fn _ref, _payload_id, _message, _metadata -> :ok end
    start_supervised!({ControlChannel, {"_system", runner_id, nil, write_fun}})

    relay = "restart_relay_#{System.unique_integer([:positive])}"
    :ok = ClassRegistry.put_overwatch(relay, self(), runner_id)
    on_exit(fn -> ClassRegistry.delete(relay) end)

    Telemetry.capture([:flezha, :state, :output_stall],
      filter: &(&1.relay_instance == relay),
      as: fn _e, _m, md -> {:output_stall, md.relay_instance, md.runner_id} end
    )

    Telemetry.capture([:flezha, :stream, :channel_down],
      filter: &(&1.runner_id == runner_id),
      as: :channel_down
    )

    %{runner_id: runner_id, relay: relay}
  end

  # fails when: a killed bound channel comes back unbound and nothing makes
  # the runner open a new stream, so its relays acknowledge input they never
  # deliver (N-13: 1,884 acknowledged, 453 counted, for 3 min 40 s).
  test "a killed bound channel closes its stream and has the runner rebind", ctx do
    name = start_bound_pool(ctx.runner_id)
    killed = GenServer.whereis(name)

    log =
      capture_log([level: :warning], fn ->
        Process.exit(killed, :kill)

        assert_killed_and_rebound(name, ctx.runner_id)
      end)

    assert [_line] =
             Regex.scan(
               ~r/data channel #{Regex.escape(inspect(name))} died \(killed: past FLEZHA_STREAM_CHANNEL_MAX_HEAP_BYTES or by hand/,
               log
             )

    assert_restart_reported(log, name, "killed")

    assert_receive {:stream_pair_closed, _socket}, @bound_ms

    # The session rebinds its data plane; the relays keep running, so no stall
    # sends them to the loop, whose remedy would re-seat their live Cells.
    refute_received {:output_stall, _relay, _runner}

    replacement = GenServer.whereis(name)
    assert is_pid(replacement) and replacement != killed
    assert GenServer.call(name, :stream_ref_status) == nil

    {fresh, close_fresh} = StreamPair.open()
    on_exit(close_fresh)
    assert {:bound, ^replacement, ^name} = StreamPool.bind_stream_to_first_nil([name], fresh)
  end

  # fails when: a channel that crashes, rather than being killed, comes back
  # unbound and nothing asks the runner for a new stream.
  test "a crashed bound channel closes its stream and has the runner rebind", ctx do
    name = start_bound_pool(ctx.runner_id)

    log =
      capture_log([level: :warning], fn ->
        GenServer.stop(name, :crashed_in_test)

        assert_receive {:channel_down, %{count: 1}, %{channel: ^name, reason: :crashed_in_test}},
                       @bound_ms

        assert_receive {:rebind, runner_id} when runner_id == ctx.runner_id, @bound_ms
      end)

    assert log =~ "data channel #{inspect(name)} died (crashed, reason :crashed_in_test)"
    {:via, Registry, registered} = name
    assert log =~ "GenServer #{inspect(registered)} terminating\n** (stop) :crashed_in_test"
    assert_restart_reported(log, name, ":crashed_in_test")
    assert_receive {:stream_pair_closed, _socket}, @bound_ms
  end

  # fails when: a channel that returns while a rebind runs is dropped, and the
  # rebind had already bound its fresh streams before the channel existed.
  test "a channel returning during a rebind is owed one after it", ctx do
    name = start_pool(ctx.runner_id)
    control = Flezha.Identificator.control_channel(ctx.runner_id)
    :sys.replace_state(control, &%{&1 | rebinding: :connecting})

    capture_log([level: :warning], fn ->
      Process.exit(GenServer.whereis(name), :kill)
      assert_receive {:channel_down, _m, %{channel: ^name, reason: :killed}}, @bound_ms
    end)

    # Answered once the replacement started, which announced itself from init.
    _ = Supervisor.which_children(Flezha.Identificator.stream_sup_primary(ctx.runner_id))
    assert %{rebinding: :owed, rebinds: 0} = session(ctx.runner_id)
    assert_owed_rebind_follows(ctx.runner_id)
  end

  # fails when: a stream a running rebind bound closes before that rebind
  # answers, and the close is dropped as the fault already being repaired, so
  # the channel stays unbound while the runner reads as connected.
  test "a stream closing after a rebind let the old ones go is owed one after it", ctx do
    name = start_bound_pool(ctx.runner_id)
    control = Flezha.Identificator.control_channel(ctx.runner_id)

    close = fn step ->
      :sys.replace_state(control, &%{&1 | rebinding: step})
      NodeTestHelper.close_bound_stream(name, "closed in test")
      # Answered once the channel handled the close and asked for its rebind.
      GenServer.call(name, :stream_ref_status)
      session(ctx.runner_id)
    end

    log =
      capture_log([level: :warning], fn ->
        assert %{rebinding: :releasing} = close.(:releasing)
        {stream, close_stream} = StreamPair.open()
        on_exit(close_stream)
        :ok = GenServer.call(name, {:bind, stream})
        assert %{rebinding: :owed, rebinds: 0} = close.(:connecting)
      end)

    assert [_, _] = Regex.scan(~r/Connection closed for IO socket \(closed in test\)/, log)
    assert_owed_rebind_follows(ctx.runner_id)
  end

  # fails when: the safety valve kills the channel and the session never
  # hears of it, or the runtime's multi-line report is all that says so.
  test "a channel past its heap cap is killed, reported in one line and rebound", ctx do
    Application.put_env(:flezha, :stream_channel_max_heap_bytes, 1_048_576)
    name = start_pool(ctx.runner_id)
    capped = GenServer.whereis(name)

    log =
      capture_log([level: :warning], fn ->
        # Held so the flood stays in the mailbox, which the cap counts; the
        # collection the test asks for is where the runtime checks it.
        :ok = :sys.suspend(capped)
        for n <- 1..20_000, do: send(capped, {:flood, n, Enum.to_list(1..20)})
        :erlang.garbage_collect(capped, async: :cap_checked)

        assert_killed_and_rebound(name, ctx.runner_id)
      end)

    assert [_line] = Regex.scan(~r/data channel .* died \(killed/, log)
    refute log =~ "maximum heap size reached"
    assert_restart_reported(log, name, "killed")
    assert GenServer.whereis(name) not in [nil, capped]
  end

  test "the heap cap is set on a channel when configured and absent when off", ctx do
    bytes = 2_097_152
    Application.put_env(:flezha, :stream_channel_max_heap_bytes, bytes)

    {:ok, capped} =
      StreamChannel.start_link({"_system", ctx.runner_id, :cap_on_channel, :primary})

    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(capped) end)

    words = div(bytes, :erlang.system_info(:wordsize))

    assert {:max_heap_size, %{size: ^words, kill: true, error_logger: false}} =
             Process.info(capped, :max_heap_size)

    Application.put_env(:flezha, :stream_channel_max_heap_bytes, 0)
    {:ok, free} = StreamChannel.start_link({"_system", ctx.runner_id, :cap_off_channel, :primary})
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(free) end)

    assert {:max_heap_size, %{size: 0}} = Process.info(free, :max_heap_size)
  end

  # fails when: a session's own first start of its pool, or the supervisor
  # stopping it, reads as a lost stream and spends a rebind.
  test "a pool's first start and its shutdown ask for no rebind", ctx do
    start_pool(ctx.runner_id)
    assert %{rebinding: nil, rebinds: 0} = session(ctx.runner_id)

    :ok = stop_supervised(StreamSup)

    assert %{rebinding: nil, rebinds: 0} = session(ctx.runner_id)
    refute_received {:channel_down, _, _}
    refute_received {:output_stall, _, _}
  end

  # The session reported the kill and asked the runner for a fresh pair set.
  defp assert_killed_and_rebound(name, runner_id) do
    assert_receive {:channel_down, %{count: 1}, %{channel: ^name, reason: :killed}}, @bound_ms
    assert_receive {:rebind, ^runner_id}, @bound_ms
  end

  # Nothing more is asked while the rebind runs; once it answers, the one it
  # owed is asked for.
  defp assert_owed_rebind_follows(runner_id) do
    refute_received {:rebind, _}
    control = GenServer.whereis(Flezha.Identificator.control_channel(runner_id))
    send(control, {:rebind_answered, {:ok, ""}})
    assert_receive {:rebind, ^runner_id}, @bound_ms
    assert %{rebinds: 1} = session(runner_id)
  end

  # The supervisor's own report of the restart, which the runtime logs for any
  # exit that is not a shutdown.
  defp assert_restart_reported(log, name, exit) do
    assert log =~ "Child #{inspect(name)} of Supervisor"
    assert log =~ "** (exit) #{exit}"
  end

  # `start_pool/1` with its first channel bound to a real stream.
  defp start_bound_pool(runner_id) do
    name = start_pool(runner_id)
    {stream, close} = StreamPair.open()
    on_exit(close)
    :ok = GenServer.call(name, {:bind, stream})
    name
  end

  # The runner's primary pool, every channel announced to the session, and the
  # first channel's name.
  defp start_pool(runner_id) do
    start_supervised!({StreamSup, {"_system", runner_id, :primary}})
    _ = session(runner_id)
    hd(StreamPool.stream_ids(runner_id, :primary))
  end

  # The control channel's state, read by a call that it answers only after every
  # announcement and exit notice already in its mailbox.
  defp session(runner_id),
    do: :sys.get_state(Flezha.Identificator.control_channel(runner_id))
end

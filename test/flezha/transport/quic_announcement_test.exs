defmodule Flezha.Transport.QuicAnnouncementTest do
  @moduledoc """
  The QUIC transport is experimental (`docs/public/publishing/maturity.md#quic`), and
  each process that switches it on says so once: the controller when its QUIC
  listener starts, however many runners connect, and a runner when it selects
  QUIC, however often it reconnects. A UDS or in-node transport says nothing.
  """
  # async: false: restarts the controller's transport listeners; the primary Logger.
  use FlezhaTest.Case, fixtures: [:tmp_dir], async: false

  import ExUnit.CaptureLog

  alias Flezha.Runner.FleetManager
  alias Flezha.State.TestHelpers
  alias Flezha.Transport.{Native, Server}
  alias FlezhaTest.Helper.{NodeTestHelper, Telemetry}

  @controller "[Transport.Server] the QUIC transport is experimental"
  # Every runner on `flz_core` says it the same way, through its own log, the
  # Elixir one included.
  @runner "the QUIC transport is experimental"

  setup do
    ns = "quic_notice_#{System.unique_integer([:positive])}"
    on_exit(fn -> FleetManager.stop_namespace(ns) end)
    {:ok, ns: ns}
  end

  defp restart_listener(transport) do
    :ok = Supervisor.terminate_child(Flezha.Supervisor, {Server, transport})
    {:ok, _} = Supervisor.restart_child(Flezha.Supervisor, {Server, transport})
  end

  defp start_runner(ns, opts) do
    {port, _runner_id} = NodeTestHelper.start_runner_port(ns, opts)
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)
    port
  end

  # Everything a runner printed, up to its exit. The port delivers its data
  # before the monitor's DOWN, so a line written at any point is in here.
  defp output_until_exit(port, seen \\ "") do
    receive do
      {^port, {:data, data}} -> output_until_exit(port, seen <> data)
      {:DOWN, _, :port, ^port, _} -> seen
    after
      60_000 -> flunk("the runner never exited; it printed:\n" <> seen)
    end
  end

  defp stop_and_read(port) do
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    System.cmd("kill", ["-TERM", "#{os_pid}"])
    output_until_exit(port)
  end

  describe "the controller" do
    # The suite's primary level is `:error`, which drops a warning before any
    # capture sees it.
    setup do
      on_exit(NodeTestHelper.override_log_level(:warning))
    end

    test "a QUIC listener announces once while several runners connect, and so does each runner",
         %{ns: ns} do
      {ports, log} =
        with_log(fn ->
          restart_listener(:quic)
          for _ <- 1..3, do: start_runner(ns, transport: :quic, runtime: :rust)
        end)

      assert NodeTestHelper.occurrences(log, @controller) == 1
      assert log =~ "docs/public/publishing/maturity.md#quic"

      for port <- ports do
        out = NodeTestHelper.read_port(port, &String.contains?(&1, @runner))
        assert out =~ "#{@runner}: its settings and behaviour may change drastically"
      end
    end

    # A second listener on a private directory: restarting the controller's own
    # would drop the suite's runners onto a reconnect they print to stderr.
    test "a UDS listener announces nothing", %{tmp: dir} do
      restore = TestHelpers.override_env(:socket_files_path, dir)
      on_exit(restore)

      {_, log} =
        with_log(fn ->
          {:ok, listener} = GenServer.start(Server, {:uds, nil})
          restore.()
          {:ok, socket} = Native.control_socket_path_in(dir)
          assert File.exists?(socket)
          :ok = GenServer.stop(listener)
        end)

      assert NodeTestHelper.occurrences(log, @controller) == 0
    end
  end

  describe "a runner" do
    test "an Elixir runner over QUIC announces once across a reconnect", %{ns: ns} do
      port =
        start_runner(ns,
          transport: :quic,
          runtime: :elixir,
          env: [{"FLEZHA_RECONNECT_MAX_BACKOFF_MS", "5"}]
        )

      Telemetry.capture([:flezha, :runner, :connected],
        as: :connected,
        filter: &(&1.namespace == ns)
      )

      # The listener's shutdown closes every connection it holds.
      restart_listener(:quic)
      assert_receive {:connected, _, _}, 60_000

      out = stop_and_read(port)
      assert NodeTestHelper.occurrences(out, @runner) == 1
    end
  end
end

defmodule Flezha.Transport.QuicAnnouncementTest do
  @moduledoc """
  The QUIC transport is experimental (`docs/publishing/maturity.md#quic`), and
  each process that switches it on says so once: the controller when its QUIC
  listener starts, however many runners connect, and a runner when it selects
  QUIC, however often it reconnects. A UDS or in-node transport says nothing.
  """
  # async: false: restarts the controller's transport listeners; the primary Logger.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.EventManager
  alias Flezha.Transport.Server
  alias FlezhaTest.Helper.NodeTestHelper

  @controller "[Transport.Server] the QUIC transport is experimental"
  @rust_runner "[flezha] the QUIC transport is experimental"
  @elixir_runner "[Flezha.SDK] the QUIC transport is experimental"

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

  defp count(text, line), do: length(String.split(text, line)) - 1

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

      assert count(log, @controller) == 1
      assert log =~ "docs/publishing/maturity.md#quic"

      for port <- ports do
        out = NodeTestHelper.read_port(port, &String.contains?(&1, @rust_runner))
        assert out =~ "#{@rust_runner}: its settings and behaviour may change drastically"
      end
    end

    test "a UDS or in-node listener announces nothing", %{ns: ns} do
      {_, log} =
        with_log(fn ->
          restart_listener(:uds)
          {:ok, _} = Server.ensure_started(:native)
          start_runner(ns, transport: :uds, runtime: :rust)
        end)

      assert count(log, @controller) == 0
    end
  end

  describe "a runner" do
    test "an Elixir runner over QUIC announces once across a reconnect", %{ns: ns} do
      port = start_runner(ns, transport: :quic, runtime: :elixir)

      EventManager.add_handler(:runner_connected_event)
      # The listener's shutdown closes every connection it holds.
      restart_listener(:quic)
      assert_receive {:runner_connected_event, {^ns, _}}, 60_000
      EventManager.rem_handler(:runner_connected_event)

      out = stop_and_read(port)
      assert out =~ "[Flezha.SDK] connection lost"
      assert count(out, @elixir_runner) == 1
    end

    test "an Elixir runner over UDS announces nothing", %{ns: ns} do
      out = ns |> start_runner(transport: :uds, runtime: :elixir) |> stop_and_read()

      assert out =~ "transport=uds"
      assert count(out, "QUIC transport is experimental") == 0
    end
  end
end

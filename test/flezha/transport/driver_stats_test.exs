defmodule Flezha.Transport.DriverStatsTest do
  @moduledoc """
  Integration coverage for the `driver_stats/1` NIF: a live driver reports a
  well-shaped map and its loop heartbeat (`loop_iterations` + `last_loop_ms`)
  advances over time — the signal that proves the driver thread is not wedged.
  Both server drivers answer the same shape, so the observability source and
  the stall classifier read one map whatever the transport is.
  """

  use ExUnit.Case, async: true

  alias Flezha.Convergence.StallClassifier
  alias Flezha.Transport.CertGenerator
  alias Flezha.Transport.Native
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    path =
      Path.join(System.tmp_dir!(), "flz_driver_stats_#{System.unique_integer([:positive])}.sock")

    {:ok, server} = Native.start_uds_server(path, self())
    on_exit(fn -> Native.stop_server(server) end)
    {:ok, server: server}
  end

  defp start_quic_server do
    CertGenerator.ensure_certs()

    cert = Application.get_env(:flezha, :server_cert_path, "app_data/certs/server.pem")
    key = Application.get_env(:flezha, :server_key_path, "app_data/certs/server.key")

    {:ok, server} = Native.start_quic_server("127.0.0.1:0", cert, key, "", self())
    on_exit(fn -> Native.stop_server(server) end)
    server
  end

  test "driver_stats returns a well-shaped liveness map", %{server: server} do
    stats = Native.driver_stats(server)

    assert is_map(stats)
    assert is_integer(stats.loop_iterations)
    assert is_integer(stats.last_loop_ms)
    assert is_integer(stats.now_ms)
    assert is_integer(stats.cmd_queue_depth)
    assert is_list(stats.streams)
  end

  test "loop heartbeat advances while the driver thread is alive", %{server: server} do
    first = Native.driver_stats(server)

    assert NodeTestHelper.wait_until(
             fn -> Native.driver_stats(server).loop_iterations > first.loop_iterations end,
             5_000
           ),
           "the driver loop's heartbeat never advanced"

    second = Native.driver_stats(server)
    assert second.now_ms >= first.now_ms
    # last_loop_ms is fresh relative to now (driver is looping, not wedged).
    assert second.now_ms - second.last_loop_ms < 1_000
  end

  test "a QUIC server reports the same liveness map a UDS server does", %{server: uds} do
    quic = start_quic_server()

    uds_stats = Native.driver_stats(uds)
    quic_stats = Native.driver_stats(quic)

    assert is_map(quic_stats),
           "a QUIC driver must report driver stats, got #{inspect(quic_stats)}"

    assert Map.keys(quic_stats) |> Enum.sort() == Map.keys(uds_stats) |> Enum.sort()
    assert is_integer(quic_stats.loop_iterations)
    assert is_integer(quic_stats.last_loop_ms)
    assert is_integer(quic_stats.now_ms)
    assert is_integer(quic_stats.cmd_queue_depth)
    assert is_list(quic_stats.streams)
  end

  test "the classifier's driver clauses fire on QUIC-sourced stats" do
    quic = start_quic_server()

    previous = Native.driver_stats(quic)
    assert is_map(previous), "a QUIC driver must report driver stats, got #{inspect(previous)}"

    # The classifier judges only an established driver — a stamped loop clock
    # and a non-zero iteration count — so the baseline must show one.
    assert NodeTestHelper.wait_until(
             fn ->
               d = Native.driver_stats(quic)
               d.loop_iterations > 0 and d.last_loop_ms > 0
             end,
             5_000
           ),
           "the QUIC driver never reported an established loop"

    previous = Native.driver_stats(quic)
    sample = Native.driver_stats(quic)

    # The driver is healthy, so the only thing this test supplies is the wall
    # clock a wedge would have moved: same iteration count, an aged now_ms.
    # Every other value is the QUIC driver's own self-report.
    wedged = %{sample | loop_iterations: previous.loop_iterations, now_ms: sample.now_ms + 30_000}

    snapshot = fn d ->
      %{drivers: [Map.put(d, :transport, :quic)], channels: [], operatives: []}
    end

    assert {:driver_loop_frozen, ctx} =
             StallClassifier.classify(snapshot.(wedged), snapshot.(previous), %{})

    assert ctx.transport == :quic
    assert ctx.loop_age_ms > 5_000
  end
end

defmodule Flezha.Transport.DriverStatsTest do
  @moduledoc """
  Integration coverage for the `driver_stats/1` NIF: a live UDS driver reports
  a well-shaped map and its loop heartbeat (`loop_iterations` + `last_loop_ms`)
  advances over time — the signal that proves the flz-uds-driver thread is not
  wedged.
  """

  use ExUnit.Case, async: true

  alias Flezha.Transport.Native

  setup do
    path =
      Path.join(System.tmp_dir!(), "flz_driver_stats_#{System.unique_integer([:positive])}.sock")

    {:ok, server} = Native.start_uds_server(path, self())
    on_exit(fn -> Native.stop_server(server) end)
    {:ok, server: server}
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
    Process.sleep(200)
    second = Native.driver_stats(server)

    assert second.loop_iterations > first.loop_iterations
    assert second.now_ms >= first.now_ms
    # last_loop_ms is fresh relative to now (driver is looping, not wedged).
    assert second.now_ms - second.last_loop_ms < 1_000
  end
end

defmodule Flezha.Plugin.Provider.WebsocketHealthTest do
  @moduledoc """
  WebSocket health contract (self-healing-control-plane, Phase 7.1 / 7.4).
  `health/1` reuses the two-tier watchdog's canonical `last_frame_at` /
  `last_data_frame_at` (no new liveness state) via a liveness probe to the Fresh
  process: healthy within both windows, unhealthy past the connection or data
  window, dead/absent connection is unhealthy. The probe reply leaves the
  watchdog's state untouched.
  """
  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.Websocket

  # A process that answers the liveness probe with fixed silence ages.
  defp liveness_stub(frame_age, data_age) do
    pid = spawn(fn -> liveness_loop(frame_age, data_age) end)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    pid
  end

  defp liveness_loop(frame_age, data_age) do
    receive do
      {:liveness_probe, from, ref} ->
        send(from, {ref, {frame_age, data_age}})
        liveness_loop(frame_age, data_age)
    end
  end

  test "healthy when within both the connection and data windows" do
    pid = liveness_stub(10, 10)

    assert Websocket.health(%{owned_pid: pid, idle_timeout_ms: 1_000, resubscribe_after_ms: 0}) ==
             :healthy
  end

  test "unhealthy (connection_idle) once the connection window has elapsed" do
    pid = liveness_stub(2_000, 2_000)

    assert Websocket.health(%{owned_pid: pid, idle_timeout_ms: 1_000, resubscribe_after_ms: 0}) ==
             {:unhealthy, :connection_idle}
  end

  test "unhealthy (data_idle) when data is stale and resubscribe is configured" do
    pid = liveness_stub(10, 2_000)

    assert Websocket.health(%{
             owned_pid: pid,
             idle_timeout_ms: 10_000,
             resubscribe_after_ms: 1_000
           }) ==
             {:unhealthy, :data_idle}
  end

  test "unhealthy when the connection pid is dead" do
    pid = liveness_stub(10, 10)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
    assert Websocket.health(%{owned_pid: pid, idle_timeout_ms: 1_000}) == {:unhealthy, :not_alive}
  end

  test "unhealthy when there is no connection pid" do
    assert Websocket.health(%{idle_timeout_ms: 1_000}) == {:unhealthy, :no_connection}
  end

  test "the Fresh liveness_probe handler reports silence ages without disturbing state (7.4)" do
    now = :erlang.monotonic_time(:millisecond)
    state = %{last_frame_at: now - 50, last_data_frame_at: now - 200}
    ref = make_ref()

    assert {:ok, ^state} = Websocket.handle_info({:liveness_probe, self(), ref}, state)
    assert_receive {^ref, {frame_age, data_age}}
    assert frame_age >= 50
    assert data_age >= 200
  end
end

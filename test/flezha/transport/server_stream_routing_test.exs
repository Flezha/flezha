defmodule Flezha.Transport.ServerStreamRoutingTest do
  @moduledoc """
  Control-channel stream correlation (self-healing-control-plane prod fix).

  The NIF delivers `{:flz_connection, handle}` with an opaque handle but
  `{:flz_data, stream_id, ...}` / `{:flz_closed, stream_id, ...}` with the
  driver's integer stream id. `Transport.Server` must key pending streams by that
  integer id so the `connect_control_channel` frame binds the ControlChannel to
  the exact stream it arrived on — not an arbitrary pending one — and so closed
  streams (e.g. `nc -zU` liveness probes) are cleaned up instead of accumulating.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Executor.FleetManager
  alias Flezha.Transport.Server

  defp connect_frame(stream_id) do
    message = Jason.encode!(%{"namespace" => "default", "factories" => []})
    {:flz_data, stream_id, "connect_control_channel", message, %{}}
  end

  test "connect_control_channel binds the handle matching the frame's stream_id" do
    test_pid = self()
    want = make_ref()
    other = make_ref()

    stub(FleetManager, :connect_handler, fn ref, _ns, _payload, _transport ->
      send(test_pid, {:connected, ref})
      {:ok, "ce_ok"}
    end)

    # Two streams pending; the frame arrives on id 7. Only id 7's handle must be
    # used, and only id 7 must be removed from the pending map.
    state = %{
      transport: :uds,
      pending_streams: %{7 => want, 9 => other}
    }

    assert {:noreply, new_state} = Server.handle_info(connect_frame(7), state)
    assert_receive {:connected, ^want}, 500
    assert new_state.pending_streams == %{9 => other}
  end

  test "connect_control_channel for an unknown stream_id does not crash or bind" do
    stub(FleetManager, :connect_handler, fn _ref, _ns, _payload, _transport ->
      flunk("connect_handler must not be called for an unknown stream_id")
    end)

    state = %{transport: :uds, pending_streams: %{1 => make_ref()}}

    assert {:noreply, new_state} = Server.handle_info(connect_frame(42), state)
    assert new_state.pending_streams == state.pending_streams
  end

  test "flz_closed removes the matching pending stream (probe-churn cleanup)" do
    keep = make_ref()
    state = %{transport: :uds, pending_streams: %{3 => make_ref(), 4 => keep}}

    assert {:noreply, s1} = Server.handle_info({:flz_closed, 3, "connection_closed"}, state)
    assert s1.pending_streams == %{4 => keep}

    # Unknown / already-handed-off id is a harmless no-op.
    assert {:noreply, s2} = Server.handle_info({:flz_closed, 99, "connection_closed"}, s1)
    assert s2.pending_streams == %{4 => keep}
  end
end

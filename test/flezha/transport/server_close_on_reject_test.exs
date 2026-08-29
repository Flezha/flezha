defmodule Flezha.Transport.ServerCloseOnRejectTest do
  @moduledoc """
  Close-on-reject (self-healing-control-plane, Phase 4 / controller-leadership).

  When `FleetManager.connect_handler/4` refuses a connect (e.g. `:shutting_down`
  during drain), the transport must close the stream — and emit a counted
  `[:flezha, :transport, :connect_rejected]` signal — so the runner detects the
  dead controller and reconnects to a live one instead of dangling. Drives the
  `connect_control_channel` callback directly with a stubbed FleetManager
  (`Native` is a NIF and can't be mocked, so close is observed via the signal and
  the handler's robustness to a bogus stream ref).
  """
  # async: false: Mimic stubs; the live controller and its fleet; the primary Logger.
  use ExUnit.Case, async: false
  use Mimic

  # Drives the connect-reject path, which logs the rejection at error by design.
  @moduletag :capture_log

  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Helper.MessageTestHelper
  alias Flezha.Transport.Server

  defp attach_rejected_handler() do
    MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)
  end

  defp connect_frame() do
    message = Jason.encode!(%{"namespace" => "default", "factories" => []})
    {:flz_data, 0, "connect_control_channel", message, %{}}
  end

  test "rejected connect closes the stream, emits connect_rejected, and does not crash" do
    attach_rejected_handler()
    stream_ref = make_ref()

    stub(FleetManager, :connect_handler, fn ^stream_ref, _ns, _payload, _transport ->
      {:error, :shutting_down}
    end)

    state = %{transport: :uds, pending_streams: %{0 => stream_ref}}

    # safe_close tolerates the bogus (non-NIF) ref, so the server does not crash.
    assert {:noreply, new_state} = Server.handle_info(connect_frame(), state)
    assert new_state.pending_streams == %{}

    assert_receive {:rejected, %{count: 1}, %{reason: :shutting_down, transport: :uds}}, 500
  end

  test "successful connect does not emit connect_rejected" do
    attach_rejected_handler()
    stream_ref = make_ref()

    stub(FleetManager, :connect_handler, fn _ref, _ns, _payload, _transport ->
      {:ok, "ce_ok"}
    end)

    state = %{transport: :uds, pending_streams: %{0 => stream_ref}}

    assert {:noreply, _} = Server.handle_info(connect_frame(), state)
    refute_received {:rejected, _, _}
  end
end

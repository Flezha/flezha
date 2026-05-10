defmodule Flezha.Shutdown.SignalHandlerTest do
  use ExUnit.Case, async: false

  alias Flezha.Shutdown.SignalHandler

  test "starts and survives init when graceful_shutdown_enabled is false" do
    # In test config `graceful_shutdown_enabled` is `false`, so the
    # SignalHandler should be running but NOT have installed OS handlers.
    # We can't easily peek at `:os.set_signal/2` registration; instead we
    # assert the GenServer is alive and `signal_count/0` works.
    assert is_pid(Process.whereis(SignalHandler))
    assert SignalHandler.signal_count() == 0
  end
end

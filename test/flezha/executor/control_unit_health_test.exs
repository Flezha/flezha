defmodule FlezhaTest.Executor.ControlUnitHealthTest do
  @moduledoc """
  Self-health observability for the ControlUnit: the `:heartbeat` tick surfaces
  the outstanding-reply backlog (`pending_refs` + oldest age) that otherwise
  grows silently when the transport stops delivering responses. The handler is
  a public function, exercised directly with a crafted state.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Executor.ControlUnit

  setup do
    prev = Logger.level()
    Logger.configure(level: :warning)
    on_exit(fn -> Logger.configure(level: prev) end)
  end

  test "heartbeat warns when a stored reply has been outstanding too long" do
    now = System.monotonic_time(:millisecond)

    # One reply outstanding for 60s exceeds the 10s age threshold -> warn.
    state = %{
      executor_id: "ex_cu",
      response_refs: %{
        make_ref() => {:reply, {self(), make_ref()}, now - 60_000}
      }
    }

    log = capture_log(fn -> assert {:noreply, ^state} = ControlUnit.handle_info(:heartbeat, state) end)

    assert log =~ "[ControlUnit] backlog executor=ex_cu"
    assert log =~ "pending_refs=1"
    assert log =~ "oldest_age_ms="
    assert log =~ "queue_len="
  end

  test "heartbeat warns when the pending-ref count crosses the threshold" do
    now = System.monotonic_time(:millisecond)

    refs =
      for _ <- 1..40, into: %{} do
        {make_ref(), {:reply, {self(), make_ref()}, now}}
      end

    state = %{executor_id: "ex_cu2", response_refs: refs}

    log = capture_log(fn -> ControlUnit.handle_info(:heartbeat, state) end)

    assert log =~ "[ControlUnit] backlog executor=ex_cu2"
    assert log =~ "pending_refs=40"
  end
end

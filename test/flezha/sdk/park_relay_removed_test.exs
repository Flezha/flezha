defmodule Flezha.SDK.ParkRelayRemovedTest do
  @moduledoc """
  A dead-letter park whose relay leaves this runner is forgotten with it. The
  controller acks a park only from the runner the relay is bound to, so a park
  kept for a relay that was stopped or moved away would be re-sent forever.

  The runner's own callbacks are driven directly on its state: a park, the
  controller's `RemRelay`, then the resend tick.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Counters, Effects, Hold, Park, Relays, RunnerServer}
  alias Flezha.SDK.RunnerServer.State

  # fails when: a runner re-sends `park_payload` every two seconds, forever,
  # for a relay the controller has moved to another runner.
  test "a relay leaving the runner takes its parks with it" do
    counters = Counters.new()
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, :process, ^gone, _}

    s = %State{
      registry: :ets.new(:park_relays, [:public, :set]),
      effects: Effects.new(),
      counters: counters,
      held: Hold.new(counters: counters),
      relays: %{"moved" => {gone, "f"}}
    }

    body = Park.body("moved/t/k#0", "input", "poison", 3, "x")
    identity = Park.identity("moved", "t", "k")
    {:noreply, s} = RunnerServer.handle_cast({:park, self(), identity, body}, s)
    assert Map.has_key?(s.parks, body.park_id)

    s = Relays.remove(s, "moved")

    assert s.parks == %{}, "a park outlived its relay on this runner"
    assert {:noreply, ^s} = RunnerServer.handle_info({:park_resend, body.park_id}, s)
  end
end

defmodule Flezha.State.RecoveryForOverwatchTest do
  @moduledoc """
  Phase-3 (convergence-self-healing §3.1-3.2) bind-driven
  replay trigger.

  Exercises `Flezha.State.Recovery.resync_for_overwatch/2`: it must
  succeed without any registry / class→runner lookup — the only
  inputs it needs are a pid + runner_id, and it routes the work to
  the Overwatch process via a cast. This proves the cold-boot
  "resync skipped reason=:output_stall" silent no-op (which depended
  on `resolve_runner/1` succeeding before any runner had been
  observed) is structurally impossible on the new path.
  """

  # async: false: Mimic stubs; the live controller and its fleet.

  use ExUnit.Case, async: false

  alias Flezha.State.Recovery

  defmodule SpyOverwatch do
    use GenServer

    def start_link(forward_to), do: GenServer.start_link(__MODULE__, forward_to)

    @impl true
    def init(forward_to), do: {:ok, %{forward_to: forward_to}}

    @impl true
    def handle_cast({:kick_restore, runner_id}, state) do
      send(state.forward_to, {:kick_restore_received, runner_id})
      {:noreply, state}
    end
  end

  test "resync_for_overwatch/2 casts :kick_restore with the runner_id" do
    {:ok, spy} = SpyOverwatch.start_link(self())

    assert :ok = Recovery.resync_for_overwatch(spy, "default_exec_1")

    assert_receive {:kick_restore_received, "default_exec_1"}, 500
  end

  test "resync_for_overwatch/2 needs no registry mock — pure pid + runner_id input" do
    # If the function tried to look up the class→runner binding via
    # HubManager / ProcessHub.tag_query, an unmocked test environment
    # would crash; the unit test asserts the new path runs with no
    # such mock present.
    {:ok, spy} = SpyOverwatch.start_link(self())

    # No `Mimic.stub(HubManager, ...)` and no Application env setup.
    assert :ok = Recovery.resync_for_overwatch(spy, "exec_9")
    assert_receive {:kick_restore_received, "exec_9"}, 500
  end
end

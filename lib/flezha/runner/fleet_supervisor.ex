defmodule Flezha.Runner.FleetSupervisor do
  @moduledoc """
  The node's per-runner supervision root.

  One child per connected runner, each a `Flezha.Runner.RunnerSupervisor`
  keyed by runner id, started and stopped by `Flezha.Runner.FleetManager`.

  Invariant: `:one_for_one` — a runner's tree failing takes down that runner
  and no other.
  """

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    Supervisor.init([], strategy: :one_for_one)
  end
end

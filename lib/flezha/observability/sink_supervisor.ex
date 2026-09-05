defmodule Flezha.Observability.SinkSupervisor do
  @moduledoc """
  `DynamicSupervisor` owning every active sink process.

  Sinks are started under a `:one_for_one` strategy so one sink
  crashing or stalling never affects peers or producers. The
  `Flezha.Observability.Registry` is the only caller of
  `DynamicSupervisor.start_child/2` against this supervisor.
  """

  alias Flezha.Process.Cspec

  use DynamicSupervisor

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Every sink is an independent child with its own failure mode — an
    # unreachable endpoint, a refused connection. The DynamicSupervisor default
    # (3 restarts in 5 s) would let a handful of them, or one that flaps, take
    # every other sink down with the supervisor. Sized for independent
    # children; a genuine restart storm still trips it.
    DynamicSupervisor.init([strategy: :one_for_one] ++ Cspec.intensity())
  end
end

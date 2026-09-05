defmodule Flezha.Observability.SourceSupervisor do
  @moduledoc """
  `DynamicSupervisor` owning every process-backed source.

  Declarative sources are application code that emits via
  `Flezha.Observability.Bus.emit/1`; nothing runs under this
  supervisor for them. A process-backed source is any GenServer that
  drives itself and emits — this supervisor starts it and the
  foundation never calls into it.
  """

  alias Flezha.Process.Cspec

  use DynamicSupervisor

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Every source is an independent child with its own failure mode — an
    # unreachable endpoint, a refused connection. The DynamicSupervisor default
    # (3 restarts in 5 s) would let a handful of them, or one that flaps, take
    # every other source down with the supervisor. Sized for independent
    # children; a genuine restart storm still trips it.
    DynamicSupervisor.init([strategy: :one_for_one] ++ Cspec.intensity())
  end
end

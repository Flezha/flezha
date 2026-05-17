defmodule Flezha.Observability.SinkSupervisor do
  @moduledoc """
  `DynamicSupervisor` owning every active sink process.

  Sinks are started under a `:one_for_one` strategy so one sink
  crashing or stalling never affects peers or producers. The
  `Flezha.Observability.Registry` is the only caller of
  `DynamicSupervisor.start_child/2` against this supervisor.
  """

  use DynamicSupervisor

  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [[]]},
      type: :supervisor
    }
  end

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end

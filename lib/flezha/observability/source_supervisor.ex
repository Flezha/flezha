defmodule Flezha.Observability.SourceSupervisor do
  @moduledoc """
  `DynamicSupervisor` owning every process-backed source.

  Declarative sources are application code that emits via
  `Flezha.Observability.Bus.emit/1`; nothing runs under this
  supervisor for them. Process-backed sources implement the
  `Flezha.Observability.Source` behaviour and are started here.
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

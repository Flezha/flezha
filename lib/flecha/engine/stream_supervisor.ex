defmodule Flecha.Engine.StreamSupervisor do
  alias Flecha.Service.ConcurrencyMapper
  alias Flecha.Engine.StreamChannel
  alias Flecha.Service.Identificator

  use Supervisor

  def start_link(engine_id) do
    Supervisor.start_link(__MODULE__, engine_id, name: Identificator.stream_sup(engine_id))
  end

  @impl true
  def init(engine_id) do
    children =
      for x <- 1..ConcurrencyMapper.concurrent_entity_count() do
        Supervisor.child_spec({StreamChannel, engine_id}, id: x)
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end

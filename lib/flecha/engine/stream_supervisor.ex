defmodule Flecha.Engine.StreamSupervisor do
  alias Flecha.Service.ConcurrencyMapper
  alias Flecha.Engine.StreamChannel
  alias Flecha.Service.Identificator

  use Supervisor

  def stream_specs(engine_id, type) do
    for x <- 1..ConcurrencyMapper.concurrent_entity_count() do
      id =
        case type do
          :primary -> Identificator.stream_channel_primary(engine_id, x)
          :secondary -> Identificator.stream_channel_secondary(engine_id, x)
        end

      Supervisor.child_spec({StreamChannel, {engine_id, id}}, id: id)
    end
  end

  def stream_ids(engine_id, type) do
    stream_specs(engine_id, type) |> Enum.map(&Map.get(&1, :id))
  end

  def start_link({engine_id, type}) do
    name =
      case type do
        :primary -> Identificator.stream_sup_primary(engine_id)
        :secondary -> Identificator.stream_sup_secondary(engine_id)
      end

    Supervisor.start_link(__MODULE__, {engine_id, type}, name: name)
  end

  @impl true
  def init({engine_id, type}) do
    children = stream_specs(engine_id, type)

    Supervisor.init(children, strategy: :one_for_one)
  end
end

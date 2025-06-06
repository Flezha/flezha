defmodule Flezha.Operator.StreamSup do
  alias Flezha.Service.ConcurrencyMapper
  alias Flezha.Operator.StreamChannel
  alias Flezha.Service.Identificator

  use Supervisor

  def stream_specs(operator_id, type) do
    for x <- 1..ConcurrencyMapper.concurrent_entity_count() do
      id =
        case type do
          :primary -> Identificator.stream_channel_primary(operator_id, x)
          :secondary -> Identificator.stream_channel_secondary(operator_id, x)
        end

      Supervisor.child_spec({StreamChannel, {operator_id, id, type}}, id: id)
    end
  end

  def stream_ids(operator_id, type) do
    stream_specs(operator_id, type) |> Enum.map(&Map.get(&1, :id))
  end

  def start_link({operator_id, type}) do
    name =
      case type do
        :primary -> Identificator.stream_sup_primary(operator_id)
        :secondary -> Identificator.stream_sup_secondary(operator_id)
      end

    Supervisor.start_link(__MODULE__, {operator_id, type}, name: name)
  end

  @impl true
  def init({operator_id, type}) do
    children = stream_specs(operator_id, type)

    Supervisor.init(children, strategy: :one_for_one)
  end
end

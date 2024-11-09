defmodule Aflecha.Subscriber.StreamSupervisor do
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Subscriber.StreamChannel
  alias Aflecha.Service.Identificator

  use Supervisor

  def stream_specs(subscriber_id, type) do
    for x <- 1..ConcurrencyMapper.concurrent_entity_count() do
      id =
        case type do
          :primary -> Identificator.stream_channel_primary(subscriber_id, x)
          :secondary -> Identificator.stream_channel_secondary(subscriber_id, x)
        end

      Supervisor.child_spec({StreamChannel, {subscriber_id, id, type}}, id: id)
    end
  end

  def stream_ids(subscriber_id, type) do
    stream_specs(subscriber_id, type) |> Enum.map(&Map.get(&1, :id))
  end

  def start_link({subscriber_id, type}) do
    name =
      case type do
        :primary -> Identificator.stream_sup_primary(subscriber_id)
        :secondary -> Identificator.stream_sup_secondary(subscriber_id)
      end

    Supervisor.start_link(__MODULE__, {subscriber_id, type}, name: name)
  end

  @impl true
  def init({subscriber_id, type}) do
    children = stream_specs(subscriber_id, type)

    Supervisor.init(children, strategy: :one_for_one)
  end
end

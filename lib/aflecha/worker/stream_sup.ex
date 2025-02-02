defmodule Aflecha.Worker.StreamSup do
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Worker.StreamChannel
  alias Aflecha.Service.Identificator

  use Supervisor

  def stream_specs(worker_group_id, type) do
    for x <- 1..ConcurrencyMapper.concurrent_entity_count() do
      id =
        case type do
          :primary -> Identificator.stream_channel_primary(worker_group_id, x)
          :secondary -> Identificator.stream_channel_secondary(worker_group_id, x)
        end

      Supervisor.child_spec({StreamChannel, {worker_group_id, id, type}}, id: id)
    end
  end

  def stream_ids(worker_group_id, type) do
    stream_specs(worker_group_id, type) |> Enum.map(&Map.get(&1, :id))
  end

  def start_link({worker_group_id, type}) do
    name =
      case type do
        :primary -> Identificator.stream_sup_primary(worker_group_id)
        :secondary -> Identificator.stream_sup_secondary(worker_group_id)
      end

    Supervisor.start_link(__MODULE__, {worker_group_id, type}, name: name)
  end

  @impl true
  def init({worker_group_id, type}) do
    children = stream_specs(worker_group_id, type)

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Flezha.ComputeEngine.StreamSup do
  alias Flezha.Service.ConfigManager
  alias Flezha.ComputeEngine.StreamChannel
  alias Flezha.Service.Identificator

  use Supervisor

  def stream_specs(compute_engine_id, type) do
    for x <- 1..ConfigManager.concurrent_channel_count() do
      id =
        case type do
          :primary -> Identificator.stream_channel_primary(compute_engine_id, x)
          :secondary -> Identificator.stream_channel_secondary(compute_engine_id, x)
        end

      Supervisor.child_spec({StreamChannel, {compute_engine_id, id, type}}, id: id)
    end
  end

  def stream_ids(compute_engine_id, type) do
    stream_specs(compute_engine_id, type) |> Enum.map(&Map.get(&1, :id))
  end

  def start_link({compute_engine_id, type}) do
    name =
      case type do
        :primary -> Identificator.stream_sup_primary(compute_engine_id)
        :secondary -> Identificator.stream_sup_secondary(compute_engine_id)
      end

    Supervisor.start_link(__MODULE__, {compute_engine_id, type}, name: name)
  end

  @impl true
  def init({compute_engine_id, type}) do
    children = stream_specs(compute_engine_id, type)

    Supervisor.init(children, strategy: :one_for_one)
  end
end

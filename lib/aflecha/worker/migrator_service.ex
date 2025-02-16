defmodule Aflecha.Worker.MigratorService do
  alias Aflecha.Service.Identificator

  def serialize_worker(operator_id, worker_id) do
    command_id = "serialize_worker"
    data = %{"worker_id" => worker_id}

    GenServer.call(Identificator.control_unit(operator_id), {:send_control, command_id, data})
  end

  def deserialize_worker(operator_id, worker_id, serialized_data) do
    command_id = "deserialize_worker"

    operator_data = %{
      "worker_id" => worker_id,
      "serialized_data" => serialized_data
    }

    GenServer.call(
      Identificator.control_unit(operator_id),
      {:send_control, command_id, operator_data}
    )
  end
end

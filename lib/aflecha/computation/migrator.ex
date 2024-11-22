defmodule Aflecha.Computation.Migrator do
  alias Aflecha.Service.Identificator

  def serialize_computation(operator_id, computation_id) do
    command_id = "serialize_computation"
    data = %{"computation_id" => computation_id}

    GenServer.call(Identificator.control_unit(operator_id), {:send_control, command_id, data})
  end

  def deserialize_computation(operator_id, computation_id, serialized_data) do
    command_id = "deserialize_computation"

    operator_data = %{
      "computation_id" => computation_id,
      "serialized_data" => serialized_data
    }

    GenServer.call(
      Identificator.control_unit(operator_id),
      {:send_control, command_id, operator_data}
    )
  end
end

defmodule Aflecha.Worker.MigratorService do
  alias Aflecha.Service.Identificator

  # TODO: these should reside inside the pipeline_manager module instead.
  def serialize_pipeline(operator_id, pipeline_id) do
    command_id = "serialize_pipeline"
    data = %{"pipeline_id" => pipeline_id}

    GenServer.call(Identificator.control_unit(operator_id), {:send_control, command_id, data})
  end

  def deserialize_pipeline(operator_id, pipeline_id, factory_id, serialized_state) do
    command_id = "deserialize_pipeline"

    operator_data = %{
      "pipeline_id" => pipeline_id,
      "factory_id" => factory_id,
      "serialized_state" => serialized_state
    }

    GenServer.call(
      Identificator.control_unit(operator_id),
      {:send_control, command_id, operator_data}
    )
  end
end

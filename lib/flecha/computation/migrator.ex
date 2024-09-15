defmodule Flecha.Computation.Migrator do
  alias Flecha.Warden
  alias Flecha.Utility.Identifier

  def serialize_computation(computation_id) do
    command_id = "42"
    data = %{"computation_id" => computation_id}

    Warden.get(:engines)
    |> Enum.map(fn {engine_id, _} ->
      res = GenServer.call(Identifier.control_unit(engine_id), {:send_control, command_id, data})

      {engine_id, res}
    end)
  end

  def deserialize_computation(computation_id, serialized_data) do
    command_id = "43"
    data = %{"computation_id" => computation_id, "serialized_data" => serialized_data}

    Warden.get(:engines)
    |> Enum.map(fn {engine_id, _} ->
      res = GenServer.call(Identifier.control_unit(engine_id), {:send_control, command_id, data})

      {engine_id, res}
    end)
  end
end

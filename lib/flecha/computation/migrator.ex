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

    Warden.get(:engines)
    |> Enum.map(fn {engine_id, _} ->
      engine_data =
        Enum.find(serialized_data, fn {engine_id_s, _} -> engine_id_s == engine_id end)

      case engine_data do
        nil ->
          nil

        serialized_engine_data ->
          engine_data = %{
            "computation_id" => computation_id,
            "serialized_data" => serialized_engine_data |> elem(1)
          }

          res =
            GenServer.call(
              Identifier.control_unit(engine_id),
              {:send_control, command_id, engine_data}
            )

          {engine_id, res}
      end
    end)
    |> Enum.filter(& &1)
  end
end

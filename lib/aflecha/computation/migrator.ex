defmodule Aflecha.Computation.Migrator do
  alias Aflecha.Service.Warden
  alias Aflecha.Service.Identificator

  def serialize_computation(computation_id) do
    command_id = "serialize_computation"
    data = %{"computation_id" => computation_id}

    Warden.get(:engines)
    |> Enum.map(fn {subscriber_id, _} ->
      res =
        GenServer.call(
          Identificator.control_unit(subscriber_id),
          {:send_control, command_id, data}
        )

      {subscriber_id, res}
    end)
  end

  def deserialize_computation(computation_id, serialized_data) do
    command_id = "deserialize_computation"

    Warden.get(:engines)
    |> Enum.map(fn {subscriber_id, _} ->
      engine_data =
        Enum.find(serialized_data, fn {engine_id_s, _} -> engine_id_s == subscriber_id end)

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
              Identificator.control_unit(subscriber_id),
              {:send_control, command_id, engine_data}
            )

          {subscriber_id, res}
      end
    end)
    |> Enum.filter(& &1)
  end
end

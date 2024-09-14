defmodule Flecha.Computation.Migrator do
  alias Flecha.Warden
  alias Flecha.Utility.Identifier

  def serialize_computation(computation_id) do
    # TODO: refactor later.
    command_id = "5000"
    data = %{"computation_id" => computation_id}

    Warden.get(:engines)
    |> Enum.map(fn {engine_id, _} ->
      GenServer.call(Identifier.control_unit(engine_id), {:send_control, command_id, data})
    end)
    |> IO.inspect(label: "HERE IS DATA")
  end
end

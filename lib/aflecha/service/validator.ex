defmodule Aflecha.Service.Validator do
  alias Aflecha.Service.Identificator

  use Aflecha.Constant

  def byte_limit_exceed(value, max_length \\ @computation_id_max_bytes) do
    case byte_size(value) > max_length do
      true -> {:error, "computation id exceeds #{@computation_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  def computation_unique(group_id, computation_id) do
    result =
      ProcessHub.child_lookup(
        Identificator.hub(group_id),
        computation_id
      )

    case result do
      nil -> :ok
      _ -> {:error, "computation id already in use"}
    end
  end
end

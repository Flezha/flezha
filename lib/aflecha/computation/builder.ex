defmodule Aflecha.Computation.Builder do
  alias Aflecha.Service.Validator

  @computation_id_max_bytes 32

  @type t() :: %__MODULE__{
          id: String.t(),
          factory_id: String.t(),
          variables: map()
        }
  @enforce_keys [:id, :factory_id]
  defstruct [:id, :factory_id, variables: %{}]

  def from_json(json_str) do
    case Jason.decode!(json_str) do
      {:ok, json_map} -> from_map(json_map)
      {:error, _} -> {:error, "invalid json"}
    end
  end

  def from_map(values_map) do
    with {:ok, id} <- get_from_map(values_map, "id"),
         {:ok, factory_id} <- get_from_map(values_map, "factory_id"),
         variables <- Map.get(values_map, "variables", %{}),
         {:ok, variables_json} <- Jason.decode(variables) do
      computation = %__MODULE__{
        id: id,
        factory_id: factory_id,
        variables: variables_json
      }

      {:ok, computation}
    else
      err -> err
    end
  end

  def validate_computation(computation) do
    case validate_id(computation.id) do
      {:error, reason} -> {:error, reason}
      :ok -> :ok
    end
  end

  defp validate_id(computation_id) do
    case Validator.byte_limit_exceed(computation_id, @computation_id_max_bytes) do
      true -> {:error, "computation id exceeds #{@computation_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  defp get_from_map(map, key) do
    case Map.get(map, key) do
      nil -> {:error, "missing key #{inspect(key)}"}
      value -> {:ok, value}
    end
  end
end

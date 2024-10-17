defmodule Flecha.Computation.Builder do
  alias Flecha.Service.Validator
  alias Flecha.Service.Warden

  @computation_id_max_bytes 32

  defstruct [:id, :factory_id, variables: %{}]

  def from_json(json_str) do
    with {:ok, json_map} <- Jason.decode!(json_str),
         {:ok, id} <- get_from_map(json_map, "id"),
         {:ok, factory_id} <- get_from_map(json_map, "factory_id"),
         variables <- Map.get(json_map, "variables", %{}) do
      computation = %__MODULE__{
        id: id,
        factory_id: factory_id,
        variables: variables
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

  def factory_match_engines(factory_id) do
    engines =
      Warden.get(:engines)
      |> Enum.filter(fn {_engine_id, factory} ->
        Enum.any?(factory, &(&1 === factory_id))
      end)

    case engines do
      [] -> {:error, "no engine found for factory: #{inspect(factory_id)}"}
      engines -> {:ok, engines}
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

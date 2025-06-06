defmodule Flezha.Pipeline.Builder do
  alias Flezha.Service.Validator

  @type t() :: %__MODULE__{
          id: String.t(),
          factory_id: String.t(),
          init_values: map(),
          opts: Keyword.t()
        }
  @enforce_keys [:id, :factory_id]
  defstruct [:id, :factory_id, init_values: %{}, opts: []]

  def from_json(json_str) do
    case Jason.decode!(json_str) do
      {:ok, json_map} -> from_map(json_map)
      {:error, _} -> {:error, "invalid json"}
    end
  end

  def from_map(values_map) do
    with {:ok, id} <- get_from_map(values_map, "id"),
         {:ok, factory_id} <- get_from_map(values_map, "factory_id"),
         init_values <- Map.get(values_map, "init_values", %{}),
         {:ok, init_values_json} <- Jason.decode(init_values) do
      pipeline = %__MODULE__{
        id: id,
        factory_id: factory_id,
        init_values: init_values_json
      }

      {:ok, pipeline}
    else
      err -> err
    end
  end

  def validate_pipeline(group_id, pipeline) do
    with :ok <- Validator.byte_limit_exceed(pipeline.id),
         :ok <- Validator.valid_group(group_id),
         :ok <- Validator.pipeline_unique(group_id, pipeline.id) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_from_map(map, key) do
    case Map.get(map, key) do
      nil -> {:error, "missing key #{inspect(key)}"}
      value -> {:ok, value}
    end
  end
end

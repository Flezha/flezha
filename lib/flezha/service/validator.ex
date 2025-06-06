defmodule Flezha.Service.Validator do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager

  use Flezha.Constant

  def byte_limit_exceed(value, max_length \\ @pipeline_id_max_bytes) do
    case byte_size(value) > max_length do
      true -> {:error, "pipeline id exceeds #{@pipeline_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  def valid_group(group_id) do
    case HubManager.hub_alive?(group_id) do
      true -> :ok
      false -> {:error, "no operators in group #{group_id}"}
    end
  end

  def pipeline_unique(group_id, pipeline_id) do
    result =
      ProcessHub.child_lookup(
        Identificator.hub(group_id),
        pipeline_id
      )

    case result do
      nil -> :ok
      _ -> {:error, "pipeline id already in use"}
    end
  end
end

defmodule Flezha.Service.Validator do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager

  use Flezha.Constant

  def byte_limit_exceed(value, max_length \\ @computation_id_max_bytes) do
    case byte_size(value) > max_length do
      true -> {:error, "computation id exceeds #{@computation_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  def valid_namespace(namespace) do
    case HubManager.hub_alive?(namespace) do
      true -> :ok
      false -> {:error, "no operators in namespace #{namespace}"}
    end
  end

  def computation_unique(namespace, computation_id) do
    result =
      ProcessHub.child_lookup(
        Identificator.hub(namespace),
        computation_id
      )

    case result do
      nil -> :ok
      _ -> {:error, "computation id already in use"}
    end
  end
end

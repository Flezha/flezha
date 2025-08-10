defmodule Flezha.Service.Validator do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager

  use Flezha.Constant

  def byte_limit_exceed(value, max_length \\ @processor_id_max_bytes) do
    case byte_size(value) > max_length do
      true -> {:error, "processor id exceeds #{@processor_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  def valid_namespace(namespace) do
    case HubManager.hub_alive?(namespace) do
      true -> :ok
      false -> {:error, "no operators in namespace #{namespace}"}
    end
  end

  def processor_unique(namespace, processor_id) do
    result =
      ProcessHub.child_lookup(
        Identificator.hub(namespace),
        processor_id
      )

    case result do
      nil -> :ok
      _ -> {:error, "processor id already in use"}
    end
  end
end

defmodule Flezha.Service.Validator do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Relay.Kind

  use Flezha.Constant

  def valid_kind(kind) do
    case Kind.known?(kind) do
      true -> :ok
      false -> {:error, "unknown relay kind: #{inspect(kind)}"}
    end
  end

  def byte_limit_exceed(value, max_length \\ @relay_id_max_bytes) do
    case byte_size(value) > max_length do
      true -> {:error, "relay id exceeds #{@relay_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  def valid_namespace(namespace) do
    case HubManager.hub_alive?(namespace) do
      true -> :ok
      false -> {:error, "no executors in namespace #{namespace}"}
    end
  end

  def relay_unique(namespace, relay_id) do
    result =
      ProcessHub.child_lookup(
        Identificator.hub(namespace),
        relay_id
      )

    case result do
      nil -> :ok
      _ -> {:error, "relay id already in use"}
    end
  end
end

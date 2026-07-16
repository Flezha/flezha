defmodule Flezha.Service.Validator do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Operative.Kind

  use Flezha.Constant

  def valid_kind(kind) do
    case Kind.known?(kind) do
      true -> :ok
      false -> {:error, "unknown operative kind: #{inspect(kind)}"}
    end
  end

  def byte_limit_exceed(value, max_length \\ @operative_id_max_bytes) do
    case byte_size(value) > max_length do
      true -> {:error, "operative id exceeds #{@operative_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  def valid_namespace(namespace) do
    case HubManager.hub_alive?(namespace) do
      true -> :ok
      false -> {:error, "no runners in namespace #{namespace}"}
    end
  end

  def operative_unique(namespace, operative_id) do
    result =
      ProcessHub.child_lookup(
        Identificator.hub(namespace),
        operative_id
      )

    case result do
      nil -> :ok
      _ -> {:error, "operative id already in use"}
    end
  end
end

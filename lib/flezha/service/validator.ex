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

  @doc """
  The hard operative-id limit in bytes. Read by callers that must budget a
  suffix against it (a partition group's `#pK`, a schedule's slot) before they
  mint an id that would be refused.
  """
  @spec id_byte_limit() :: pos_integer()
  def id_byte_limit(), do: @operative_id_max_bytes

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

  @id_in_use "operative id already in use"

  @doc """
  The refusal a start gets when the id is already claimed. Named so the
  start-authority seam can recognise a lost claim without re-typing the
  operator-facing string.
  """
  @spec id_in_use() :: String.t()
  def id_in_use(), do: @id_in_use

  def operative_unique(namespace, operative_id) do
    result =
      ProcessHub.child_lookup(
        Identificator.hub(namespace),
        operative_id
      )

    case result do
      nil -> :ok
      _ -> {:error, @id_in_use}
    end
  end
end

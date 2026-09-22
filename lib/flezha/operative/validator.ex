defmodule Flezha.Service.Validator do
  alias Flezha.Identificator
  alias Flezha.Service.HubManager

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

defmodule Flezha.Service.Validator do
  alias Flezha.Identificator
  alias Flezha.Service.HubManager

  def valid_namespace(namespace) do
    case HubManager.hub_alive?(namespace) do
      true -> :ok
      false -> {:error, {:no_runners, namespace}}
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
      _ -> {:error, {:id_in_use, relay_id}}
    end
  end
end

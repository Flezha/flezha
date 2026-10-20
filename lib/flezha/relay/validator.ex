defmodule Flezha.Service.Validator do
  alias Flezha.Identificator
  alias Flezha.Process.Hub
  alias Flezha.Service.HubManager

  def valid_namespace(namespace) do
    case HubManager.hub_alive?(namespace) do
      true -> :ok
      false -> {:error, {:no_runners, namespace}}
    end
  end

  def relay_unique(namespace, relay_id) do
    case Hub.with_live(Identificator.hub(namespace), nil, &ProcessHub.child_lookup(&1, relay_id)) do
      nil -> :ok
      _ -> {:error, {:id_in_use, relay_id}}
    end
  end
end

defmodule Flezha.Relay.Builder do
  @moduledoc """
  Constructor for relay specs handed to `Flezha.Service.RelayManager.start_relay/2`.

  ## Recognised opts

  The `opts` keyword list is opaque to most callers but a few keys are
  contractually meaningful:

    * `:default_input_type` — string tag for the relay's default input
      type (used by the Overwatch's `:io_in` handler when metadata omits it).
    * `:plugins` — list of `%Flezha.Plugin{}` to subscribe at relay start.
    * `:relay_class` — optional string overriding the relay class identifier
      used as the on-disk persistence key under
      `app_data/state/{relay_class}/`. Defaults to `relay.id` when absent.
  """

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
      relay = %__MODULE__{
        id: id,
        factory_id: factory_id,
        init_values: init_values_json
      }

      {:ok, relay}
    else
      err -> err
    end
  end

  def validate_relay(namespace, relay) do
    with :ok <- Validator.byte_limit_exceed(relay.id),
         :ok <- Validator.valid_namespace(namespace),
         :ok <- Validator.relay_unique(namespace, relay.id) do
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

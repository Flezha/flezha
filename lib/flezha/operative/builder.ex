defmodule Flezha.Operative.Builder do
  @moduledoc """
  Constructor for operative specs handed to `Flezha.Service.OperativeManager.start_operative/2`.

  ## Recognised opts

  The `opts` keyword list is opaque to most callers but a few keys are
  contractually meaningful:

    * `:default_input_type` — string tag for the operative's default input
      type (used by the Overwatch's `:io_in` handler when metadata omits it).
    * `:plugins` — list of `%Flezha.Plugin{}` to subscribe at operative start.
    * `:operative_class` — optional string overriding the operative class identifier
      used as the on-disk persistence key under
      `app_data/state/{operative_class}/`. Defaults to `operative.id` when absent.

  The `:kind` field (an atom, default `:continuous`) is the operative's compute-unit
  kind. It is set at start and frozen for the operative's lifetime; see
  `Flezha.Operative.Kind` for the kind/capability model.
  """

  alias Flezha.Service.Validator

  @type t() :: %__MODULE__{
          id: String.t(),
          factory_id: String.t(),
          kind: atom(),
          init_values: map(),
          opts: Keyword.t()
        }
  @enforce_keys [:id, :factory_id]
  defstruct [:id, :factory_id, kind: :continuous, init_values: %{}, opts: []]

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
      operative = %__MODULE__{
        id: id,
        factory_id: factory_id,
        init_values: init_values_json
      }

      {:ok, operative}
    else
      err -> err
    end
  end

  def validate_operative(namespace, operative) do
    with :ok <- Validator.valid_kind(operative.kind),
         :ok <- Validator.byte_limit_exceed(operative.id),
         :ok <- Validator.valid_namespace(namespace),
         :ok <- Validator.operative_unique(namespace, operative.id) do
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

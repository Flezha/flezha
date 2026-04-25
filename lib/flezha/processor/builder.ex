defmodule Flezha.Processor.Builder do
  @moduledoc """
  Constructor for processor specs handed to `Flezha.Service.ProcessorManager.start_processor/2`.

  ## Recognised opts

  The `opts` keyword list is opaque to most callers but a few keys are
  contractually meaningful:

    * `:default_input_type` — string tag for the processor's default input
      type (used by the Overwatch's `:io_in` handler when metadata omits it).
    * `:snapshot_interval` — overrides the agent-wide
      `FLEZHA_OVERWATCH_STATE_SNAPSHOT_INTERVAL_MS` for this processor.
      Accepts a non-negative integer (ms), the string `"disabled"`, or `:disabled`.
    * `:plugins` — list of `%Flezha.Plugin{}` to subscribe at processor start.
    * `:initial_state_binary` — opaque SDK-serialised state binary handed to
      the compute engine via the `ProcessorApplyState` control command before
      the first input is forwarded. Used by `Flezha.Cluster.Snapshot` on
      restore so a re-spawned processor begins with primed state. SDKs that
      do not implement Apply respond with `apply_state_not_supported` and
      the processor proceeds with empty default state.
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
      processor = %__MODULE__{
        id: id,
        factory_id: factory_id,
        init_values: init_values_json
      }

      {:ok, processor}
    else
      err -> err
    end
  end

  def validate_processor(namespace, processor) do
    with :ok <- Validator.byte_limit_exceed(processor.id),
         :ok <- Validator.valid_namespace(namespace),
         :ok <- Validator.processor_unique(namespace, processor.id) do
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

defmodule Flezha.Dlq.Entry do
  @moduledoc """
  The record a parked payload is, and the one constructor every writer builds it
  through.

  Four writers park a payload: the runner-side settlement, the outbox, the
  records path and the operative-forward provider. Each used to hand
  `Flezha.Dlq.Store` a map it built by hand, in three shapes, and a writer that
  forgot a key wrote a partial row that only failed later, at read time. The
  keys and the directory layout on disk are exactly what they were; what is new
  is that the required ones are checked before the write.

  `Flezha.Dlq.Park` stays the runner's wire frame for a park: this is the record
  the frame becomes on the controller, not a second copy of it.
  """

  @typedoc """
  `origin_offset` is `nil` outside the durability tier; `park_id` is set only by
  the runner-driven records path, which acks the park by that id.
  """
  @type t() :: %__MODULE__{
          operative_class: binary(),
          procedure_id: binary(),
          entity_key: binary(),
          channel: binary(),
          origin_offset: integer() | nil,
          error: binary(),
          attempts: non_neg_integer(),
          payload: binary() | nil,
          park_id: binary() | nil
        }

  defstruct [
    :operative_class,
    :procedure_id,
    :entity_key,
    :channel,
    :origin_offset,
    :error,
    :payload,
    :park_id,
    attempts: 0
  ]

  # The identity of the parked payload. Without all four an entry cannot be
  # listed, replayed or attributed, so it is refused rather than written.
  @required [:operative_class, :procedure_id, :entity_key, :channel]

  @doc """
  Build one entry, refusing a missing required field. `:error` is stringified
  here so every writer's reason reads the same way on disk, whether it started
  as a sentence or as a term.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(fields) do
    entry = struct(__MODULE__, normalise(fields))

    case Enum.reject(@required, &is_binary(Map.fetch!(entry, &1))) do
      [] -> {:ok, entry}
      missing -> {:error, {:dlq_entry_missing, missing}}
    end
  end

  @doc "Build one entry, raising on a missing required field."
  @spec new!(map() | keyword()) :: t()
  def new!(fields) do
    case new(fields) do
      {:ok, entry} -> entry
      {:error, reason} -> raise ArgumentError, "invalid dead-letter entry: #{inspect(reason)}"
    end
  end

  defp normalise(fields) do
    fields
    |> Map.new()
    |> Map.update(:error, nil, &stringify/1)
  end

  defp stringify(reason) when is_binary(reason), do: reason
  defp stringify(reason), do: inspect(reason)
end

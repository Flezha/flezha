defmodule Flezha.Observability.Signal do
  @moduledoc """
  Unified signal envelope produced by every observability source and
  consumed by every observability sink.

  One struct, four kinds — sinks pattern-match on `:kind` when they
  care and ignore it when they don't. See the rationale in
  `docs/observability_foundation.md` (decision D1).

  ## Fields

    * `:kind` — one of `:metric`, `:event`, `:log`, `:trace_span`.
    * `:source` — atom name of a source registered with
      `Flezha.Observability.Registry`.
    * `:name` — atom signal name within the source.
    * `:measurements` — numeric measurements as a map.
    * `:metadata` — categorical tags / fields as a map.
    * `:ts` — `System.monotonic_time(:microsecond)` at emit time.

  Construct via `new/1`; the constructor validates field shapes and
  auto-fills `:ts` when omitted.
  """

  @enforce_keys [:kind, :source, :name]
  defstruct [
    :kind,
    :source,
    :name,
    :ts,
    measurements: %{},
    metadata: %{}
  ]

  @kinds [:metric, :event, :log, :trace_span]

  @type kind :: :metric | :event | :log | :trace_span

  @type t :: %__MODULE__{
          kind: kind(),
          source: atom(),
          name: atom(),
          measurements: map(),
          metadata: map(),
          ts: integer()
        }

  @doc """
  The closed set of accepted `:kind` values.
  """
  @spec kinds() :: [kind()]
  def kinds(), do: @kinds

  @doc """
  Build a signal from a map or keyword list. Validates field shapes
  and fills `:ts` with `System.monotonic_time(:microsecond)` when not
  supplied. Raises `ArgumentError` on invalid input.
  """
  @spec new(map() | keyword()) :: t()
  def new(fields) when is_list(fields), do: new(Map.new(fields))

  def new(%{} = fields) do
    kind = Map.get(fields, :kind)
    source = Map.get(fields, :source)
    name = Map.get(fields, :name)
    measurements = Map.get(fields, :measurements, %{})
    metadata = Map.get(fields, :metadata, %{})
    ts = Map.get(fields, :ts) || System.monotonic_time(:microsecond)

    validate_kind!(kind)
    validate_atom!(:source, source)
    validate_atom!(:name, name)
    validate_map!(:measurements, measurements)
    validate_map!(:metadata, metadata)

    %__MODULE__{
      kind: kind,
      source: source,
      name: name,
      measurements: measurements,
      metadata: metadata,
      ts: ts
    }
  end

  defp validate_kind!(kind) when kind in @kinds, do: :ok

  defp validate_kind!(kind) do
    raise ArgumentError,
          "invalid Signal kind #{inspect(kind)} — expected one of #{inspect(@kinds)}"
  end

  defp validate_atom!(_field, value) when is_atom(value) and not is_nil(value), do: :ok

  defp validate_atom!(field, value) do
    raise ArgumentError, "Signal #{field} must be an atom, got #{inspect(value)}"
  end

  defp validate_map!(_field, %{} = value) when not is_struct(value), do: :ok

  defp validate_map!(field, value) do
    raise ArgumentError,
          "Signal #{field} must be a plain map, got #{inspect(value)}"
  end
end

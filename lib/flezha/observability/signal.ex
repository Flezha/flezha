defmodule Flezha.Observability.Signal do
  @moduledoc """
  Unified signal envelope produced by every observability source and
  consumed by every observability sink.

  One struct, four kinds — sinks pattern-match on `:kind` when they
  care and ignore it when they don't. See the rationale in
  `docs/operations/observability.md`.

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

  @doc """
  Render one metadata key or value as the text an encoder can put on a wire.

  A signal's metadata holds arbitrary terms — the Logger bridge alone attaches
  a map and an `{m, f, a}` tuple to every log event, and the WAL writer a
  3-tuple entity id. `to_string/1` cannot carry them: it raises for a map, a
  tuple or a pid, and for a list it returns the binary of the elements,
  silently changing what the term says. So only the types with an unambiguous
  `String.Chars` rendering take that route; everything else is inspected, lists
  included. The limits are explicit so a long term is never elided into `"..."`
  and read as complete.

  A binary is text only if it is valid UTF-8. The Prometheus scrape response
  declares `charset=utf-8` and protobuf defines a `string` field as UTF-8, so a
  binary that is not valid UTF-8 is inspected too — `<<255, 0, 254>>` — rather
  than passed through to corrupt the body it lands in.

  `nil` is the one atom whose `String.Chars` rendering is not its name: it is
  `""`, which on either wire means a key with no name at all, colliding with a
  genuine `""` key. It renders `"nil"`.

  `inspect/2` cannot raise — `Inspect.Opts` defaults to `safe: true` — so this
  function is total for every term.
  """
  @spec metadata_text(term()) :: String.t()
  def metadata_text(term) when is_binary(term) do
    if String.valid?(term), do: term, else: inspect_term(term)
  end

  def metadata_text(nil), do: "nil"

  def metadata_text(term) when is_atom(term) or is_integer(term) or is_float(term),
    do: to_string(term)

  def metadata_text(term), do: inspect_term(term)

  defp inspect_term(term), do: inspect(term, limit: :infinity, printable_limit: :infinity)

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

defmodule Flezha.Observability.Signal do
  @moduledoc """
  Unified signal envelope: one struct, four kinds
  (`:metric`, `:event`, `:log`, `:trace_span`), produced by every
  observability source and consumed by every sink.

  `:measurements` carries the numbers, `:metadata` the categorical
  tags, `:source` and `:name` the identity, `:ts` the emit time as
  `System.monotonic_time(:microsecond)`.

  Construct via `new/1`, which validates the field shapes and fills
  `:ts` when it is omitted.
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
    if kind == :metric, do: validate_metric_name!(name)
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
  3-tuple cell id. `to_string/1` cannot carry them: it raises for a map, a
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

  @doc """
  Render one metadata value for a format that carries typed values — the audit
  log's JSON lines and `flz observe tail --json`.

  The companion to `metadata_text/1`, and the reason there are two: a wire that
  can only hold text has to render `nil` as `"nil"` and a list by inspection,
  while JSON has a null, an array and an object and loses nothing by keeping
  them. A number and a boolean pass through; an atom takes its own name; a
  list and a plain map recurse, a map's keys through `metadata_text/1` because
  a key is text in either format. Everything else — a struct, a tuple, a pid —
  is inspected, exactly as `metadata_text/1` inspects it.

  A binary goes through `metadata_text/1` itself: JSON has no bytes type, so a
  binary that is not valid UTF-8 has to be rendered rather than carried, and
  the check that decides belongs to the one function both renderers ask.

  One rule, so two renderers of one format cannot answer differently for the
  same term.
  """
  @spec metadata_json(term()) :: term()
  def metadata_json(term) when is_binary(term), do: metadata_text(term)

  def metadata_json(term) when is_number(term) or is_boolean(term) or is_nil(term),
    do: term

  def metadata_json(term) when is_atom(term), do: Atom.to_string(term)
  def metadata_json(term) when is_list(term), do: Enum.map(term, &metadata_json/1)

  def metadata_json(%{} = term) when not is_struct(term),
    do: Map.new(term, fn {k, v} -> {metadata_text(k), metadata_json(v)} end)

  def metadata_json(term), do: inspect_term(term)

  @doc """
  The one log line a signal is written as: `[source] name`, then each metadata
  and measurement as `key=value`, keys and values through `metadata_text/1`.
  """
  @spec line(t()) :: String.t()
  def line(%__MODULE__{source: source, name: name} = signal) do
    pairs =
      for {key, value} <- Enum.concat(signal.metadata, signal.measurements),
          do: [?\s, metadata_text(key), ?=, metadata_text(value)]

    IO.iodata_to_binary([?[, Atom.to_string(source), "] ", Atom.to_string(name) | pairs])
  end

  @doc """
  Is `name` legal in the Prometheus text exposition — `[a-zA-Z_][a-zA-Z0-9_]*`,
  and outside the reserved `__` namespace?

  One predicate answers for a metric name and for a label name. Prometheus lets
  a metric name additionally carry `:`, which is reserved for recording rules
  and which nothing here emits, so a name that passes this is legal in either
  position.

  A name that fails is a scrape-level failure, not a series-level one:
  Prometheus rejects the whole exposition on a parse error. Which is why a
  developer-chosen name is refused where it is declared and a metadata key,
  which arrives at runtime from a third party, is sanitized instead.

  The common case is a name that is already legal, so this is a scan that
  allocates nothing and answers true.
  """
  @spec legal_exposition_name?(binary()) :: boolean()
  def legal_exposition_name?("__" <> _), do: false

  def legal_exposition_name?(<<c, rest::binary>>)
      when c in ?a..?z or c in ?A..?Z or c == ?_,
      do: legal_name_tail?(rest)

  def legal_exposition_name?(_), do: false

  defp legal_name_tail?(<<>>), do: true

  defp legal_name_tail?(<<c, rest::binary>>)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_,
       do: legal_name_tail?(rest)

  defp legal_name_tail?(_), do: false

  @doc """
  A metric's exposition name: its source and its name joined by the character
  the exporter's wire uses — `"_"` for Prometheus, `"."` for OpenTelemetry.

  One function, because two exporters must never disagree about which metric
  they are naming. The joiner is the only thing that differs between them, so
  it is a parameter rather than a second copy of the rule.
  """
  @spec exposition_name(atom(), atom(), binary()) :: String.t()
  def exposition_name(source, name, joiner),
    do: Atom.to_string(source) <> joiner <> Atom.to_string(name)

  defp validate_metric_name!(name) do
    if legal_exposition_name?(Atom.to_string(name)) do
      :ok
    else
      raise ArgumentError,
            "invalid metric name #{inspect(name)} — a metric name is rendered into the " <>
              "exposition and must match [a-zA-Z_][a-zA-Z0-9_]* outside the reserved __ namespace"
    end
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

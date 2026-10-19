defmodule Flezha.Observability.Source do
  @moduledoc """
  Behaviour for observability signal producers.

  A source is **declarative** — application code that calls
  `Flezha.Observability.Bus.emit/1` and owns no process — or
  **process-backed**, a GenServer under
  `Flezha.Observability.SourceSupervisor`. Both register through
  `Flezha.Observability.Registry.register_source/1`; registration is
  what makes a source visible, disable-able and budgeted.

  The foundation never calls a source back — a poller arms its own tick,
  and a process-backed source's `init/1` is GenServer's — so the sole
  callback is the registration schema, declared for every polled source
  by `Flezha.Observability.OtpBackend.Poller`.
  """

  @callback config_schema() :: map()

  @outcomes [:normal, :failure]

  @typedoc """
  One signal: `{kind, "what it says"}`, or `{kind, words}`, the words being
  `meaning:` and optionally `label:`, `outcome:` and `sentence:`.
  """
  @type declaration :: {Flezha.Observability.Signal.kind(), String.t() | keyword()}

  @doc """
  Signal declarations for a registration's `signals`. Each answers its meaning
  as `description`, a plain `label` (the humanized name unless declared), an
  `outcome` (`:normal` unless declared) and, when declared, a `sentence`
  template. Malformed words are the registry's to refuse.
  """
  @spec signals(keyword(declaration())) :: [map()]
  def signals(declared) do
    for {name, {kind, words}} <- declared, do: signal(name, kind, words)
  end

  defp signal(name, kind, meaning) when is_binary(meaning),
    do: signal(name, kind, meaning: meaning)

  defp signal(name, kind, words) do
    %{
      name: name,
      kind: kind,
      description: Keyword.get(words, :meaning),
      label: Keyword.get_lazy(words, :label, fn -> humanize(name) end),
      outcome: Keyword.get(words, :outcome, :normal)
    }
    |> put_present(:sentence, Keyword.get(words, :sentence))
  end

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)

  @doc """
  A name as plain words: `:order_refused` reads "Order refused".

      iex> Flezha.Observability.Source.humanize("kafka_consumer")
      "Kafka consumer"
  """
  @spec humanize(atom() | String.t()) :: String.t()
  def humanize(name), do: name |> to_string() |> String.replace("_", " ") |> String.capitalize()

  @doc """
  Whether `value` is valid for the signal word `key`: a non-blank `meaning`
  or `label`, an `outcome` from `outcomes/0`, or a `sentence` that parses.
  Any other key is not a word.
  """
  @spec valid_word?(atom(), term()) :: boolean()
  def valid_word?(key, value) when key in [:meaning, :label], do: text?(value)
  def valid_word?(:outcome, value), do: value in @outcomes
  def valid_word?(:sentence, value), do: text?(value) and placeholders(value) != :error
  def valid_word?(_key, _value), do: false

  @doc "Whether `value` is text a reader can be shown: a binary that is not blank."
  @spec text?(term()) :: boolean()
  def text?(value), do: is_binary(value) and String.trim(value) != ""

  @doc "The outcomes a signal may declare."
  @spec outcomes() :: [atom()]
  def outcomes(), do: @outcomes

  @doc """
  The placeholders of a sentence template, in order: `{key}` as `{key, nil}`
  and `{key:noun}` as `{key, noun}`. A key is a lowercase identifier and a noun
  lowercase words; any other brace makes the template `:error`.

      iex> Flezha.Observability.Source.placeholders("{relay} saved {count:record}")
      {:ok, [{"relay", nil}, {"count", "record"}]}
  """
  @spec placeholders(String.t()) :: {:ok, [{String.t(), String.t() | nil}]} | :error
  def placeholders(template) when is_binary(template) do
    template
    |> String.split(~r/\{[^{}]*\}/, include_captures: true, trim: true)
    |> Enum.reduce_while({:ok, []}, fn piece, {:ok, acc} ->
      case placeholder(piece) do
        :text -> {:cont, {:ok, acc}}
        {:ok, found} -> {:cont, {:ok, [found | acc]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, found} -> {:ok, Enum.reverse(found)}
      :error -> :error
    end
  end

  def placeholders(_other), do: :error

  defp placeholder("{" <> _ = piece) do
    case Regex.run(~r/^\{([a-z][a-z0-9_]*)(?::([a-z]+(?: [a-z]+)*))?\}$/, piece) do
      [_, key] -> {:ok, {key, nil}}
      [_, key, noun] -> {:ok, {key, noun}}
      nil -> :error
    end
  end

  defp placeholder(piece), do: if(String.contains?(piece, ["{", "}"]), do: :error, else: :text)
end

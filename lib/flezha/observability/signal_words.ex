defmodule Flezha.Observability.SignalWords do
  @moduledoc """
  The words a reader is told about a signal, checked: its meaning, label,
  outcome, sentence template and log level, and a name said in plain words.

  It names no other Flezha module, so every layer may use it: a plugin's own
  telemetry verbs are checked here exactly as a core source's signals are.
  """

  @outcomes [:normal, :failure]

  @doc """
  A name as plain words: `:order_refused` reads "Order refused".

      iex> Flezha.Observability.SignalWords.humanize("kafka_consumer")
      "Kafka consumer"
  """
  @spec humanize(atom() | String.t()) :: String.t()
  def humanize(name), do: name |> to_string() |> String.replace("_", " ") |> String.capitalize()

  @doc """
  Whether `value` is valid for the signal word `key`: a non-blank `meaning`
  or `label`, an `outcome` from `outcomes/0`, a `sentence` that parses, or a
  `level` of `Logger`'s, at which the signal's event is also written as a log
  line. Any other key is not a word.
  """
  @spec valid_word?(atom(), term()) :: boolean()
  def valid_word?(key, value) when key in [:meaning, :label], do: text?(value)
  def valid_word?(:outcome, value), do: value in @outcomes
  def valid_word?(:level, value), do: value in Logger.levels()
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

      iex> Flezha.Observability.SignalWords.placeholders("{relay} saved {count:record}")
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

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

  alias Flezha.Observability.SignalWords

  @callback config_schema() :: map()

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
      label: Keyword.get_lazy(words, :label, fn -> SignalWords.humanize(name) end),
      outcome: Keyword.get(words, :outcome, :normal)
    }
    |> put_present(:sentence, Keyword.get(words, :sentence))
  end

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)
end

defmodule Flezha.Observability.Source do
  @moduledoc """
  The contract of an observability signal producer, and the one way a source
  says what it is and what it can emit.

      use Flezha.Observability.Source,
        name: :audit_log,
        area: :controller,
        title: "Audit log",
        description: "The audit log: who read it, and its files starting and expiring.",
        signals: [audit_rotated: {:event, label: "New file", meaning: "The audit log started a new file."}]

  generates `__source__/0`, the registration (its `kinds` derived from the
  signals; the registry's default budget unless `cardinality_budget:` is
  given), and `emit/3`, `emit(name, measurements, metadata \\\\ %{})`, one
  clause per declared signal carrying its declared kind, so a name the source
  does not declare raises `FunctionClauseError` where it is emitted. A
  declaration whose records a shared emitter writes says `emit: false` and gets
  no `emit/3`.

  The registry registers every declarative source of the `:flezha` application
  when it starts (`declared/0`): no owner registers its own, and every one comes
  back after a registry restart. A process-backed source
  (`flavour: :process_backed`, as `Flezha.Observability.OtpBackend.Poller`
  declares it) registers its `__source__/0` from its own `init/1`, so it is
  listed only while it runs.

  The foundation never calls a source back, so the behaviour's sole callback is
  the registration schema, which the poller declares.
  """

  alias Flezha.Observability.{Signal, SignalWords}

  @callback config_schema() :: map()

  # Where a source sits, for a reader grouping the listing.
  @areas [:relays, :storage, :plugins, :controller, :machine]

  @typedoc """
  One signal: `{kind, "what it says"}`, or `{kind, words}`, the words being
  `meaning:` and optionally `label:`, `outcome:` and `sentence:`.
  """
  @type declaration :: {Signal.kind(), String.t() | keyword()}

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @flezha_source Flezha.Observability.Source.registration(opts)

      @doc "This source's registration."
      def __source__(), do: @flezha_source

      if Keyword.get(opts, :emit, true) do
        @doc "Put one of this source's declared signals on the bus."
        def emit(name, measurements, metadata \\ %{})

        source = @flezha_source.name

        for %{name: name, kind: kind} <- @flezha_source.signals do
          def emit(unquote(name), measurements, metadata) do
            Flezha.Observability.Bus.emit(
              Flezha.Observability.Signal.new(%{
                kind: unquote(kind),
                source: unquote(source),
                name: unquote(name),
                measurements: measurements,
                metadata: metadata
              })
            )
          end
        end
      end
    end
  end

  @doc """
  The registration a declaration gives: `name`, `area`, `title`,
  `description`, `signals` (as `signals/1` takes them), `flavour` (default
  `:declarative`) and an optional `cardinality_budget`; `kinds` are the
  signals' own.
  """
  @spec registration(keyword()) :: map()
  def registration(opts) do
    signals = signals(Keyword.fetch!(opts, :signals))

    opts
    |> Keyword.take([:name, :area, :title, :description, :cardinality_budget])
    |> Map.new()
    |> Map.merge(%{
      kinds: signals |> Enum.map(& &1.kind) |> Enum.uniq(),
      flavour: Keyword.get(opts, :flavour, :declarative),
      signals: signals
    })
  end

  @doc """
  Every declarative source the `:flezha` application's modules declare, read
  when the registry starts.
  """
  @spec declared() :: [map()]
  def declared() do
    for module <- Application.spec(:flezha, :modules) || [],
        Code.ensure_loaded?(module),
        function_exported?(module, :__source__, 0),
        %{flavour: :declarative} = entry <- [module.__source__()],
        do: entry
  end

  @doc "The areas a source may declare."
  @spec areas() :: [atom()]
  def areas(), do: @areas

  @doc """
  Signal declarations for a registration's `signals`. Each answers its meaning
  as `description`, a plain `label` (the humanized name unless declared), an
  `outcome` (`:normal` unless declared) and, when declared, a `sentence`
  template. Malformed words are `validate/1`'s to refuse.
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

  @doc "Whether a registration is one the registry takes, or why not."
  @spec validate(term()) :: :ok | {:error, term()}
  def validate(%{name: name, kinds: kinds, flavour: flavour} = entry)
      when is_atom(name) and is_list(kinds) and flavour in [:declarative, :process_backed] do
    cond do
      not Enum.all?(kinds, &(&1 in Signal.kinds())) ->
        {:error, {:invalid_kinds, kinds}}

      # A source name is the head of every metric name it emits, and a metric
      # name outside the exposition grammar fails the whole scrape. Refused
      # here, where the developer wrote it, rather than rewritten at scrape
      # time — a source name is chosen, not forwarded from a third party.
      not Signal.legal_exposition_name?(Atom.to_string(name)) ->
        {:error, {:invalid_source_name, name}}

      true ->
        validate_meaning(entry)
    end
  end

  def validate(_), do: {:error, :invalid_source_entry}

  # What a reader is told about the source: every key is optional.
  defp validate_meaning(entry) do
    %{title: title, description: description, area: area, signals: signals} =
      Map.merge(%{title: nil, description: nil, area: nil, signals: []}, entry)

    cond do
      not (is_nil(title) or SignalWords.text?(title)) ->
        {:error, {:invalid_title, title}}

      not (is_nil(description) or SignalWords.text?(description)) ->
        {:error, {:invalid_description, description}}

      area not in [nil | @areas] ->
        {:error, {:invalid_area, area}}

      not signal_decls?(signals) ->
        {:error, {:invalid_signals, signals}}

      true ->
        :ok
    end
  end

  defp signal_decls?(signals) when is_list(signals) do
    Enum.all?(signals, fn
      %{name: name, kind: kind, description: _} = signal ->
        is_atom(name) and kind in Signal.kinds() and words?(signal)

      _other ->
        false
    end)
  end

  defp signal_decls?(_other), do: false

  # The meaning is required; the other words are checked when present.
  defp words?(signal) do
    signal
    |> Map.take([:label, :outcome, :sentence])
    |> Map.put(:meaning, signal.description)
    |> Enum.all?(fn {k, v} -> SignalWords.valid_word?(k, v) end)
  end
end

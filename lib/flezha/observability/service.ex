defmodule Flezha.Service.Observability do
  @moduledoc """
  Service-API surface for the observability foundation. Every public function
  takes a `Flezha.Service.CallerContext` as its last argument and binds a row
  of `Flezha.AuthZ.Verbs`.

  With the master flag `:observability` → `enabled: false`, every public
  function returns `{:error, :observability_disabled}`.

  The subscriptions and the two mutations are audited on the
  `:observability_service` source before the effect, under the function's
  name — the seam the audit log consumes. The effects are public so
  `Flezha.Service.Cli.Observe` renders the same answers under the same rows.
  """

  use Flezha.AuthZ.Verb

  @source [type: :atom, doc: "A registered observability source."]

  alias Flezha.AuthZ
  alias Flezha.Config
  alias Flezha.Observability
  alias Flezha.Observability.{Registry, RingBufferSink}
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @type query_params :: %{
          required(:source) => atom(),
          optional(:from_ts) => integer(),
          optional(:limit) => non_neg_integer()
        }

  # ---------- Subscriptions ----------

  @doc "Subscribe the calling process to the live signals of `source`."
  @spec subscribe(atom(), caller_context()) :: :ok | {:error, term()}
  verb subscribe(source, caller_context) when is_atom(source),
    verb: :observability_subscribe,
    stream: true,
    args: [source: @source] do
    with :ok <- check_enabled(),
         {:ok, _} <- registered(source),
         do: Observability.subscribe(source)
  end

  @doc """
  Undo a `subscribe/2` or `tail/2`.

  A subscription established on this surface is undone on it, so a consumer
  never has to reach past the service API to release what it took through it.
  Authorized as `subscribe/2` is: the two are one capability.
  """
  @spec unsubscribe(atom(), caller_context()) :: :ok | {:error, term()}
  verb unsubscribe(source, caller_context) when is_atom(source),
    verb: :observability_unsubscribe,
    args: [source: @source] do
    with :ok <- check_enabled(), do: Observability.unsubscribe(source)
  end

  @doc """
  Stream live signals for a single source to the caller process.

  Subscribes to the foundation's `PubSubSink` topic for `source` and
  forwards `{:flezha_signal, signal}` messages to the caller. The
  caller is responsible for receiving and unsubscribing on shutdown.

  Returns `{:error, :pubsub_sink_disabled}` if the PubSub sink is
  not enabled (it cannot stream without the underlying broadcast).
  """
  @spec tail(atom(), caller_context()) :: :ok | {:error, term()}
  verb tail(source, caller_context) when is_atom(source),
    verb: :observability_tail,
    stream: true,
    args: [source: @source] do
    with :ok <- check_enabled(),
         {:ok, _} <- registered(source),
         :ok <- check_pubsub_sink(),
         do: Observability.subscribe(source)
  end

  # ---------- Reads ----------

  @doc "The signals the ring buffer holds for `params.source`, in the caller's namespace."
  @spec query(query_params() | keyword(), caller_context()) ::
          {:ok, [Signal.t()]} | {:error, term()}
  verb query(params, caller_context),
    verb: :observability_query,
    args: [params: [type: :keyword, doc: "`source`, `from_ts` and `limit`."]] do
    params = Map.new(params)

    with :ok <- check_enabled(),
         {:ok, written} <- Map.fetch(params, :source),
         {:ok, source} <- registered(written) do
      from_ts = Map.get(params, :from_ts, 0)
      opts = if l = Map.get(params, :limit), do: [limit: l], else: []

      results = RingBufferSink.query(source, from_ts, opts)
      {:ok, filter_by_namespace(results, caller_context)}
    else
      :error -> {:error, :missing_source}
      other -> other
    end
  end

  @doc "Every registered observability source and its status."
  @spec list_sources(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list_sources(caller_context), verb: :observability_sources do
    with :ok <- check_enabled(), do: {:ok, Registry.list_sources()}
  end

  @doc "Every registered observability sink."
  @spec list_sinks(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list_sinks(caller_context), verb: :observability_sinks do
    with :ok <- check_enabled(), do: {:ok, Registry.list_sinks()}
  end

  # ---------- Mutations ----------

  @doc "Enable the source `name`."
  @spec enable_source(atom(), caller_context()) :: :ok | {:error, term()}
  verb enable_source(name, caller_context) when is_atom(name),
    verb: :observability_enable,
    args: [name: @source] do
    with :ok <- check_enabled(), {:ok, _} <- registered(name), do: Registry.enable_source(name)
  end

  @doc "Disable the source `name`."
  @spec disable_source(atom(), caller_context()) :: :ok | {:error, term()}
  verb disable_source(name, caller_context) when is_atom(name),
    verb: :observability_disable,
    args: [name: @source] do
    with :ok <- check_enabled(), {:ok, _} <- registered(name), do: Registry.disable_source(name)
  end

  # ---------- Internal helpers ----------

  # The registered source `written` names, as an atom or as its text; one the
  # registry does not hold is refused naming the ones it does.
  defp registered(written) do
    sources = Enum.map(Registry.list_sources(), & &1.name)

    case Enum.find(sources, &(&1 == written or Atom.to_string(&1) == written)) do
      nil ->
        {:error,
         Flezha.Error.new("usage.unknown_word", noun: "source", value: written, allowed: sources)}

      source ->
        {:ok, source}
    end
  end

  defp check_pubsub_sink() do
    if Config.get(:observability_pubsub_sink_enabled),
      do: :ok,
      else: {:error, :pubsub_sink_disabled}
  end

  # Filter ring-buffer query results to signals visible to the
  # caller's namespace. `admin@_system` sees everything; every other
  # caller sees only their own namespace's signals (and signals
  # whose metadata carries no namespace — pre-change emits).
  defp filter_by_namespace(signals, %{role: :admin, namespace: "_system"}), do: signals
  defp filter_by_namespace(signals, %{source: :internal}), do: signals

  defp filter_by_namespace(signals, caller_context) do
    bound_ns = Map.get(caller_context, :namespace)

    cond do
      not AuthZ.enabled?() ->
        signals

      is_nil(bound_ns) ->
        signals

      true ->
        Enum.filter(signals, fn signal ->
          case Map.get(signal.metadata, :namespace) do
            nil -> true
            ns -> ns == bound_ns
          end
        end)
    end
  end

  defp check_enabled() do
    if Observability.enabled?(), do: :ok, else: {:error, :observability_disabled}
  end
end

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

  alias Flezha.AuthZ
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

  @spec subscribe(atom(), caller_context()) :: :ok | {:error, term()}
  verb subscribe(source, caller_context) when is_atom(source), verb: :observability_subscribe do
    with :ok <- check_enabled(), do: Observability.subscribe(source)
  end

  @doc """
  Undo a `subscribe/2` or `tail/2`.

  A subscription established on this surface is undone on it, so a consumer
  never has to reach past the service API to release what it took through it.
  Authorized as `subscribe/2` is: the two are one capability.
  """
  @spec unsubscribe(atom(), caller_context()) :: :ok | {:error, term()}
  verb unsubscribe(source, caller_context) when is_atom(source),
    verb: :observability_unsubscribe do
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
  verb tail(source, caller_context) when is_atom(source), verb: :observability_tail do
    tail_source(source)
  end

  # ---------- Reads ----------

  @spec query(query_params(), caller_context()) ::
          {:ok, [Signal.t()]} | {:error, term()}
  verb query(params, caller_context) when is_map(params), verb: :observability_query do
    with :ok <- check_enabled(),
         {:ok, source} <- Map.fetch(params, :source) do
      from_ts = Map.get(params, :from_ts, 0)
      opts = if l = Map.get(params, :limit), do: [limit: l], else: []

      results = RingBufferSink.query(source, from_ts, opts)
      {:ok, filter_by_namespace(results, caller_context)}
    else
      :error -> {:error, :missing_source}
      other -> other
    end
  end

  @spec list_sources(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list_sources(caller_context), verb: :observability_sources do
    sources()
  end

  @spec list_sinks(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list_sinks(caller_context), verb: :observability_sinks do
    sinks()
  end

  # ---------- Mutations ----------

  @spec enable_source(atom(), caller_context()) :: :ok | {:error, term()}
  verb enable_source(name, caller_context) when is_atom(name), verb: :observability_enable do
    enable(name)
  end

  @spec disable_source(atom(), caller_context()) :: :ok | {:error, term()}
  verb disable_source(name, caller_context) when is_atom(name), verb: :observability_disable do
    disable(name)
  end

  # ---------- The effects the terminal shares ----------

  @doc false
  @spec sources() :: {:ok, [map()]} | {:error, term()}
  def sources, do: with(:ok <- check_enabled(), do: {:ok, Registry.list_sources()})

  @doc false
  @spec sinks() :: {:ok, [map()]} | {:error, term()}
  def sinks, do: with(:ok <- check_enabled(), do: {:ok, Registry.list_sinks()})

  @doc false
  @spec enable(atom()) :: :ok | {:error, term()}
  def enable(name), do: with(:ok <- check_enabled(), do: Registry.enable_source(name))

  @doc false
  @spec disable(atom()) :: :ok | {:error, term()}
  def disable(name), do: with(:ok <- check_enabled(), do: Registry.disable_source(name))

  @doc false
  @spec tail_source(atom()) :: :ok | {:error, term()}
  def tail_source(source) do
    with :ok <- check_enabled(),
         :ok <- check_pubsub_sink() do
      Observability.subscribe(source)
    end
  end

  # ---------- Internal helpers ----------

  defp check_pubsub_sink() do
    enabled? =
      Application.get_env(:flezha, :observability, [])
      |> Keyword.get(:pubsub_sink_enabled, true)

    if enabled?, do: :ok, else: {:error, :pubsub_sink_disabled}
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

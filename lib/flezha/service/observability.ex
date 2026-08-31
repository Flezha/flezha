defmodule Flezha.Service.Observability do
  @moduledoc """
  Service-API surface for the observability foundation. Every public function
  takes a `Flezha.Service.CallerContext` as its last argument.

  With the master flag `:observability` → `enabled: false`, every public
  function returns `{:error, :observability_disabled}`.

  `subscribe/2`, `enable_source/2` and `disable_source/2` emit an audit
  `:event` on the bus carrying the function name, the caller's `source` and
  its optional `request_id` — the seam the audit log consumes.
  """

  alias Flezha.AuthZ
  alias Flezha.Observability
  alias Flezha.Observability.{Registry, RingBufferSink}
  alias Flezha.Service.CallerContext

  @audit_source :observability_service

  @type caller_context :: CallerContext.t()

  @type query_params :: %{
          required(:source) => atom(),
          optional(:from_ts) => integer(),
          optional(:limit) => non_neg_integer()
        }

  # ---------- Subscriptions ----------

  @spec subscribe(atom(), caller_context()) :: :ok | {:error, term()}
  def subscribe(source, caller_context) when is_atom(source) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"observability.subscribe") do
      emit_audit(:subscribe, caller_context, %{source: source})
      Observability.subscribe(source)
    end
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
  def tail(source, caller_context) when is_atom(source) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"observability.subscribe"),
         :ok <- check_pubsub_sink() do
      emit_audit(:tail, caller_context, %{source: source})
      Observability.subscribe(source)
    end
  end

  defp check_pubsub_sink() do
    enabled? =
      Application.get_env(:flezha, :observability, [])
      |> Keyword.get(:pubsub_sink_enabled, true)

    if enabled? do
      :ok
    else
      {:error, :pubsub_sink_disabled}
    end
  end

  # ---------- Reads ----------

  @spec query(query_params(), caller_context()) ::
          {:ok, [Signal.t()]} | {:error, term()}
  def query(%{} = params, caller_context) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"observability.query"),
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
  def list_sources(caller_context) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"observability.query") do
      {:ok, Registry.list_sources()}
    end
  end

  @spec list_sinks(caller_context()) :: {:ok, [map()]} | {:error, term()}
  def list_sinks(caller_context) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"observability.query") do
      {:ok, Registry.list_sinks()}
    end
  end

  # ---------- Mutations ----------

  @spec enable_source(atom(), caller_context()) :: :ok | {:error, term()}
  def enable_source(name, caller_context) when is_atom(name) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"observability.enable_source"),
         :ok <- emit_audit(:enable_source, caller_context, %{source: name}) do
      Registry.enable_source(name)
    end
  end

  @spec disable_source(atom(), caller_context()) :: :ok | {:error, term()}
  def disable_source(name, caller_context) when is_atom(name) do
    with :ok <- check_enabled(),
         :ok <- CallerContext.authorize(caller_context, :"observability.disable_source"),
         :ok <- emit_audit(:disable_source, caller_context, %{source: name}) do
      Registry.disable_source(name)
    end
  end

  # Build a scope map for `Flezha.AuthZ.authorize/3`. The caller's
  # bound namespace, when present, is used; otherwise we default to
  # `_system` so internal callers and the disabled-auth path
  # continue to work.
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

  # ---------- Internal helpers ----------

  defp check_enabled() do
    if Observability.enabled?(), do: :ok, else: {:error, :observability_disabled}
  end

  defp emit_audit(action, caller_context, extra) do
    CallerContext.emit(@audit_source, action, caller_context, extra)
  end
end

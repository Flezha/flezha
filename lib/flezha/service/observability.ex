defmodule Flezha.Service.Observability do
  @moduledoc """
  Service-API surface for the observability foundation.

  Callable from both the CLI (via thin wrappers in
  `Flezha.Service.CliManager`) and — once it lands — the Phoenix
  `flezha_ui` directly. Every public function takes a
  `caller_context` as its last argument; the context shape is locked
  here so the future `platform-authn-authz` change can plug
  authorization in without modifying any caller.

  When the master flag `:observability` → `enabled: false`, every
  public function returns `{:error, :observability_disabled}`.

  The `subscribe/2`, `enable_source/2`, `disable_source/2` calls emit
  an audit `:event` signal on the bus recording the function name,
  the caller's `source`, and the optional `request_id` — this is the
  audit seam `audit-log` will consume.
  """

  alias Flezha.AuthZ
  alias Flezha.Observability
  alias Flezha.Observability.{Bus, Registry, RingBufferSink, Signal}

  @valid_sources [:cli, :ui, :sdk, :internal]
  @audit_source :observability_service

  @type caller_context :: %{
          required(:source) => :cli | :ui | :sdk | :internal,
          optional(:principal) => term() | nil,
          optional(:request_id) => binary() | nil
        }

  @type query_params :: %{
          required(:source) => atom(),
          optional(:from_ts) => integer(),
          optional(:limit) => non_neg_integer()
        }

  # ---------- Subscriptions ----------

  @spec subscribe(atom(), caller_context()) :: :ok | {:error, term()}
  def subscribe(source, caller_context) when is_atom(source) do
    with :ok <- check_enabled(),
         :ok <- validate_caller(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context),
         :ok <-
           AuthZ.authorize(caller_context, :"observability.subscribe", scope_for(caller_context)) do
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
         :ok <- validate_caller(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context),
         :ok <-
           AuthZ.authorize(caller_context, :"observability.subscribe", scope_for(caller_context)),
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
         :ok <- validate_caller(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context),
         :ok <- AuthZ.authorize(caller_context, :"observability.query", scope_for(caller_context)),
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
         :ok <- validate_caller(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context),
         :ok <- AuthZ.authorize(caller_context, :"observability.query", scope_for(caller_context)) do
      {:ok, Registry.list_sources()}
    end
  end

  @spec list_sinks(caller_context()) :: {:ok, [map()]} | {:error, term()}
  def list_sinks(caller_context) do
    with :ok <- check_enabled(),
         :ok <- validate_caller(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context),
         :ok <- AuthZ.authorize(caller_context, :"observability.query", scope_for(caller_context)) do
      {:ok, Registry.list_sinks()}
    end
  end

  # ---------- Mutations ----------

  @spec enable_source(atom(), caller_context()) :: :ok | {:error, term()}
  def enable_source(name, caller_context) when is_atom(name) do
    with :ok <- check_enabled(),
         :ok <- validate_caller(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context),
         :ok <-
           AuthZ.authorize(
             caller_context,
             :"observability.enable_source",
             scope_for(caller_context)
           ),
         :ok <- emit_audit(:enable_source, caller_context, %{source: name}) do
      Registry.enable_source(name)
    end
  end

  @spec disable_source(atom(), caller_context()) :: :ok | {:error, term()}
  def disable_source(name, caller_context) when is_atom(name) do
    with :ok <- check_enabled(),
         :ok <- validate_caller(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context),
         :ok <-
           AuthZ.authorize(
             caller_context,
             :"observability.disable_source",
             scope_for(caller_context)
           ),
         :ok <- emit_audit(:disable_source, caller_context, %{source: name}) do
      Registry.disable_source(name)
    end
  end

  # Build a scope map for `Flezha.AuthZ.authorize/3`. The caller's
  # bound namespace, when present, is used; otherwise we default to
  # `_system` so internal callers and the disabled-auth path
  # continue to work.
  defp scope_for(caller_context) do
    %{namespace: Map.get(caller_context, :namespace) || "_system"}
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

  # ---------- Internal helpers ----------

  defp check_enabled() do
    if Observability.enabled?(), do: :ok, else: {:error, :observability_disabled}
  end

  defp validate_caller(%{source: src}) when src in @valid_sources, do: :ok
  defp validate_caller(_), do: {:error, :invalid_caller_context}

  defp emit_audit(action, caller_context, extra) do
    metadata =
      extra
      |> Map.put(:action, action)
      |> Map.put(:caller_source, Map.get(caller_context, :source))
      |> maybe_put(:request_id, Map.get(caller_context, :request_id))

    signal =
      Signal.new(%{
        kind: :event,
        source: @audit_source,
        name: action,
        measurements: %{count: 1},
        metadata: metadata
      })

    Bus.emit(signal)
    :ok
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end

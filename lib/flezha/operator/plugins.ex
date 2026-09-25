defmodule Flezha.Operator.Plugins do
  @moduledoc """
  The operator's plugin extension table: two-phase bring-up at bind, and the
  runtime add / restart / persist operations after it.

  The two phases exist because activation does real upstream I/O. `prepare/2`
  validates each plugin's options and runs `init_provider/1`, leaving it
  `:prepared` and holding no process at all; `bring_up/1` starts each provider's
  declared worker and calls `activate/1`, which is where TLS connects and
  subscribe handshakes go out. A bind that fails between them leaves nothing
  talking to an upstream, which is what makes a parked operator silent rather
  than half-live.

  Operations that are a bare `Flezha.Plugin.Extension.Table` call are made
  against `Table` directly by the callback that receives them — this module
  holds only what carries logic of its own.
  """

  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.PluginManager

  # Run one extension chain, building its context from the catalog.
  #
  # `Flezha.Plugin.Host`'s catalog already declares which `Ctx` struct each chain
  # takes, so a caller names the chain and supplies only its own fields — never
  # the struct. The identity every context carries is stamped once, here, which is
  # what keeps a newly declared chain from having to remember it.
  #
  # `struct/2` rather than `struct!/2` is deliberate: the contexts differ on
  # purpose (`Ctx.Init` carries `factory_id` where the others carry `runner_id`),
  # and each takes the identity fields it declares and ignores the rest.
  #
  # A chain the catalog does not declare answers exactly as `Extension.run/3`
  # does for one — there is no context to build, and a caller that treats an
  # unknown chain as a passthrough must keep doing so.
  @spec run_chain(Overwatch.t(), atom(), map()) :: term()
  defp run_chain(%Overwatch{} = state, name, fields) do
    case Map.fetch(state.extensions.catalog, name) do
      {:ok, %{ctx_module: ctx_module}} ->
        identity = %{
          operator_id: state.id,
          namespace: state.namespace,
          runner_id: state.runner_id,
          factory_id: state.factory_id
        }

        Extension.run(state.extensions, name, struct(ctx_module, Map.merge(identity, fields)))

      :error ->
        {:error, {:unknown_hook, name}}
    end
  end

  @doc """
  Run one direction of the plugin IO chain (`:on_input` or `:on_output`) over a
  payload. `nil` means a plugin filtered it; a chain error passes the payload
  through unchanged.
  """
  @spec transform_io(Overwatch.t(), :on_input | :on_output, binary(), map()) ::
          {binary(), map()} | nil
  def transform_io(%Overwatch{} = state, chain, message, metadata) do
    if Extension.active?(state.extensions, chain) do
      case run_chain(state, chain, %{message: message, metadata: metadata}) do
        {:ok, %Ctx.IO{message: msg, metadata: md}} -> {msg, md}
        {:skip, _reason} -> nil
        {:error, _reason} -> {message, metadata}
      end
    else
      {message, metadata}
    end
  end

  @doc """
  Phase 1: register each plugin's GenServer + struct (`init_provider/1` runs),
  but do NOT activate upstream resources.

  Idempotent on repeat — `Table.bulk_subscribe/2` rejects duplicate plugin_ids,
  so a retry-driven re-entry skips without re-init.

  A failure answers `{:error, {:plugin_prepare_failed, plugin_id, reason}}`:
  the operator parks on that reason, so it has to name the plugin.
  """
  @spec prepare(Overwatch.t(), list()) :: {:ok, Overwatch.t()} | {:error, term()}
  def prepare(%Overwatch{} = state, plugins) do
    # `operator.opts[:plugins]` accepts two shapes: bare `%Plugin{}` (the
    # cold-start `flz operator start --plugins` path emits this) and
    # `{plugin_id, %Plugin{}}` tuples (the cspec-persistence path written
    # by `Flezha.Service.PluginManager` through `ProcessRegistry.update/3`
    # after a successful `flz plugin enable`). Both normalise into the
    # tuple form `Table.bulk_subscribe/2` expects.
    if state.extensions.plugins != %{} do
      {:ok, state}
    else
      entries =
        Enum.map(plugins, fn
          {id, %Plugin{} = plugin} -> {id, plugin}
          %Plugin{id: id} = plugin -> {id, plugin}
        end)

      case Table.bulk_subscribe(state.extensions, entries) do
        {:ok, table} ->
          {:ok, %{state | extensions: table}}

        # The id is the only thing naming WHICH plugin parked the operator. It
        # rides `init_error` out through `:registry_runner_set` to whoever asked
        # for the start, so discarding it here left the caller with a bare
        # `{:module_not_loaded, Provider}` and no row to blame.
        {:error, {plugin_id, reason}} ->
          {:error, {:plugin_prepare_failed, plugin_id, reason}}
      end
    end
  end

  @doc """
  Phase 2: bring every subscribed plugin up. The ONE bring-up: it starts each
  provider's declared worker and calls `activate/1`, where upstream TLS
  connects, subscribe handshakes and other resource acquisition happen, and
  answers the plugins that did not reach `:running` with the reason each gave.

  Bind, a runtime enable, a plugin heal and a handover all reach it. What a
  caller does with the list is its own: bind lets the convergence loop heal
  them, a runtime enable refuses the one plugin it was asked about.
  """
  @spec bring_up(Overwatch.t()) :: {Overwatch.t(), [{binary(), term()}]}
  def bring_up(%Overwatch{} = state) do
    {table, failed} = Table.bulk_activate(state.extensions, env(state))
    {%Overwatch{state | extensions: table}, failed}
  end

  @doc """
  Runtime add: the SDK is already up, so bring the plugin up immediately. The
  boot path goes through `prepare/2` + `activate/1` instead.
  """
  @spec subscribe(Overwatch.t(), term(), Plugin.t()) ::
          {:ok, Overwatch.t()} | {:error, term(), Overwatch.t()}
  def subscribe(%Overwatch{} = state, plugin_id, %Plugin{} = plugin) do
    # Idempotent on a plugin already on the table, because that is exactly the
    # state a `start_plugin` heal finds: the subscription succeeded and only the
    # upstream acquisition failed, leaving it `:prepared`. Re-subscribing would
    # be refused as a duplicate id and the plugin could never be healed, so
    # re-ACTIVATING is the retry. An already-`:running` plugin is skipped by
    # `bulk_activate/2`, so the same call is a no-op for one that needs nothing.
    if Table.phase(state.extensions, plugin_id) == :unknown do
      case Table.bulk_subscribe(state.extensions, [{plugin_id, plugin}]) do
        {:ok, table} -> activated(%Overwatch{state | extensions: table}, plugin_id)
        {:error, {_id, reason}} -> {:error, reason, state}
      end
    else
      activated(state, plugin_id)
    end
  end

  @doc """
  The convergence loop's `restart_plugin`, for a declared plugin whose provider
  self-reports unhealthy or whose worker crashed: re-run its provider from the
  declared manifest (`Table.attempt_restart/3`) and bring it up, so the worker
  is started afresh and `activate/1` reopens upstream resources.

  On a failed restart the plugin is recorded down, so the next tick diffs it as
  unhealthy and retries it on the loop's table.
  """
  @spec restart(Overwatch.t(), term(), Plugin.t()) ::
          {:ok, Overwatch.t()} | {:error, term(), Overwatch.t()}
  def restart(%Overwatch{} = state, plugin_id, %Plugin{} = plugin) do
    case Table.attempt_restart(state.extensions, plugin_id, plugin) do
      {:ok, table} -> activated(%Overwatch{state | extensions: table}, plugin_id)
      {:error, reason, table} -> {:error, reason, %{state | extensions: table}}
    end
  end

  # Bring up, then answer for the ONE plugin the caller asked about. A subscribe
  # that succeeded while `activate/1` failed leaves a plugin holding no upstream
  # resources; reporting `{:ok, _}` for it makes the convergence loop's
  # `start_plugin` / `restart_plugin` heal advance the per-target backoff as
  # though the divergence were repaired, and, for a restart, turns a plugin the
  # loop detected as unhealthy into a `:prepared` one it cannot see. The remedy
  # must never be able to hide the fault.
  defp activated(%Overwatch{} = state, plugin_id) do
    {state, failed} = bring_up(state)

    case List.keyfind(failed, plugin_id, 0) do
      nil -> {:ok, state}
      {^plugin_id, reason} -> {:error, {:activation_failed, reason}, state}
    end
  end

  @doc """
  Operator-level facts providers need at activation (the tier drives the
  source adapters' blocking-ingest mode).
  """
  @spec env(Overwatch.t()) :: map()
  def env(%Overwatch{} = state), do: %{effectively_once: state.effectively_once}

  @doc """
  Drop the `PluginManager` registrations a teardown released and answer the
  table it left. `Extension.Table` is a data structure and issues no effects,
  so it reports the ids it dropped and the Overwatch that owns it casts.
  """
  @spec unregister({Table.t(), [binary()]}) :: Table.t()
  def unregister({table, plugin_ids}) do
    Enum.each(plugin_ids, &PluginManager.unregister_plugin/1)
    table
  end

  @doc """
  A provider committed durable state of its own (the websocket provider's
  applied subscription set). It lands in two places for two restart kinds: the
  extension table, which `attempt_restart/3` re-runs `init_provider/1` from, and
  the durable plugin manifest, which a controller restart re-applies plugins
  from.

  The durable half writes the hub's declared set and its registry row through
  one seam, and carries a retry ladder, so it runs in a task — the Overwatch
  loop carries the operator's data path.
  """
  @spec persist_opt(Overwatch.t(), term(), term(), term()) :: Overwatch.t()
  def persist_opt(%Overwatch{} = state, plugin_id, key, value) do
    Task.start(fn ->
      PluginManager.persist_plugin_opt(state.namespace, state.id, plugin_id, key, value)
    end)

    %{state | extensions: Table.put_plugin_opt(state.extensions, plugin_id, key, value)}
  end

  @doc """
  Resolve the operator's init values.

  Per the `operator-init-state` capability spec: a cold start with empty
  `init_values` runs the `:on_operator_init` chain; a cold start with non-empty
  ones MUST NOT run it and uses the inline values as-is.

  This is also the path for in-place restart plugin restoration: the
  `operator.opts[:plugins]` list of the cspec the operator was started with is
  re-subscribed by `prepare/2`, and a restart-driven start builds that cspec
  from the hub's declared set. That is
  independent of the per-Cell state restore, which rehydrates SDK-side
  WAL/snapshot state through the data plane. A HotSwap migration is handled in
  `Flezha.Operator.Handover` and never reaches here.
  """
  @spec resolve_init_state(Overwatch.t(), map()) ::
          {:ok, Overwatch.t(), map()} | {:error, term()}
  def resolve_init_state(%Overwatch{} = state, %{init_values: init_values} = operator)
      when init_values == %{} do
    fields = %{
      operator_id: operator.id,
      factory_id: operator.factory_id,
      accumulated: operator.init_values
    }

    case run_chain(state, :on_operator_init, fields) do
      {:ok, %Ctx.Init{accumulated: resolved}} ->
        {:ok, %{state | init_values: resolved}, %{operator | init_values: resolved}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def resolve_init_state(%Overwatch{} = state, operator) do
    # Non-empty inline init_values bypass the chain.
    {:ok, %{state | init_values: operator.init_values}, operator}
  end

  @doc """
  Run the `:on_terminate` chain.
  """
  @spec fire_on_terminate(Overwatch.t(), term()) :: :ok
  def fire_on_terminate(%Overwatch{} = state, reason) do
    run_chain(state, :on_terminate, %{reason: reason})
    :ok
  end

  @doc """
  Deliver an inbound `:flezha_request` to the `:on_message` handler chain.
  The handler key is passed through as it arrives: it is matched against the
  hook's `key` sub-option, which is a string.
  """
  @spec handle_request(Overwatch.t(), binary(), binary(), pid(), reference()) :: :ok
  def handle_request(%Overwatch{} = state, handler_key, payload, reply_to, ref) do
    run_chain(state, :on_message, %{
      key: handler_key,
      payload: payload,
      reply_to: reply_to,
      ref: ref
    })

    :ok
  end
end

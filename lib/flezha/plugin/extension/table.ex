defmodule Flezha.Plugin.Extension.Table do
  @moduledoc """
  Per-host registry of extension subscriptions, compiled dispatch chains, and
  subscriber lifecycle state.

  The struct is owned here and the three companions mutate disjoint parts of
  it: `Subscription` the subscriber lists, `Chain` the compiled closures and
  the `active_mask`, `Health` the health verdicts and the monitor, timer and
  pid hygiene. None of them names this module, so effects and lifecycle stay
  here.

  Plugins enter in the `:prepared` phase — `init_provider/1` has run and the
  GenServer is registered, but no upstream resource is held. `bulk_activate/1`
  moves them to `:running` once the SDK side is bound, firing each provider's
  optional `activate/1` exactly once per binding. A subscriber whose owned pid
  crashes is recorded as `{:down, reason}`; the table holds no retry counter,
  timer or policy — the restart is the convergence loop's, through
  `attempt_restart/3`.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table.Chain
  alias Flezha.Plugin.Extension.Table.Health
  alias Flezha.Plugin.Extension.Table.Subscription
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  defstruct catalog: %{},
            active_mask: 0,
            subs: %{},
            request_subs: %{},
            chains: %{},
            plugins: %{},
            phases: %{},
            monitors: %{},
            owned_pids: %{},
            timers: %{},
            started_at: %{},
            host_pid: nil,
            # Operative-level facts resolved at bind time (currently
            # `:effectively_once`), merged into every provider's opts at
            # activation — including plugin-heal re-activations.
            activation_env: %{}

  @type t() :: %__MODULE__{}

  @type entry :: %{
          plugin_id: binary(),
          provider: module(),
          provider_opts: map(),
          sub_opts: keyword(),
          position: non_neg_integer() | nil,
          owned_pid: pid() | nil,
          monitor_ref: reference() | nil,
          timer_ref: reference() | nil
        }

  @doc """
  Build a new table from a catalog map (as produced by
  `Flezha.Plugin.Host.build_catalog/2`).
  """
  @spec new(map()) :: t()
  def new(catalog) when is_map(catalog) do
    subs =
      Enum.reduce(catalog, %{}, fn
        {name, %{kind: :request}}, acc -> Map.put(acc, name, [])
        {name, _entry}, acc -> Map.put(acc, name, [])
      end)

    request_subs =
      Enum.reduce(catalog, %{}, fn
        {name, %{kind: :request}}, acc -> Map.put(acc, name, %{})
        _, acc -> acc
      end)

    %__MODULE__{
      catalog: catalog,
      subs: subs,
      request_subs: request_subs,
      host_pid: self()
    }
  end

  @doc """
  Subscribe `plugin` to every extension point listed in its
  `subscriptions` field. Returns `{:ok, table}` or `{:error, reason}`.
  """
  @spec subscribe(t(), binary(), Plugin.t()) :: {:ok, t()} | {:error, term()}
  def subscribe(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin)
      when is_binary(plugin_id) do
    with :ok <- Subscription.validate_plugin_unique(table, plugin_id),
         {:ok, plugin} <- Subscription.canonicalise(table.catalog, plugin),
         :ok <- Subscription.validate_subscriptions(table, plugin) do
      try do
        {:ok, do_subscribe(table, plugin_id, plugin, plugin.subscriptions)}
      catch
        :throw, {:init_provider_failed, reason} -> {:error, reason}
      end
    end
  end

  @doc """
  Bulk-subscribe a list of `{plugin_id, %Flezha.Plugin{}}` tuples. The list
  is applied in order; on first failure the table is returned unchanged and
  the error is reported, naming the plugin that refused and the rows the halt
  left unattempted.
  """
  @spec bulk_subscribe(t(), [{binary(), Plugin.t()}]) ::
          {:ok, t()} | {:error, {binary(), term()}}
  def bulk_subscribe(%__MODULE__{} = table, plugins) when is_list(plugins) do
    plugins
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, table}, fn {{plugin_id, plugin}, index}, {:ok, acc} ->
      case subscribe(acc, plugin_id, plugin) do
        {:ok, new_table} ->
          {:cont, {:ok, new_table}}

        {:error, reason} ->
          log_halt(plugin_id, plugin, reason, Enum.drop(plugins, index + 1))
          {:halt, {:error, {plugin_id, reason}}}
      end
    end)
  end

  # The halt is the only point holding all three facts an operator needs: which
  # plugin refused, what its provider was, and which rows the halt therefore
  # left unattempted. `invoke_activate/4` already reports phase 2 this way;
  # phase 1 reported nothing at all, so a parked operative named no plugin.
  defp log_halt(plugin_id, %Plugin{provider: provider}, reason, unattempted) do
    Logger.error(
      "[Extension.Table] prepare failed for plugin_id=#{plugin_id} " <>
        "provider=#{inspect(provider)}: #{inspect(reason)}; not attempted: " <>
        inspect(Enum.map(unattempted, &elem(&1, 0)))
    )
  end

  @doc """
  Transition every `:prepared` plugin in the table to `:running` by invoking
  its provider's optional `activate/1` callback. A provider without an
  `activate/1` export transitions silently (its meaningful work is triggered
  by `:on_input` / `:on_output` and is naturally quiescent until input
  arrives). Already-running plugins are skipped.

  Invoked by operative hosts after `start_operative_sdk/2` returns `{:ok, _}` — see
  `operative-extensions` "Two-phase plugin lifecycle gated on SDK readiness".

  An `activate/1` callback that fails or raises is logged at `:error` level and
  the plugin remains in `:prepared`. It is also RETURNED: the second element is
  `[{plugin_id, reason}]` for every plugin that did not reach `:running`, so a
  caller can answer its own caller honestly instead of recording a success it
  did not achieve. Deriving that from the phase map afterwards would lose the
  reason, which is the only thing naming what went wrong.
  """
  @spec bulk_activate(t(), map()) :: {t(), [{binary(), term()}]}
  def bulk_activate(%__MODULE__{} = table, activation_env \\ %{}) do
    # Remember the env so plugin-heal re-activations (attempt_restart)
    # re-apply it without the host re-supplying it.
    table = %{table | activation_env: Map.merge(table.activation_env, activation_env)}

    {table, failed} =
      Enum.reduce(table.plugins, {table, []}, &activate_if_prepared/2)

    {table, Enum.reverse(failed)}
  end

  # A down subscriber has no provider to activate: its `init_provider/1` must
  # be re-run first (`attempt_restart/3`), which is the loop's restart.
  defp activate_if_prepared({plugin_id, plugin}, {acc, failed}) do
    case Map.get(acc.phases, plugin_id, :prepared) do
      :running ->
        {acc, failed}

      {:down, _reason} ->
        {acc, failed}

      _ ->
        case activate_plugin(acc, plugin_id, plugin) do
          {activated, :ok} -> {activated, failed}
          {activated, {:error, reason}} -> {activated, [{plugin_id, reason} | failed]}
        end
    end
  end

  defp activate_plugin(table, plugin_id, %Plugin{provider: provider, opts: opts}) do
    cond do
      not Code.ensure_loaded?(provider) ->
        Logger.warning(
          "[Extension.Table] activate skipped — provider not loaded plugin_id=#{plugin_id}"
        )

        {put_phase(table, plugin_id, :running), :ok}

      not function_exported?(provider, :activate, 1) ->
        {put_phase(table, plugin_id, :running), :ok}

      true ->
        # Operative-level facts resolved at bind time (currently
        # `:effectively_once`), merged last so stored opts cannot spoof them.
        invoke_activate(
          table,
          plugin_id,
          provider,
          Map.merge(opts || %{}, table.activation_env)
        )
    end
  end

  defp invoke_activate(table, plugin_id, provider, opts) do
    case invoke(:activate, plugin_id, provider, fn -> provider.activate(opts) end) do
      {:ok, pid} when is_pid(pid) ->
        {table
         |> track_owned_pid(plugin_id, pid)
         |> put_phase(plugin_id, :running), :ok}

      ok when ok == :ok or (is_tuple(ok) and elem(ok, 0) == :ok) ->
        {put_phase(table, plugin_id, :running), :ok}

      {:error, reason} ->
        Logger.error(
          "[Extension.Table] activate failed for plugin_id=#{plugin_id}: #{inspect(reason)}"
        )

        {table, {:error, reason}}

      {:crashed, kind, payload} ->
        {table, {:error, {kind, payload}}}

      other ->
        Logger.error(
          "[Extension.Table] activate invalid return for plugin_id=#{plugin_id}: " <>
            inspect(other)
        )

        {table, {:error, {:invalid_activate_return, other}}}
    end
  end

  # Every provider callback this table invokes goes through here: what a
  # provider raises, throws or exits with is a refusal of the operation, never
  # a crash of the host that asked for it. The stacktrace is logged so a
  # provider bug stays as legible as the crash it replaces.
  defp invoke(callback, plugin_id, provider, fun) do
    fun.()
  catch
    kind, payload ->
      Logger.error(
        "[Extension.Table] #{callback} crashed for plugin_id=#{plugin_id} " <>
          "provider=#{inspect(provider)}: " <>
          Exception.format(kind, payload, __STACKTRACE__)
      )

      {:crashed, kind, payload}
  end

  defp put_phase(table, plugin_id, phase),
    do: %{table | phases: Map.put(table.phases, plugin_id, phase)}

  @doc """
  Return the lifecycle phase of `plugin_id`: `:prepared`, `:running`,
  `{:down, reason}` once its subscriber crashed, or `:unknown` if the plugin is
  not registered on this table.
  """
  @spec phase(t(), binary()) :: :prepared | :running | {:down, term()} | :unknown
  def phase(%__MODULE__{phases: phases}, plugin_id) when is_binary(plugin_id) do
    Map.get(phases, plugin_id, :unknown)
  end

  @doc "The subscribers whose owned pid crashed, with the exit reason each recorded."
  @spec down_ids(t()) :: %{binary() => term()}
  def down_ids(%__MODULE__{phases: phases}) do
    for {id, {:down, reason}} <- phases, into: %{}, do: {id, reason}
  end

  @doc """
  How long each running subscriber's owned pid has been up, in milliseconds —
  the evidence the loop reads to call a restarted plugin repaired.
  """
  @spec uptimes(t()) :: %{binary() => non_neg_integer()}
  def uptimes(%__MODULE__{phases: phases, started_at: started_at}) do
    now = :erlang.monotonic_time(:millisecond)

    for {id, at} <- started_at, Map.get(phases, id) == :running, into: %{}, do: {id, now - at}
  end

  @doc """
  The ids of every plugin subscribed on this table, whatever its phase. This is
  the set an "is this plugin declared here at all" question reads — a plugin the
  declaration does not name is extra even while it is still `:prepared`.
  """
  @spec subscribed_ids(t()) :: [binary()]
  def subscribed_ids(%__MODULE__{plugins: plugins}), do: Map.keys(plugins)

  @doc """
  The ids of the plugins that reached `:running` — the ones actually holding the
  upstream resources their provider's `activate/1` acquires.

  This, not the subscribed set, answers "which plugins are running". A plugin
  whose `activate/1` failed stays subscribed and `:prepared`; counting it as
  running is what let a source that never connected read as healthy forever.
  """
  @spec running_ids(t()) :: [binary()]
  def running_ids(%__MODULE__{plugins: plugins, phases: phases}) do
    for {id, _plugin} <- plugins, Map.get(phases, id, :prepared) == :running, do: id
  end

  @doc """
  Health verdict for every plugin on the table (plugin health contract). See
  `Flezha.Plugin.Extension.Table.Health.probe/1` for the rules.
  """
  @spec health(t()) :: %{binary() => ProviderBehaviour.health_verdict()}
  def health(%__MODULE__{} = table), do: Health.probe(table)

  @doc """
  Remove every subscription owned by `plugin_id`, cancel its monitors,
  timers, and owned pids, and rebuild affected dispatch chains. Answers the
  table and the ids it dropped — `[]` for a plugin it never held — which the
  caller unregisters.
  """
  @spec unsubscribe(t(), binary()) :: {t(), [binary()]}
  def unsubscribe(%__MODULE__{} = table, plugin_id) do
    case Map.get(table.plugins, plugin_id) do
      nil -> {table, []}
      plugin -> {do_unsubscribe(table, plugin_id, plugin), [plugin_id]}
    end
  end

  @doc """
  Fire the timer for `{plugin_id, name}`. Invokes the subscriber's
  `on_interval/2` callback and reschedules the next tick.
  """
  @spec fire_timer(t(), atom(), binary()) :: t()
  def fire_timer(%__MODULE__{} = table, name, plugin_id) do
    case Subscription.find_entry(table, name, plugin_id) do
      nil ->
        table

      entry ->
        ctx = %Ctx.Tick{
          operative_id: Map.get(table.plugins, plugin_id).operative_id,
          namespace: Map.get(table.plugins, plugin_id).namespace,
          runner_id: nil,
          plugin_id: plugin_id
        }

        try do
          entry.provider.on_interval(ctx, entry.provider_opts)
        catch
          kind, reason ->
            Logger.error(
              "[Extension.Table] on_interval raised for plugin #{plugin_id}: " <>
                "#{inspect(kind)}/#{inspect(reason)}"
            )
        end

        Subscription.reschedule_timer(table, name, plugin_id, entry)
    end
  end

  @doc """
  Handle a `{:DOWN, ref, :process, pid, reason}` from the host. A subscriber
  that exited `:normal` is unsubscribed; one that crashed is recorded as
  `{:down, reason}` for the host to declare — the table restarts nothing.
  Returns `{:ok, table}` or `:ignore`.
  """
  @spec handle_down(t(), reference(), pid(), term(), keyword()) ::
          {:ok, t()} | :ignore
  def handle_down(%__MODULE__{} = table, ref, _pid, reason, _opts \\ []) do
    case Map.get(table.monitors, ref) do
      nil ->
        :ignore

      plugin_id ->
        plugin = Map.fetch!(table.plugins, plugin_id)
        table = %{table | monitors: Map.delete(table.monitors, ref)}
        table = %{table | owned_pids: Map.delete(table.owned_pids, plugin_id)}

        case reason do
          :normal ->
            Logger.info("[Extension.Table] subscriber #{plugin_id} terminated normally")
            {:ok, do_unsubscribe(table, plugin_id, plugin)}

          _ ->
            Logger.error("[Extension.Table] subscriber #{plugin_id} crashed: #{inspect(reason)}")
            {:ok, put_phase(table, plugin_id, {:down, reason})}
        end
    end
  end

  @doc """
  Re-run a plugin's provider: release whatever owned pid it still holds, run
  `init_provider/1` afresh, and re-register its subscriptions against the fresh
  instance. The plugin re-enters `:prepared` when its provider can activate
  (the caller activates it), `:running` otherwise. On a failed
  `init_provider/1` the plugin is recorded `{:down, reason}`, so it keeps
  diffing as one the loop must restart.

  It re-runs from the entry the TABLE holds, and only falls back to `plugin` —
  the loop's declared manifest — for one the table does not hold: a provider
  that commits durable state of its own (the websocket provider's applied
  subscription set) writes it back through `put_plugin_opt/4`, and re-running
  from the manifest would silently discard it.

  The one restart path: the convergence loop's `restart_plugin` reaches it
  through `Flezha.Operative.Plugins.restart/3`.
  """
  @spec attempt_restart(t(), binary(), Plugin.t()) :: {:ok, t()} | {:error, term(), t()}
  def attempt_restart(%__MODULE__{} = table, plugin_id, %Plugin{} = declared) do
    Logger.info("[Extension.Table] attempt_restart begin plugin_id=#{plugin_id}")
    %Plugin{} = held = Map.get(table.plugins, plugin_id, declared)
    plugin = %Plugin{held | id: plugin_id}
    table = release_owned_pid(table, plugin_id, plugin)

    case run_init_provider(plugin) do
      {:ok, fresh_plugin, owned_pid} ->
        table =
          %{table | plugins: Map.put(table.plugins, plugin_id, fresh_plugin)}
          |> put_phase(plugin_id, Health.restarted_phase(plugin))
          |> track_owned_pid(plugin_id, owned_pid)

        {:ok, resubscribe(table, plugin_id, fresh_plugin)}

      {:error, reason} ->
        Logger.error(
          "[Extension.Table] init_provider failed for plugin_id=#{plugin_id} reason=#{inspect(reason)}"
        )

        {:error, reason, put_phase(table, plugin_id, {:down, reason})}
    end
  end

  @doc """
  Tear down every subscription: cancel timers, demonitor pids, signal owned
  pids to stop. Answers the emptied table and the ids it dropped, which the
  caller unregisters.
  """
  @spec teardown_all(t()) :: {t(), [binary()]}
  def teardown_all(%__MODULE__{} = table) do
    Enum.each(table.plugins, fn {plugin_id, plugin} ->
      terminate_provider(table, plugin_id, plugin)
    end)

    Enum.each(table.owned_pids, fn {_plugin_id, {pid, ref}} ->
      Health.safe_demonitor(ref)
      Health.safe_kill(pid)
    end)

    Enum.each(table.timers, fn {timer_ref, _} -> Health.safe_cancel_timer(timer_ref) end)

    {new(table.catalog), Enum.map(table.plugins, fn {plugin_id, _plugin} -> plugin_id end)}
  end

  @doc """
  Re-establish subscriptions from a serialisable shape (the list of
  `%Flezha.Plugin{}` entries carried through handover). Live refs are
  rebuilt on this node.
  """
  @spec rehydrate(t(), [{binary(), Plugin.t()}]) :: t()
  def rehydrate(%__MODULE__{} = table, plugins) when is_list(plugins) do
    Enum.reduce(plugins, table, fn {plugin_id, plugin}, acc ->
      case subscribe(acc, plugin_id, plugin) do
        {:ok, new_table} ->
          new_table

        {:error, reason} ->
          Logger.warning("[Extension.Table] failed to rehydrate #{plugin_id}: #{inspect(reason)}")

          acc
      end
    end)
  end

  @doc """
  Return the plugins as a serialisable list of `{plugin_id, %Flezha.Plugin{}}`
  tuples (with transient fields stripped). Used for handover / recovery.
  """
  @spec serializable_plugins(t()) :: [{binary(), Plugin.t()}]
  def serializable_plugins(%__MODULE__{plugins: plugins}) do
    Enum.map(plugins, fn {plugin_id, plugin} ->
      {plugin_id, Plugin.strip_transient(plugin)}
    end)
  end

  @doc "Every subscriber entry registered for the given extension name."
  @spec subscribers(t(), atom()) :: [entry()]
  defdelegate subscribers(table, name), to: Subscription

  @doc """
  Canonicalise a plugin's subscriptions against `catalog` and the declared
  sub-option types. See `Flezha.Plugin.Extension.Table.Subscription`.
  """
  @spec canonicalise(map(), Plugin.t()) :: {:ok, Plugin.t()} | {:error, term()}
  defdelegate canonicalise(catalog, plugin), to: Subscription

  @doc """
  Record a value the provider itself committed back onto its stored opts.

  `attempt_restart/3` re-runs `init_provider/1` from exactly this map, so a
  provider that holds durable state of its own (the websocket provider's
  applied subscription set) restores it across a plugin restart by writing it
  here. The subscription entries are updated too: `build_entry/3` copies the
  opts, and leaving those stale would mean state that survives a restart but
  not a dispatch. Unknown `plugin_id` is a no-op — a provider whose plugin was
  disabled mid-flight has nothing to write to.
  """
  @spec put_plugin_opt(t(), binary(), atom(), term()) :: t()
  def put_plugin_opt(%__MODULE__{} = table, plugin_id, key, value) do
    case Map.get(table.plugins, plugin_id) do
      nil ->
        table

      %Plugin{} = plugin ->
        opts = Map.put(plugin.opts || %{}, key, value)

        subs =
          Map.new(table.subs, fn {name, entries} ->
            {name,
             Enum.map(entries, fn
               %{plugin_id: ^plugin_id} = entry -> %{entry | provider_opts: opts}
               entry -> entry
             end)}
          end)

        %{
          table
          | plugins: Map.put(table.plugins, plugin_id, %Plugin{plugin | opts: opts}),
            subs: subs
        }
    end
  end

  ## Private — subscribe

  defp do_subscribe(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin, subs) do
    plugin = %Plugin{plugin | id: plugin_id}

    case run_init_provider(plugin) do
      {:ok, plugin, owned_pid} ->
        table = %{table | plugins: Map.put(table.plugins, plugin_id, plugin)}
        table = %{table | phases: Map.put(table.phases, plugin_id, :prepared)}
        table = track_owned_pid(table, plugin_id, owned_pid)

        table =
          Enum.reduce(subs, table, fn {name, sub_opts}, acc ->
            %{kind: kind} = Map.fetch!(acc.catalog, name)

            acc
            |> Subscription.add_subscription(kind, name, plugin_id, plugin, sub_opts)
            |> Chain.refresh(name)
          end)

        table

      {:error, reason} ->
        throw({:init_provider_failed, reason})
    end
  end

  defp run_init_provider(%Plugin{provider: provider, opts: opts} = plugin) do
    cond do
      not Code.ensure_loaded?(provider) ->
        {:error, {:module_not_loaded, provider}}

      function_exported?(provider, :init_provider, 1) ->
        settle_init(
          plugin,
          invoke(:init_provider, plugin.id, provider, fn ->
            provider.init_provider(with_host_identity(opts || %{}, plugin))
          end)
        )

      true ->
        {:ok, plugin, nil}
    end
  end

  # What the callback answered, in the shape both call sites read.
  defp settle_init(%Plugin{} = plugin, {:ok, new_opts}),
    do: settle_init(plugin, {:ok, new_opts, nil})

  defp settle_init(%Plugin{} = plugin, {:ok, new_opts, pid}) when is_pid(pid) or is_nil(pid),
    do: {:ok, %Plugin{plugin | opts: new_opts}, pid}

  defp settle_init(_plugin, {:error, _} = err), do: err

  defp settle_init(%Plugin{provider: provider}, {:crashed, kind, payload}),
    do: {:error, {:init_provider_crashed, provider, kind, payload}}

  defp settle_init(_plugin, other), do: {:error, {:invalid_init_provider_return, other}}

  # The host operative's own identity, merged last so stored opts cannot spoof
  # it — the same rule `bulk_activate/2` applies to `activation_env`. A provider
  # whose destination is another operative needs it to refuse a self-target or a
  # cross-namespace target at enable time; a provider that persists its own
  # committed state needs `:source_plugin_id` to name the cspec row to write.
  defp with_host_identity(opts, %Plugin{id: id, namespace: ns, operative_id: operative_id}) do
    Map.merge(opts, %{
      source_namespace: ns,
      source_operative_id: operative_id,
      source_plugin_id: id
    })
  end

  defp track_owned_pid(table, _plugin_id, nil), do: table

  defp track_owned_pid(%__MODULE__{} = table, plugin_id, pid) when is_pid(pid) do
    ref = Process.monitor(pid)

    %{
      table
      | owned_pids: Map.put(table.owned_pids, plugin_id, {pid, ref}),
        monitors: Map.put(table.monitors, ref, plugin_id),
        started_at: Map.put(table.started_at, plugin_id, :erlang.monotonic_time(:millisecond))
    }
  end

  ## Private — unsubscribe

  defp do_unsubscribe(%__MODULE__{} = table, plugin_id, %Plugin{subscriptions: subs} = plugin) do
    table = release_owned_pid(table, plugin_id, plugin)

    table =
      Enum.reduce(subs, table, fn {name, _sub_opts}, acc ->
        acc |> Subscription.remove_subscription(name, plugin_id) |> Chain.refresh(name)
      end)

    %{
      table
      | plugins: Map.delete(table.plugins, plugin_id),
        phases: Map.delete(table.phases, plugin_id),
        started_at: Map.delete(table.started_at, plugin_id)
    }
  end

  # Let the provider release what it holds, then drop the owned pid and its
  # monitor if still present — the half of an unsubscribe a restart shares.
  defp release_owned_pid(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin) do
    terminate_provider(table, plugin_id, plugin)

    case Map.get(table.owned_pids, plugin_id) do
      nil ->
        table

      {pid, ref} ->
        Health.safe_demonitor(ref)
        Health.safe_kill(pid)

        %{
          table
          | owned_pids: Map.delete(table.owned_pids, plugin_id),
            monitors: Map.delete(table.monitors, ref)
        }
    end
  end

  # Let a provider release what the host cannot see. `:owned_pid` is the one
  # process the table tracks; a provider that fans out (one worker per target
  # partition) owns more, and this is where they are stopped.
  defp terminate_provider(table, plugin_id, %Plugin{provider: provider, opts: opts}) do
    if function_exported?(provider, :terminate_provider, 1) do
      try do
        provider.terminate_provider(with_owned_pid(table, plugin_id, opts))
      catch
        kind, reason ->
          Logger.warning(
            "[Extension.Table] terminate_provider raised for plugin_id=#{plugin_id}: " <>
              "#{inspect(kind)} #{inspect(reason)}"
          )
      end
    end

    :ok
  end

  defp with_owned_pid(table, plugin_id, opts) do
    case Map.get(table.owned_pids, plugin_id) do
      {pid, _ref} -> Map.put(opts, :owned_pid, pid)
      nil -> opts
    end
  end

  ## Private — active-mask + chains

  ## Private — request dispatch

  # (handled in Flezha.Plugin.Extension.run/3 via table.request_subs directly)

  ## Private — timer

  ## Private — lookups

  ## Private — restart

  # Re-register the plugin's subscriptions against its fresh instance: every
  # entry carries a copy of the provider opts, so a restart that kept the old
  # entries would dispatch to state the provider does not hold.
  defp resubscribe(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin) do
    subs = plugin.subscriptions

    table =
      Enum.reduce(subs, table, fn {name, _sub_opts}, acc ->
        acc |> Subscription.remove_subscription(name, plugin_id) |> Chain.refresh(name)
      end)

    Enum.reduce(subs, table, fn {name, sub_opts}, acc ->
      %{kind: kind} = Map.fetch!(acc.catalog, name)

      acc
      |> Subscription.add_subscription(kind, name, plugin_id, plugin, sub_opts)
      |> Chain.refresh(name)
    end)
  end
end

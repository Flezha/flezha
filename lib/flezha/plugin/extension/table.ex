defmodule Flezha.Plugin.Extension.Table do
  @moduledoc """
  Per-host registry of extension subscriptions, compiled dispatch chains,
  and subscriber lifecycle state (monitors, timer refs, restart counters).

  Shape:

      %Table{
        catalog:        %{name => %{kind, ctx_module, bit_index}},
        active_mask:    integer(),
        subs:           %{name => [entry]},     # for :fold/:transform/:fanout/:timer
        request_subs:   %{name => %{key => {plugin_id, entry}}},
        chains:         %{name => (ctx -> result)},
        plugins:        %{plugin_id => %Flezha.Plugin{}},
        phases:         %{plugin_id => :prepared | :running},
        monitors:       %{ref => plugin_id},
        owned_pids:     %{plugin_id => {pid, ref}},
        timers:         %{timer_ref => {plugin_id, name}},
        retries:        %{plugin_id => non_neg_integer()},
        started_at:     %{plugin_id => monotonic_ms},
        host_pid:       pid()
      }

  Plugins enter the table in the `:prepared` phase: their
  `init_provider/1` has run, the GenServer is registered, but the provider
  has not been instructed to acquire upstream resources. The host transitions
  every plugin to `:running` via `bulk_activate/1` once the operative's SDK side
  has confirmed bound — the provider's optional `activate/1` callback fires
  exactly once per binding at that point. See the `operative-extensions`
  capability "Two-phase plugin lifecycle gated on SDK readiness".
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx

  require Logger

  # Restart backoff: only reset the retry counter once an owned pid has
  # stayed up at least this long, so a provider that fails fast (e.g. a WS
  # whose upstream keeps 429-ing) accumulates backoff instead of hammering.
  @stable_uptime_ms 60_000
  @max_retry_delay_ms 60_000

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
            retries: %{},
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
  def subscribe(%__MODULE__{} = table, plugin_id, %Plugin{subscriptions: subs} = plugin)
      when is_binary(plugin_id) do
    with :ok <- validate_plugin_unique(table, plugin_id),
         :ok <- validate_subscriptions(table, plugin) do
      try do
        {:ok, do_subscribe(table, plugin_id, plugin, subs)}
      catch
        :throw, {:init_provider_failed, reason} -> {:error, reason}
      end
    end
  end

  @doc """
  Bulk-subscribe a list of `{plugin_id, %Flezha.Plugin{}}` tuples. The list
  is applied in order; on first failure the table is returned unchanged and
  the error is reported.
  """
  @spec bulk_subscribe(t(), [{binary(), Plugin.t()}]) ::
          {:ok, t()} | {:error, {binary(), term()}}
  def bulk_subscribe(%__MODULE__{} = table, plugins) when is_list(plugins) do
    Enum.reduce_while(plugins, {:ok, table}, fn {plugin_id, plugin}, {:ok, acc} ->
      case subscribe(acc, plugin_id, plugin) do
        {:ok, new_table} -> {:cont, {:ok, new_table}}
        {:error, reason} -> {:halt, {:error, {plugin_id, reason}}}
      end
    end)
  end

  @doc """
  Transition every `:prepared` plugin in the table to `:running` by invoking
  its provider's optional `activate/1` callback. A provider without an
  `activate/1` export transitions silently (its meaningful work is triggered
  by `:on_input` / `:on_output` and is naturally quiescent until input
  arrives). Already-running plugins are skipped.

  Invoked by operative hosts after `start_operative_sdk/2` returns `{:ok, _}` — see
  `operative-extensions` "Two-phase plugin lifecycle gated on SDK readiness".

  An `activate/1` callback that raises is logged at `:error` level; the
  plugin remains in `:prepared` and the host SHOULD treat the binding as
  unhealthy on its next teardown cycle.
  """
  @spec bulk_activate(t(), map()) :: t()
  def bulk_activate(%__MODULE__{} = table, activation_env \\ %{}) do
    # Remember the env so plugin-heal re-activations (attempt_restart)
    # re-apply it without the host re-supplying it.
    table = %{table | activation_env: Map.merge(table.activation_env, activation_env)}

    Enum.reduce(table.plugins, table, fn {plugin_id, plugin}, acc ->
      case Map.get(acc.phases, plugin_id, :prepared) do
        :running ->
          acc

        :prepared ->
          activate_plugin(acc, plugin_id, plugin)
      end
    end)
  end

  defp activate_plugin(table, plugin_id, %Plugin{provider: provider, opts: opts}) do
    cond do
      not Code.ensure_loaded?(provider) ->
        Logger.warning(
          "[Extension.Table] activate skipped — provider not loaded plugin_id=#{plugin_id}"
        )

        put_phase(table, plugin_id, :running)

      not function_exported?(provider, :activate, 1) ->
        put_phase(table, plugin_id, :running)

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
    try do
      case provider.activate(opts) do
        {:ok, pid} when is_pid(pid) ->
          table
          |> track_owned_pid(plugin_id, pid)
          |> put_phase(plugin_id, :running)

        ok when ok == :ok or (is_tuple(ok) and elem(ok, 0) == :ok) ->
          put_phase(table, plugin_id, :running)

        {:error, reason} ->
          Logger.error(
            "[Extension.Table] activate failed for plugin_id=#{plugin_id}: #{inspect(reason)}"
          )

          table

        other ->
          Logger.error(
            "[Extension.Table] activate invalid return for plugin_id=#{plugin_id}: " <>
              inspect(other)
          )

          table
      end
    catch
      kind, reason ->
        Logger.error(
          "[Extension.Table] activate raised for plugin_id=#{plugin_id}: " <>
            "#{inspect(kind)}/#{inspect(reason)}"
        )

        table
    end
  end

  defp put_phase(table, plugin_id, phase),
    do: %{table | phases: Map.put(table.phases, plugin_id, phase)}

  @doc """
  Return the lifecycle phase of `plugin_id`: `:prepared`, `:running`, or
  `:unknown` if the plugin is not registered on this table.
  """
  @spec phase(t(), binary()) :: :prepared | :running | :unknown
  def phase(%__MODULE__{phases: phases}, plugin_id) when is_binary(plugin_id) do
    Map.get(phases, plugin_id, :unknown)
  end

  @doc """
  Health verdict for every plugin on the table (plugin health contract). Only
  `:running` plugins are evaluated — a `:prepared` plugin has not acquired
  upstream resources yet, so it is reported `:healthy`. Providers that export
  `health/1` are queried (with `:owned_pid` injected); providers without it
  default to "owned pid alive (or absent) ⇒ healthy". A provider whose `health/1`
  raises or times out is reported `:healthy` so the reconciler never escalates on
  an uncertain probe. Returns `%{plugin_id => :healthy | {:unhealthy, reason}}`.
  """
  @spec health(t()) :: %{binary() => Flezha.Plugin.ProviderBehaviour.health_verdict()}
  def health(%__MODULE__{} = table) do
    Map.new(table.plugins, fn {plugin_id, plugin} ->
      {plugin_id, plugin_health(table, plugin_id, plugin)}
    end)
  end

  defp plugin_health(table, plugin_id, plugin) do
    case Map.get(table.phases, plugin_id, :prepared) do
      :running -> running_plugin_health(table, plugin_id, plugin)
      _ -> :healthy
    end
  end

  defp running_plugin_health(table, plugin_id, %Plugin{provider: provider, opts: opts}) do
    owned_pid =
      case Map.get(table.owned_pids, plugin_id) do
        {pid, _ref} -> pid
        _ -> nil
      end

    cond do
      not Code.ensure_loaded?(provider) -> {:unhealthy, :provider_not_loaded}
      function_exported?(provider, :health, 1) -> safe_health(provider, opts, owned_pid)
      is_nil(owned_pid) -> :healthy
      Process.alive?(owned_pid) -> :healthy
      true -> {:unhealthy, :owned_pid_dead}
    end
  end

  defp safe_health(provider, opts, owned_pid) do
    case provider.health(Map.put(opts || %{}, :owned_pid, owned_pid)) do
      :healthy -> :healthy
      {:unhealthy, _reason} = unhealthy -> unhealthy
      _ -> :healthy
    end
  rescue
    _ -> :healthy
  catch
    _, _ -> :healthy
  end

  @doc """
  Remove every subscription owned by `plugin_id`, cancel its monitors,
  timers, and owned pids, and rebuild affected dispatch chains.
  """
  @spec unsubscribe(t(), binary()) :: t()
  def unsubscribe(%__MODULE__{} = table, plugin_id) do
    case Map.get(table.plugins, plugin_id) do
      nil -> table
      plugin -> do_unsubscribe(table, plugin_id, plugin)
    end
  end

  @doc """
  Fire the timer for `{plugin_id, name}`. Invokes the subscriber's
  `on_interval/2` callback and reschedules the next tick.
  """
  @spec fire_timer(t(), atom(), binary()) :: t()
  def fire_timer(%__MODULE__{} = table, name, plugin_id) do
    case find_entry(table, name, plugin_id) do
      nil ->
        table

      entry ->
        ctx = %Ctx.Tick{
          operative_id: Map.get(table.plugins, plugin_id).operative_id,
          namespace: Map.get(table.plugins, plugin_id).namespace,
          executor_id: nil,
          plugin_id: plugin_id
        }

        try do
          apply(entry.provider, :on_interval, [ctx, entry.provider_opts])
        catch
          kind, reason ->
            Logger.error(
              "[Extension.Table] on_interval raised for plugin #{plugin_id}: " <>
                "#{inspect(kind)}/#{inspect(reason)}"
            )
        end

        reschedule_timer(table, name, plugin_id, entry)
    end
  end

  @doc """
  Handle a `{:DOWN, ref, :process, pid, reason}` from the host. If `ref` is
  owned by a subscriber, applies the kind-appropriate restart policy.
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

            {:ok,
             table |> reset_retries_if_stable(plugin_id) |> schedule_retry(plugin_id, plugin)}
        end
    end
  end

  @doc """
  Retry a previously-failing subscriber. Called when a `{:retry_plugin_restart, plugin_id}`
  message is delivered to the host.
  """
  @spec retry(t(), binary(), keyword()) :: t()
  def retry(%__MODULE__{} = table, plugin_id, _opts \\ []) do
    Logger.info("[Extension.Table] retry firing plugin_id=#{plugin_id}")

    case Map.get(table.plugins, plugin_id) do
      nil ->
        Logger.warning("[Extension.Table] retry requested for unknown plugin #{plugin_id}")
        table

      plugin ->
        attempt_restart(table, plugin_id, plugin)
    end
  end

  @doc """
  Tear down every subscription. Cancels timers, demonitors pids, signals
  owned pids to stop, and removes PluginManager registrations.
  """
  @spec teardown_all(t()) :: t()
  def teardown_all(%__MODULE__{} = table) do
    Enum.each(table.owned_pids, fn {_plugin_id, {pid, ref}} ->
      safe_demonitor(ref)
      safe_kill(pid)
    end)

    Enum.each(table.timers, fn {timer_ref, _} -> safe_cancel_timer(timer_ref) end)

    Enum.each(table.plugins, fn {plugin_id, _plugin} ->
      try do
        Flezha.Service.PluginManager.unregister_plugin(plugin_id)
      catch
        _, _ -> :ok
      end
    end)

    new(table.catalog)
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

  @doc "Return every subscriber entry registered for the given extension name."
  @spec subscribers(t(), atom()) :: [entry()]
  def subscribers(%__MODULE__{} = table, name) do
    case Map.get(table.catalog, name) do
      %{kind: :request} ->
        table.request_subs
        |> Map.get(name, %{})
        |> Map.values()
        |> Enum.map(fn {_plugin_id, entry} -> entry end)

      _ ->
        Map.get(table.subs, name, [])
    end
  end

  ## Private — validation

  defp validate_plugin_unique(%__MODULE__{plugins: plugins}, plugin_id) do
    if Map.has_key?(plugins, plugin_id) do
      {:error, {:plugin_already_subscribed, plugin_id}}
    else
      :ok
    end
  end

  defp validate_subscriptions(%__MODULE__{catalog: catalog} = table, %Plugin{} = plugin) do
    Enum.reduce_while(plugin.subscriptions, :ok, fn {name, sub_opts}, :ok ->
      with {:ok, entry} <- fetch_catalog_entry(catalog, name),
           :ok <- validate_callback(plugin.provider, name, entry.kind),
           :ok <- validate_sub_opts(entry.kind, sub_opts),
           :ok <- validate_kind_specific(entry.kind, name, sub_opts, table, plugin) do
        {:cont, :ok}
      else
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp fetch_catalog_entry(catalog, name) do
    case Map.fetch(catalog, name) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, {:unknown_extension, name}}
    end
  end

  defp validate_callback(provider, name, kind) do
    callback = callback_name(name, kind)

    cond do
      not Code.ensure_loaded?(provider) ->
        {:error, {:module_not_loaded, provider}}

      not function_exported?(provider, callback, 2) ->
        {:error, {:missing_callback, callback}}

      true ->
        :ok
    end
  end

  defp callback_name(name, _kind), do: name

  defp validate_sub_opts(_kind, sub_opts) when is_list(sub_opts), do: :ok

  defp validate_sub_opts(_kind, _other),
    do: {:error, {:invalid_subscription_opts, :not_a_keyword}}

  # `position` only makes sense for ordered kinds (:fold, :transform).
  defp validate_kind_specific(kind, _name, sub_opts, _table, _plugin)
       when kind in [:fanout, :timer] do
    if Keyword.has_key?(sub_opts, :position) do
      {:error, :position_not_applicable}
    else
      case kind do
        :timer -> validate_interval(sub_opts)
        :fanout -> :ok
      end
    end
  end

  defp validate_kind_specific(:request, name, sub_opts, table, _plugin) do
    cond do
      Keyword.has_key?(sub_opts, :position) ->
        {:error, :position_not_applicable}

      not Keyword.has_key?(sub_opts, :key) ->
        {:error, {:invalid_subscription_opts, :missing_key}}

      true ->
        key = Keyword.fetch!(sub_opts, :key)

        case get_in(table.request_subs, [name, key]) do
          nil -> :ok
          _ -> {:error, {:key_conflict, name, key}}
        end
    end
  end

  defp validate_kind_specific(:fold, _name, sub_opts, _table, _plugin) do
    with :ok <- validate_timeout(sub_opts),
         :ok <- validate_on_error(sub_opts) do
      :ok
    end
  end

  defp validate_kind_specific(:transform, _name, _sub_opts, _table, _plugin), do: :ok

  defp validate_interval(sub_opts) do
    case Keyword.get(sub_opts, :interval_ms) do
      n when is_integer(n) and n > 0 -> :ok
      other -> {:error, {:invalid_subscription_opts, {:interval_ms, other}}}
    end
  end

  defp validate_timeout(sub_opts) do
    case Keyword.get(sub_opts, :timeout_ms, 5_000) do
      n when is_integer(n) and n > 0 -> :ok
      other -> {:error, {:invalid_subscription_opts, {:timeout_ms, other}}}
    end
  end

  defp validate_on_error(sub_opts) do
    case Keyword.get(sub_opts, :on_error, :fail) do
      :fail -> :ok
      :empty -> :ok
      other -> {:error, {:invalid_subscription_opts, {:on_error, other}}}
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
            add_subscription(acc, kind, name, plugin_id, plugin, sub_opts)
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
        case provider.init_provider(opts || %{}) do
          {:ok, new_opts} -> {:ok, %Plugin{plugin | opts: new_opts}, nil}
          {:ok, new_opts, pid} when is_pid(pid) -> {:ok, %Plugin{plugin | opts: new_opts}, pid}
          {:ok, new_opts, nil} -> {:ok, %Plugin{plugin | opts: new_opts}, nil}
          {:error, _} = err -> err
          other -> {:error, {:invalid_init_provider_return, other}}
        end

      true ->
        {:ok, plugin, nil}
    end
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

  defp add_subscription(table, kind, name, plugin_id, plugin, sub_opts)
       when kind in [:fold, :transform] do
    entry = build_entry(plugin_id, plugin, sub_opts)

    current = Map.get(table.subs, name, [])
    position = Keyword.get(sub_opts, :position)
    new_list = insert_by_position(current, entry, position)

    table
    |> Map.update!(:subs, &Map.put(&1, name, new_list))
    |> rebuild_chain(name)
    |> mark_active(name, true)
  end

  defp add_subscription(table, :fanout, name, plugin_id, plugin, sub_opts) do
    entry = build_entry(plugin_id, plugin, sub_opts)
    current = Map.get(table.subs, name, [])
    new_list = current ++ [entry]

    table
    |> Map.update!(:subs, &Map.put(&1, name, new_list))
    |> rebuild_chain(name)
    |> mark_active(name, true)
  end

  defp add_subscription(table, :request, name, plugin_id, plugin, sub_opts) do
    key = Keyword.fetch!(sub_opts, :key)
    current = Map.get(table.request_subs, name, %{})
    entry = build_entry(plugin_id, plugin, sub_opts)
    new_map = Map.put(current, key, {plugin_id, entry})

    table
    |> Map.update!(:request_subs, &Map.put(&1, name, new_map))
    |> mark_active(name, true)
  end

  defp add_subscription(table, :timer, name, plugin_id, plugin, sub_opts) do
    entry = build_entry(plugin_id, plugin, sub_opts)

    interval_ms = Keyword.fetch!(sub_opts, :interval_ms)
    timer_ref = Process.send_after(self(), {:extension_timer, name, plugin_id}, interval_ms)

    entry = %{entry | timer_ref: timer_ref}
    current = Map.get(table.subs, name, [])
    new_list = current ++ [entry]

    table
    |> Map.update!(:subs, &Map.put(&1, name, new_list))
    |> Map.update!(:timers, &Map.put(&1, timer_ref, {plugin_id, name}))
    |> mark_active(name, true)
  end

  defp build_entry(plugin_id, %Plugin{provider: provider, opts: opts}, sub_opts) do
    %{
      plugin_id: plugin_id,
      provider: provider,
      provider_opts: opts || %{},
      sub_opts: sub_opts,
      position: Keyword.get(sub_opts, :position),
      owned_pid: nil,
      monitor_ref: nil,
      timer_ref: nil
    }
  end

  defp insert_by_position(list, entry, nil) do
    position = length(list)
    list ++ [%{entry | position: position}]
  end

  defp insert_by_position(list, entry, position)
       when is_integer(position) and position >= 0 do
    {before_split, after_split} = Enum.split(list, position)
    entry = %{entry | position: position}

    shifted =
      after_split
      |> Enum.with_index(position + 1)
      |> Enum.map(fn {e, new_pos} -> %{e | position: new_pos} end)

    before_split ++ [entry] ++ shifted
  end

  ## Private — unsubscribe

  defp do_unsubscribe(%__MODULE__{} = table, plugin_id, %Plugin{subscriptions: subs}) do
    table =
      Enum.reduce(subs, table, fn {name, _sub_opts}, acc ->
        remove_subscription(acc, name, plugin_id)
      end)

    # Release monitor + pid if still present
    table =
      case Map.get(table.owned_pids, plugin_id) do
        nil ->
          table

        {pid, ref} ->
          safe_demonitor(ref)
          safe_kill(pid)

          %{
            table
            | owned_pids: Map.delete(table.owned_pids, plugin_id),
              monitors: Map.delete(table.monitors, ref)
          }
      end

    try do
      Flezha.Service.PluginManager.unregister_plugin(plugin_id)
    catch
      _, _ -> :ok
    end

    %{
      table
      | plugins: Map.delete(table.plugins, plugin_id),
        phases: Map.delete(table.phases, plugin_id),
        retries: Map.delete(table.retries, plugin_id),
        started_at: Map.delete(table.started_at, plugin_id)
    }
  end

  defp remove_subscription(table, name, plugin_id) do
    case Map.fetch!(table.catalog, name) do
      %{kind: :request} ->
        request_map =
          table.request_subs
          |> Map.get(name, %{})
          |> Enum.reject(fn {_key, {pid_id, _entry}} -> pid_id == plugin_id end)
          |> Map.new()

        table = %{table | request_subs: Map.put(table.request_subs, name, request_map)}

        if map_size(request_map) == 0, do: mark_active(table, name, false), else: table

      %{kind: :timer} ->
        {removed, remaining} =
          Enum.split_with(Map.get(table.subs, name, []), &(&1.plugin_id == plugin_id))

        Enum.each(removed, fn e ->
          safe_cancel_timer(e.timer_ref)
        end)

        timers_without =
          Enum.reduce(removed, table.timers, fn e, acc -> Map.delete(acc, e.timer_ref) end)

        table = %{
          table
          | subs: Map.put(table.subs, name, remaining),
            timers: timers_without
        }

        if remaining == [], do: mark_active(table, name, false), else: table

      %{kind: kind} when kind in [:fold, :transform] ->
        current = Map.get(table.subs, name, [])
        remaining = Enum.reject(current, &(&1.plugin_id == plugin_id))
        remaining = recalc_positions(remaining)

        table = %{table | subs: Map.put(table.subs, name, remaining)}
        table = rebuild_chain(table, name)

        if remaining == [], do: mark_active(table, name, false), else: table

      %{kind: :fanout} ->
        current = Map.get(table.subs, name, [])
        remaining = Enum.reject(current, &(&1.plugin_id == plugin_id))

        table = %{table | subs: Map.put(table.subs, name, remaining)}
        table = rebuild_chain(table, name)

        if remaining == [], do: mark_active(table, name, false), else: table
    end
  end

  defp recalc_positions(list) do
    list
    |> Enum.with_index()
    |> Enum.map(fn {entry, idx} -> %{entry | position: idx} end)
  end

  ## Private — active-mask + chains

  defp mark_active(%__MODULE__{} = table, name, active?) do
    case Map.fetch(table.catalog, name) do
      {:ok, %{bit_index: bit}} ->
        mask_bit = Bitwise.bsl(1, bit)

        new_mask =
          if active? do
            Bitwise.bor(table.active_mask, mask_bit)
          else
            Bitwise.band(table.active_mask, Bitwise.bnot(mask_bit))
          end

        %{table | active_mask: new_mask}

      :error ->
        table
    end
  end

  defp rebuild_chain(%__MODULE__{} = table, name) do
    %{kind: kind} = Map.fetch!(table.catalog, name)
    entries = Map.get(table.subs, name, [])
    ordered = Enum.sort_by(entries, & &1.position)
    fun = compile_chain(kind, name, ordered)
    %{table | chains: Map.put(table.chains, name, fun)}
  end

  defp compile_chain(_kind, _name, []), do: nil

  defp compile_chain(:fold, name, entries) do
    fn ctx ->
      Enum.reduce_while(entries, {:ok, ctx}, fn entry, {:ok, acc_ctx} ->
        timeout_ms = Keyword.get(entry.sub_opts, :timeout_ms, 5_000)
        on_error = Keyword.get(entry.sub_opts, :on_error, :fail)

        case invoke_fold(name, entry.provider, acc_ctx, entry.provider_opts, timeout_ms) do
          {:ok, new_ctx} ->
            merged = merge_fold_ctx(acc_ctx, new_ctx)
            {:cont, {:ok, merged}}

          {:error, reason} ->
            handle_fold_error(on_error, acc_ctx, reason)

          :timeout ->
            handle_fold_error(on_error, acc_ctx, :timeout)

          {:exception, kind, reason, stacktrace} ->
            handle_fold_error(on_error, acc_ctx, {:exception, kind, reason, stacktrace})
        end
      end)
    end
  end

  defp compile_chain(:transform, name, entries) do
    fn ctx ->
      Enum.reduce_while(entries, {:ok, ctx}, fn entry, {:ok, current_ctx} ->
        try do
          case apply(entry.provider, name, [current_ctx, entry.provider_opts]) do
            {:ok, new_ctx} ->
              {:cont, {:ok, new_ctx}}

            {:skip, reason} ->
              {:halt, {:skip, reason}}

            {:error, reason} ->
              Logger.error(
                "[Extension.Table] transform subscriber #{entry.plugin_id} returned error: " <>
                  "#{inspect(reason)}"
              )

              {:cont, {:ok, current_ctx}}
          end
        catch
          kind, reason ->
            Logger.error(
              "[Extension.Table] transform subscriber #{entry.plugin_id} raised " <>
                "#{inspect(kind)}: #{inspect(reason)}"
            )

            {:cont, {:ok, current_ctx}}
        end
      end)
    end
  end

  defp compile_chain(:fanout, name, entries) do
    fn ctx ->
      Enum.each(entries, fn entry ->
        try do
          apply(entry.provider, name, [ctx, entry.provider_opts])
        catch
          kind, reason ->
            Logger.error(
              "[Extension.Table] fanout subscriber #{entry.plugin_id} raised " <>
                "#{inspect(kind)}: #{inspect(reason)}"
            )
        end
      end)

      :ok
    end
  end

  defp invoke_fold(name, provider, ctx, opts, timeout_ms) do
    task =
      Task.async(fn ->
        try do
          {:ok_return, apply(provider, name, [ctx, opts])}
        catch
          kind, reason -> {:exception, kind, reason, __STACKTRACE__}
        end
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, {:ok_return, {:ok, %_{} = new_ctx}}} ->
        {:ok, new_ctx}

      {:ok, {:ok_return, {:ok, other}}} ->
        {:error, {:invalid_return, other}}

      {:ok, {:ok_return, {:error, reason}}} ->
        {:error, reason}

      {:ok, {:ok_return, other}} ->
        {:error, {:invalid_return, other}}

      {:ok, {:exception, _, _, _} = exc} ->
        exc

      {:exit, reason} ->
        {:error, {:exit, reason}}

      nil ->
        Task.shutdown(task, :brutal_kill)
        :timeout
    end
  end

  ## Private — :fold merge/error handling

  defp merge_fold_ctx(%Ctx.Init{accumulated: acc} = _prev, %Ctx.Init{accumulated: new} = next) do
    merged = Map.merge(acc, new)
    %Ctx.Init{next | accumulated: merged}
  end

  defp merge_fold_ctx(_prev, next), do: next

  defp handle_fold_error(:fail, _acc_ctx, reason), do: {:halt, {:error, reason}}

  defp handle_fold_error(:empty, acc_ctx, reason) do
    Logger.debug(fn ->
      "[Extension.Table] fold subscriber skipped on error: #{inspect(reason)}"
    end)

    {:cont, {:ok, acc_ctx}}
  end

  ## Private — request dispatch

  # (handled in Flezha.Plugin.Extension.run/3 via table.request_subs directly)

  ## Private — timer

  defp reschedule_timer(%__MODULE__{} = table, name, plugin_id, entry) do
    safe_cancel_timer(entry.timer_ref)
    timers = Map.delete(table.timers, entry.timer_ref)

    interval_ms = Keyword.fetch!(entry.sub_opts, :interval_ms)

    new_timer =
      Process.send_after(self(), {:extension_timer, name, plugin_id}, interval_ms)

    updated_entry = %{entry | timer_ref: new_timer}

    subs =
      Map.update!(table.subs, name, fn list ->
        Enum.map(list, fn e ->
          if e.plugin_id == plugin_id, do: updated_entry, else: e
        end)
      end)

    %{table | subs: subs, timers: Map.put(timers, new_timer, {plugin_id, name})}
  end

  ## Private — lookups

  defp find_entry(%__MODULE__{} = table, name, plugin_id) do
    case Map.fetch(table.catalog, name) do
      {:ok, %{kind: :request}} ->
        table.request_subs
        |> Map.get(name, %{})
        |> Map.values()
        |> Enum.find_value(fn {pid_id, entry} ->
          if pid_id == plugin_id, do: entry
        end)

      {:ok, _} ->
        table.subs
        |> Map.get(name, [])
        |> Enum.find(&(&1.plugin_id == plugin_id))

      :error ->
        nil
    end
  end

  ## Private — restart/retry

  # Reset the backoff counter only when the just-crashed owned pid stayed up
  # long enough to count as a real recovery; a fast-failing provider keeps
  # its accumulated count so `retry_delay/1` grows.
  defp reset_retries_if_stable(%__MODULE__{} = table, plugin_id) do
    case Map.get(table.started_at, plugin_id) do
      nil ->
        table

      started_at ->
        uptime = :erlang.monotonic_time(:millisecond) - started_at

        if uptime >= @stable_uptime_ms do
          %{table | retries: Map.delete(table.retries, plugin_id)}
        else
          table
        end
    end
  end

  defp schedule_retry(%__MODULE__{} = table, plugin_id, _plugin) do
    count = Map.get(table.retries, plugin_id, 0)
    delay = retry_delay(count)

    Logger.info(
      "[Extension.Table] scheduling retry plugin_id=#{plugin_id} delay_ms=#{delay} attempt=#{count + 1}"
    )

    Process.send_after(self(), {:retry_plugin_restart, plugin_id}, delay)
    %{table | retries: Map.put(table.retries, plugin_id, count + 1)}
  end

  defp attempt_restart(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin) do
    Logger.info("[Extension.Table] attempt_restart begin plugin_id=#{plugin_id}")

    prior_phase = Map.get(table.phases, plugin_id, :prepared)

    case run_init_provider(plugin) do
      {:ok, fresh_plugin, owned_pid} ->
        table = %{table | plugins: Map.put(table.plugins, plugin_id, fresh_plugin)}
        # Restart drops back to :prepared so a downstream re-activate fires
        # the provider's `activate/1` to reopen upstream resources.
        table = %{table | phases: Map.put(table.phases, plugin_id, :prepared)}
        table = track_owned_pid(table, plugin_id, owned_pid)

        subs = fresh_plugin.subscriptions

        table =
          Enum.reduce(subs, table, fn {name, _}, acc ->
            remove_subscription(acc, name, plugin_id)
          end)

        new_table =
          Enum.reduce(subs, table, fn {name, sub_opts}, acc ->
            %{kind: kind} = Map.fetch!(acc.catalog, name)
            add_subscription(acc, kind, name, plugin_id, fresh_plugin, sub_opts)
          end)

        # NB: do NOT reset the retry counter here. `init_provider/1` only
        # prepares the provider; the upstream connection opens later in
        # `activate/1` and may still fail. Resetting on prepare made a
        # connection that keeps failing (e.g. a 429-throttled WS) retry at
        # the base delay forever. The counter is reset only after an owned
        # pid stays up `@stable_uptime_ms` (see `reset_retries_if_stable/2`).
        if prior_phase == :running do
          activate_plugin(new_table, plugin_id, fresh_plugin)
        else
          new_table
        end

      {:error, reason} ->
        Logger.error(
          "[Extension.Table] init_provider failed for plugin_id=#{plugin_id} reason=#{inspect(reason)}"
        )

        schedule_retry(table, plugin_id, plugin)
    end
  rescue
    err ->
      Logger.error("[Extension.Table] restart failed for #{plugin_id}: #{inspect(err)}")
      schedule_retry(table, plugin_id, plugin)
  end

  defp retry_delay(count) do
    exp = min(count, 20)

    (4000 * :math.pow(2, exp))
    |> round()
    |> min(@max_retry_delay_ms)
  end

  ## Private — helpers

  defp safe_demonitor(nil), do: :ok
  defp safe_demonitor(ref), do: Process.demonitor(ref, [:flush])

  defp safe_cancel_timer(nil), do: :ok

  defp safe_cancel_timer(ref) do
    _ = Process.cancel_timer(ref)
    :ok
  end

  defp safe_kill(nil), do: :ok

  defp safe_kill(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      Process.exit(pid, :shutdown)
    end

    :ok
  end
end

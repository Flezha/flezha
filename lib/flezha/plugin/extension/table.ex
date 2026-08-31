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
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  # Restart backoff: only reset the retry counter once an owned pid has
  # stayed up at least this long, so a provider that fails fast (e.g. a WS
  # whose upstream keeps 429-ing) accumulates backoff instead of hammering.
  @stable_uptime_ms 60_000
  @max_retry_delay_ms 60_000
  @retry_base_delay_ms 4_000

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
  def subscribe(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin)
      when is_binary(plugin_id) do
    with :ok <- validate_plugin_unique(table, plugin_id),
         {:ok, plugin} <- canonicalise(table.catalog, plugin),
         :ok <- validate_subscriptions(table, plugin) do
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
      Enum.reduce(table.plugins, {table, []}, fn {plugin_id, plugin}, {acc, failed} ->
        case Map.get(acc.phases, plugin_id, :prepared) do
          :running ->
            {acc, failed}

          :prepared ->
            case activate_plugin(acc, plugin_id, plugin) do
              {activated, :ok} -> {activated, failed}
              {activated, {:error, reason}} -> {activated, [{plugin_id, reason} | failed]}
            end
        end
      end)

    {table, Enum.reverse(failed)}
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
    try do
      case provider.activate(opts) do
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

        other ->
          Logger.error(
            "[Extension.Table] activate invalid return for plugin_id=#{plugin_id}: " <>
              inspect(other)
          )

          {table, {:error, {:invalid_activate_return, other}}}
      end
    catch
      kind, reason ->
        Logger.error(
          "[Extension.Table] activate raised for plugin_id=#{plugin_id}: " <>
            "#{inspect(kind)}/#{inspect(reason)}"
        )

        {table, {:error, {kind, reason}}}
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
  Health verdict for every plugin on the table (plugin health contract). Only
  `:running` plugins are evaluated — a `:prepared` plugin has not acquired
  upstream resources yet, so it is reported `:healthy`, and that verdict is NOT
  evidence the plugin is running (read the phase for that). Providers that
  export `health/1` are queried (with `:owned_pid` injected); providers without
  it default to "owned pid alive (or absent) ⇒ healthy".

  A probe that could not be completed — the provider reported
  `{:fail_open, reason}`, raised, or answered with nonsense — is reported
  `:healthy` so the reconciler never escalates on uncertainty, AND emitted as
  telemetry naming this plugin. Returns
  `%{plugin_id => :healthy | {:unhealthy, reason}}`.
  """
  @spec health(t()) :: %{binary() => ProviderBehaviour.health_verdict()}
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
      not Code.ensure_loaded?(provider) ->
        {:unhealthy, :provider_not_loaded}

      function_exported?(provider, :health, 1) ->
        safe_health(plugin_id, provider, opts, owned_pid)

      is_nil(owned_pid) ->
        :healthy

      Process.alive?(owned_pid) ->
        :healthy

      true ->
        {:unhealthy, :owned_pid_dead}
    end
  end

  # An uncertain probe reports healthy so the reconciler never escalates on one
  # — but it is emitted, because a wedged provider is otherwise
  # indistinguishable from a working one at every layer above this call. This is
  # the single place that happens: a provider reports its own uncertainty as
  # `{:fail_open, reason}` and this is the only layer that knows which plugin
  # to name.
  defp safe_health(plugin_id, provider, opts, owned_pid) do
    case provider.health(Map.put(opts || %{}, :owned_pid, owned_pid)) do
      :healthy -> :healthy
      {:unhealthy, _reason} = unhealthy -> unhealthy
      {:fail_open, reason} -> ProviderBehaviour.fail_open(plugin_id, provider, reason)
      other -> ProviderBehaviour.fail_open(plugin_id, provider, {:invalid_return, other})
    end
  rescue
    error -> ProviderBehaviour.fail_open(plugin_id, provider, {:raised, error})
  catch
    kind, reason -> ProviderBehaviour.fail_open(plugin_id, provider, {kind, reason})
  end

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
    case find_entry(table, name, plugin_id) do
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
      safe_demonitor(ref)
      safe_kill(pid)
    end)

    Enum.each(table.timers, fn {timer_ref, _} -> safe_cancel_timer(timer_ref) end)

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

  # `position` orders a chain, and the unordered kinds have none — so they
  # declare it refused rather than unknown, which is the answer callers read.
  @position_refused {:refused, :position_not_applicable}

  # The whole sub-option vocabulary, by dispatch kind: every name an operator
  # may write and the type its value carries. Coercion and refusal both read
  # it, so a new sub-option is declared in the module that validates it.
  @sub_opt_types %{
    fold: [position: :int, timeout_ms: :int, on_error: {:enum, ["fail", "empty"]}],
    transform: [position: :int],
    fanout: [position: @position_refused],
    request: [key: :string, position: @position_refused],
    timer: [interval_ms: :int, position: @position_refused]
  }

  # A kind absent from the table would raise at its first subscribe. Fail the
  # build instead.
  @undeclared_kinds Flezha.Plugin.Host.valid_kinds() -- Map.keys(@sub_opt_types)
  if @undeclared_kinds != [] do
    raise "@sub_opt_types declares no sub-options for #{inspect(@undeclared_kinds)}"
  end

  # What a sub-option means when the operator omits it — read by the validator
  # and by the dispatch chain, so it is written once.
  @sub_opt_defaults [timeout_ms: 5_000, on_error: "fail"]

  defp sub_opt(sub_opts, name),
    do: Keyword.get(sub_opts, name, Keyword.get(@sub_opt_defaults, name))

  @doc """
  Canonicalise a plugin's subscriptions against `catalog` and the declared
  sub-option types: `{atom, keyword}` tuples whose values carry their declared
  type.

  An extension name may arrive as text or as an atom, and sub-opts as a map or
  a keyword list — the shapes the CLI, the operative-start spec, an installed
  package manifest and direct construction produce. Both are resolved against
  a declaration rather than interned, so operator text never creates an atom;
  a name that resolves to nothing is refused.

  `subscribe/3` runs it, so the table stores canonical values and a provider
  restart, an unsubscribe and a handover all observe them. A caller that writes
  durable intent runs it first, so nothing ill-typed is ever persisted. It is
  idempotent: canonical input is returned unchanged.
  """
  @spec canonicalise(map(), Plugin.t()) :: {:ok, Plugin.t()} | {:error, term()}
  def canonicalise(catalog, %Plugin{subscriptions: subs} = plugin)
      when is_map(catalog) and is_list(subs) do
    subs
    |> Enum.reduce_while({:ok, []}, fn sub, {:ok, acc} ->
      case canonicalise_subscription(catalog, sub) do
        {:ok, canonical} -> {:cont, {:ok, [canonical | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, %Plugin{plugin | subscriptions: Enum.reverse(acc)}}
      err -> err
    end
  end

  def canonicalise(catalog, %Plugin{}) when is_map(catalog),
    do: {:error, {:invalid_subscription, :not_a_list}}

  defp canonicalise_subscription(catalog, {name, sub_opts}) do
    with {:ok, name, kind} <- resolve_extension(catalog, name),
         {:ok, opts} <- canonicalise_sub_opts(kind, sub_opts) do
      {:ok, {name, opts}}
    end
  end

  defp canonicalise_subscription(_catalog, other),
    do: {:error, {:invalid_subscription, other}}

  defp resolve_extension(catalog, name) when is_atom(name) and not is_nil(name) do
    case Map.fetch(catalog, name) do
      {:ok, %{kind: kind}} -> {:ok, name, kind}
      :error -> {:error, {:unknown_extension, name}}
    end
  end

  defp resolve_extension(catalog, name) do
    case Enum.find(catalog, fn {known, _entry} -> same_name?(known, name) end) do
      {known, %{kind: kind}} -> {:ok, known, kind}
      nil -> {:error, {:unknown_extension, name}}
    end
  end

  defp canonicalise_sub_opts(kind, sub_opts) do
    declared = Map.fetch!(@sub_opt_types, kind)

    with {:ok, pairs} <- declared_order(declared, sub_opts) do
      pairs
      |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
        with {:ok, name, type} <- resolve_sub_opt(declared, kind, key),
             {:ok, coerced} <- coerce_sub_opt(type, name, value) do
          {:cont, {:ok, [{name, coerced} | acc]}}
        else
          {:error, _} = err -> {:halt, err}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        err -> err
      end
    end
  end

  # A map carries no order to preserve, so it is emitted in declared order:
  # one declaration canonicalises to one term whatever shape it arrived in.
  # A keyword list already has one, and keeping it makes canonical input
  # come back unchanged.
  defp declared_order(declared, sub_opts) when is_map(sub_opts) do
    {:ok,
     Enum.sort_by(sub_opts, fn {key, _value} ->
       Enum.find_index(declared, fn {name, _type} -> same_name?(name, key) end) ||
         length(declared)
     end)}
  end

  defp declared_order(_declared, sub_opts) when is_list(sub_opts) do
    if Enum.all?(sub_opts, &match?({_k, _v}, &1)),
      do: {:ok, sub_opts},
      else: {:error, {:invalid_subscription_opts, :not_a_keyword}}
  end

  defp declared_order(_declared, _sub_opts),
    do: {:error, {:invalid_subscription_opts, :not_a_keyword}}

  defp resolve_sub_opt(declared, kind, key) do
    case Enum.find(declared, fn {name, _type} -> same_name?(name, key) end) do
      {name, type} -> {:ok, name, type}
      nil -> {:error, {:unknown_subscription_opt, kind, key}}
    end
  end

  defp same_name?(declared, key) when is_atom(key) and not is_nil(key),
    do: declared == key

  defp same_name?(declared, key) when is_binary(key), do: Atom.to_string(declared) == key
  defp same_name?(_declared, _key), do: false

  # Reuses the provider layer's parse_int/2 — the same "the CLI ships text"
  # problem, already solved once.
  defp coerce_sub_opt(:int, name, value) when is_integer(value) or is_binary(value) do
    case ProviderBehaviour.parse_int(value, name) do
      {:ok, int} -> {:ok, int}
      {:error, _msg} -> invalid_sub_opt(name, value)
    end
  end

  defp coerce_sub_opt(:string, _name, value) when is_binary(value), do: {:ok, value}

  defp coerce_sub_opt(:string, _name, value) when is_atom(value) and not is_nil(value),
    do: {:ok, Atom.to_string(value)}

  defp coerce_sub_opt({:enum, allowed}, name, value)
       when is_binary(value) or (is_atom(value) and not is_nil(value)) do
    text = to_string(value)

    if text in allowed, do: {:ok, text}, else: invalid_sub_opt(name, value)
  end

  defp coerce_sub_opt({:refused, reason}, _name, _value), do: {:error, reason}

  defp coerce_sub_opt(_type, name, value), do: invalid_sub_opt(name, value)

  defp invalid_sub_opt(name, value), do: {:error, {:invalid_subscription_opts, {name, value}}}

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

  defp validate_kind_specific(:timer, _name, sub_opts, _table, _plugin),
    do: validate_positive(sub_opts, :interval_ms)

  defp validate_kind_specific(:fanout, _name, _sub_opts, _table, _plugin), do: :ok

  defp validate_kind_specific(:request, name, sub_opts, table, _plugin) do
    case Keyword.fetch(sub_opts, :key) do
      :error ->
        {:error, {:invalid_subscription_opts, :missing_key}}

      {:ok, key} ->
        case get_in(table.request_subs, [name, key]) do
          nil -> :ok
          _ -> {:error, {:key_conflict, name, key}}
        end
    end
  end

  defp validate_kind_specific(:fold, _name, sub_opts, _table, _plugin) do
    with :ok <- validate_position(sub_opts) do
      validate_positive(sub_opts, :timeout_ms)
    end
  end

  defp validate_kind_specific(:transform, _name, sub_opts, _table, _plugin),
    do: validate_position(sub_opts)

  # Canonicalisation types `position` as an integer; only the range is left,
  # and a negative one would still reach `insert_by_position/3`.
  defp validate_position(sub_opts) do
    case Keyword.get(sub_opts, :position) do
      nil -> :ok
      n when is_integer(n) and n >= 0 -> :ok
      other -> {:error, {:invalid_subscription_opts, {:position, other}}}
    end
  end

  # The bounds a type cannot express. `interval_ms` has no default, so an
  # absent one fails the guard — which is what makes it required.
  defp validate_positive(sub_opts, name) do
    case sub_opt(sub_opts, name) do
      n when is_integer(n) and n > 0 -> :ok
      other -> {:error, {:invalid_subscription_opts, {name, other}}}
    end
  end

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
        case provider.init_provider(with_host_identity(opts || %{}, plugin)) do
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

  defp do_unsubscribe(%__MODULE__{} = table, plugin_id, %Plugin{subscriptions: subs} = plugin) do
    terminate_provider(table, plugin_id, plugin)

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

    %{
      table
      | plugins: Map.delete(table.plugins, plugin_id),
        phases: Map.delete(table.phases, plugin_id),
        retries: Map.delete(table.retries, plugin_id),
        started_at: Map.delete(table.started_at, plugin_id)
    }
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
        timeout_ms = sub_opt(entry.sub_opts, :timeout_ms)
        on_error = sub_opt(entry.sub_opts, :on_error)

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

  defp handle_fold_error("fail", _acc_ctx, reason), do: {:halt, {:error, reason}}

  defp handle_fold_error("empty", acc_ctx, reason) do
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

  defp restarted_phase(%Plugin{provider: provider}) do
    if Code.ensure_loaded?(provider) and function_exported?(provider, :activate, 1),
      do: :prepared,
      else: :running
  end

  defp attempt_restart(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin) do
    Logger.info("[Extension.Table] attempt_restart begin plugin_id=#{plugin_id}")

    prior_phase = Map.get(table.phases, plugin_id, :prepared)

    case run_init_provider(plugin) do
      {:ok, fresh_plugin, owned_pid} ->
        table = %{table | plugins: Map.put(table.plugins, plugin_id, fresh_plugin)}
        # Restart drops back to :prepared so a downstream re-activate fires
        # the provider's `activate/1` to reopen upstream resources — but a
        # provider that exports none has nothing to reopen, and leaving it
        # :prepared would have `plugin_health/3` report it healthy without ever
        # probing until the next bind. Same rule `activate_plugin/3` applies.
        table = %{table | phases: Map.put(table.phases, plugin_id, restarted_phase(plugin))}
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
          {activated, _outcome} = activate_plugin(new_table, plugin_id, fresh_plugin)
          activated
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
    base = Application.get_env(:flezha, :plugin_retry_base_delay_ms, @retry_base_delay_ms)

    # Cap the exponent, not just the product: an unbounded count would build a
    # bignum the clamp then throws away.
    ProviderBehaviour.retry_backoff_ms(base, @max_retry_delay_ms, min(count, 20) + 1)
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

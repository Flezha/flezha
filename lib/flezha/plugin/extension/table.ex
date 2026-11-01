defmodule Flezha.Plugin.Extension.Table do
  @moduledoc """
  Per-host registry of plugin hooks, compiled dispatch chains, and
  subscriber lifecycle state.

  The struct is owned here and the three companions mutate disjoint parts of
  it: `Subscription` the subscriber lists, `Chain` the compiled closures and
  the `active_mask`, `Health` the health verdicts and the monitor, timer and
  pid hygiene. None of them names this module, so effects and lifecycle stay
  here.

  Plugins enter `:prepared` (`init_provider/1` ran, nothing else did);
  `bulk_activate/2` makes them `:running` once the SDK side is bound, starting
  each provider's DECLARED worker, then `activate/1`. A crashed worker leaves
  its plugin `{:down, reason}`, which the convergence loop restarts through
  `attempt_restart/3`.

  Everything the table does to a provider it reads from that provider's
  declaration (`Flezha.Plugin.ProviderBehaviour.Decl`): which callbacks it
  overrode, which options it takes, which worker it runs. Nothing is discovered
  by probing exports, and no provider starts or stops a lifecycle process of its
  own.

  A relay's table is built with `shared:`, the module running shareable
  plugins once per controller, which a shareable plugin joins at activation
  (`Workers.subscriber?/2`). A binding that cannot come up is recorded
  `{:not_started, reason}` (`put_not_started/5`).
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table.Chain
  alias Flezha.Plugin.Extension.Table.Health
  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Subscription
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Provider.NotStarted
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias Flezha.SDK.SafeCall

  require Logger

  defstruct catalog: %{},
            active_mask: 0,
            subs: %{},
            request_subs: %{},
            chains: %{},
            plugins: %{},
            entries: %{},
            # `monitor_ref => {plugin_id, worker_key}`: the reverse index a
            # `:DOWN` needs to find the record the dead worker belongs to.
            monitors: %{},
            timers: %{},
            host_pid: nil,
            # The module serving shareable plugins on this controller, or nil
            # for a table that runs every plugin it holds itself.
            shared: nil,
            # Relay-level facts resolved at bind time
            # (`Relay.Plugins.env/1`), merged into every provider's opts at
            # activation, including plugin-heal re-activations.
            activation_env: %{}

  @type t() :: %__MODULE__{}

  @type phase() :: Entry.phase()

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
  the plugin host's `build_catalog/2`).
  """
  @spec new(map(), keyword()) :: t()
  def new(catalog, opts \\ []) when is_map(catalog) do
    subs = Map.new(catalog, fn {name, _entry} -> {name, []} end)

    request_subs = for {name, %{kind: :request}} <- catalog, into: %{}, do: {name, %{}}

    %__MODULE__{
      catalog: catalog,
      subs: subs,
      request_subs: request_subs,
      host_pid: self(),
      shared: Keyword.get(opts, :shared)
    }
  end

  @doc """
  Subscribe a list of `{plugin_id, %Flezha.Plugin{}}` tuples, running each
  provider's `init_provider/1`. The list is applied in order; on first failure
  the table is returned unchanged and the error is reported, naming the plugin
  that refused and the rows the halt left unattempted.

  This is the table's ONE subscribe: a single plugin is a list of one. A second
  entry point is what let the bulk path park a relay with no plugin named
  while the single path answered honestly.
  """
  @spec bulk_subscribe(t(), [{binary(), Plugin.t()}]) ::
          {:ok, t()} | {:error, {binary(), term()}}
  def bulk_subscribe(%__MODULE__{} = table, plugins) when is_list(plugins) do
    plugins
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, table}, fn {{plugin_id, plugin}, index}, {:ok, acc} ->
      case subscribe_one(acc, plugin_id, plugin) do
        {:ok, new_table} ->
          {:cont, {:ok, new_table}}

        {:error, reason} ->
          reason = missing_provider(reason)
          log_halt(plugin_id, plugin, reason, Enum.drop(plugins, index + 1))
          {:halt, {:error, {plugin_id, reason}}}
      end
    end)
  end

  # A provider module this VM does not hold is, when a package on this node
  # declares it, that package's refusal or pending install, which is what the
  # user repairs.
  defp missing_provider({:module_not_loaded, provider, _controller} = reason),
    do: Installer.unavailable(provider) || reason

  defp missing_provider(reason), do: reason

  defp subscribe_one(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin)
       when is_binary(plugin_id) do
    with :ok <- Subscription.validate_plugin_unique(table, plugin_id),
         {:ok, plugin} <- Subscription.canonicalise(table.catalog, plugin),
         :ok <- Subscription.validate_subscriptions(table, plugin) do
      do_subscribe(table, plugin_id, plugin)
    end
  end

  # The halt is the only point holding all three facts a user needs: which
  # plugin refused, what its provider was, and which rows the halt therefore
  # left unattempted.
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

  Invoked by relay hosts after `start_relay_sdk/2` returns `{:ok, _}` — see
  `relay-extensions` "Two-phase plugin lifecycle gated on SDK readiness".

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
    case phase(acc, plugin_id) do
      :running ->
        {acc, failed}

      {:down, _reason} ->
        {acc, failed}

      {:not_started, _reason} ->
        {acc, failed}

      _ ->
        case activate_plugin(acc, plugin_id, plugin) do
          {activated, :ok} -> {activated, failed}
          {activated, {:error, reason}} -> {activated, [{plugin_id, reason} | failed]}
        end
    end
  end

  # Activation is two steps in one order for every provider alike: start what
  # the declaration says it runs, then let it acquire what only it can. A
  # provider that declares a worker and overrides nothing is fully activated by
  # the first step.
  defp activate_plugin(table, plugin_id, %Plugin{provider: provider, opts: opts} = plugin) do
    cond do
      not Code.ensure_loaded?(provider) ->
        Logger.warning(
          "[Extension.Table] activate skipped, provider not loaded plugin_id=#{plugin_id}"
        )

        {put_phase(table, plugin_id, :running, :provider_not_loaded), :ok}

      Workers.subscriber?(table, provider) ->
        case Workers.join(table, plugin_id, plugin) do
          {table, :ok} -> {put_phase(table, plugin_id, :running, :joined), :ok}
          {table, {:error, _} = refused} -> {table, refused}
        end

      # Under a worker key of its own, beside the shared copy's.
      Workers.own_source?(table, provider) ->
        own_id = "#{plugin_id}@#{plugin.relay_id}"
        own = %{plugin_id: own_id, worker_key: {plugin.namespace, own_id}}
        table = put_plugin_opts(table, plugin_id, own)
        start_and_activate(table, plugin_id, provider, Map.merge(opts, own))

      true ->
        start_and_activate(table, plugin_id, provider, opts)
    end
  end

  # Relay-level facts resolved at bind time (`Relay.Plugins.env/1`) are
  # merged last, so stored opts cannot spoof them.
  defp start_and_activate(table, plugin_id, provider, opts) do
    opts = Map.merge(opts || %{}, table.activation_env)

    case Workers.start(table, plugin_id, provider, opts, invoker(plugin_id, provider)) do
      {table, :ok} ->
        invoke_activate(table, plugin_id, provider, opts)

      {table, {:error, reason}} ->
        {table, {:error, Loader.refusal(:provider_failed, provider, reason)}}
    end
  end

  defp invoke_activate(table, plugin_id, provider, opts) do
    case invoke(:activate, plugin_id, provider, fn -> provider.activate(opts) end) do
      ok when ok == :ok or (is_tuple(ok) and elem(ok, 0) == :ok) ->
        {put_phase(table, plugin_id, :running, :activated), :ok}

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

  # `Workers` invokes `worker_specs/1` through the table's own catch, so a
  # provider that crashes naming its workers refuses the activation rather than
  # taking the host down with it.
  defp invoker(plugin_id, provider),
    do: &invoke(:worker_specs, plugin_id, provider, &1)

  # Every provider callback this table invokes goes through here: what a
  # provider raises, throws or exits with is a refusal of the operation, never a
  # crash of the host that asked for it. The stacktrace is logged so a provider
  # bug stays as legible as the crash it replaces.
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

  ## Private - the lifecycle record

  # The ONE call that changes a phase, so the lifecycle event has one home.
  defp put_phase(table, plugin_id, phase, reason) do
    entry = Entry.of(table, plugin_id)
    provider = provider_of(table, plugin_id)

    %{
      table
      | entries:
          Map.put(
            table.entries,
            plugin_id,
            Entry.transition(entry, plugin_id, provider, phase, reason)
          )
    }
  end

  defp provider_of(%__MODULE__{plugins: plugins}, plugin_id) do
    case Map.get(plugins, plugin_id) do
      %Plugin{provider: provider} -> provider
      nil -> nil
    end
  end

  @doc "The event every phase transition emits, for handlers to attach to."
  @spec phase_event() :: [atom()]
  defdelegate phase_event(), to: Entry

  @doc "`provider`'s compile-time declaration."
  @spec decl(module()) :: ProviderBehaviour.Decl.t()
  def decl(provider), do: provider.__provider__()

  @doc """
  Return the lifecycle phase of `plugin_id`: `:prepared`, `:running`,
  `{:down, reason}` once its worker crashed, or `:unknown` if the plugin is not
  registered on this table.
  """
  @spec phase(t(), binary()) :: phase() | :unknown
  def phase(%__MODULE__{entries: entries}, plugin_id) when is_binary(plugin_id) do
    case Map.get(entries, plugin_id) do
      %Entry{phase: phase} when not is_nil(phase) -> phase
      _ -> :unknown
    end
  end

  @doc """
  Is `plugin_id` holding the upstream resources its activation acquires?

  This, not "is it on the table", answers "is this plugin running": a plugin
  whose `activate/1` failed stays subscribed and `:prepared`, and counting it as
  running is what let a source that never connected read as healthy forever.
  """
  @spec running?(t(), binary()) :: boolean()
  def running?(%__MODULE__{} = table, plugin_id), do: phase(table, plugin_id) == :running

  @doc """
  Is `plugin_id` declared on this table at all, whatever its phase? A plugin the
  declaration does not name is extra even while it is still `:prepared`.
  """
  @spec subscribed?(t(), binary()) :: boolean()
  def subscribed?(%__MODULE__{plugins: plugins}, plugin_id),
    do: Map.has_key?(plugins, plugin_id)

  @doc """
  The bindings in the phase `{tag, reason}`, with the reason each recorded:
  `:down`, a subscriber whose worker crashed, or `:not_started`, a binding that
  cannot come up here.
  """
  @spec phase_reasons(t(), :down | :not_started) :: %{binary() => term()}
  def phase_reasons(%__MODULE__{entries: entries}, tag) do
    for {id, %Entry{phase: {^tag, reason}}} <- entries, into: %{}, do: {id, reason}
  end

  @doc """
  How long each running subscriber has been up, in milliseconds: the evidence
  the loop reads to call a restarted plugin repaired.
  """
  @spec uptimes(t()) :: %{binary() => non_neg_integer()}
  def uptimes(%__MODULE__{entries: entries}) do
    now = :erlang.monotonic_time(:millisecond)

    for {id, %Entry{phase: :running, started_at: at}} <- entries,
        is_integer(at),
        into: %{},
        do: {id, now - at}
  end

  @doc "The processes the table started for `plugin_id`, by key; none while `:prepared`."
  @spec workers(t(), binary()) :: %{term() => pid()}
  def workers(%__MODULE__{} = table, plugin_id), do: Entry.pids(Entry.of(table, plugin_id))

  @doc "Whether `plugin_id` is backed by the shared copy it joined."
  defdelegate joined?(table, plugin_id), to: Workers

  @doc "The one process the host tracks as `plugin_id`'s own, if it started one."
  @spec owned_pid(t(), binary()) :: pid() | nil
  def owned_pid(%__MODULE__{} = table, plugin_id),
    do: Entry.owned_pid(Entry.of(table, plugin_id))

  @doc """
  A provider's stored opts with the processes the table started injected. See
  `Flezha.Plugin.Extension.Table.Health.with_worker_pids/2`, the one site.
  """
  @spec with_worker_pids(t(), binary(), map()) :: map()
  def with_worker_pids(%__MODULE__{} = table, plugin_id, opts),
    do: Health.with_worker_pids(Entry.of(table, plugin_id), opts)

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
  def running_ids(%__MODULE__{plugins: plugins} = table) do
    for {id, _plugin} <- plugins, running?(table, id), do: id
  end

  @doc """
  Health verdict for every plugin on the table (plugin health contract). See
  `Flezha.Plugin.Extension.Table.Health.probe/1` for the rules.
  """
  @spec health(t()) :: %{binary() => ProviderBehaviour.health_verdict()}
  def health(%__MODULE__{} = table), do: Health.probe(table)

  @doc """
  Remove every subscription owned by `plugin_id`, cancel its monitors,
  timers, and owned pids, and rebuild affected dispatch chains. A subscriber
  leaves its shared copy `leave` (`:now` stops a copy it was the last on at
  once, `:idle` after the idle stop).
  """
  @spec unsubscribe(t(), binary(), :now | :idle) :: t()
  def unsubscribe(%__MODULE__{} = table, plugin_id, leave \\ :now) do
    case Map.get(table.plugins, plugin_id) do
      nil -> table
      plugin -> do_unsubscribe(table, plugin_id, plugin, leave)
    end
  end

  @doc """
  Record `plugin` as declared on this host and not started, for `reason`: it
  holds no worker and runs no hook, its health is `{:unhealthy, reason}`, and
  the convergence loop tries it again as one that is missing. An `on_output`
  hook is held by `Flezha.Plugin.Provider.NotStarted`, so a guaranteed output
  waits in the outbox for it, and a plain one is counted on `counters`.
  """
  @spec put_not_started(t(), binary(), Plugin.t(), term(), :counters.counters_ref() | nil) :: t()
  def put_not_started(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin, reason, counters) do
    table = unsubscribe(table, plugin_id, :idle)

    held =
      for {:on_output, sub_opts} <- plugin.hooks,
          do: {:on_output, Keyword.delete(sub_opts, :position)}

    placeholder = %Plugin{plugin | provider: NotStarted, opts: %{counters: counters}, hooks: held}

    {:ok, table} = do_subscribe(table, plugin_id, placeholder)
    put_phase(table, plugin_id, {:not_started, reason}, reason)
  end

  @doc """
  Apply `plugin`'s options to `plugin_id` in place, for options its provider
  declares live: the owner of the plugin's workers calls the provider's
  `reconfigure/2`, a subscriber only takes the new options.
  """
  @spec reconfigure(t(), binary(), Plugin.t()) :: {:ok, t()} | {:error, term()}
  def reconfigure(%__MODULE__{} = table, plugin_id, %Plugin{provider: provider} = plugin) do
    with %Plugin{opts: old} <-
           Map.get(table.plugins, plugin_id, {:error, {:plugin_not_found, plugin_id}}),
         {:ok, new} <- ProviderOptions.normalise_opts(provider, plugin.opts),
         :ok <- reconfigure_owned(table, plugin_id, provider, old, new) do
      live = Map.take(new, ProviderBehaviour.Decl.live_options(decl(provider)))
      {:ok, put_plugin_opts(table, plugin_id, live)}
    end
  end

  defp reconfigure_owned(table, plugin_id, provider, old, new) do
    if Workers.subscriber?(table, provider) do
      :ok
    else
      case invoke(:reconfigure, plugin_id, provider, fn -> provider.reconfigure(old, new) end) do
        :ok -> :ok
        {:error, _} = refused -> refused
        other -> {:error, {:invalid_reconfigure_return, other}}
      end
    end
  end

  @doc """
  Fire the timer for `{plugin_id, name}`. Invokes the subscriber's
  `on_interval/2` callback and reschedules the next tick.
  """
  @spec fire_timer(t(), atom(), binary()) :: t()
  defdelegate fire_timer(table, name, plugin_id), to: Subscription

  @doc """
  Handle a host's `{:DOWN, ref, :process, pid, reason}`. A worker that exited
  `:normal` unsubscribes its plugin; any other exit records it `{:down, reason}`
  for the host to declare (the table restarts nothing), logged as a crash unless
  orderly (`SafeCall.orderly?/1`). Returns `{:ok, table}` or `:ignore`.
  """
  @spec handle_down(t(), reference(), pid(), term(), keyword()) ::
          {:ok, t()} | :ignore
  def handle_down(%__MODULE__{} = table, ref, _pid, reason, _opts \\ []) do
    case Map.get(table.monitors, ref) do
      nil ->
        :ignore

      {plugin_id, key} ->
        plugin = Map.fetch!(table.plugins, plugin_id)

        table =
          %{table | monitors: Map.delete(table.monitors, ref)}
          |> Map.update!(:entries, fn entries ->
            Map.put(entries, plugin_id, Entry.drop_worker(Entry.of(table, plugin_id), key))
          end)

        case {key, reason} do
          {_key, :normal} ->
            {:ok, do_unsubscribe(table, plugin_id, plugin, :idle)}

          # A shared copy that handed over or died: the subscriber joins the
          # copy that runs now, and the loop restarts it if none does.
          {:shared, reason} ->
            {:ok, rejoin(table, plugin_id, plugin, reason)}

          _ ->
            down = "[Extension.Table] subscriber #{plugin_id}"

            if SafeCall.orderly?(reason),
              do: Logger.info("#{down} stopped: #{inspect(reason)}"),
              else: Logger.error("#{down} crashed: #{inspect(reason)}")

            {:ok, put_phase(table, plugin_id, {:down, reason}, reason)}
        end
    end
  end

  @doc """
  Re-run a plugin's provider: release the workers it still holds, run
  `init_provider/1` afresh, and re-register its hooks against the fresh
  instance. The plugin re-enters `:prepared`, which the caller activates. On a
  failed `init_provider/1` the plugin is recorded `{:down, reason}`, so it keeps
  diffing as one the loop must restart.

  It re-runs from the entry the TABLE holds, and only falls back to `plugin`,
  the loop's declared manifest, for one the table does not hold: a provider that
  commits durable state of its own (the websocket provider's applied
  subscription set) writes it back through `put_plugin_opt/4`, and re-running
  from the manifest would silently discard it.

  The one restart path: the convergence loop's `restart_plugin` reaches it
  through `Flezha.Relay.Plugins.restart/3`.
  """
  @spec attempt_restart(t(), binary(), Plugin.t()) :: {:ok, t()} | {:error, term(), t()}
  def attempt_restart(%__MODULE__{} = table, plugin_id, %Plugin{} = declared) do
    %Plugin{} = held = Map.get(table.plugins, plugin_id, declared)
    plugin = %Plugin{held | id: plugin_id}
    table = release_workers(table, plugin_id, plugin, :idle)

    case run_init_provider(table, plugin) do
      {:ok, fresh_plugin} ->
        table =
          %{table | plugins: Map.put(table.plugins, plugin_id, fresh_plugin)}
          |> put_phase(plugin_id, :prepared, :restarted)

        {:ok, Chain.resubscribe(table, plugin_id, fresh_plugin)}

      {:error, reason} ->
        Logger.error(
          "[Extension.Table] init_provider failed for plugin_id=#{plugin_id} reason=#{inspect(reason)}"
        )

        {:error, reason, put_phase(table, plugin_id, {:down, reason}, reason)}
    end
  end

  @doc """
  Tear down every subscription, in any phase: cancel timers, stop workers
  through their supervisor, and leave every shared copy `leave`. Answers the
  emptied table.
  """
  @spec teardown_all(t(), :now | :idle) :: t()
  def teardown_all(%__MODULE__{} = table, leave \\ :idle) do
    table =
      Enum.reduce(table.plugins, table, fn {plugin_id, plugin}, acc ->
        release_workers(acc, plugin_id, plugin, leave)
      end)

    Enum.each(table.timers, fn {timer_ref, _} -> Health.safe_cancel_timer(timer_ref) end)

    new(table.catalog, shared: table.shared)
  end

  @doc """
  Merge the table a bind task built from `base` with `live`, the one this host
  kept changing while the task bound (`Workers.keep_changed/4`).
  """
  @spec keep_changed(t(), t(), t()) :: t()
  def keep_changed(%__MODULE__{} = bound, %__MODULE__{} = base, %__MODULE__{} = live),
    do: Workers.keep_changed(bound, base, live, &terminate_provider/3)

  @doc "Every subscriber entry registered for the given extension name."
  @spec subscribers(t(), atom()) :: [entry()]
  defdelegate subscribers(table, name), to: Subscription

  @doc """
  Canonicalise a plugin's hooks against `catalog` and the declared
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
  def put_plugin_opt(table, plugin_id, key, value),
    do: put_plugin_opts(table, plugin_id, %{key => value})

  defp put_plugin_opts(%__MODULE__{} = table, plugin_id, new) do
    case Map.get(table.plugins, plugin_id) do
      nil ->
        table

      %Plugin{} = plugin ->
        opts = Map.merge(plugin.opts || %{}, new)

        %{
          Subscription.put_provider_opts(table, plugin_id, opts)
          | plugins: Map.put(table.plugins, plugin_id, %Plugin{plugin | opts: opts})
        }
    end
  end

  ## Private - subscribe

  defp do_subscribe(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin) do
    plugin = %Plugin{plugin | id: plugin_id}

    case run_init_provider(table, plugin) do
      {:ok, plugin} ->
        {:ok,
         %{table | plugins: Map.put(table.plugins, plugin_id, plugin)}
         |> put_phase(plugin_id, :prepared, :prepared)
         |> Chain.add_hooks(plugin_id, plugin)}

      {:error, _reason} = err ->
        err
    end
  end

  # Preparation is: the declaration normalises the options, then the provider
  # interprets them. Nothing is started here for any provider, so a plugin on an
  # relay whose SDK side never binds holds no process at all.
  defp run_init_provider(table, %Plugin{provider: provider, opts: opts} = plugin) do
    if Code.ensure_loaded?(provider) do
      case ProviderOptions.normalise_opts(
             provider,
             with_host_identity(table, opts || %{}, plugin)
           ) do
        {:ok, opts} ->
          settle_init(
            plugin,
            invoke(:init_provider, plugin.id, provider, fn -> provider.init_provider(opts) end)
          )

        {:error, _reason} = err ->
          err
      end
    else
      {:error, {:module_not_loaded, provider, node()}}
    end
  end

  defp settle_init(%Plugin{} = plugin, {:ok, new_opts}) when is_map(new_opts),
    do: {:ok, %Plugin{plugin | opts: WorkerSup.keyed(new_opts)}}

  defp settle_init(%Plugin{provider: provider}, {:error, reason}),
    do: {:error, Loader.refusal(:provider_refused, provider, reason)}

  defp settle_init(%Plugin{provider: provider}, {:crashed, kind, payload}),
    do: {:error, {:init_provider_crashed, provider, kind, payload}}

  defp settle_init(_plugin, other), do: {:error, {:invalid_init_provider_return, other}}

  # The host relay's own identity, merged last so stored opts cannot spoof it, as
  # `bulk_activate/2` merges `activation_env`. A provider reads it to refuse a
  # self- or cross-namespace target, to name its cspec row (`:source_plugin_id`)
  # and to allocate only for the callbacks it was enabled for; a worker key is
  # named from it, a teardown's raw manifest options included.
  defp with_host_identity(table, opts, %Plugin{} = plugin) do
    Map.merge(opts, %{
      source_namespace: plugin.namespace,
      source_relay_id: plugin.relay_id,
      source_plugin_id: plugin.id,
      overwatcher_pid: table.host_pid,
      subscribed_extensions: Enum.map(plugin.hooks, &elem(&1, 0))
    })
  end

  ## Private - unsubscribe

  defp do_unsubscribe(%__MODULE__{} = table, plugin_id, %Plugin{} = plugin, leave) do
    table = release_workers(table, plugin_id, plugin, leave)
    Entry.departed(Entry.of(table, plugin_id), plugin_id)
    Workers.forget(table, plugin_id, plugin)
  end

  # Let the provider release what the table cannot see, then stop every worker
  # the table started for it. The half of an unsubscribe a restart shares. A
  # subscriber holds no worker: it leaves its shared copy instead.
  defp release_workers(table, plugin_id, %Plugin{provider: provider} = plugin, leave) do
    cond do
      not Code.ensure_loaded?(provider) ->
        table

      Workers.subscriber?(table, provider) ->
        Workers.leave(table, plugin_id, leave)

      true ->
        terminate_provider(table, plugin_id, plugin)
        opts = with_host_identity(table, plugin.opts || %{}, plugin)
        Workers.stop_all(table, plugin_id, provider, opts, invoker(plugin_id, provider))
    end
  end

  # The teardown hook for what is neither a worker nor an option. Called on both
  # exits, with the processes the table started injected at the one site.
  defp terminate_provider(table, plugin_id, %Plugin{provider: provider, opts: opts}) do
    _ =
      invoke(:terminate_provider, plugin_id, provider, fn ->
        provider.terminate_provider(with_worker_pids(table, plugin_id, opts))
      end)

    :ok
  end

  defp rejoin(table, plugin_id, plugin, reason) do
    case Workers.join(table, plugin_id, plugin) do
      {table, :ok} -> table
      {table, {:error, _}} -> put_phase(table, plugin_id, {:down, reason}, reason)
    end
  end
end

defmodule Flezha.Plugin.Shared do
  @moduledoc """
  The running copy of a shareable plugin: one per `(namespace, name)` on each
  controller where a relay bound to it runs, started and found through
  `Flezha.Plugin.Provider.WorkerSup`, or the cluster's one copy of a `single`
  plugin, which its `Flezha.Plugin.Home` starts beside itself.

  It holds the plugin in an extension table of its own, which owns the
  provider's workers. It is no stop on the record path: a feeding worker
  injects each record straight into every bound relay without waiting
  (`Flezha.Plugin.Host.targets/1`), and the copy only tells it which relays
  are bound, at each join and leave. A relay whose input is durable runs
  such a source on its own (`Workers.own_source?/2`).

  When its last relay leaves it stops: at once when the leave was
  deliberate, after `plugin_idle_stop_ms` when the relay went down or moved,
  unless one joins first. Every copy is a member of the plugin's directory
  group, which is how a disable, an update and `flz plugin list` find it.

  A controller plugin's copy is the same host with no relay: its table holds
  the controller catalogue declared here, each hook of which takes the event
  `Flezha.Observability.HookSink` hands over; it listens by joining its hooks'
  directory groups, and it never stops idle. Its home places it
  (`Flezha.Plugin.Home`). One whose workers cannot start runs without them,
  its health naming why and `running_on/2` leaving its controller out, and
  tries again every `convergence_tick_ms`, as one whose worker went down does.
  Its waiting events are one forwarder's backlog
  (`Flezha.Plugin.Shared.Forwarder`), which hands it one at a time;
  the copy runs each hook's chain in a task and answers when the chain returns,
  so a callback that blocks holds back only its own plugin.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:idle_stop, :retired, :restart, :successor]

  use Flezha.Plugin.Host,
    extensions: [
      {:on_relay_started, :fanout, Flezha.Plugin.Shared},
      {:on_relay_stopped, :fanout, Flezha.Plugin.Shared},
      {:on_relay_failed, :fanout, Flezha.Plugin.Shared},
      {:on_dead_letter, :fanout, Flezha.Plugin.Shared},
      {:on_plugin_unhealthy, :fanout, Flezha.Plugin.Shared}
    ]

  require Logger

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Parallel
  alias Flezha.Relay.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Extension.Table.Health
  alias Flezha.Plugin.Extension.Table.Subscription
  alias Flezha.Plugin.Home
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Decl
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Flezha.Plugin.Shared.Forwarder
  alias Flezha.Process.Watch

  @channel_opts [:channel, :channel_in, :channel_out]

  # Its subscribers read the reason as "join the copy that runs now".
  @retired {:shutdown, :retired}

  @task_sup Flezha.Relay.MessageHandlerTaskSup

  # A relay whose input is durable runs a source of its own
  # (`Workers.own_source?/2`), so the copy's workers serve none.
  @activation_env %{durable_input: false}

  defstruct [
    :namespace,
    :name,
    :plugin,
    :table,
    :retiring,
    # A controller plugin's forwarder, the task running its hook now, and the
    # home that placed it.
    :hooks,
    :hook_task,
    :home,
    # Whether its provider ever injected into the copy itself.
    :misfed,
    # Why its workers did not start, while it waits to try again.
    :waiting,
    hook_groups: MapSet.new(),
    # Each bound relay's `Flezha.Plugin.Host.target/3`, by its pid.
    subscribers: %{},
    watch: Watch.new(),
    dropped: nil,
    timers: %{}
  ]

  ## Client

  @doc """
  Subscribe `overwatch` to the running copy of `plugin`, starting the one on
  this controller when none runs. `plugin` is the binding as the relay's
  table resolved it: the copy's workers stamp its entry channel on what they
  inject. Its input is not durable, or it would run the source on its own.
  """
  @spec join(Plugin.t(), pid()) :: {:ok, pid()} | {:error, term()}
  def join(%Plugin{} = plugin, overwatch),
    do: join_target(plugin, Host.target(overwatch, plugin.opts[:entry_channel], false))

  defp join_target(%Plugin{single: true, namespace: namespace, id: name}, target) do
    # The one copy of a `single` plugin runs beside its home, wherever the hub
    # placed it: the running one is taken first, so a copy retiring in a
    # hand-over, which refuses, is passed over for its successor; one that
    # stopped when its last relay left is started again by the home.
    Enum.find_value(Directory.plugin_hosts(namespace, name), &joined(join_copy(&1, target))) ||
      with({:ok, copy} <- home_copy(namespace, name), do: join_copy(copy, target))
  end

  defp join_target(%Plugin{} = plugin, target) do
    with {:ok, copy} <- locate(plugin) do
      case join_copy(copy, target) do
        # The copy stopped idle while the join waited on it; the next locate
        # finds it gone and starts another.
        {:error, {:unreachable, _}} ->
          with {:ok, copy} <- locate(plugin), do: join_copy(copy, target)

        joined ->
          joined
      end
    end
  end

  defp joined({:ok, _copy} = joined), do: joined
  defp joined({:error, _refused}), do: nil

  defp join_copy(copy, target) do
    case call(copy, {:join, target}) do
      :ok -> {:ok, copy}
      {:error, _} = refused -> refused
    end
  end

  # A controller plugin's copy is restarted when it fails: nothing joins it to
  # start it again.
  defp locate(%Plugin{namespace: namespace, id: name} = plugin, home \\ nil) do
    WorkerSup.ensure_worker(
      WorkerSup.family(__MODULE__),
      {to_string(namespace), name},
      {__MODULE__, %{namespace: to_string(namespace), plugin: entry(plugin), home: home}},
      &Process.alive?/1,
      if(Plugin.controller?(plugin), do: :transient, else: :temporary)
    )
  end

  @doc """
  A controller plugin's entry with its hooks canonical and checked against the
  catalogue and its provider, and its options against its declaration and its
  provider's preparation, which does no I/O. A controller plugin is never bound
  to a relay, and names a hook unless its provider runs a worker and takes
  no hook at all.
  """
  @spec checked_entry(Plugin.t()) :: {:ok, Plugin.t()} | {:error, term()}
  def checked_entry(%Plugin{id: name, relay_id: relay_id}) when relay_id != nil,
    do: {:error, {:plugin_not_bindable, name}}

  def checked_entry(%Plugin{provider: provider} = plugin) do
    table = Table.new(__extensions_catalog__())

    with :ok <- hooked(plugin),
         {:ok, plugin} <- Table.canonicalise(table.catalog, plugin),
         :ok <- Subscription.validate_subscriptions(table, plugin),
         {:ok, opts} <- Options.normalise_opts(provider, plugin.opts),
         prepared when elem(prepared, 0) == :ok <- provider.init_provider(opts),
         do: {:ok, plugin}
  end

  defp hooked(%Plugin{hooks: [], provider: provider}) do
    case provider.__provider__() do
      %{worker: worker, extensions: []} when worker != nil -> :ok
      _hooks_or_no_worker -> {:error, {:invalid_hook, :empty}}
    end
  end

  defp hooked(%Plugin{}), do: :ok

  @doc """
  Run the copy of the controller plugin `entry` on this controller for its
  `home`, unless this controller lacks its provider. The copy stops when the
  home that placed it last stops: a disable stops the home, and a home that
  starts again, or elsewhere, places its copies again.
  """
  @spec place(Plugin.t(), pid()) :: :ok
  def place(%Plugin{} = entry, home) do
    with true <- Plugin.controller?(entry),
         {:ok, copy} <- locate(entry, home),
         do: GenServer.cast(copy, {:home, home})

    :ok
  end

  defp home_copy(namespace, name) do
    case Home.row(namespace, name) do
      {_spec, [{_node, home} | _]} -> call(home, :copy)
      _none -> {:error, {:plugin_not_running, name}}
    end
  end

  # What a copy runs: the plugin with only the options a user writes, no
  # relay, and every hook it can be bound on, so it holds what any binding
  # needs (a websocket's outbound half); a controller plugin's hooks are its
  # entry's own. It keeps the channel it was started with, which a provider may
  # require; each bound relay's own is its target's.
  @doc false
  def entry(%Plugin{provider: provider} = plugin) do
    hooks =
      if Plugin.controller?(plugin),
        do: plugin.hooks,
        else: for(hook <- Decl.io_hooks(provider.__provider__()), do: {hook, []})

    %Plugin{Plugin.declared_opts_only(plugin) | relay_id: nil, hooks: hooks}
  end

  # A changed entry keeps the channel the running copy was started with.
  defp updated(%Plugin{opts: running}, %Plugin{} = changed),
    do: entry(Plugin.resolve(%Plugin{changed | opts: Map.take(running, @channel_opts)}, changed))

  @doc "Leave `copy`: `:now` stops a copy left empty at once, `:idle` after the idle stop."
  @spec leave(pid(), pid(), :now | :idle) :: :ok
  def leave(copy, overwatch, how), do: GenServer.cast(copy, {:leave, overwatch, how})

  @doc """
  The copy's health: the provider's own verdict, or `plugin_lagging` while the
  copy has dropped hook events, or records a provider wrongly injected into it,
  within the last probe window.
  """
  @spec health(pid()) :: Flezha.Plugin.ProviderBehaviour.probe_result()
  def health(copy) do
    case call(copy, :health) do
      {:error, {:unreachable, reason}} -> {:fail_open, {:shared_copy_unreachable, reason}}
      verdict -> verdict
    end
  end

  @doc """
  The controllers where the enabled `plugin` runs, and those where a
  controller plugin's copy lags. A copy waiting for its workers to start runs
  nowhere. A relay plugin runs where its shared copy runs and on each node
  `relays` (each read relay's node with the plugin ids it has running) names
  for it.
  """
  @spec running_on(Plugin.t(), [{node(), [binary()]}]) ::
          %{controllers: [node()], lagging: [node()]}
  def running_on(%Plugin{namespace: namespace, id: name} = plugin, relays) do
    if Plugin.controller?(plugin),
      do: copies_running_on(namespace, name),
      else: %{controllers: relay_plugin_nodes(namespace, name, relays), lagging: []}
  end

  defp relay_plugin_nodes(namespace, name, relays) do
    on_relays = for {on, ids} <- relays, Enum.any?(ids, &(Plugin.base_id(&1) == name)), do: on
    Enum.sort(Enum.uniq(Directory.plugin_nodes(namespace, name) ++ on_relays))
  end

  defp copies_running_on(namespace, name) do
    verdicts = for copy <- Directory.plugin_hosts(namespace, name), do: {node(copy), health(copy)}

    %{
      controllers: for({on, verdict} <- verdicts, not waiting?(verdict), do: on) |> Enum.sort(),
      lagging: for({on, {:unhealthy, {:plugin_lagging, _}}} <- verdicts, do: on)
    }
  end

  defp waiting?(verdict), do: match?({:unhealthy, {:plugin_not_started, _}}, verdict)

  @doc "Apply a changed entry: in place when `mode` is `:live`, by a restart otherwise."
  @spec update(pid(), Plugin.t(), :live | :restart) :: :ok
  def update(copy, %Plugin{} = entry, mode), do: GenServer.cast(copy, {:update, entry, mode})

  @doc """
  Stop `copy` once a copy that replaces it runs with its upstream open: the old
  copy of a `single` plugin whose home moved, so the new connection opens
  before the old one closes. The successor's provider says its upstream is not
  open yet with `connected: false` in its `inspect_state/1`. A copy nobody is
  subscribed to stops at once, and one whose successor never comes or never
  opens stops after `plugin_idle_stop_ms`.
  """
  @spec retire(pid()) :: :ok
  def retire(copy), do: GenServer.cast(copy, :retire)

  @doc """
  The copies of `namespace`'s plugins on this controller that are retiring,
  all asked at once under `plugin_call_timeout_ms`, never past `deadline`
  (monotonic milliseconds): one that cannot answer in time counts as retiring.
  """
  @spec retiring(String.t(), integer()) :: [pid()]
  def retiring(namespace, deadline) do
    left = max(deadline - System.monotonic_time(:millisecond), 0)
    ask = &GenServer.call(&1, :retiring?, :infinity)

    for {copy, answer} <-
          Parallel.results(Directory.local_plugin_hosts(namespace), ask,
            timeout: min(Durations.get(:plugin_call_timeout_ms), left)
          ),
        answer != {:ok, false},
        do: copy
  end

  @doc "Stop `copy` and the workers it holds."
  @spec stop(pid()) :: :ok
  def stop(copy) do
    GenServer.stop(copy, :normal, Durations.get(:plugin_call_timeout_ms))
  catch
    :exit, _gone -> :ok
  end

  @doc "The targets of the relays subscribed to `copy`: a `single` hand-over's state."
  @spec subscribers(pid()) :: [Host.target()]
  def subscribers(copy), do: call(copy, :subscribers)

  @doc "Subscribe every relay a hand-over carried."
  @spec adopt(pid(), [Host.target()]) :: :ok
  def adopt(copy, targets), do: Enum.each(targets, &call(copy, {:join, &1}))

  defp call(copy, request) do
    GenServer.call(copy, request, Durations.get(:plugin_call_timeout_ms))
  catch
    :exit, reason -> {:error, {:unreachable, reason}}
  end

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(%{plugin: %Plugin{}} = args) do
    case Map.fetch(args, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, args, name: name)
      :error -> GenServer.start_link(__MODULE__, args)
    end
  end

  ## Server

  @impl true
  def init(%{namespace: namespace, plugin: %Plugin{id: name} = plugin} = args) do
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      namespace: namespace,
      name: name,
      plugin: plugin,
      table: Table.new(catalog(plugin))
    }

    case activate(state) do
      {:ok, state} ->
        {:ok, state |> placed(args[:home]) |> listen_for_hooks() |> idle_if_empty()}

      # Nothing joins a controller plugin's copy to start it again: it waits,
      # retrying as one whose worker went down does.
      {:error, reason} ->
        if Plugin.controller?(plugin),
          do: {:ok, state |> placed(args[:home]) |> failed(reason, :start)},
          else: {:stop, reason}
    end
  end

  # The plugin subscribed and its workers started, or the refusal with
  # whatever started torn down.
  defp activate(%{table: table, name: name, plugin: plugin} = state) do
    with {:ok, activated} <- Table.bulk_subscribe(table, [{name, plugin}]),
         {activated, []} <- Table.bulk_activate(activated, activation_env(state)) do
      {:ok, %{state | table: activated, waiting: nil}}
    else
      {:error, {_id, reason}} ->
        {:error, reason}

      {activated, [{_id, reason} | _]} ->
        Table.teardown_all(activated)
        {:error, reason}
    end
  end

  defp placed(state, home) do
    Directory.join_plugin(self(), state.namespace, state.name)
    watch_home(state, home)
  end

  defp catalog(plugin) do
    if Plugin.controller?(plugin),
      do: __extensions_catalog__(),
      else: Overwatch.__extensions_catalog__()
  end

  # A copy without `single` hears its own controller, the one `single` copy
  # every controller. Listening again after a restart follows changed hooks.
  defp listen_for_hooks(%{plugin: plugin} = state) do
    if Plugin.controller?(plugin) do
      reach = if plugin.single, do: :anywhere, else: :here

      groups =
        MapSet.new(plugin.hooks, fn {hook, _} ->
          Directory.hook_group(reach, state.namespace, hook)
        end)

      %{
        state
        | hooks: state.hooks || hook_forwarder(),
          hook_groups: Directory.sync_membership(groups, state.hook_groups)
      }
    else
      state
    end
  end

  # Watching only the home that placed the copy last, which may be a new one
  # after a move.
  defp watch_home(%{home: home} = state, home), do: state
  defp watch_home(state, nil), do: state

  defp watch_home(state, home) do
    watch = if state.home, do: Watch.unwatch(state.watch, state.home, :home), else: state.watch
    %{state | home: home, watch: Watch.watch(watch, home, :home)}
  end

  defp hook_forwarder do
    {:ok, forwarder} = Forwarder.start_link(self())
    forwarder
  end

  @impl true
  def handle_call({:join, _target}, _from, %{retiring: ref} = state)
      when is_reference(ref),
      do: {:reply, {:error, :retiring}, state}

  def handle_call({:join, %{pid: overwatch} = target}, _from, state) do
    state = cancel_timer(state, :idle_stop)

    state =
      if Map.has_key?(state.subscribers, overwatch),
        do: state,
        else: %{state | watch: Watch.watch(state.watch, overwatch, :subscriber)}

    {:reply, :ok, retarget(put_in(state.subscribers[overwatch], target))}
  end

  def handle_call(:health, _from, %{waiting: reason} = state) when reason != nil,
    do: {:reply, {:unhealthy, {:plugin_not_started, reason}}, state}

  def handle_call(:health, _from, state),
    do:
      {:reply, lagging(state) || Map.get(Table.health(state.table), state.name, :healthy), state}

  def handle_call(:connecting?, _from, state), do: {:reply, connecting?(state), state}

  def handle_call(:retiring?, _from, state), do: {:reply, is_reference(state.retiring), state}

  # The forwarder's next hook event: run in a task, answered when its chain
  # returns, so the forwarder sends the one after.
  def handle_call({:hook, %{hook: hook} = event}, from, %{table: table} = state) do
    {:ok, task} =
      Task.Supervisor.start_child(@task_sup, fn ->
        try do
          Extension.run(table, hook, event)
        after
          GenServer.reply(from, :ok)
        end
      end)

    {:noreply, %{state | hook_task: task}}
  end

  def handle_call(:subscribers, _from, state),
    do: {:reply, Map.values(state.subscribers), state}

  def handle_call({kind, _message, _metadata}, _from, state) when kind in [:io_in, :ingest],
    do: {:reply, {:error, :not_a_target}, not_a_target(state)}

  @impl true
  def handle_cast({:io_in, _message, _metadata}, state), do: {:noreply, not_a_target(state)}

  def handle_cast({:home, home}, state), do: {:noreply, watch_home(state, home)}

  def handle_cast({:leave, overwatch, how}, state) do
    case Map.pop(state.subscribers, overwatch) do
      {nil, _} -> {:noreply, state}
      {_target, rest} -> left(%{state | subscribers: rest}, overwatch, how)
    end
  end

  # A committed value describes the one copy every relay shares, so it
  # belongs to the plugin's entry: it is written through a relay bound to
  # it, the one path that holds the start authority's write.
  def handle_cast({:plugin_opt_persist, id, key, value}, state) do
    table = Table.put_plugin_opt(state.table, id, key, value)

    case Map.keys(state.subscribers) do
      [overwatch | _] -> GenServer.cast(overwatch, {:plugin_opt_persist, id, key, value})
      [] -> :ok
    end

    {:noreply, %{state | table: table}}
  end

  def handle_cast(:retire, %{subscribers: subscribers} = state) when subscribers == %{},
    do: {:stop, @retired, state}

  def handle_cast(:retire, state) do
    case Directory.monitor_plugin(state.namespace, state.name) do
      {ref, copies} -> retire_once_open(%{retire_after(state) | retiring: ref}, copies)
      nil -> {:stop, @retired, state}
    end
  end

  def handle_cast({:dropped, count}, state), do: {:noreply, dropped(state, count)}

  def handle_cast({:update, %Plugin{} = changed, :live}, state) do
    entry = updated(state.plugin, changed)

    case Table.reconfigure(state.table, state.name, entry) do
      {:ok, table} ->
        {:noreply, %{state | table: table, plugin: entry}}

      {:error, reason} ->
        Logger.warning("[Plugin.Shared] #{state.name} refused a live update: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  def handle_cast({:update, %Plugin{} = changed, :restart}, state),
    do: {:noreply, restart(%{state | plugin: updated(state.plugin, changed)})}

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Watch.down(state.watch, ref) do
      {[:home], watch} ->
        {:stop, :normal, %{state | watch: watch}}

      {tags, watch} when is_list(tags) ->
        left(%{state | subscribers: Map.delete(state.subscribers, pid), watch: watch}, pid, :idle)

      _not_a_subscriber ->
        worker_down(state, ref, pid, reason)
    end
  end

  def handle_info(:idle_stop, %{subscribers: subscribers} = state) when subscribers == %{},
    do: {:stop, :normal, timer_fired(state, :idle_stop)}

  def handle_info(:idle_stop, state), do: {:noreply, timer_fired(state, :idle_stop)}

  # A retiring copy stops when its successor runs with its upstream open, or
  # when none did in time.
  def handle_info({ref, :join, _group, copies}, %{retiring: ref} = state),
    do: retire_once_open(state, copies)

  def handle_info(:successor, state) do
    copies = Directory.plugin_hosts(state.namespace, state.name)
    retire_once_open(timer_fired(state, :successor), copies)
  end

  def handle_info(:retired, state) do
    if timer_armed?(state, :successor),
      do: Logger.warning("[Plugin.Shared] #{state.name} retired before its successor opened")

    {:stop, @retired, timer_fired(state, :retired)}
  end

  def handle_info(:restart, state), do: {:noreply, restart(timer_fired(state, :restart))}

  def handle_info({:hook, event}, %{hooks: forwarder} = state) when is_pid(forwarder) do
    Forwarder.push(forwarder, event)
    {:noreply, state}
  end

  def handle_info({:EXIT, forwarder, reason}, %{hooks: forwarder} = state),
    do: {:stop, reason, state}

  def handle_info(_other, state), do: {:noreply, state}

  # A crash report shows the plugin without its secrets, and the table, which
  # holds every copy of its options, by the plugins it holds.
  @impl true
  def format_status(status) do
    Map.update(status, :state, nil, fn
      %__MODULE__{plugin: %Plugin{} = plugin, table: table} = state ->
        %{
          state
          | plugin: %Plugin{plugin | opts: Plugin.redact(plugin.opts)},
            table: Table.subscribed_ids(table)
        }

      other ->
        other
    end)
  end

  # A hook still running, or waiting, goes with its plugin.
  @impl true
  def terminate(_reason, state) do
    if state.hooks, do: Forwarder.stop(state.hooks)
    if state.hook_task, do: Task.Supervisor.terminate_child(@task_sup, state.hook_task)
    Table.teardown_all(state.table, :now)
    :ok
  end

  ## Private

  # A feeding provider's workers inject into the bound relays themselves;
  # each is told of every change of them, so none looks them up per record.
  defp retarget(%{plugin: %Plugin{provider: provider}, table: table, name: name} = state) do
    if provider.__provider__().feeds do
      targets = Map.values(state.subscribers)

      Enum.each(Table.workers(table, name), fn {_key, worker} ->
        Host.retarget(worker, targets)
      end)
    end

    state
  end

  # A feeding provider that injects into its `overwatcher_pid`, the copy, not
  # into `Flezha.Plugin.Host.targets/1`: its record is dropped and counted as
  # a hook event the copy could not take is.
  defp not_a_target(state) do
    state = dropped(state, 1)

    unless state.misfed do
      Logger.warning(
        "[Plugin.Shared] #{state.name}: #{inspect(state.plugin.provider)} injected into its " <>
          "shared copy, which drops the record; a shareable source injects into " <>
          "Flezha.Plugin.Host.targets/1"
      )
    end

    %{state | misfed: true}
  end

  defp activation_env(state),
    do:
      Map.merge(@activation_env, %{targets: Map.values(state.subscribers), targets_from: self()})

  defp left(state, overwatch, how) do
    {_tags, watch} = Watch.forget(state.watch, overwatch)
    state = retarget(%{state | watch: watch})

    cond do
      state.subscribers != %{} -> {:noreply, state}
      how == :now -> {:stop, :normal, state}
      true -> {:noreply, idle_if_empty(state)}
    end
  end

  defp idle_if_empty(%{hooks: nil, subscribers: subscribers} = state) when subscribers == %{} do
    if timer_armed?(state, :idle_stop),
      do: state,
      else: arm_timer(state, :idle_stop, Durations.get(:plugin_idle_stop_ms), :idle_stop)
  end

  defp idle_if_empty(state), do: state

  defp retire_after(state),
    do: arm_timer(state, :retired, Durations.get(:plugin_idle_stop_ms), :retired)

  defp retire_once_open(state, copies) do
    successors = Enum.reject(copies, &(&1 == self()))

    cond do
      successors == [] ->
        {:noreply, state}

      Enum.any?(successors, &(call(&1, :connecting?) == false)) ->
        {:stop, @retired, state}

      true ->
        {:noreply,
         arm_timer(state, :successor, Durations.get(:plugin_successor_poll_ms), :successor)}
    end
  end

  # Read through the caller-facing report rather than `health/1`: a copy whose
  # upstream is only opening must not read unhealthy, or the convergence loop
  # restarts it.
  defp connecting?(%{table: table, name: name}),
    do: match?(%{connected: false}, Health.inspect_state(table, name))

  # The provider's own worker went down: the copy restarts it after a probe
  # window, since no convergence loop watches a table that belongs to no relay.
  defp worker_down(state, ref, pid, reason) do
    case Table.handle_down(state.table, ref, pid, reason) do
      {:ok, table} ->
        {:noreply, restart_later(%{state | table: table})}

      :ignore ->
        {:noreply, state}
    end
  end

  # An update restarts a waiting copy at once, so its retry is no longer due.
  defp restart(state) do
    state = %{cancel_timer(state, :restart) | table: Table.teardown_all(state.table, :now)}

    case activate(state) do
      {:ok, state} -> listen_for_hooks(state)
      {:error, reason} -> failed(state, reason, :restart)
    end
  end

  # The refusal alone: the table carries the plugin's options, a token among them.
  defp failed(state, reason, attempt) do
    Logger.warning("[Plugin.Shared] #{state.name} did not #{attempt}: #{inspect(reason)}")
    restart_later(%{state | waiting: reason})
  end

  defp restart_later(state),
    do: arm_timer(state, :restart, Durations.get(:convergence_tick_ms), :restart)

  defp lagging(state) do
    case recent_drops(state, System.monotonic_time(:millisecond)) do
      {0, _at} -> nil
      {count, _at} -> {:unhealthy, {:plugin_lagging, count}}
    end
  end

  defp dropped(state, count) do
    now = System.monotonic_time(:millisecond)
    {total, _at} = recent_drops(state, now)
    %{state | dropped: {total + count, now}}
  end

  # What the copy dropped within the last probe window.
  defp recent_drops(state, now) do
    case state.dropped do
      {count, at} ->
        if now - at <= Durations.get(:convergence_tick_ms), do: {count, at}, else: {0, at}

      nil ->
        {0, nil}
    end
  end
end

defmodule Flezha.Plugin.Shared do
  @moduledoc """
  The running copy of a shareable plugin: one per `(namespace, name)` on each
  controller where an operator bound to it runs, started and found through
  `Flezha.Plugin.Provider.WorkerSup`, or the cluster's one copy of a `single`
  plugin, which its `Flezha.Plugin.Home` starts beside itself.

  It holds the plugin in an extension table of its own, which owns the
  provider's workers, and it is the `overwatcher_pid` those workers are given,
  so no provider changes how it injects. A record a worker hands it goes to the
  forwarder of every bound operator (`Flezha.Plugin.Shared.Forwarder`) and is
  answered at once: one operator never holds the upstream.

  When its last operator leaves it stops: at once when the leave was
  deliberate, after `plugin_idle_stop_ms` when the operator went down or moved,
  unless one joins first. Every copy is a member of the plugin's directory
  group, which is how a disable, an update and `flz plugin list` find it.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:idle_stop, :retired, :restart, :successor]

  require Logger

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Extension.Table.Health
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Decl
  alias Flezha.Plugin.Shared.Forwarder
  alias Flezha.Process.Watch

  @channel_opts [:channel, :channel_in, :channel_out]

  # Its subscribers read the reason as "join the copy that runs now".
  @retired {:shutdown, :retired}

  defstruct [
    :namespace,
    :name,
    :plugin,
    :table,
    :retiring,
    subscribers: %{},
    watch: Watch.new(),
    dropped: %{},
    timers: %{}
  ]

  ## Client

  @doc """
  Subscribe `overwatch` to the running copy of `plugin`, starting the one on
  this controller when none runs. `plugin` is the binding as the operator's
  table resolved it; the operator's entry channel is stamped on every record
  its forwarder injects.
  """
  @spec join(Plugin.t(), pid()) :: {:ok, pid()} | {:error, term()}
  def join(%Plugin{single: true, namespace: namespace, id: name} = plugin, overwatch) do
    # The one copy of a `single` plugin runs beside its home, wherever the hub
    # placed it: the running one is taken first, so a copy retiring in a
    # hand-over, which refuses, is passed over for its successor; one that
    # stopped when its last operator left is started again by the home.
    Enum.find_value(
      Directory.plugin_hosts(namespace, name),
      &joined(join_copy(&1, plugin, overwatch))
    ) ||
      with({:ok, copy} <- home_copy(namespace, name), do: join_copy(copy, plugin, overwatch))
  end

  def join(%Plugin{} = plugin, overwatch) do
    with {:ok, copy} <- locate(plugin) do
      case join_copy(copy, plugin, overwatch) do
        # The copy stopped idle while the join waited on it; the next locate
        # finds it gone and starts another.
        {:error, {:unreachable, _}} ->
          with {:ok, copy} <- locate(plugin), do: join_copy(copy, plugin, overwatch)

        joined ->
          joined
      end
    end
  end

  defp joined({:ok, _copy} = joined), do: joined
  defp joined({:error, _refused}), do: nil

  defp join_copy(copy, plugin, overwatch) do
    case call(copy, {:join, overwatch, plugin.opts[:entry_channel]}) do
      :ok -> {:ok, copy}
      {:error, _} = refused -> refused
    end
  end

  defp locate(%Plugin{namespace: namespace, id: name} = plugin) do
    WorkerSup.ensure_worker(
      WorkerSup.family(__MODULE__),
      {to_string(namespace), name},
      {__MODULE__, %{namespace: to_string(namespace), plugin: entry(plugin)}}
    )
  end

  defp home_copy(namespace, name) do
    case ProcessHub.child_lookup(Identificator.hub(namespace), {:plugin, name}) do
      {_spec, [{_node, home} | _]} -> call(home, :copy)
      _none -> {:error, {:plugin_not_running, name}}
    end
  end

  # What a copy runs: the plugin with only the options a user writes, no
  # operator, and every hook it can be bound on, so it holds what any binding
  # needs (a websocket's outbound half). It keeps the channel it was started
  # with, which a provider may require; `fan_out/3` takes that stamp off again
  # and each forwarder stamps its own operator's.
  @doc false
  def entry(%Plugin{provider: provider} = plugin) do
    hooks = for hook <- Decl.io_hooks(provider.__provider__()), do: {hook, []}
    %Plugin{Plugin.declared_opts_only(plugin) | operator_id: nil, hooks: hooks}
  end

  # A changed entry keeps the channel the running copy was started with.
  defp updated(%Plugin{opts: running}, %Plugin{} = changed),
    do: entry(Plugin.resolve(%Plugin{changed | opts: Map.take(running, @channel_opts)}, changed))

  @doc "Leave `copy`: `:now` stops a copy left empty at once, `:idle` after the idle stop."
  @spec leave(pid(), pid(), :now | :idle) :: :ok
  def leave(copy, overwatch, how), do: GenServer.cast(copy, {:leave, overwatch, how})

  @doc """
  The copy's health for `overwatch`: `plugin_lagging` while its forwarder has
  dropped records within the last probe window, the provider's own verdict
  otherwise.
  """
  @spec health(pid(), pid()) :: Flezha.Plugin.ProviderBehaviour.probe_result()
  def health(copy, overwatch) do
    case call(copy, {:health, overwatch}) do
      {:error, {:unreachable, reason}} -> {:fail_open, {:shared_copy_unreachable, reason}}
      verdict -> verdict
    end
  end

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

  @doc "Stop `copy` and the workers it holds."
  @spec stop(pid()) :: :ok
  def stop(copy) do
    GenServer.stop(copy, :normal, Durations.get(:plugin_call_timeout_ms))
  catch
    :exit, _gone -> :ok
  end

  @doc "The operators subscribed to `copy`, with their channels: a `single` hand-over's state."
  @spec subscribers(pid()) :: [{pid(), binary() | nil}]
  def subscribers(copy), do: call(copy, :subscribers)

  @doc "Subscribe every operator a hand-over carried."
  @spec adopt(pid(), [{pid(), binary() | nil}]) :: :ok
  def adopt(copy, subscribers),
    do:
      Enum.each(subscribers, fn {overwatch, channel} ->
        call(copy, {:join, overwatch, channel})
      end)

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
  def init(%{namespace: namespace, plugin: %Plugin{id: name} = plugin}) do
    Process.flag(:trap_exit, true)
    table = Table.new(Overwatch.__extensions_catalog__())

    with {:ok, table} <- Table.bulk_subscribe(table, [{name, plugin}]),
         {table, []} <- Table.bulk_activate(table) do
      Directory.join_plugin(self(), namespace, name)
      state = %__MODULE__{namespace: namespace, name: name, plugin: plugin, table: table}
      {:ok, idle_if_empty(state)}
    else
      {:error, {_id, reason}} -> {:stop, reason}
      {table, [{_id, reason}]} -> {:stop, teardown(table, reason)}
    end
  end

  defp teardown(table, reason) do
    Table.teardown_all(table)
    reason
  end

  @impl true
  def handle_call({:join, _overwatch, _channel}, _from, %{retiring: ref} = state)
      when is_reference(ref),
      do: {:reply, {:error, :retiring}, state}

  def handle_call({:join, overwatch, channel}, _from, state) do
    state = cancel_timer(state, :idle_stop)

    case Map.get(state.subscribers, overwatch) do
      %{forwarder: forwarder} = sub ->
        Forwarder.restamp(forwarder, channel)
        {:reply, :ok, put_in(state.subscribers[overwatch], %{sub | channel: channel})}

      nil ->
        {:ok, forwarder} = Forwarder.start_link(%{overwatch: overwatch, channel: channel})
        state = %{state | watch: Watch.watch(state.watch, overwatch, :subscriber)}

        {:reply, :ok,
         put_in(state.subscribers[overwatch], %{forwarder: forwarder, channel: channel})}
    end
  end

  def handle_call({:health, overwatch}, _from, state) do
    {:reply,
     lagging(state, overwatch) || Map.get(Table.health(state.table), state.name, :healthy), state}
  end

  def handle_call(:connecting?, _from, state), do: {:reply, connecting?(state), state}

  def handle_call(:subscribers, _from, state),
    do: {:reply, Enum.map(state.subscribers, fn {ow, sub} -> {ow, sub.channel} end), state}

  # A worker's injection, in any mode: every forwarder takes it, and the
  # worker is answered at once.
  def handle_call({kind, message, metadata}, _from, state) when kind in [:io_in, :ingest] do
    fan_out(state, message, metadata)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:io_in, message, metadata}, state) do
    fan_out(state, message, metadata)
    {:noreply, state}
  end

  def handle_cast({:leave, overwatch, how}, state) do
    case Map.pop(state.subscribers, overwatch) do
      {nil, _} -> {:noreply, state}
      {sub, rest} -> left(%{state | subscribers: rest}, overwatch, sub, how)
    end
  end

  # A committed value describes the one copy every operator shares, so it
  # belongs to the plugin's entry: it is written through an operator bound to
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

  def handle_cast({:dropped, overwatch, count}, state) do
    now = System.monotonic_time(:millisecond)
    {total, _at} = recent_drops(state, overwatch, now)
    {:noreply, put_in(state.dropped[overwatch], {total + count, now})}
  end

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
      {tags, watch} when is_list(tags) ->
        {sub, rest} = Map.pop(state.subscribers, pid)
        left(%{state | subscribers: rest, watch: watch}, pid, sub, :idle)

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

  def handle_info({:EXIT, forwarder, reason}, state) do
    case Enum.find(state.subscribers, fn {_ow, sub} -> sub.forwarder == forwarder end) do
      {overwatch, _sub} ->
        Logger.warning("[Plugin.Shared] #{state.name} forwarder ended: #{inspect(reason)}")
        {_tags, watch} = Watch.forget(state.watch, overwatch)
        subscribers = Map.delete(state.subscribers, overwatch)
        {:noreply, idle_if_empty(%{state | subscribers: subscribers, watch: watch})}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Table.teardown_all(state.table, :now)
    :ok
  end

  ## Private

  defp fan_out(state, message, metadata) do
    metadata = unstamped(state, metadata)

    Enum.each(state.subscribers, fn {_ow, sub} ->
      Forwarder.push(sub.forwarder, message, metadata)
    end)
  end

  # A channel the copy's own options name was stamped by its worker, and no
  # other writer of that key is allowed beside it, so it is the starting
  # operator's and comes off.
  defp unstamped(%{table: %Table{plugins: plugins}, name: name}, metadata) do
    case plugins do
      %{^name => %Plugin{opts: %{entry_channel: channel}}} when channel != nil ->
        Map.delete(metadata, "channel")

      _ ->
        metadata
    end
  end

  defp left(state, overwatch, sub, how) do
    {_tags, watch} = Watch.forget(state.watch, overwatch)
    Forwarder.stop(sub.forwarder)
    state = %{state | watch: watch, dropped: Map.delete(state.dropped, overwatch)}

    cond do
      state.subscribers != %{} -> {:noreply, state}
      how == :now -> {:stop, :normal, state}
      true -> {:noreply, idle_if_empty(state)}
    end
  end

  defp idle_if_empty(%{subscribers: subscribers} = state) when subscribers == %{} do
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
  # window, since no convergence loop watches a table that belongs to no operator.
  defp worker_down(state, ref, pid, reason) do
    case Table.handle_down(state.table, ref, pid, reason) do
      {:ok, table} ->
        {:noreply, restart_later(%{state | table: table})}

      :ignore ->
        {:noreply, state}
    end
  end

  defp restart(state) do
    table = Table.teardown_all(state.table, :now)

    with {:ok, table} <- Table.bulk_subscribe(table, [{state.name, state.plugin}]),
         {table, []} <- Table.bulk_activate(table) do
      %{state | table: table}
    else
      failed ->
        Logger.warning("[Plugin.Shared] #{state.name} did not restart: #{inspect(failed)}")
        restart_later(%{state | table: table})
    end
  end

  defp restart_later(state),
    do: arm_timer(state, :restart, Durations.get(:convergence_tick_ms), :restart)

  defp lagging(state, overwatch) do
    case recent_drops(state, overwatch, System.monotonic_time(:millisecond)) do
      {0, _at} -> nil
      {count, _at} -> {:unhealthy, {:plugin_lagging, count}}
    end
  end

  # What `overwatch`'s forwarder dropped within the last probe window.
  defp recent_drops(state, overwatch, now) do
    case Map.get(state.dropped, overwatch) do
      {count, at} ->
        if now - at <= Durations.get(:convergence_tick_ms), do: {count, at}, else: {0, at}

      nil ->
        {0, nil}
    end
  end
end

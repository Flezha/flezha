defmodule Flezha.Plugin.Home do
  @moduledoc """
  A named plugin's hub child: the process the namespace hub places for the
  `{:plugin, name}` entry of its declared set, so a plugin is recorded, placed,
  restored and stopped exactly as a relay is.

  A plugin without `single` runs nothing here: its copies run where its
  relays run (`Flezha.Plugin.Shared`). A `single` plugin's one copy runs
  beside this home: started with it, stopped after the idle stop while no
  relay joins it, and started again by the next one that does. On a move
  the old copy outlives its home, which a cold swap stops first: while relays
  are subscribed it keeps running until the new home's copy has its upstream
  open, so the new connection opens before the old one closes, and a copy
  nobody reads stops at once.

  A controller plugin has no relay to start its copies, so the home of one
  without `single` places them: when it starts, and on every join or leave in
  its namespace's hub group, it runs a copy on each controller holding the hub
  (one that lacks the provider runs none) and stops the copy where the hub
  left.
  """

  use GenServer
  use ProcessHub.Strategy.Migration.HotSwap
  use ProcessHub.Strategy.Migration.ColdSwap, declare_behaviour: false

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Plugin.Shared
  alias Flezha.Process.Cspec
  alias Flezha.Process.Hub

  @doc """
  Run `fun` inside the home of the plugin `name`, one call at a time: a bind of
  a plugin that cannot be shared checks who holds it and writes its binding
  there, so two binds cannot both find it free.
  """
  @spec serialized(binary(), binary(), (-> result)) :: result | {:error, term()}
        when result: term()
  def serialized(namespace, name, fun) do
    case row(namespace, name, :hub_down) do
      {_spec, [{_node, home} | _]} ->
        GenServer.call(home, {:serialized, fun}, Durations.get(:route_call_timeout_ms))

      :hub_down ->
        {:error, :registry_unavailable}

      _none ->
        {:error, {:plugin_not_found, name}}
    end
  catch
    :exit, reason -> {:error, {:unreachable, reason}}
  end

  @doc "The `{:plugin, name}` row of `namespace`'s hub, `nil` while it has none, `on_dead` while it is down."
  @spec row(binary(), binary(), term()) :: {map(), [{node(), pid()}]} | nil | term()
  def row(namespace, name, on_dead \\ nil),
    do:
      Hub.with_live(
        Identificator.hub(namespace),
        on_dead,
        &ProcessHub.child_lookup(&1, {:plugin, name})
      )

  # The copy is linked: one that stopped because its last relay left is
  # started again by the next one that joins; one that died takes its home
  # along, and the hub starts it again.
  @impl true
  def handle_info({:EXIT, copy, :normal}, %{copy: copy} = state),
    do: {:noreply, %{state | copy: nil}}

  def handle_info({:EXIT, copy, reason}, %{copy: copy} = state), do: {:stop, reason, state}

  def handle_info({ref, change, _group, _hubs}, %{hubs: ref} = state)
      when change in [:join, :leave],
      do: {:noreply, place(state)}

  def handle_info(_other, state), do: {:noreply, state}

  @spec start_link({binary(), Plugin.t()}) :: GenServer.on_start()
  def start_link({namespace, %Plugin{} = entry}),
    do: GenServer.start_link(__MODULE__, {namespace, entry})

  @impl true
  def init({namespace, %Plugin{id: name, single: single} = entry}) do
    Process.flag(:trap_exit, true)
    state = %{namespace: namespace, name: name, copy: nil, entry: entry, hubs: nil}

    case single && start_copy(state, entry) do
      {:ok, state} -> started(state)
      {:error, reason} -> {:stop, reason}
      false -> started(state)
    end
  end

  defp started(%{entry: entry} = state),
    do:
      if(Plugin.controller?(entry) and not entry.single,
        do: {:ok, state, {:continue, :place}},
        else: {:ok, state}
      )

  @impl true
  def handle_continue(:place, %{namespace: namespace} = state) do
    {ref, _hubs} = Directory.monitor_hubs(namespace)
    {:noreply, place(%{state | hubs: ref})}
  end

  defp place(%{namespace: namespace, name: name} = state) do
    %Plugin{} =
      entry =
      case current_entry(state) do
        {:ok, entry} -> entry
        {:error, _unreadable} -> state.entry
      end

    entry = %Plugin{entry | namespace: namespace}

    hubs = Directory.hub_nodes(namespace)

    _ =
      :erpc.multicall(
        hubs,
        Shared,
        :place,
        [entry, self()],
        Durations.get(:plugin_call_timeout_ms)
      )

    for copy <- Directory.plugin_hosts(namespace, name),
        node(copy) not in hubs,
        do: Shared.stop(copy)

    state
  end

  @impl true
  def handle_call(:copy, _from, %{copy: copy} = state) when is_pid(copy),
    do: {:reply, {:ok, copy}, state}

  def handle_call(:copy, _from, state) do
    with {:ok, entry} <- current_entry(state),
         {:ok, state} <- start_copy(state, entry) do
      {:reply, {:ok, state.copy}, state}
    else
      {:error, _} = refused -> {:reply, refused, state}
    end
  end

  def handle_call({:serialized, fun}, _from, state), do: {:reply, fun.(), state}

  # The entry as the hub's row holds it now: an update and a committed value
  # rewrite the row, never this process's start argument.
  defp current_entry(%{namespace: namespace, name: name}) do
    with {cspec, _nodes} <- row(namespace, name),
         {:ok, {_namespace, %Plugin{} = entry}} <- Cspec.start_arg(cspec) do
      {:ok, entry}
    else
      _unreadable -> {:error, {:plugin_not_running, name}}
    end
  end

  defp start_copy(%{namespace: namespace} = state, %Plugin{} = entry) do
    case Shared.start_link(%{
           namespace: namespace,
           plugin: Shared.entry(%Plugin{entry | namespace: namespace})
         }) do
      {:ok, copy} -> {:ok, %{state | copy: copy}}
      {:error, _} = refused -> refused
    end
  end

  @impl true
  def prepare_handover_state(%{copy: nil}), do: []
  def prepare_handover_state(%{copy: copy}), do: Shared.subscribers(copy)

  @impl true
  def alter_handover_state(%{copy: nil} = state, _subscribers), do: state

  def alter_handover_state(%{copy: copy} = state, subscribers) do
    Shared.adopt(copy, subscribers)
    state
  end

  @impl true
  def terminate(_reason, %{copy: nil}), do: :ok

  def terminate(_reason, %{copy: copy}) do
    Process.unlink(copy)
    Shared.retire(copy)
  end
end

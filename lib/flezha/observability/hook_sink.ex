defmodule Flezha.Observability.HookSink do
  @moduledoc """
  The sink that tells controller plugins what happened: it turns signals into
  hook events and hands each to the plugin copies listening for it.

  An event is a map with `hook`, `namespace`, `controller` (the node it happened
  on), `at` (UTC, ISO 8601) and `text` (one line a person reads), plus the
  hook's own fields: `relay` and `factory` for `on_relay_started`,
  `relay` and `reason` (`:stopped` or `:completed`) for
  `on_relay_stopped`, `relay` and `error` (an error code) for
  `on_relay_failed`, `relay`, `entry`, `channel` and `error` for
  `on_dead_letter`, and `plugin`, `relay` and `error` for
  `on_plugin_unhealthy`. No event carries a relay's data.

  Three signals carry them. Start and stop are the relay's own events,
  `relay_started` and `relay_stopped` of source `:flezha_relay`, which
  every sink reads. A dead letter is a telemetry event the bridge lifts as source
  `:flezha_hooks` and emits to the sinks naming that source only
  (`Flezha.Observability.Bus.emit_named/1`), never to a `sources: :any` one. The
  two failures are the convergence judge's transitions
  (`[:flezha, :health, :verdict]`), which already fire once per health change:
  every controller judges every relay, so this sink takes only those of
  subjects on its own node, and it announces a relay's failure once, until
  the relay reads recovered, stops here or is judged on another controller.

  A copy listens by joining its hooks' directory groups
  (`Flezha.Cluster.Directory.hook_group/3`): a copy without `single` hears its
  own controller, the one `single` copy hears every controller. The sink runs on
  every controller and is registered with the bus only while a copy listens
  here, so where none does an emit sends no message. Delivery is best effort: a
  copy holds what it has not run in its own bounded queue
  (`Flezha.Plugin.Shared`).
  """

  use GenServer

  @behaviour Flezha.Observability.Sink

  alias Flezha.Cluster.Directory
  alias Flezha.Convergence.Verdict
  alias Flezha.Observability.{Registry, Signal}
  alias Flezha.Plugin
  alias Flezha.State.Paths

  @source :flezha_hooks

  @doc "The name the sink is registered under while a copy listens."
  @spec sink_name() :: atom()
  def sink_name(), do: @source

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: [:event], sources: [@source, :flezha_health, :flezha_relay]}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  # A row an earlier incarnation left names a dead pid.
  @impl true
  def init(nil) do
    Registry.unregister_sink(@source)

    case Directory.monitor_scope() do
      {ref, _groups} -> {:ok, settle(%{ref: ref, registered: false, failed: MapSet.new()})}
      nil -> {:ok, %{ref: nil, registered: false, failed: MapSet.new()}}
    end
  end

  @impl true
  def handle_info({ref, _change, group, _pids}, %{ref: ref} = state) do
    {:noreply, if(Directory.hook_group?(group), do: settle(state), else: state)}
  end

  def handle_info({:signal, %Signal{} = signal}, state) do
    {event, state} = read(signal, state)

    if event,
      do:
        for(
          copy <- Directory.hook_targets(event.namespace, event.hook),
          do: send(copy, {:hook, event})
        )

    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # In the bus's table exactly while a copy listens on this controller.
  defp settle(%{registered: registered} = state) do
    case {Directory.hooks_wanted?(), registered} do
      {true, false} ->
        Registry.register_external_sink(%{
          name: @source,
          module: __MODULE__,
          pid: self(),
          filter: filter()
        })

        %{state | registered: true}

      {false, true} ->
        Registry.unregister_sink(@source)
        %{state | registered: false, failed: MapSet.new()}

      _unchanged ->
        state
    end
  end

  # A relay's failure is announced once, until it reads recovered, is stopped
  # here or is judged on another controller: the reads between, unknown or
  # unbound (no node) while it is rebound, are none of these. `failed` holds
  # the relays announced and not recovered since.
  defp read(%Signal{name: :verdict, metadata: %{node: node} = transition}, state)
       when node == node(),
       do:
         turned(
           Verdict.turn(transition.kind, transition.previous, transition.state),
           transition,
           state
         )

  defp read(%Signal{name: :verdict, metadata: %{kind: :relay, id: id, node: node}}, state)
       when not is_nil(node),
       do: {nil, forget(state, id)}

  defp read(%Signal{source: :flezha_relay, name: :relay_started, metadata: f}, state),
    do: {event(:on_relay_started, f), state}

  defp read(%Signal{source: :flezha_relay, name: :relay_stopped, metadata: f}, state),
    do: {event(:on_relay_stopped, f), forget(state, {f.namespace, f.relay})}

  # The DLQ store names the relay's class; its namespace is read here, where
  # a copy listens, and not on every park.
  defp read(%Signal{source: @source, name: :on_dead_letter, metadata: fields}, state),
    do:
      {event(
         :on_dead_letter,
         Map.put_new_lazy(fields, :namespace, fn -> Paths.namespace_of_class(fields.relay) end)
       ), state}

  defp read(_signal, state), do: {nil, state}

  defp turned(:relay_failed, %{id: {namespace, relay} = id} = transition, state) do
    if MapSet.member?(state.failed, id) do
      {nil, state}
    else
      event =
        event(:on_relay_failed, %{
          namespace: namespace,
          relay: relay,
          error: code(cause(transition.reason))
        })

      {event, %{state | failed: MapSet.put(state.failed, id)}}
    end
  end

  defp turned(:relay_recovered, %{id: id}, state),
    do: {nil, forget(state, id)}

  defp turned(:plugin_unhealthy, %{id: {namespace, relay, plugin}} = transition, state) do
    event =
      event(:on_plugin_unhealthy, %{
        namespace: namespace,
        relay: relay,
        plugin: Plugin.base_id(plugin),
        error: code(cause(transition.reason))
      })

    {event, state}
  end

  defp turned(nil, _transition, state), do: {nil, state}

  defp forget(state, id), do: %{state | failed: MapSet.delete(state.failed, id)}

  # The judge wraps what it read; the code is the cause's.
  defp cause({:halted, reason}), do: reason
  defp cause({:plugin_unhealthy, reason}), do: reason
  defp cause(reason), do: reason

  # A health reason is a provider's own word, not an error that reached a
  # caller, so one the catalogue does not hold is named as such and not
  # reported as a defect.
  defp code(reason) do
    if Flezha.Error.catalogued?(reason, nil),
      do: Flezha.Error.from_reason(reason, nil).code,
      else: "internal.uncatalogued"
  end

  defp event(hook, fields) do
    Map.merge(fields, %{
      hook: hook,
      controller: node(),
      at: DateTime.to_iso8601(DateTime.utc_now()),
      text: text(hook, fields)
    })
  end

  defp text(:on_relay_started, f),
    do: "relay #{f.relay} in #{f.namespace} started on #{node()}"

  defp text(:on_relay_stopped, f), do: "relay #{f.relay} in #{f.namespace} #{f.reason}"

  defp text(:on_relay_failed, f),
    do: "relay #{f.relay} in #{f.namespace} failed: #{f.error}"

  defp text(:on_dead_letter, f),
    do: "a record of relay #{f.relay} in #{f.namespace} was dead-lettered: #{f.error}"

  defp text(:on_plugin_unhealthy, f),
    do: "plugin #{f.plugin} on relay #{f.relay} in #{f.namespace} is unhealthy: #{f.error}"
end

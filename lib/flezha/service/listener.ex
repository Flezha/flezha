defmodule Flezha.Service.Listener do
  @moduledoc """
  The one listener in the controller: `flz operator listen`, `flz operator send
  --await` and any plugin (the HTTP plugin's SSE stream first) read an
  operator's frames through `listen/4`.

  `listen/4` enables a `Flezha.Plugin.Provider.MonitoredIOForward` plugin under
  a generated name and binds the operator (every member of a partition group)
  to it, with a relay on the caller's own node as its receiver, and hands the
  caller one event at a time.
  The relay (`Flezha.Service.Listener.Relay`) answers the plugin's heartbeat and
  holds what the handler has not taken, inside `listener_pending_bytes` and
  `listener_pending_frames`, so a slow handler grows neither the operator's
  memory nor the caller's mailbox. Every way out unbinds and disables the
  plugin by the operator's namespace and id (`detach/3`), which holds wherever
  the operator runs by then.

  It does not authorize: the CLI's verbs authorize through their rows before
  they call it, and a plugin that calls it is trusted code that decides for
  itself who may read.
  """

  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Service.HubManager
  alias Flezha.Service.Listener.Relay
  alias Flezha.Service.PluginManager

  @type event ::
          {:output | :input | :part, binary(), map()}
          | {:dead_lettered, Flezha.Error.t(), map()}
          | :idle
  @type outcome ::
          {:ok, :window_closed | :done | :caller_down | :operator_ended}
          | {:error, Flezha.Error.t()}

  @hooks [output: [{:on_output, []}, {:on_preview, []}], input: [{:on_input, []}]]

  @doc """
  Listen to `operator_id` in `namespace` from the calling process until
  `handler` answers `:done`, the window closes, the watched process exits, the
  operator ends, or the listener is ended by its bound or by another caller's
  disable.

  `handler` takes an `t:event/0` and answers `:continue` or `:done`; it runs in
  the calling process, and a raise from it detaches and propagates. Options:

    * `direction` - `:output` (outputs, parts and dead-letter notices, the
      default) or `:input` (the inputs the operator ingests).
    * `window_ms` - a positive integer, or `:infinity` (the default).
    * `idle_ms` - a positive integer, or `:infinity` (the default): after that
      long with no event handed over, `handler` is called with `:idle`.
    * `before` - a function run once after the attach and before the first
      event (`send --await` delivers its input there); an answer other than
      `:ok` ends the listener with that answer.
    * `watch` - a process whose exit ends the listener with `:caller_down`.
  """
  @spec listen(String.t(), String.t(), keyword(), (event() -> :continue | :done)) :: outcome()
  def listen(namespace, operator_id, opts, handler) when is_function(handler, 1) do
    direction = Keyword.get(opts, :direction, :output)

    Keyword.has_key?(@hooks, direction) ||
      raise ArgumentError, "direction must be :output or :input"

    target = %{
      namespace: namespace,
      operator_id: operator_id,
      plugin_id: Identificator.mint(:plugin, "io_listener"),
      direction: direction,
      window_ms: span!(opts, :window_ms),
      idle_ms: span!(opts, :idle_ms)
    }

    # A target neither registered nor declared is refused on one read, with
    # nothing to detach: the enable and the detach would each wait out a hub
    # that has not settled before saying so.
    if attachment(namespace, operator_id, target.plugin_id) == :operator_ended,
      do: refused({:operator_not_found, namespace, operator_id}),
      else: attached(target, opts, handler)
  end

  defp attached(
         %{namespace: namespace, operator_id: operator_id, plugin_id: plugin_id} = t,
         opts,
         handler
       ) do
    {:ok, relay} =
      Relay.start_link(%{
        caller: self(),
        watch: Keyword.get(opts, :watch),
        namespace: namespace,
        operator_id: operator_id,
        plugin_id: plugin_id,
        direction: t.direction
      })

    try do
      with :ok <- attach(namespace, operator_id, plugin_id, t.direction, relay),
           :ok <- Keyword.get(opts, :before, fn -> :ok end).() do
        loop(%{
          relay: relay,
          handler: handler,
          asked: false,
          deadline: deadline(t.window_ms),
          idle_ms: t.idle_ms
        })
      end
    after
      PluginManager.detach(namespace, operator_id, plugin_id)
      Relay.stop(relay)
    end
  end

  defp attach(namespace, operator_id, plugin_id, direction, relay) do
    plugin = %Plugin{
      id: plugin_id,
      provider: MonitoredIOForward,
      namespace: namespace,
      operator_id: operator_id,
      opts: %{
        recv_pid: relay,
        plugin_id: plugin_id,
        operator_id: operator_id,
        namespace: namespace
      },
      hooks: Keyword.fetch!(@hooks, direction)
    }

    case PluginManager.enable_plugin(plugin_id, plugin) do
      {:ok, summary} when is_binary(summary) -> :ok
      {:ok, :already_enabled} -> refused({:plugin_id_taken, plugin_id, namespace})
      {:error, reason} -> refused(reason)
    end
  end

  defp refused(reason), do: {:error, Flezha.Error.from_reason(reason, nil)}

  # One request to the relay is outstanding at a time; an idle call leaves it
  # standing, so the event it answers is never lost or reordered.
  defp loop(%{relay: relay} = l) do
    l = ask(l)

    receive do
      {:listener_event, ^relay, event} -> handle(%{l | asked: false}, event)
      {:listener_end, ^relay, outcome} -> outcome
      {:EXIT, ^relay, reason} -> exit(reason)
    after
      wait(l) ->
        if expired?(l.deadline), do: {:ok, :window_closed}, else: handle(l, :idle)
    end
  end

  defp handle(l, event) do
    case l.handler.(event) do
      :continue -> loop(l)
      :done -> {:ok, :done}
    end
  end

  defp ask(%{asked: true} = l), do: l

  defp ask(%{relay: relay} = l) do
    Relay.next(relay)
    %{l | asked: true}
  end

  defp span!(opts, name) do
    case Keyword.get(opts, name, :infinity) do
      ms when ms == :infinity or (is_integer(ms) and ms > 0) ->
        ms

      other ->
        raise ArgumentError,
              "#{name} must be a positive integer or :infinity, got #{inspect(other)}"
    end
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(ms), do: now() + ms

  defp expired?(:infinity), do: false
  defp expired?(deadline), do: now() >= deadline

  defp wait(%{deadline: :infinity, idle_ms: idle}), do: idle
  defp wait(%{deadline: deadline, idle_ms: :infinity}), do: max(deadline - now(), 0)
  defp wait(%{deadline: deadline, idle_ms: idle}), do: min(max(deadline - now(), 0), idle)

  defp now, do: System.monotonic_time(:millisecond)

  ## The listener's plugin

  @doc """
  Whether `operator_id` (or a member of the group it names) is still bound to
  `name`: `:attached` while any target carries the binding, `:detached` when
  the targets are registered without it, `:operator_ended` when none is
  registered or declared, and `:unknown` when the rows cannot be read.
  """
  @spec attachment(binary(), binary(), binary()) ::
          :attached | :detached | :operator_ended | :unknown
  def attachment(namespace, operator_id, name) do
    args = [namespace, operator_id, name]

    with {:error, _unreadable} <-
           PluginManager.on_hub_node(namespace, {__MODULE__, :attachment_here}, args, :unknown),
         do: :unknown
  end

  @doc false
  def attachment_here(namespace, operator_id, name) do
    declared = MapSet.new(HubManager.declared_operators(namespace), & &1.id)

    namespace
    |> PluginManager.targets(operator_id)
    |> Enum.map(fn {member_id, _} -> row_attachment(namespace, member_id, name, declared) end)
    |> Enum.reduce(&stronger_attachment/2)
  end

  # A declared operator with no row is between two homes, a move or a rescale
  # away from one, and not ended.
  defp row_attachment(namespace, operator_id, name, declared) do
    case HubManager.operator_bindings(namespace, operator_id) do
      {:ok, bindings} ->
        if Enum.any?(bindings, &(Plugin.base_id(elem(&1, 0)) == name)),
          do: :attached,
          else: :detached

      {:error, {:operator_not_found, _, _}} ->
        if declared?(declared, operator_id), do: :unknown, else: :operator_ended

      {:error, _hub_down} ->
        :unknown
    end
  end

  defp declared?(declared, operator_id) do
    MapSet.member?(declared, operator_id) or
      Enum.any?(declared, &(Plugin.group_of(&1) == operator_id))
  end

  # One member still carrying the row is enough to go on.
  defp stronger_attachment(a, b),
    do: Enum.find([:attached, :unknown, :detached, :operator_ended], &(&1 in [a, b]))
end

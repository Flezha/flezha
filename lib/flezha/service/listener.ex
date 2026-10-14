defmodule Flezha.Service.Listener do
  @moduledoc """
  The one listener in the controller: `flz relay listen`, `flz relay send
  --await` and any plugin (the HTTP plugin's SSE stream first) read a
  relay's frames through `listen/4`.

  `listen/4` enables a `Flezha.Plugin.Provider.MonitoredIOForward` plugin under
  a generated name and binds the relay (every member of a partition group)
  to it, with a receiver on the caller's own node, and hands the caller one
  event at a time.
  The receiver (`Flezha.Service.Listener.Receiver`) answers the plugin's heartbeat and
  holds what the handler has not taken, inside `listener_pending_bytes` and
  `listener_pending_frames`, so a slow handler grows neither the relay's
  memory nor the caller's mailbox. Every way out unbinds and disables the
  plugin by the relay's namespace and id (`detach/3`), which holds wherever
  the relay runs by then.

  It does not authorize: the CLI's verbs authorize through their rows before
  they call it, and a plugin that calls it is trusted code that decides for
  itself who may read.
  """

  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Flezha.Service.HubManager
  alias Flezha.Service.Listener.Receiver
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RelayApi

  @type event ::
          {:output | :input | :part | :call | :answered, binary(), map()}
          | {:dead_lettered | :timed_out | :refused, Flezha.Error.t(), map()}
          | {:tally, %{String.t() => non_neg_integer()}, map()}
          | :idle
  @type outcome ::
          {:ok, :window_closed | :done | :caller_down | :relay_ended}
          | {:error, Flezha.Error.t()}

  @hooks [
    output: [{:on_output, []}, {:on_preview, []}],
    input: [{:on_input, []}],
    calls: [{:on_call, []}],
    # First in the chain, so no other output plugin's skip hides a frame.
    tally: [{:on_output, [position: 0]}]
  ]

  @doc """
  Listen to `relay_id` in `namespace` from the calling process until
  `handler` answers `:done`, the window closes, the watched process exits, the
  relay ends, or the listener is ended by its bound or by another caller's
  disable.

  `handler` takes an `t:event/0` and answers `:continue` or `:done`; it runs in
  the calling process, and a raise from it detaches and propagates. Options:

    * `direction` - `:output` (outputs, parts and dead-letter notices, the
      default), `:input` (the inputs the relay ingests) or `:calls` (each
      call the relay takes, `{:call, request, metadata}`, then how it ended:
      `{:answered, answer, metadata}`, `{:timed_out, error, metadata}` or
      `{:refused, error, metadata}`, paired by `flz.call_id`) or `:tally`
      (about once per `tally_ms`, default 1000, `{:tally, totals, %{read_at_ms:,
      epochs:}}`: the output frames counted per channel since the plugin was
      last prepared, summed over a group's members, with each member's
      epoch, which changes when its count restarts; a channel the count does
      not name is counted under `""`).
    * `channels` - the channels a tally names; the relay's announced output
      channels when absent. None is refused as `listener.no_channels`.
    * `window_ms` - a positive integer, or `:infinity` (the default).
    * `idle_ms` - a positive integer, or `:infinity` (the default): after that
      long with no event handed over, `handler` is called with `:idle`.
    * `before` - a function run once after the attach and before the first
      event (`send --await` delivers its input there); an answer other than
      `:ok` ends the listener with that answer.
    * `watch` - a process whose exit ends the listener with `:caller_down`.
  """
  @spec listen(String.t(), String.t(), keyword(), (event() -> :continue | :done)) :: outcome()
  def listen(namespace, relay_id, opts, handler) when is_function(handler, 1) do
    direction = Keyword.get(opts, :direction, :output)

    Keyword.has_key?(@hooks, direction) ||
      raise ArgumentError, "direction must be :output, :input, :calls or :tally"

    target = %{
      namespace: namespace,
      relay_id: relay_id,
      plugin_id: Identificator.mint(:plugin, "io_listener"),
      direction: direction,
      window_ms: span!(opts, :window_ms),
      idle_ms: span!(opts, :idle_ms)
    }

    # A target neither registered nor declared is refused on one read, with
    # nothing to detach: the enable and the detach would each wait out a hub
    # that has not settled before saying so.
    with :ok <- registered(namespace, relay_id, target.plugin_id),
         {:ok, plugin_opts} <- plugin_opts(target, opts),
         do: attached(Map.put(target, :plugin_opts, plugin_opts), opts, handler)
  end

  defp registered(namespace, relay_id, plugin_id) do
    if attachment(namespace, relay_id, plugin_id) == :relay_ended,
      do: refused({:relay_not_found, namespace, relay_id}),
      else: :ok
  end

  # The plugin options a direction adds: a tally's channels and interval, the
  # interval also the receiver's.
  defp plugin_opts(%{direction: :tally} = t, opts) do
    with {:ok, channels} <- tally_channels(t.namespace, t.relay_id, opts) do
      tally_ms = Keyword.get(opts, :tally_ms, Options.defaults(MonitoredIOForward).tally_ms)
      {:ok, %{mode: "tally", channels: channels, tally_ms: tally_ms}}
    end
  end

  defp plugin_opts(_target, _opts), do: {:ok, %{}}

  defp tally_channels(namespace, relay_id, opts) do
    case Keyword.fetch(opts, :channels) do
      {:ok, channels} -> some_channels(relay_id, channels)
      :error -> announced_outputs(namespace, relay_id)
    end
  end

  # A call end is not an output: what it emits goes to the callee.
  defp announced_outputs(namespace, relay_id) do
    case RelayApi.graph_of(namespace, relay_id) do
      {:ok, %{graph: graph}} ->
        channels = if graph, do: graph.channels, else: []
        some_channels(relay_id, for(%{plugin_end: "output", name: n} <- channels, do: n))

      {:error, reason} ->
        refused(reason)
    end
  end

  defp some_channels(relay_id, channels) when channels in [[], ""],
    do: {:error, Flezha.Error.new("listener.no_channels", relay: relay_id)}

  defp some_channels(_relay_id, channels), do: {:ok, channels}

  defp attached(
         %{namespace: namespace, relay_id: relay_id, plugin_id: plugin_id} = t,
         opts,
         handler
       ) do
    {:ok, receiver} =
      Receiver.start_link(%{
        caller: self(),
        watch: Keyword.get(opts, :watch),
        namespace: namespace,
        relay_id: relay_id,
        plugin_id: plugin_id,
        direction: t.direction,
        tally_ms: t.plugin_opts[:tally_ms]
      })

    try do
      with :ok <- attach(t, receiver),
           :ok <- Keyword.get(opts, :before, fn -> :ok end).() do
        loop(%{
          receiver: receiver,
          handler: handler,
          asked: false,
          deadline: deadline(t.window_ms),
          idle_ms: t.idle_ms
        })
      end
    after
      PluginManager.detach(namespace, relay_id, plugin_id)
      Receiver.stop(receiver)
    end
  end

  defp attach(%{namespace: namespace, relay_id: relay_id, plugin_id: plugin_id} = t, receiver) do
    plugin = %Plugin{
      id: plugin_id,
      provider: MonitoredIOForward,
      namespace: namespace,
      relay_id: relay_id,
      opts:
        Map.merge(t.plugin_opts, %{
          recv_pid: receiver,
          plugin_id: plugin_id,
          relay_id: relay_id,
          namespace: namespace
        }),
      hooks: Keyword.fetch!(@hooks, t.direction)
    }

    case PluginManager.enable_plugin(plugin_id, plugin) do
      {:ok, summary} when is_binary(summary) -> :ok
      {:ok, :already_enabled} -> refused({:plugin_id_taken, plugin_id, namespace})
      {:error, reason} -> refused(reason)
    end
  end

  defp refused(reason), do: {:error, Flezha.Error.from_reason(reason, nil)}

  # One request to the receiver is outstanding at a time; an idle call leaves it
  # standing, so the event it answers is never lost or reordered.
  defp loop(%{receiver: receiver} = l) do
    l = ask(l)

    receive do
      {:listener_event, ^receiver, event} -> handle(%{l | asked: false}, event)
      {:listener_end, ^receiver, outcome} -> outcome
      {:EXIT, ^receiver, reason} -> exit(reason)
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

  defp ask(%{receiver: receiver} = l) do
    Receiver.next(receiver)
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
  Whether `relay_id` (or a member of the group it names) is still bound to
  `name`: `:attached` while any target carries the binding, `:detached` when
  the targets are registered without it, `:relay_ended` when none is
  registered or declared, and `:unknown` when the rows cannot be read.
  """
  @spec attachment(binary(), binary(), binary()) ::
          :attached | :detached | :relay_ended | :unknown
  def attachment(namespace, relay_id, name) do
    args = [namespace, relay_id, name]

    with {:error, _unreadable} <-
           PluginManager.on_hub_node(namespace, {__MODULE__, :attachment_here}, args, :unknown),
         do: :unknown
  end

  @doc false
  def attachment_here(namespace, relay_id, name) do
    declared = MapSet.new(HubManager.declared_relays(namespace), & &1.id)

    namespace
    |> PluginManager.targets(relay_id)
    |> Enum.map(fn {member_id, _} -> row_attachment(namespace, member_id, name, declared) end)
    |> Enum.reduce(&stronger_attachment/2)
  end

  # A declared relay with no row is between two homes, a move or a rescale
  # away from one, and not ended.
  defp row_attachment(namespace, relay_id, name, declared) do
    case HubManager.relay_bindings(namespace, relay_id) do
      {:ok, bindings} ->
        if Enum.any?(bindings, &(Plugin.base_id(elem(&1, 0)) == name)),
          do: :attached,
          else: :detached

      {:error, {:relay_not_found, _, _}} ->
        if declared?(declared, relay_id), do: :unknown, else: :relay_ended

      {:error, _hub_down} ->
        :unknown
    end
  end

  defp declared?(declared, relay_id) do
    MapSet.member?(declared, relay_id) or
      Enum.any?(declared, &(Plugin.group_of(&1) == relay_id))
  end

  # One member still carrying the row is enough to go on.
  defp stronger_attachment(a, b),
    do: Enum.find([:attached, :unknown, :detached, :relay_ended], &(&1 in [a, b]))
end

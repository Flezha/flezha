defmodule Flezha.Service.Listener.Relay do
  @moduledoc """
  A listener's receiver, on the listener's own node and linked to it: the
  `recv_pid` of its `MonitoredIOForward` plugin. It answers the plugin's
  heartbeat itself, holds the frames the listener has not taken, and hands them
  over one per request (`next/1`), so a listener whose handler blocks bounds
  its backlog here rather than in its mailbox.

  The backlog stays inside `listener_pending_bytes` and
  `listener_pending_frames`: a frame that would pass either drops held parts,
  oldest first; a part that still does not fit is dropped; an output or a
  notice that still does not fit ends the listener with `listener.overrun`.

  When its plugin is released (a move, a restart, a teardown or a disable all
  look alike from here) it asks the registry which it was, once per
  `listener_release_check_ms`, until a heartbeat arrives again or the same
  answer comes twice running: a row that still carries the plugin is a move or
  a restart and the listener goes on; an operator no longer registered ends it
  with `:operator_ended`; a registered operator without the row was disabled by
  someone else, `listener.detached`. Asking twice keeps a member a rescale is
  restarting from reading as ended.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:release_check]

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Process.Backlog
  alias Flezha.Process.Watch
  alias Flezha.Service.Listener

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @doc "Ask for the next event; it arrives as `{:listener_event | :listener_end, relay, _}`."
  @spec next(pid()) :: :ok
  def next(relay) do
    send(relay, {:next, self()})
    :ok
  end

  @doc "Stop the relay without taking its caller along."
  @spec stop(pid()) :: :ok
  def stop(relay) do
    Process.unlink(relay)
    GenServer.stop(relay, :normal)
  catch
    :exit, _gone -> :ok
  end

  @impl true
  def init(args) do
    watch =
      if is_pid(args.watch), do: Watch.watch(Watch.new(), args.watch, :watch), else: Watch.new()

    {:ok,
     Map.merge(args, %{
       watched: watch,
       backlog:
         Backlog.new(Config.get(:listener_pending_bytes), Config.get(:listener_pending_frames)),
       asked: false,
       ended: false,
       released: false,
       reading: nil
     })}
  end

  @impl true
  def handle_info({:heartbeat_keepalive, from}, state) do
    send(from, {:heartbeat_keepalive, :ok})
    {:noreply, %{state | released: false, reading: nil}}
  end

  def handle_info({:message_forward, {payload, metadata}}, state),
    do: {:noreply, hold(state, {state.direction, payload, metadata})}

  def handle_info({:preview_forward, {payload, metadata}}, state),
    do: {:noreply, hold(state, preview(payload, metadata))}

  def handle_info({:next, _caller}, state), do: {:noreply, hand(%{state | asked: true})}

  def handle_info({:listener_released, _plugin_id}, state),
    do: {:noreply, recheck(%{state | released: true})}

  def handle_info(:release_check, %{released: false} = state), do: {:noreply, state}
  def handle_info(:release_check, state), do: {:noreply, check_release(state)}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Watch.down(state.watched, ref) do
      {:unknown, _} -> {:noreply, state}
      {_tags, watched} -> {:noreply, finish_now(%{state | watched: watched}, {:ok, :caller_down})}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  # A part carries its number; anything else on the preview hook is a notice.
  defp preview(payload, %{"flz.part" => _} = metadata), do: {:part, payload, metadata}

  defp preview(payload, metadata) do
    error =
      case Flezha.SDK.Error.from_document(payload) do
        %{code: code, details: details} -> Flezha.Error.new(code, details)
        nil -> Flezha.Error.new("internal.uncatalogued", reason: "an unreadable notice")
      end

    {:dead_lettered, error, metadata}
  end

  defp hold(%{ended: true} = state, _event), do: state

  defp hold(state, {kind, payload, _metadata} = event) do
    size = if is_binary(payload), do: byte_size(payload), else: 0
    {backlog, _dropped} = Backlog.make_room(state.backlog, size, &part?/1)
    state = %{state | backlog: backlog}

    cond do
      Backlog.fits?(backlog, size) -> hand(%{state | backlog: Backlog.push(backlog, event, size)})
      kind == :part -> state
      true -> overrun(state)
    end
  end

  # Held parts go oldest first to make room.
  defp part?({kind, _payload, _metadata}), do: kind == :part
  defp part?(_end), do: false

  defp overrun(state) do
    error =
      Flezha.Error.new("listener.overrun",
        operator: state.operator_id,
        bytes: state.backlog.max_bytes,
        frames: state.backlog.max_frames
      )

    finish_now(state, {:error, error})
  end

  defp hand(%{asked: false} = state), do: state

  defp hand(state) do
    case Backlog.out(state.backlog) do
      {:empty, _} ->
        state

      {{:end, outcome}, _rest} ->
        send(state.caller, {:listener_end, self(), outcome})
        %{state | backlog: Backlog.clear(state.backlog), asked: false}

      {event, rest} ->
        send(state.caller, {:listener_event, self(), event})
        %{state | backlog: rest, asked: false}
    end
  end

  # An end waits behind what was held before it, and nothing is held after it.
  # One that cannot wait drops what is held.
  defp finish_now(%{ended: true} = state, _outcome), do: state

  defp finish_now(state, outcome),
    do: finish(%{state | backlog: Backlog.clear(state.backlog)}, outcome)

  defp finish(%{ended: true} = state, _outcome), do: state

  defp finish(state, outcome),
    do: hand(%{state | ended: true, backlog: Backlog.push(state.backlog, {:end, outcome}, 0)})

  defp check_release(state) do
    case {Listener.attachment(state.namespace, state.operator_id, state.plugin_id), state.reading} do
      {:operator_ended, :operator_ended} -> finish(state, {:ok, :operator_ended})
      {:detached, :detached} -> finish(state, {:error, detached(state)})
      {reading, _} -> recheck(%{state | reading: reading})
    end
  end

  defp recheck(state),
    do:
      arm_timer(state, :release_check, Durations.get(:listener_release_check_ms), :release_check)

  defp detached(state), do: Flezha.Error.new("listener.detached", operator: state.operator_id)
end

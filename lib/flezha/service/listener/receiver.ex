defmodule Flezha.Service.Listener.Receiver do
  @moduledoc """
  A listener's receiver, on the listener's own node and watching it: the
  `recv_pid` of its `MonitoredIOForward` plugin. It answers the plugin's
  heartbeat itself, holds the frames the listener has not taken, and hands them
  over one per request (`next/1`), so a listener whose handler blocks bounds
  its backlog here rather than in its mailbox.

  The backlog stays inside `listener_pending_bytes` and
  `listener_pending_frames`: a frame that would pass either drops held parts
  and tallies, oldest first; a part or tally that still does not fit is
  dropped; an output, a notice, a call or a call's outcome that still does not
  fit ends the listener with `listener.overrun`. A tally can be dropped because
  each one carries whole totals: the next says everything an older one did.

  For a tally it keeps each member's latest report and hands on their sum at
  most once per `tally_ms`, so the members of a group, each reporting on its
  own clock, make one event per interval rather than one each.

  When its caller dies without ending the listener, it detaches the plugin
  itself and goes with it.

  When its plugin is released (a move, a restart, a teardown or a disable all
  look alike from here) it asks the registry which it was, once per
  `listener_release_check_ms`, until a heartbeat arrives again or the same
  answer comes twice running: a row that still carries the plugin is a move or
  a restart and the listener goes on; a relay no longer registered ends it
  with `:relay_ended`; a registered relay without the row was disabled by
  someone else, `listener.detached`. Asking twice keeps a member a rescale is
  restarting from reading as ended.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:release_check, :tally]

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Process.Backlog
  alias Flezha.Process.Watch
  alias Flezha.Service.Listener
  alias Flezha.Service.PluginManager

  @spec start(map()) :: GenServer.on_start()
  def start(args), do: GenServer.start(__MODULE__, args)

  @doc "Ask for the next event; it arrives as `{:listener_event | :listener_end, receiver, _}`."
  @spec next(pid()) :: :ok
  def next(receiver) do
    send(receiver, {:next, self()})
    :ok
  end

  @spec stop(pid()) :: :ok
  def stop(receiver) do
    GenServer.stop(receiver, :normal)
  catch
    :exit, _gone -> :ok
  end

  @impl true
  def init(args) do
    # Monitored rather than linked: a caller killed outright never runs the
    # listener's own detach, so the receiver does it.
    watch = Watch.watch(Watch.new(), args.caller, :caller)
    watch = if is_pid(args.watch), do: Watch.watch(watch, args.watch, :watch), else: watch

    {:ok,
     Map.merge(args, %{
       watched: watch,
       backlog:
         Backlog.new(Config.get(:listener_pending_bytes), Config.get(:listener_pending_frames)),
       asked: false,
       ended: false,
       released: false,
       reading: nil,
       tallies: %{},
       tally_handed_at: nil
     })}
  end

  @impl true
  def handle_info({:heartbeat_keepalive, from}, state) do
    send(from, {:heartbeat_keepalive, :ok})
    {:noreply, %{state | released: false, reading: nil}}
  end

  def handle_info({:message_forward, {payload, metadata}}, %{direction: :calls} = state),
    do: {:noreply, hold(state, call_event(payload, metadata))}

  def handle_info({:message_forward, {payload, metadata}}, state),
    do: {:noreply, hold(state, {state.direction, payload, metadata})}

  def handle_info({:preview_forward, {payload, metadata}}, state),
    do: {:noreply, hold(state, preview(payload, metadata))}

  def handle_info({:tally_report, member, epoch, totals}, state) do
    tallies = Map.put(state.tallies, member, {epoch, totals})
    {:noreply, tally_due(%{state | tallies: tallies})}
  end

  def handle_info(:tally_due, state) do
    state = %{timer_fired(state, :tally) | tally_handed_at: now()}
    {:noreply, hold(state, tally(state.tallies))}
  end

  def handle_info({:next, _caller}, state), do: {:noreply, hand(%{state | asked: true})}

  def handle_info({:listener_released, _plugin_id}, state),
    do: {:noreply, recheck(%{state | released: true})}

  def handle_info(:release_check, %{released: false} = state), do: {:noreply, state}
  def handle_info(:release_check, state), do: {:noreply, check_release(state)}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Watch.down(state.watched, ref) do
      {:unknown, _} ->
        {:noreply, state}

      {tags, watched} ->
        if :caller in tags do
          PluginManager.detach(state.namespace, state.relay_id, state.plugin_id)
          {:stop, :normal, state}
        else
          {:noreply, finish_now(%{state | watched: watched}, {:ok, :caller_down})}
        end
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  # A part carries its number; anything else on the preview hook is a notice.
  defp preview(payload, %{"flz.part" => _} = metadata), do: {:part, payload, metadata}

  defp preview(payload, metadata), do: {:dead_lettered, error_of(payload), metadata}

  # A call frame names its kind; an outcome that is not an answer carries an
  # error document.
  defp call_event(payload, metadata) do
    case Map.pop(metadata, "flz.call_event") do
      {"call", metadata} -> {:call, payload, metadata}
      {"answered", metadata} -> {:answered, payload, metadata}
      {"timed_out", metadata} -> {:timed_out, error_of(payload), metadata}
      {"refused", metadata} -> {:refused, error_of(payload), metadata}
    end
  end

  # The next tally is handed on one interval after the last, or at once.
  defp tally_due(state) do
    if timer_armed?(state, :tally) do
      state
    else
      wait = if state.tally_handed_at, do: state.tally_handed_at + state.tally_ms - now(), else: 0
      arm_timer(state, :tally, max(wait, 0), :tally_due)
    end
  end

  defp now, do: System.monotonic_time(:millisecond)

  # Every member's latest totals summed, with the epoch each was counted under.
  defp tally(tallies) do
    {totals, epochs} =
      Enum.reduce(tallies, {%{}, %{}}, fn {member, {epoch, counted}}, {totals, epochs} ->
        {Map.merge(totals, counted, fn _channel, a, b -> a + b end),
         Map.put(epochs, member, epoch)}
      end)

    {:tally, totals, %{read_at_ms: now(), epochs: epochs}}
  end

  defp error_of(document) do
    case Flezha.SDK.Error.from_document(document) do
      %{code: code, details: details} -> Flezha.Error.new(code, details)
      nil -> Flezha.Error.new("internal.uncatalogued", reason: "an unreadable error document")
    end
  end

  defp hold(%{ended: true} = state, _event), do: state

  defp hold(state, {_kind, payload, _metadata} = event) do
    size = if is_binary(payload), do: byte_size(payload), else: 0
    {backlog, _dropped} = Backlog.make_room(state.backlog, size, &droppable?/1)
    state = %{state | backlog: backlog}

    cond do
      Backlog.fits?(backlog, size) -> hand(%{state | backlog: Backlog.push(backlog, event, size)})
      droppable?(event) -> state
      true -> overrun(state)
    end
  end

  # Held parts and tallies go oldest first to make room.
  defp droppable?({kind, _payload, _metadata}), do: kind in [:part, :tally]
  defp droppable?(_end), do: false

  defp overrun(state) do
    error =
      Flezha.Error.new("listener.overrun",
        relay: state.relay_id,
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
    case {Listener.attachment(state.namespace, state.relay_id, state.plugin_id), state.reading} do
      {:relay_ended, :relay_ended} -> finish(state, {:ok, :relay_ended})
      {:detached, :detached} -> finish(state, {:error, detached(state)})
      {reading, _} -> recheck(%{state | reading: reading})
    end
  end

  defp recheck(state),
    do:
      arm_timer(state, :release_check, Durations.get(:listener_release_check_ms), :release_check)

  defp detached(state), do: Flezha.Error.new("listener.detached", relay: state.relay_id)
end

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
  alias Flezha.Process.Watch
  alias Flezha.Service.PluginManager

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
       queue: :queue.new(),
       bytes: 0,
       frames: 0,
       asked: false,
       ended: false,
       released: false,
       reading: nil,
       max_bytes: Config.get(:listener_pending_bytes),
       max_frames: Config.get(:listener_pending_frames)
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
    state = make_room(state, size)

    cond do
      fits?(state, size) ->
        queued = %{state | queue: :queue.in({event, size}, state.queue)}
        hand(%{queued | bytes: state.bytes + size, frames: state.frames + 1})

      kind == :part ->
        state

      true ->
        overrun(state)
    end
  end

  defp fits?(state, size),
    do: state.frames + 1 <= state.max_frames and state.bytes + size <= state.max_bytes

  # Held parts go oldest first until `size` fits, or none is left.
  defp make_room(state, size) do
    if fits?(state, size) do
      state
    else
      {dropped, kept} = drop_oldest_part(:queue.to_list(state.queue))

      case dropped do
        nil ->
          state

        dropped_size ->
          make_room(
            %{
              state
              | queue: :queue.from_list(kept),
                bytes: state.bytes - dropped_size,
                frames: state.frames - 1
            },
            size
          )
      end
    end
  end

  defp drop_oldest_part(held) do
    case Enum.split_while(held, fn {{kind, _, _}, _size} -> kind != :part end) do
      {_all, []} -> {nil, held}
      {before, [{_part, size} | rest]} -> {size, before ++ rest}
    end
  end

  defp overrun(state) do
    error =
      Flezha.Error.new("listener.overrun",
        operator: state.operator_id,
        bytes: state.max_bytes,
        frames: state.max_frames
      )

    finish_now(state, {:error, error})
  end

  defp hand(%{asked: false} = state), do: state

  defp hand(state) do
    case :queue.out(state.queue) do
      {:empty, _} ->
        state

      {{:value, {{:end, outcome}, _size}}, _rest} ->
        send(state.caller, {:listener_end, self(), outcome})
        %{state | queue: :queue.new(), asked: false}

      {{:value, {event, size}}, rest} ->
        send(state.caller, {:listener_event, self(), event})
        %{state | queue: rest, bytes: state.bytes - size, frames: state.frames - 1, asked: false}
    end
  end

  # An end waits behind what was held before it, and nothing is held after it.
  # One that cannot wait drops what is held.
  defp finish_now(%{ended: true} = state, _outcome), do: state

  defp finish_now(state, outcome),
    do: finish(%{state | queue: :queue.new(), bytes: 0, frames: 0}, outcome)

  defp finish(%{ended: true} = state, _outcome), do: state

  defp finish(state, outcome) do
    ended = %{state | ended: true, queue: :queue.in({{:end, outcome}, 0}, state.queue)}
    hand(ended)
  end

  defp check_release(state) do
    case {PluginManager.attachment(state.namespace, state.operator_id, state.plugin_id),
          state.reading} do
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

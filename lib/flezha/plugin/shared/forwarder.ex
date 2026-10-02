defmodule Flezha.Plugin.Shared.Forwarder do
  @moduledoc """
  The backlog of a controller plugin's copy: the hook events it is told of,
  handed back to the copy one at a time, each run before the next, inside
  `plugin_backlog_bytes` and `plugin_backlog_frames`, so a slow hook decides
  its own pace and nobody else's.

  An event that does not fit drops the oldest waiting ones; what it dropped is
  reported to the copy, which is what makes the plugin's health say
  `plugin_lagging`.
  """

  use GenServer

  alias Flezha.Config
  alias Flezha.Process.Backlog

  @spec start_link(pid()) :: GenServer.on_start()
  def start_link(copy), do: GenServer.start_link(__MODULE__, copy)

  @doc "Hold one hook event for the copy."
  @spec push(pid(), map()) :: :ok
  def push(forwarder, event), do: GenServer.cast(forwarder, {:push, event})

  @doc "Stop without taking the copy along."
  @spec stop(pid()) :: :ok
  def stop(forwarder) do
    Process.unlink(forwarder)
    GenServer.stop(forwarder, :normal)
  catch
    :exit, _gone -> :ok
  end

  @impl true
  def init(copy) do
    backlog =
      Backlog.new(Config.get(:plugin_backlog_bytes), Config.get(:plugin_backlog_frames))

    {:ok, %{copy: copy, backlog: backlog, injecting: nil}}
  end

  @impl true
  def handle_cast({:push, event}, state) do
    size = :erlang.external_size(event)
    {backlog, dropped} = Backlog.make_room(state.backlog, size, fn _held -> true end)

    {backlog, dropped} =
      if Backlog.fits?(backlog, size),
        do: {Backlog.push(backlog, event, size), dropped},
        else: {backlog, dropped + 1}

    if dropped > 0, do: GenServer.cast(state.copy, {:dropped, dropped})
    {:noreply, inject_next(%{state | backlog: backlog})}
  end

  # The event in flight answered, or the copy went away: the next one goes. A
  # copy that went away takes this linked forwarder along.
  @impl true
  def handle_info(message, %{injecting: request} = state) when request != nil do
    case :gen_server.check_response(message, request) do
      :no_reply -> {:noreply, state}
      _answered -> {:noreply, inject_next(%{state | injecting: nil})}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  # One event in flight at a time, sent as a request, so the backlog keeps
  # taking events while the copy runs the one before. Every message this
  # process handles is O(1), so its mailbox drains as fast as events arrive,
  # however slow the hook is.
  defp inject_next(%{injecting: nil} = state) do
    case Backlog.out(state.backlog) do
      {:empty, _} ->
        state

      {event, backlog} ->
        %{
          state
          | backlog: backlog,
            injecting: :gen_server.send_request(state.copy, {:hook, event})
        }
    end
  end

  defp inject_next(state), do: state
end

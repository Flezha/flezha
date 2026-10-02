defmodule Flezha.Plugin.Shared.Forwarder do
  @moduledoc """
  One bound operator's backlog on a shared plugin: the records its operator has
  not taken, inside `plugin_backlog_bytes` and `plugin_backlog_frames`, injected
  one at a time, each confirmed before the next, so the operator's own credit
  decides its pace and nobody else's.

  A record that does not fit drops the oldest waiting ones; what it dropped is
  reported to the copy that started it, which is what makes the operator's
  health say `plugin_lagging`. The operator's entry channel, when its binding
  names one, is stamped on every record it injects.
  """

  use GenServer

  alias Flezha.Config
  alias Flezha.Plugin.Host
  alias Flezha.Process.Backlog

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(%{overwatch: _, channel: _} = args),
    do: GenServer.start_link(__MODULE__, Map.put(args, :copy, self()))

  @doc "Hold one record for the operator."
  @spec push(pid(), binary(), map()) :: :ok
  def push(forwarder, message, metadata),
    do: GenServer.cast(forwarder, {:push, message, metadata})

  @doc "Stamp `channel` on what is injected from now on."
  @spec restamp(pid(), binary() | nil) :: :ok
  def restamp(forwarder, channel), do: GenServer.cast(forwarder, {:restamp, channel})

  @doc "Stop without taking the copy along."
  @spec stop(pid()) :: :ok
  def stop(forwarder) do
    Process.unlink(forwarder)
    GenServer.stop(forwarder, :normal)
  catch
    :exit, _gone -> :ok
  end

  @impl true
  def init(args) do
    backlog =
      Backlog.new(Config.get(:plugin_backlog_bytes), Config.get(:plugin_backlog_frames))

    {:ok, Map.merge(args, %{backlog: backlog, injecting: nil})}
  end

  @impl true
  def handle_cast({:push, message, metadata}, state) do
    size = byte_size(message)
    {backlog, dropped} = Backlog.make_room(state.backlog, size, fn _held -> true end)

    {backlog, dropped} =
      if Backlog.fits?(backlog, size),
        do: {Backlog.push(backlog, {message, metadata}, size), dropped},
        else: {backlog, dropped + 1}

    if dropped > 0, do: GenServer.cast(state.copy, {:dropped, state.overwatch, dropped})
    {:noreply, inject_next(%{state | backlog: backlog})}
  end

  def handle_cast({:restamp, channel}, state), do: {:noreply, %{state | channel: channel}}

  # The injection in flight answered, or its operator went away: the next one
  # goes. An operator that went away is heard of by its copy, which stops this
  # forwarder.
  @impl true
  def handle_info(message, %{injecting: request} = state) when request != nil do
    case :gen_server.check_response(message, request) do
      :no_reply -> {:noreply, state}
      _answered -> {:noreply, inject_next(%{state | injecting: nil})}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  # One record in flight at a time, sent as a request, so the backlog keeps
  # taking records while the operator confirms the one before. Every message
  # this process handles is O(1), so its mailbox drains as fast as its copy
  # fills it, however slow its operator is.
  defp inject_next(%{injecting: nil} = state) do
    case Backlog.out(state.backlog) do
      {:empty, _} ->
        state

      {{message, metadata}, backlog} ->
        request = Host.inject(state.overwatch, message, stamp(metadata, state.channel), :request)
        %{state | backlog: backlog, injecting: request}
    end
  end

  defp inject_next(state), do: state

  defp stamp(metadata, nil), do: metadata
  defp stamp(metadata, channel), do: Map.put(metadata, "channel", channel)
end

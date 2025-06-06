defmodule Flezha.Socket.Dispatcher do
  require Logger

  use GenServer

  def start_link(dispatch_fn) do
    GenServer.start_link(__MODULE__, dispatch_fn, name: __MODULE__)
  end

  @impl true
  def init(dispatch_fn) do
    lsock = GenServer.call(:control_listener, :get_socket)

    {
      :ok,
      %{dispatch_fn: dispatch_fn, lsock: lsock},
      {:continue, :dispatch}
    }
  end

  @impl true
  def handle_continue(:dispatch, state) do
    loop_accept(state.lsock, state.dispatch_fn)

    {:noreply, state, {:continue, :dispatch}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Dispatcher received unexpected message #{inspect(msg)}")

    {:noreply, state}
  end

  defp loop_accept(lsock, dispatch_fn) do
    case :gen_tcp.accept(lsock) do
      {:ok, socket} -> dispatch_fn.(socket)
      {:error, reason} -> Logger.error("Failed to accept connection: #{inspect(reason)}")
    end
  end
end

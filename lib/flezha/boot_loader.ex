defmodule Flezha.BootLoader do
  require Logger

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    run_pre_hooks()

    Flezha.Socket.Support.prepare_socket_dir()

    {:ok, %{boot_ready: false}, {:continue, :boot}}
  end

  @impl true
  def handle_continue(:boot, state) do
    run_post_hooks()

    {:noreply, Map.put(state, :boot_ready, true)}
  end

  @impl true
  def handle_call(:boot_ready?, _from, state) do
    {:reply, Map.get(state, :boot_ready, false), state}
  end

  @impl true
  def handle_info({:operator_connected_event, {"test_temp_namespace", _}}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.error("Unexpected message at boot validator: #{inspect(msg)} on node #{node()}")

    {:noreply, state}
  end

  defp run_pre_hooks() do
    Application.get_env(:flezha, :pre_hooks, [])
    |> Enum.each(& &1.())
  end

  defp run_post_hooks() do
    Application.get_env(:flezha, :post_hooks, [])
    |> Enum.each(& &1.())
  end
end

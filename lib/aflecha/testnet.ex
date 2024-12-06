defmodule Aflecha.Testnet do
  require Logger

  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    command = "npm start --prefix test/apps/testnet"

    port =
      Port.open(
        {:spawn, command},
        [:binary]
      )

    Port.monitor(port)

    {:ok, port}
  end

  @impl true
  def handle_info({:DOWN, _ref, :port, _port, :normal}, state) do
    Logger.error("Testnet port closed")

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.info("Testnet emitted message: #{inspect(msg)}")

    {:noreply, state}
  end
end

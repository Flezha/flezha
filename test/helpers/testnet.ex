defmodule Flezha.Testnet do
  @moduledoc """
  Supervises the Node.js testnet daemon the `:testnet`-tagged scenarios drive.
  Compiled outside `:prod` only.
  """

  require Logger

  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  The port the daemon binds, derived from the run in `config/runtime.exs` so two
  suites on one checkout do not both take it.
  """
  def ws_port, do: Application.fetch_env!(:flezha, :testnet_ws_port)

  @doc "The endpoint the `:testnet` scenarios dial."
  def ws_uri, do: "ws://localhost:#{ws_port()}"

  @doc """
  The frame that asks the daemon's generator to emit `message` every
  `interval_ms`. The daemon's contract lives beside the daemon, so a scenario
  that dials it does not respell the protocol.
  """
  def subscribe_message(message, interval_ms) do
    Jason.encode!(%{
      event_type: "subscribe",
      event_data: %{default_message: message, interval: interval_ms}
    })
  end

  @impl true
  def init(_) do
    command = "npm start --prefix test/apps/testnet"

    port =
      Port.open(
        {:spawn, command},
        [:binary, :stderr_to_stdout, env: [{~c"FLEZHA_TESTNET_WS_PORT", ~c"#{ws_port()}"}]]
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

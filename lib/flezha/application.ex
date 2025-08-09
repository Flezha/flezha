defmodule Flezha.Application do
  @moduledoc false

  alias Flezha.Socket.Support, as: SocketSupport
  alias Flezha.Operator.FleetManager

  require Logger

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Flezha.Service.EventManager,
      Flezha.BootLoader,
      Flezha.Service.Identificator,
      Flezha.Service.HubManager,
      Flezha.Operator.FleetSupervisor,
      FleetManager,
      {Flezha.Socket.Listener, {:control_listener, &SocketSupport.control_socket_path/0}},
      {Flezha.Socket.Dispatcher, &FleetManager.connect_handler/1},
      Flezha.Service.PluginManager,
      Flezha.Service.PortPilot,
      Flezha.Cluster.Orchestrator
    ]

    # If Testnet is enabled, we start the TestnetSupervisor
    children =
      if Application.get_env(:flezha, :testnet_enabled) do
        [{Flezha.Testnet, []} | children]
      else
        children
      end

    opts = [strategy: :one_for_one, name: Flezha.Supervisor]
    result = Supervisor.start_link(children, opts)

    validate_boot_ready()

    result
  end

  @impl true
  def prep_stop(_state) do
    # We need to stop the hub manager first to keep the operators
    # running while computations are stopped.
    Flezha.Service.HubManager.stop_hub_manager()
  end

  defp validate_boot_ready() do
    case GenServer.call(Flezha.BootLoader, :boot_ready?, 7000) do
      true -> :ok
      false -> raise "Boot not ready"
    end
  end
end

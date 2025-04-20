defmodule Aflecha.Application do
  @moduledoc false

  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Operator.FleetManager

  require Logger

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Aflecha.Service.EventManager,
      Aflecha.BootLoader,
      Aflecha.Service.Identificator,
      Aflecha.HubManager,
      Aflecha.Operator.FleetSupervisor,
      FleetManager,
      {Aflecha.Socket.Listener, {:control_listener, &SocketSupport.control_socket_path/0}},
      {Aflecha.Socket.Dispatcher, &FleetManager.connect_handler/1},
      Aflecha.Service.PortPilot

      # Aflecha.Service.Warden,
      # Aflecha.Service.Identificator,
      # Aflecha.Service.EventManager,
      # {Aflecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      # {Aflecha.Socket.Dispatcher, &Aflecha.Operator.FleetManager.connect_handler/1},
      # Aflecha.Service.PortPilot,
      # Aflecha.Cluster.Orchestrator,
      # Aflecha.Operator.Controller
    ]

    # If Testnet is enabled, we start the TestnetSupervisor
    children =
      if Application.get_env(:aflecha, :testnet_enabled) do
        [{Aflecha.Testnet, []} | children]
      else
        children
      end

    opts = [strategy: :one_for_one, name: Aflecha.Supervisor]
    result = Supervisor.start_link(children, opts)

    validate_boot_ready()

    result
  end

  @impl true
  def prep_stop(_state) do
    # We need to stop the hub manager first to keep the operators
    # running while pipelines are stopped.
    Aflecha.HubManager.stop_hub_manager()
  end

  defp validate_boot_ready() do
    case GenServer.call(Aflecha.BootLoader, :boot_ready?, 7000) do
      true -> :ok
      false -> raise "Boot not ready"
    end
  end
end

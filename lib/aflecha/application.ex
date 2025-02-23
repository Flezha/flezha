defmodule Aflecha.Application do
  @moduledoc false
  alias Aflecha.Socket.Support

  require Logger

  use Application

  @impl true
  def start(_type, _args) do
    Aflecha.Socket.Support.prepare_socket_dir()

    children = [
      Aflecha.Service.EventManager,
      Aflecha.BootLoader,
      Aflecha.Service.Identificator,
      Aflecha.HubManager,
      Aflecha.Operator.FleetManager,
      {Aflecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      {Aflecha.Socket.Dispatcher, &Aflecha.Operator.FleetManager.connect_handler/1},
      Aflecha.Operator.FleetSupervisor,
      Aflecha.Service.PortPilot,
      Aflecha.Plugin.PluginManagerSupervisor

      # Aflecha.Operator.FleetManager,
      # Aflecha.Service.Warden,
      # Aflecha.Service.Identificator,
      # Aflecha.Service.EventManager,
      # Aflecha.Operator.FleetSupervisor,
      # Aflecha.Operator.FleetManager,
      # {Aflecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      # {Aflecha.Socket.Dispatcher, &Aflecha.Operator.FleetManager.connect_handler/1}

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

  defp validate_boot_ready() do
    case GenServer.call(Aflecha.BootLoader, :boot_ready?) do
      true -> :ok
      false -> raise "Boot not ready"
    end
  end
end

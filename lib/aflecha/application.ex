defmodule Aflecha.Application do
  @moduledoc false
  alias Aflecha.Socket.Support

  require Logger

  use Application

  @impl true
  def start(_type, _args) do
    Aflecha.Socket.Support.prepare_socket_dir()

    children = [
      Aflecha.Service.Identificator,
      Aflecha.HubManager,
      Aflecha.Service.EventManager,
      Aflecha.Worker.NodeManager,
      {Aflecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      {Aflecha.Socket.Dispatcher, &Aflecha.Worker.NodeManager.connect_handler/1},
      Aflecha.Worker.FleetSupervisor,
      Aflecha.Service.PortPilot

      # Aflecha.Worker.NodeManager,
      # Aflecha.Service.Warden,
      # Aflecha.Service.Identificator,
      # Aflecha.Service.EventManager,
      # Aflecha.Worker.FleetSupervisor,
      # Aflecha.Worker.NodeManager,
      # {Aflecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      # {Aflecha.Socket.Dispatcher, &Aflecha.Worker.NodeManager.connect_handler/1}

      # Aflecha.Service.Warden,
      # Aflecha.Service.Identificator,
      # Aflecha.Service.EventManager,
      # {Aflecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      # {Aflecha.Socket.Dispatcher, &Aflecha.Worker.NodeManager.connect_handler/1},
      # Aflecha.Service.PortPilot,
      # Aflecha.Cluster.Orchestrator,
      # Aflecha.Worker.Controller
    ]

    # If Testnet is enabled, we start the TestnetSupervisor
    children =
      if Application.get_env(:aflecha, :testnet_enabled) do
        [{Aflecha.Testnet, []} | children]
      else
        children
      end

    opts = [strategy: :one_for_one, name: Aflecha.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

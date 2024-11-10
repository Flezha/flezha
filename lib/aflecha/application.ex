defmodule Aflecha.Application do
  @moduledoc false
  alias Aflecha.Socket.Support

  require Logger

  use Application

  @impl true
  def start(_type, _args) do
    Aflecha.Socket.Support.prepare_socket_dir()

    children = [
      Aflecha.Subscriber.SubscribersSupervisor,
      Aflecha.Service.Warden,
      Aflecha.Service.Identificator,
      Aflecha.Service.EventManager,
      {Aflecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      {Aflecha.Socket.Dispatcher, &Aflecha.Subscriber.Toolbox.sub_conn_fn/1},
      Aflecha.Service.PortPilot,
      Aflecha.Cluster.Orchestrator
    ]

    opts = [strategy: :one_for_one, name: Aflecha.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

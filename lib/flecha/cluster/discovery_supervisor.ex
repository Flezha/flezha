defmodule Flecha.Cluster.DiscoverySupervisor do
  alias Flecha.Cluster.StaticNodes
  alias Flecha.Cluster.K8SPod

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    children = [
      StaticNodes,
      K8SPod
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

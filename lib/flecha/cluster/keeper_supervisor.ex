defmodule Flecha.Cluster.KeeperSupervisor do
  alias Flecha.Cluster.StaticNodes

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    children = [
      StaticNodes
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

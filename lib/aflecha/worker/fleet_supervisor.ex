defmodule Aflecha.Worker.FleetSupervisor do
  alias Aflecha.Worker.WorkerSupervisor

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    Supervisor.init([], strategy: :one_for_one)
  end

  def start_operator(group_id, worker_id, sock) do
    Supervisor.start_child(__MODULE__, %{
      id: group_id,
      start: {WorkerSupervisor, :start_link, [{group_id, worker_id, sock}]}
    })
  end

  def default_operator() do
    child =
      DynamicSupervisor.which_children(__MODULE__)
      |> Enum.at(0)

    case child do
      {group_id, _, _, _} -> group_id
      _ -> raise "No group available"
    end
  end
end

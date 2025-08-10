defmodule Flezha.ComputeEngine.FleetSupervisor do
  alias Flezha.ComputeEngine.ComputeEngineSupervisor

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    Supervisor.init([], strategy: :one_for_one)
  end

  def stop_fleet() do
    Supervisor.stop(__MODULE__)
  end

  def start_compute_engine_sup(namespace, compute_engine_id, sock) do
    Supervisor.start_child(__MODULE__, %{
      id: compute_engine_id,
      start: {ComputeEngineSupervisor, :start_link, [{namespace, compute_engine_id, sock}]}
    })
  end

  def stop_compute_engine_sup(compute_engine_id) do
    Supervisor.terminate_child(__MODULE__, compute_engine_id)
    Supervisor.delete_child(__MODULE__, compute_engine_id)
  end

  def default_compute_engine() do
    child =
      DynamicSupervisor.which_children(__MODULE__)
      |> Enum.at(0)

    case child do
      {compute_engine_id, _, _, _} -> compute_engine_id
      _ -> raise "No compute_engine available"
    end
  end
end

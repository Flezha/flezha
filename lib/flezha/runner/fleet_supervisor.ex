defmodule Flezha.Runner.FleetSupervisor do
  alias Flezha.Runner.RunnerSupervisor

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

  def start_runner_sup(namespace, runner_id, sock, transport) do
    Supervisor.start_child(__MODULE__, %{
      id: runner_id,
      start: {RunnerSupervisor, :start_link, [{namespace, runner_id, sock, transport}]}
    })
  end

  def stop_runner_sup(runner_id) do
    Supervisor.terminate_child(__MODULE__, runner_id)
    Supervisor.delete_child(__MODULE__, runner_id)
  end

  def default_runner() do
    child =
      DynamicSupervisor.which_children(__MODULE__)
      |> Enum.at(0)

    case child do
      {runner_id, _, _, _} -> runner_id
      _ -> raise "No runner available"
    end
  end
end

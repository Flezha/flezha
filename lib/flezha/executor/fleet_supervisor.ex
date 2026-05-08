defmodule Flezha.Executor.FleetSupervisor do
  alias Flezha.Executor.ExecutorSupervisor

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

  def start_executor_sup(namespace, executor_id, sock, transport) do
    Supervisor.start_child(__MODULE__, %{
      id: executor_id,
      start:
        {ExecutorSupervisor, :start_link, [{namespace, executor_id, sock, transport}]}
    })
  end

  def stop_executor_sup(executor_id) do
    Supervisor.terminate_child(__MODULE__, executor_id)
    Supervisor.delete_child(__MODULE__, executor_id)
  end

  def default_executor() do
    child =
      DynamicSupervisor.which_children(__MODULE__)
      |> Enum.at(0)

    case child do
      {executor_id, _, _, _} -> executor_id
      _ -> raise "No executor available"
    end
  end
end

defmodule Aflecha.Operator.OperatorsSup do
  alias Aflecha.Operator.OperatorUnitSup

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    Supervisor.init([], strategy: :one_for_one)
  end

  def start_operator(operator_id, sock) do
    Supervisor.start_child(__MODULE__, %{
      id: operator_id,
      start: {OperatorUnitSup, :start_link, [{operator_id, sock}]}
    })
  end

  def default_operator() do
    child =
      DynamicSupervisor.which_children(__MODULE__)
      |> Enum.at(0)

    case child do
      {operator_id, _, _, _} -> operator_id
      _ -> raise "No operators available"
    end
  end
end

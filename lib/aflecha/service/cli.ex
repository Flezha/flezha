defmodule Aflecha.Service.Cli do
  alias Aflecha.Computation.Service.Activator

  def computation_start(operator_id, computation_id, factory_id, variables) do
    computation = %Aflecha.Computation.Builder{
      id: computation_id,
      factory_id: factory_id,
      variables: Aflecha.Service.Payloader.json_to_map(variables)
    }

    Activator.start_computation(operator_id(operator_id), computation)
  end

  defp operator_id(operator_id) do
    case operator_id do
      "" -> Aflecha.Operator.OperatorsSup.default_operator()
      _ -> operator_id
    end
  end
end

defmodule Aflecha.Operator.Controller do
  alias Aflecha.Computation.Builder, as: ComputationBuilder
  alias Aflecha.Service.Identificator

  use GenServer

  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: __MODULE__)
  end

  @impl true
  def init(_) do
    expect_factories()

    {:ok, %{}}
  end

  @impl true
  def handle_call({:get_computation_state, operator_id, computation_id}, _from, state) do
    computation_state =
      GenServer.call(
        Identificator.control_unit(operator_id),
        {:get_computation_state, computation_id}
      )

    {:reply, computation_state, state}
  end

  @impl true
  def handle_call({:add_computation, operator_id, computation}, _from, state) do
    response =
      case ComputationBuilder.validate_computation(computation) do
        {:error, reason} ->
          {:error, reason}

        :ok ->
          # TODO: we should validate the operator_id also.
          add_computation(operator_id, computation)
      end

    {:reply, response, state}
  end

  @impl true
  def handle_call({:rem_computation, operator_id, computation_id}, _from, state) do
    response = rem_computation(operator_id, computation_id)

    {:reply, response, state}
  end

  @impl true
  def handle_info({:factory_registered, _factory_id}, state) do
    {:noreply, state}
  end

  defp add_computation(operator_id, computation) do
    child_spec = %{
      id: computation.id,
      start: {Aflecha.Computation.WatchDog, :start_link, [{operator_id, computation}]}
    }

    result =
      ProcessHub.start_child(Identificator.hub(operator_id), child_spec, async_wait: true)
      |> ProcessHub.await()

    GenServer.call(Identificator.control_unit(operator_id), {:add_computation, computation})

    case result do
      {:ok, _} -> {:ok, "computation added"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp rem_computation(operator_id, computation_id) do
    GenServer.call(
      Identificator.control_unit(operator_id),
      {:rem_computation, computation_id}
    )

    result =
      ProcessHub.stop_child(Identificator.hub(operator_id), computation_id, async_wait: true)
      |> ProcessHub.await()

    case result do
      {:ok, _} -> {:ok, "computation removed"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp expect_factories do
    Aflecha.Service.EventManager.add_handler(:factory_registered)

    count = Application.get_env(:aflecha, :factories_expected_count)

    if is_integer(count) && count > 0 do
      for _x <- 1..count do
        receive do
          {:factory_registered, _factory_id} ->
            :ok
        after
          3000 -> raise "Registered factories not equal to expected count"
        end
      end
    end
  end
end

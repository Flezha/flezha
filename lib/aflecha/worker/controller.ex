defmodule Aflecha.Worker.Controller do
  alias Aflecha.Service.Identificator

  @impl true
  def handle_call({:get_computation_state, group_id, computation_id}, _from, state) do
    computation_state =
      GenServer.call(
        Identificator.control_unit(group_id),
        {:get_computation_state, computation_id}
      )

    {:reply, computation_state, state}
  end

  @impl true
  def handle_call({:start_computation, group_id, computation}, _from, state) do
    {:reply, response, state}
  end

  @impl true
  def handle_call({:stop_computation, group_id, computation_id}, _from, state) do
    response = rem_computation(group_id, computation_id)

    {:reply, response, state}
  end

  @impl true
  def handle_info({:factory_registered, _factory_id}, state) do
    {:noreply, state}
  end

  defp rem_computation(group_id, computation_id) do
    GenServer.call(
      Identificator.control_unit(group_id),
      {:stop_computation, computation_id}
    )

    result =
      ProcessHub.stop_child(Identificator.hub(group_id), computation_id, async_wait: true)
      |> ProcessHub.await()

    case result do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end

defmodule Aflecha.Subscriber.Controller do
  alias Aflecha.Computation.Builder, as: ComputationBuilder
  alias Aflecha.Service.Warden
  alias Aflecha.Service.Identificator

  use GenServer

  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: Identificator.controller(id))
  end

  @impl true
  def init(sub_id) do
    expect_factories()

    {:ok, %{subscriber_id: sub_id}}
  end

  @impl true
  def handle_call({:add_computation, computation}, _from, state) do
    response =
      case ComputationBuilder.validate_computation(computation) do
        {:error, reason} ->
          {:error, reason}

        :ok ->
          case ComputationBuilder.factory_match_engines(computation.factory_id) do
            {:error, reason} -> {:error, reason}
            {:ok, engines} -> Map.put(computation, :engines, engines) |> add_computation()
          end
      end

    {:reply, response, state}
  end

  @impl true
  def handle_call({:rem_computation, computation_id}, _from, state) do
    response = rem_computation(computation_id)

    {:reply, response, state}
  end

  defp add_computation(computation) do
    child_spec = %{
      id: computation.id,
      start: {Aflecha.Computation.WatchDog, :start_link, [computation]}
    }

    result =
      ProcessHub.start_child(:aflecha_hub, child_spec, async_wait: true)
      |> ProcessHub.await()

    Warden.get(:engines)
    |> Enum.each(fn {subscriber_id, _} ->
      GenServer.call(Identificator.control_unit(subscriber_id), {:add_computation, computation})
    end)

    case result do
      {:ok, _} -> {:ok, "computation added"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp rem_computation(computation_id) do
    Warden.get(:engines)
    |> Enum.each(fn {subscriber_id, _} ->
      GenServer.call(
        Identificator.control_unit(subscriber_id),
        {:rem_computation, computation_id}
      )
    end)

    result =
      ProcessHub.stop_child(:aflecha_hub, computation_id, async_wait: true)
      |> ProcessHub.await()

    case result do
      {:ok, _} -> {:ok, "computation removed"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp expect_factories do
    count = Aflecha.Service.EventManager.add_handler(:factories_expected_count)

    if is_integer(count) && count > 0 do
      for _x <- count do
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

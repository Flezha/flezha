defmodule Flecha.Controller do
  alias Flecha.Service.Validator
  alias Flecha.Warden
  alias Flecha.Service.Identificator

  use GenServer

  @computation_id_max_bytes 32

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    expect_factories()

    {:ok, %{}}
  end

  @impl true
  def handle_call({:add_computation, computation}, _from, state) do
    response =
      case validate_computation(computation) do
        {:error, reason} -> {:error, reason}
        :ok -> add_computation(computation)
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
      start: {Flecha.Computation, :start_link, [computation]}
    }

    result =
      ProcessHub.start_child(:flecha_hub, child_spec, async_wait: true)
      |> ProcessHub.await()

    Warden.get(:engines)
    |> Enum.each(fn {engine_id, _} ->
      GenServer.call(Identificator.control_unit(engine_id), {:add_computation, computation})
    end)

    case result do
      {:ok, _} -> {:ok, "computation added"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp rem_computation(computation_id) do
    Warden.get(:engines)
    |> Enum.each(fn {engine_id, _} ->
      GenServer.call(
        Identificator.control_unit(engine_id),
        {:rem_computation, computation_id}
      )
    end)

    result =
      ProcessHub.stop_child(:flecha_hub, computation_id, async_wait: true)
      |> ProcessHub.await()

    case result do
      {:ok, _} -> {:ok, "computation removed"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_computation(computation) do
    case Validator.byte_limit_exceed(computation.id, @computation_id_max_bytes) do
      true -> {:error, "computation id exceeds #{@computation_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  defp expect_factories do
    count = Flecha.Service.EventManager.add_handler(:factories_expected_count)

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

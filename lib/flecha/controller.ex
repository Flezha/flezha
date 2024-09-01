defmodule Flecha.Controller do
  alias Flecha.EngineManager
  alias Flecha.Service.Validator

  use GenServer

  @computation_id_max_bytes 32

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:add_computation, computation}, _from, state) do
    response = case validate_computation(computation) do
      {:error, reason} -> {:error, reason}
      :ok -> add_computation(computation)
    end

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

    GenServer.call(EngineManager, {:add_computation, computation})

    case result do
      {:ok, _} -> {:ok, "computation added"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_computation(computation) do
    case Validator.byte_limit_exceed(computation.id, @computation_id_max_bytes) do
      true -> {:error, "computation id exceeds #{@computation_id_max_bytes} bytes"}
      false -> :ok
    end
  end
end

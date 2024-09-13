defmodule Flecha.Controller do
  alias Flecha.Service.Validator
  alias Flecha.Warden
  alias Flecha.Utility.Identifier

  use GenServer

  @computation_id_max_bytes 32

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    init_hook = Application.fetch_env!(:flecha_core, :controller_init_hook)
    init_hook.()

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
      GenServer.call(Identifier.control_unit(engine_id), {:add_computation, computation})
    end)

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

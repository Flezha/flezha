defmodule Flezha.Service.ComputationManager do
  @moduledoc """
  Manages the computation startings and stoppings.
  """

  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Operator.FleetManager
  alias Flezha.Computation.Builder, as: ComputationBuilder

  def computation_registry(group_id) do
    case HubManager.get_registry(group_id) do
      {:ok, registry} ->
        registry

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_computation_ids(group_id) do
    computations =
      group_id
      |> HubManager.get_all_computations()
      |> Enum.map(&elem(&1, 0))

    {:ok, computations}
  end

  def get_computation_pid(group_id, computation_id) do
    case HubManager.get_pid(group_id, computation_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, "no pid found"} -> {:error, "computation not found"}
      err -> err
    end
  end

  def get_overwatch_state(group_id, computation_id) do
    case get_computation_pid(group_id, computation_id) do
      {:ok, computation_pid} ->
        {:ok, GenServer.call(computation_pid, :get_overwatch_state)}

      err ->
        err
    end
  end

  @doc """
  Starts a new computation to the operator.
  """
  @spec start_computation(String.t(), ComputationBuilder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def start_computation(group_id, computation) do
    case ComputationBuilder.validate_computation(group_id, computation) do
      {:error, reason} ->
        {:error, reason}

      :ok ->
        # TODO: we should validate the group_id also.
        init_computation(group_id, computation)
    end
  end

  @doc """
  Removes a computation.
  """
  @spec stop_computation(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_computation(group_id, computation_id) do
    case get_computation_pid(group_id, computation_id) do
      {:ok, computation_pid} ->
        case GenServer.call(computation_pid, :stop_computation) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end

  def stop_all_computations(group_id) do
    computations = HubManager.get_all_computations(group_id)

    Enum.map(computations, fn {_computation_id, computation_pid} ->
      if Process.alive?(computation_pid) do
        GenServer.call(computation_pid, :stop_computation)
      else
        {:error, "computation not alive"}
      end
    end)
  end

  def send_io(computation_id, group_id, message, metadata) do
    case get_computation_pid(group_id, computation_id) do
      {:ok, computation_pid} ->
        GenServer.cast(computation_pid, {:io_in, message, metadata})

      err ->
        err
    end
  end

  def create_snapshot(group_id, computation_id) do
    case get_computation_pid(group_id, computation_id) do
      {:ok, computation_pid} ->
        GenServer.call(computation_pid, :create_snapshot)

      err ->
        err
    end
  end

  defp init_computation(group_id, computation) do
    child_spec = %{
      id: computation.id,
      start: {
        Flezha.Computation.Overwatch,
        :start_link,
        [{group_id, computation}]
      },
      restart: :transient
    }

    result =
      Identificator.hub(group_id)
      |> ProcessHub.start_child(child_spec,
        async_wait: true,
        # We will get the errors from the start result.
        disable_logging: true
      )
      |> ProcessHub.await()

    case result do
      {:ok, {_computation_id, [{node, pid}]}} ->
        {:ok, operator_id} = GenServer.call(pid, :registry_operator_set)

        {:ok,
         %{
           computation_id: computation.id,
           operator_id: operator_id,
           node: node,
           pid: pid
         }}

      {:error, reason} ->
        {{computation_id, node, {error_msg, _}}, _partial_success} = reason

        {
          :error,
          %{
            computation_id: computation_id,
            node: node,
            reason: error_msg
          }
        }
    end
  end

  def exist?(group_id, computation_id) do
    case get_computation_pid(group_id, computation_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  def get_computation_state(group_id, computation_id) do
    case get_computation_pid(group_id, computation_id) do
      {:ok, computation_pid} -> GenServer.call(computation_pid, :get_computation_state)
      err -> err
    end
  end

  def assign_operator(group_id) do
    select_operator(FleetManager.active_operators(group_id))
  end

  defp select_operator(operator_ids) do
    # TODO: add load balancing here.
    Enum.random(operator_ids)
  end
end

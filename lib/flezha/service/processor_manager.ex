defmodule Flezha.Service.ProcessorManager do
  @moduledoc """
  Manages the processor startings and stoppings.
  """

  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Operator.FleetManager
  alias Flezha.Processor.Builder, as: ProcessorBuilder

  def processor_registry(namespace) do
    case HubManager.get_registry(namespace) do
      {:ok, registry} ->
        registry

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_processor_ids(namespace) do
    processors =
      namespace
      |> HubManager.get_all_processors()
      |> Enum.map(&elem(&1, 0))

    {:ok, processors}
  end

  def get_processor_pid(namespace, processor_id) do
    case HubManager.get_pid(namespace, processor_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, "no pid found"} -> {:error, "processor not found"}
      err -> err
    end
  end

  def get_overwatch_state(namespace, processor_id) do
    case get_processor_pid(namespace, processor_id) do
      {:ok, processor_pid} ->
        {:ok, GenServer.call(processor_pid, :get_overwatch_state)}

      err ->
        err
    end
  end

  @doc """
  Starts a new processor to the operator.
  """
  @spec start_processor(String.t(), ProcessorBuilder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def start_processor(namespace, processor) do
    case ProcessorBuilder.validate_processor(namespace, processor) do
      {:error, reason} ->
        {:error, reason}

      :ok ->
        # TODO: we should validate the namespace also.
        init_processor(namespace, processor)
    end
  end

  @doc """
  Removes a processor.
  """
  @spec stop_processor(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_processor(namespace, processor_id) do
    case get_processor_pid(namespace, processor_id) do
      {:ok, processor_pid} ->
        case GenServer.call(processor_pid, :stop_processor) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end

  def stop_all_processors(namespace) do
    processors = HubManager.get_all_processors(namespace)

    Enum.map(processors, fn {_processor_id, processor_pid} ->
      if Process.alive?(processor_pid) do
        GenServer.call(processor_pid, :stop_processor)
      else
        {:error, "processor not alive"}
      end
    end)
  end

  def send_io(processor_id, namespace, message, metadata) do
    case get_processor_pid(namespace, processor_id) do
      {:ok, processor_pid} ->
        GenServer.cast(processor_pid, {:io_in, message, metadata})

      err ->
        err
    end
  end

  def create_snapshot(namespace, processor_id) do
    case get_processor_pid(namespace, processor_id) do
      {:ok, processor_pid} ->
        GenServer.call(processor_pid, :create_snapshot)

      err ->
        err
    end
  end

  defp init_processor(namespace, processor) do
    child_spec = %{
      id: processor.id,
      start: {
        Flezha.Processor.Overwatch,
        :start_link,
        [{namespace, processor}]
      },
      restart: :transient
    }

    result =
      Identificator.hub(namespace)
      |> ProcessHub.start_child(child_spec,
        async_wait: true,
        # We will get the errors from the start result.
        disable_logging: true
      )
      |> ProcessHub.await()

    case result do
      {:ok, {_processor_id, [{node, pid}]}} ->
        {:ok, operator_id} = GenServer.call(pid, :registry_operator_set)

        {:ok,
         %{
           processor_id: processor.id,
           operator_id: operator_id,
           node: node,
           pid: pid
         }}

      {:error, reason} ->
        {{processor_id, node, {error_msg, _}}, _partial_success} = reason

        {
          :error,
          %{
            processor_id: processor_id,
            node: node,
            reason: error_msg
          }
        }
    end
  end

  def exist?(namespace, processor_id) do
    case get_processor_pid(namespace, processor_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  def get_processor_state(namespace, processor_id) do
    case get_processor_pid(namespace, processor_id) do
      {:ok, processor_pid} -> GenServer.call(processor_pid, :get_processor_state)
      err -> err
    end
  end

  def assign_operator(namespace) do
    select_operator(FleetManager.active_operators(namespace))
  end

  defp select_operator(operator_ids) do
    # TODO: add load balancing here.
    Enum.random(operator_ids)
  end
end

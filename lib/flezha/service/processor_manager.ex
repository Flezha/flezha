defmodule Flezha.Service.ProcessorManager do
  @moduledoc """
  Manages the processor startings and stoppings.
  """

  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.ComputeEngine.FleetManager
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
  Starts a new processor to the compute_engine.
  """
  @spec start_processor(String.t(), ProcessorBuilder.t()) ::
          {:ok, map()} | {:error, String.t()}
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
        awaitable: true,
        # We will get the errors from the start result.
        disable_logging: true
      )
      |> ProcessHub.Future.await()

    case ProcessHub.StartResult.status() do
      :ok ->
        pid = ProcessHub.StartResult.pid(result)
        node = ProcessHub.StartResult.node(result)

        {:ok, compute_engine_id} = GenServer.call(pid, :registry_compute_engine_set)

        {:ok,
         %{
           processor_id: processor.id,
           compute_engine_id: compute_engine_id,
           node: node,
           pid: pid
         }}

      :error ->
        reason =
          result
          |> ProcessHub.StartResult.errors()
          |> List.first()
          |> elem(1)

        {
          :error,
          %{
            processor_id: processor_id,
            node: node,
            reason: inspect(reason)
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

  def assign_compute_engine(namespace) do
    select_compute_engine(FleetManager.active_compute_engines(namespace))
  end

  defp select_compute_engine(compute_engine_ids) do
    # TODO: add load balancing here.
    Enum.random(compute_engine_ids)
  end
end

defmodule Flezha.Service.ProcessorManager do
  @moduledoc """
  Manages the processor startings and stoppings.
  """

  require Logger

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
    hub_id = Identificator.hub(namespace)
    processors = HubManager.get_all_processors(namespace)
    processor_ids = Enum.map(processors, fn {id, _pid} -> id end)

    if processor_ids != [] do
      try do
        hub_id
        |> ProcessHub.stop_children(processor_ids, awaitable: true, timeout: 15_000)
        |> ProcessHub.Future.await()
      catch
        :exit, _ -> :ok
      end
    end

    :ok
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
        disable_logging: true,
        timeout: 15_000
      )
      |> ProcessHub.Future.await()

    case result do
      %ProcessHub.StartResult{} ->
        our_started =
          Enum.find(result.started || [], fn {cid, _} -> cid == processor.id end)

        case our_started do
          {_cid, [{processor_node, pid}]} ->
            {:ok, compute_engine_id} = GenServer.call(pid, :registry_compute_engine_set)

            {:ok,
             %{
               processor_id: processor.id,
               compute_engine_id: compute_engine_id,
               node: processor_node,
               pid: pid
             }}

          _ ->
            Logger.warning(
              "Processor #{processor.id} failed to start. " <>
                "Started: #{inspect(result.started)}, Errors: #{inspect(result.errors)}, " <>
                "Status: #{inspect(result.status)}"
            )

            reason =
              case Enum.find(result.errors || [], fn {cid, _} -> cid == processor.id end) do
                {_cid, {:error, reason}} -> reason
                {_cid, {reason, _child_spec}} -> reason
                {_cid, reason} -> reason
                _ -> :unknown
              end

            {:error, %{processor_id: processor.id, node: node(), reason: reason}}
        end

      {:error, reason} ->
        {:error, %{processor_id: processor.id, node: node(), reason: reason}}
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
    case FleetManager.active_compute_engines(namespace) do
      [] -> nil
      compute_engine_ids -> Enum.random(compute_engine_ids)
    end
  end
end

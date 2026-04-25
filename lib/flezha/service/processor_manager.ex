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
    with :ok <- ProcessorBuilder.validate_processor(namespace, processor),
         :ok <- validate_factory(namespace, processor.factory_id) do
      # TODO: we should validate the namespace also.
      init_processor(namespace, processor)
    else
      {:error, reason} when is_binary(reason) ->
        {:error, %{processor_id: processor.id, node: node(), reason: reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_factory(namespace, factory_id) when is_binary(factory_id) do
    case FleetManager.compute_engines_for_factory(namespace, factory_id) do
      [] -> {:error, "Factory with id #{factory_id} does not exist"}
      [_ | _] -> :ok
    end
  end

  defp validate_factory(_namespace, _factory_id), do: :ok

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

  @no_response_retry_attempts 8
  @no_response_retry_initial_ms 150

  defp init_processor(namespace, processor, attempt \\ 0) do
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

    # `:no_response` surfaces when ProcessHub's `with_partition_check`
    # short-circuits `execute/2` without sending a response — i.e. the
    # local hub's `dist_sup` was momentarily missing or dead, so
    # `Service.State.is_partitioned?/1` returned `true`. The dist_sup
    # restarts on its own; the only thing the caller can do is wait it
    # out. Retry up to @no_response_retry_attempts times with
    # exponential backoff (150, 300, 600, 1200, 2400, ... ms, capped at
    # 5s) so a transient partition during agent/cluster startup, a
    # snapshot/restore taking down `dist_sup`, or parallel-suite test
    # churn doesn't surface as a hard failure. Stable steady-state
    # `:no_response` (8 attempts, ~14s of wait) still propagates.
    cond do
      no_response?(result) and attempt < @no_response_retry_attempts ->
        backoff = min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000)
        Process.sleep(backoff)
        init_processor(namespace, processor, attempt + 1)

      true ->
        finalize_init_processor(processor, result)
    end
  end

  defp no_response?(%ProcessHub.StartResult{errors: errors}) when is_list(errors) do
    Enum.any?(errors, fn
      {_cid, :no_response} -> true
      {_cid, {:no_response, _}} -> true
      {_cid, {:error, :no_response}} -> true
      _ -> false
    end)
  end

  defp no_response?(_result), do: false

  defp finalize_init_processor(processor, result) do
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
    assign_compute_engine(namespace, nil)
  end

  @doc """
  Pick an active compute engine for a new processor.

  When `factory_id` is given, restricts the pool to compute engines that
  actually advertise that factory. Without this filter a mixed-factory
  namespace (e.g. market-processor owning `crypto_trades` + market-app
  owning `market-init-state`) would random-pick the wrong node half the
  time, routing every input frame to a process that doesn't exist on
  the foreign node. The Java SDK surfaces this as a steady stream of
  `DefaultProcessorDispatcher.enqueue: processor 'X' not found`
  warnings.
  """
  def assign_compute_engine(namespace, nil) do
    case FleetManager.active_compute_engines(namespace) do
      [] -> nil
      compute_engine_ids -> Enum.random(compute_engine_ids)
    end
  end

  def assign_compute_engine(namespace, factory_id) when is_binary(factory_id) do
    case FleetManager.compute_engines_for_factory(namespace, factory_id) do
      [] -> nil
      compute_engine_ids -> Enum.random(compute_engine_ids)
    end
  end
end

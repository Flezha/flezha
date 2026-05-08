defmodule Flezha.Service.RelayManager do
  @moduledoc """
  Manages the relay startings and stoppings.
  """

  require Logger

  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Executor.FleetManager
  alias Flezha.Relay.Builder, as: RelayBuilder

  def relay_registry(namespace) do
    case HubManager.get_registry(namespace) do
      {:ok, registry} ->
        registry

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_relay_ids(namespace) do
    relays =
      namespace
      |> HubManager.get_all_relays()
      |> Enum.map(&elem(&1, 0))

    {:ok, relays}
  end

  def get_relay_pid(namespace, relay_id) do
    case HubManager.get_pid(namespace, relay_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, "no pid found"} -> {:error, "relay not found"}
      err -> err
    end
  end

  def get_overwatch_state(namespace, relay_id) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, relay_pid} ->
        {:ok, GenServer.call(relay_pid, :get_overwatch_state)}

      err ->
        err
    end
  end

  @doc """
  Starts a new relay to the executor.
  """
  @spec start_relay(String.t(), RelayBuilder.t()) ::
          {:ok, map()} | {:error, String.t()}
  def start_relay(namespace, relay) do
    with :ok <- RelayBuilder.validate_relay(namespace, relay),
         :ok <- validate_factory(namespace, relay.factory_id) do
      # TODO: we should validate the namespace also.
      init_relay(namespace, relay)
    else
      {:error, reason} when is_binary(reason) ->
        {:error, %{relay_id: relay.id, node: node(), reason: reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_factory(namespace, factory_id) when is_binary(factory_id) do
    case FleetManager.executors_for_factory(namespace, factory_id) do
      [] -> {:error, "Factory with id #{factory_id} does not exist"}
      [_ | _] -> :ok
    end
  end

  defp validate_factory(_namespace, _factory_id), do: :ok

  @doc """
  Removes a relay.
  """
  @spec stop_relay(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_relay(namespace, relay_id) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, relay_pid} ->
        case GenServer.call(relay_pid, :stop_relay) do
          :ok ->
            # Drop any plugins keyed against `(namespace, relay_id)` from
            # the global PluginManager registry now that the relay is
            # gone. Without this, a plugin enabled in one test bleeds
            # into the next as `{:error, "plugin already registered"}`
            # on re-enable.
            _ = Flezha.Service.PluginManager.disable_relay_plugins(namespace, relay_id)
            :ok

          {:error, reason} ->
            {:error, reason}
        end

      err ->
        err
    end
  end

  def stop_all_relays(namespace) do
    hub_id = Identificator.hub(namespace)
    relays = HubManager.get_all_relays(namespace)
    relay_ids = Enum.map(relays, fn {id, _pid} -> id end)

    # Disable plugins per-relay before the namespace is torn down so the
    # PluginManager registry stays in sync with the runtime.
    Enum.each(relay_ids, fn relay_id ->
      _ = Flezha.Service.PluginManager.disable_relay_plugins(namespace, relay_id)
    end)

    if relay_ids != [] do
      try do
        hub_id
        |> ProcessHub.stop_children(relay_ids, awaitable: true, timeout: 15_000)
        |> ProcessHub.Future.await()
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  def send_io(relay_id, namespace, message, metadata) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, relay_pid} ->
        GenServer.cast(relay_pid, {:io_in, message, metadata})

      err ->
        err
    end
  end

  def create_snapshot(namespace, relay_id) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, relay_pid} ->
        GenServer.call(relay_pid, :create_snapshot)

      err ->
        err
    end
  end

  @no_response_retry_attempts 8
  @no_response_retry_initial_ms 150

  defp init_relay(namespace, relay, attempt \\ 0) do
    child_spec = %{
      id: relay.id,
      start: {
        Flezha.Relay.Overwatch,
        :start_link,
        [{namespace, relay}]
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
    # 5s) so a transient partition during controller/cluster startup, a
    # snapshot/restore taking down `dist_sup`, or parallel-suite test
    # churn doesn't surface as a hard failure. Stable steady-state
    # `:no_response` (8 attempts, ~14s of wait) still propagates.
    cond do
      no_response?(result) and attempt < @no_response_retry_attempts ->
        backoff = min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000)
        Process.sleep(backoff)
        init_relay(namespace, relay, attempt + 1)

      true ->
        finalize_init_relay(relay, result)
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

  defp finalize_init_relay(relay, result) do
    case result do
      %ProcessHub.StartResult{} ->
        our_started =
          Enum.find(result.started || [], fn {cid, _} -> cid == relay.id end)

        case our_started do
          {_cid, [{relay_node, pid}]} ->
            {:ok, executor_id} = GenServer.call(pid, :registry_executor_set)

            {:ok,
             %{
               relay_id: relay.id,
               executor_id: executor_id,
               node: relay_node,
               pid: pid
             }}

          _ ->
            Logger.warning(
              "Relay #{relay.id} failed to start. " <>
                "Started: #{inspect(result.started)}, Errors: #{inspect(result.errors)}, " <>
                "Status: #{inspect(result.status)}"
            )

            reason =
              case Enum.find(result.errors || [], fn {cid, _} -> cid == relay.id end) do
                {_cid, {:error, reason}} -> reason
                {_cid, {reason, _child_spec}} -> reason
                {_cid, reason} -> reason
                _ -> :unknown
              end

            {:error, %{relay_id: relay.id, node: node(), reason: reason}}
        end

      {:error, reason} ->
        {:error, %{relay_id: relay.id, node: node(), reason: reason}}
    end
  end

  def exist?(namespace, relay_id) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  def get_partition_state(namespace, relay_id) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, relay_pid} -> GenServer.call(relay_pid, :get_partition_state)
      err -> err
    end
  end

  def assign_executor(namespace) do
    assign_executor(namespace, nil)
  end

  @doc """
  Pick an active compute engine for a new relay.

  When `factory_id` is given, restricts the pool to compute engines that
  actually advertise that factory. Without this filter a mixed-factory
  namespace (e.g. market-relay owning `crypto_trades` + market-app
  owning `market-init-state`) would random-pick the wrong node half the
  time, routing every input frame to a process that doesn't exist on
  the foreign node. The Java SDK surfaces this as a steady stream of
  `DefaultRelayDispatcher.enqueue: relay 'X' not found`
  warnings.
  """
  def assign_executor(namespace, nil) do
    case FleetManager.active_executors(namespace) do
      [] -> nil
      executor_ids -> Enum.random(executor_ids)
    end
  end

  def assign_executor(namespace, factory_id) when is_binary(factory_id) do
    case FleetManager.executors_for_factory(namespace, factory_id) do
      [] -> nil
      executor_ids -> Enum.random(executor_ids)
    end
  end
end

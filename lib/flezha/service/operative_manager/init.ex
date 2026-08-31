defmodule Flezha.Service.OperativeManager.Init do
  @moduledoc """
  Creating the operative under the hub and settling what the start returned —
  the `:no_response` retry ladder, the claim-loss concession, and the quiet
  teardown of a child that lost its claim.
  """

  require Logger

  alias Flezha.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager.Teardown
  alias Flezha.Service.Validator

  # Losing the id claim is not a failed start: another path already runs this
  # operative. Conceding is the WHOLE remedy — the winner is never stopped,
  # restarted, or rebound, which is exactly what the generic failure path
  # (`stop_child_quietly/2`) would have done to it.
  #
  # A claim refused with no live winner behind it is a different thing — a
  # stale registry row — and stays an error so it is not silently accepted.
  def claim_lost(exist?, namespace, operative_id) do
    if exist?.(namespace, operative_id) do
      Logger.debug(
        "Operative #{operative_id} start conceded: id already claimed by a live instance"
      )

      :duplicate
    else
      {:error, %{operative_id: operative_id, node: node(), reason: Validator.id_in_use()}}
    end
  end

  @no_response_retry_attempts 8

  @no_response_retry_initial_ms 150

  def init_operative(exist?, namespace, operative, attempt \\ 0) do
    result = start_child(namespace, operative)

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
        Process.sleep(min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000))
        init_operative(exist?, namespace, operative, attempt + 1)

      true ->
        finalize_init_operative(exist?, namespace, operative, result)
    end
  end

  defp start_child(namespace, operative) do
    child_spec = %{
      id: operative.id,
      start: {
        Flezha.Operative.Overwatch,
        :start_link,
        [{namespace, operative}]
      },
      restart: :transient
    }

    Identificator.hub(namespace)
    |> ProcessHub.start_child(child_spec,
      awaitable: true,
      # We will get the errors from the start result.
      disable_logging: true,
      timeout: 15_000,
      # A partition member's identity is on its row from the first
      # registration, so `PartitionGroup.members/2` sees it before it binds.
      metadata: partition_metadata(operative),
      # Declared intent: the start records the child in the hub's declared
      # set BEFORE anything runs, so a whole-cluster cold start restores it
      # and a deliberate stop (hub stop path) removes it for good. Off in
      # lockstep with auto-recovery — a hub without the reconcile refuses
      # durable starts.
      durable: HubManager.durable_starts?()
    )
    |> ProcessHub.Future.await()
  end

  defp partition_metadata(operative) do
    case Flezha.Partition.identity(operative.opts) do
      nil -> %{}
      partition -> %{partition: partition}
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

  defp finalize_init_operative(exist?, namespace, operative, result) do
    case result do
      %ProcessHub.StartResult{} ->
        case Enum.find(result.started || [], fn {cid, _} -> cid == operative.id end) do
          {_cid, [{operative_node, pid}]} ->
            bind_started(namespace, operative, operative_node, pid)

          _ ->
            refuse_start(exist?, namespace, operative, result)
        end

      {:error, reason} ->
        settle_claim(exist?, namespace, operative, reason)
    end
  end

  defp bind_started(namespace, operative, operative_node, pid) do
    # ProcessHub reports `started` as soon as `start_link/1`
    # returns; the operative's `handle_continue(:bind_runner, …)`
    # surfaces a plugin-chain failure via the `init_error` field
    # on Overwatch state — `:registry_runner_set` returns
    # `{:error, _}` instead of crashing the GenServer. The
    # `:exit` catch covers older failure modes (timeout, kill).
    try do
      case GenServer.call(pid, :registry_runner_set) do
        {:ok, runner_id} ->
          {:ok,
           %{
             operative_id: operative.id,
             runner_id: runner_id,
             node: operative_node,
             pid: pid
           }}

        {:error, reason} ->
          _ = stop_child_quietly(namespace, operative.id)
          {:error, %{operative_id: operative.id, node: node(), reason: reason}}
      end
    catch
      # Started, then exited cleanly before we could read its binding —
      # a run-once workflow operative that already completed, or a concurrent stop. A
      # clean exit is a successful start, not a failure; no live binding
      # remains, so `runner_id` is nil.
      :exit, {reason, _} when reason in [:normal, :noproc] ->
        {:ok, %{operative_id: operative.id, runner_id: nil, node: operative_node, pid: pid}}

      :exit, {reason, _} ->
        Logger.warning(
          "Operative #{operative.id} appeared in `started` but died during " <>
            "post-init continue (reason=#{inspect(reason)})"
        )

        _ = stop_child_quietly(namespace, operative.id)
        {:error, %{operative_id: operative.id, node: node(), reason: reason}}
    end
  end

  defp refuse_start(exist?, namespace, operative, result) do
    reason =
      case Enum.find(result.errors || [], fn {cid, _} -> cid == operative.id end) do
        {_cid, {:error, reason}} -> reason
        {_cid, {reason, _child_spec}} -> reason
        {_cid, reason} -> reason
        _ -> :unknown
      end

    if claim_loss?(reason) do
      claim_lost(exist?, namespace, operative.id)
    else
      Logger.warning(
        "Operative #{operative.id} failed to start. " <>
          "Started: #{inspect(result.started)}, Errors: #{inspect(result.errors)}, " <>
          "Status: #{inspect(result.status)}"
      )

      _ = stop_child_quietly(namespace, operative.id)
      {:error, %{operative_id: operative.id, node: node(), reason: reason}}
    end
  end

  defp settle_claim(exist?, namespace, operative, reason) do
    if claim_loss?(reason),
      do: claim_lost(exist?, namespace, operative.id),
      else: {:error, %{operative_id: operative.id, node: node(), reason: reason}}
  end

  # The registry refused the id: a live instance already holds it. Taking the
  # failure path on that verdict would call `stop_child_quietly/2` on the id —
  # killing the winner — which is precisely the start-authority violation the
  # claim exists to prevent.
  def claim_loss?(:already_started), do: true
  def claim_loss?({:already_started, _ids}), do: true
  def claim_loss?(reason), do: reason == Validator.id_in_use()

  defp stop_child_quietly(namespace, operative_id) do
    _ = Teardown.hub_stop_children(namespace, [operative_id], 5_000)
    :ok
  end
end

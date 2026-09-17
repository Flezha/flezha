defmodule Flezha.Service.OperativeManager.Init do
  @moduledoc """
  Creating the operative under the hub and settling what the start returned —
  the `:no_response` retry ladder, the claim-loss concession, and the quiet
  teardown of a child that lost its claim.
  """

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.Identificator
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager.Teardown
  alias Flezha.Service.Validator
  alias ProcessHub.Service.DeclaredChildren

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

  # The declared set is written at start and at stop and has nowhere else to
  # learn anything, so a setting changed while an operative runs never reaches
  # the record a restart rebuilds it from. This is that update, and it lives
  # here because `durable-operator-intent` puts every declared-set write behind
  # the start authority: the plugin manager asks, it does not reach in.

  # One re-check after a write that changed something, so a write that raced a
  # concurrent one converges on the row both of them merged into, and a declared
  # entry left stale by an earlier failure is repaired by the next write.
  #
  # Defence, not a demonstrated fix: `declared_mutate_contract_test.exs`'s
  # concurrency case passes with this at 1 as well, so the interleaving it
  # guards is one that test cannot force. A third pass would only repeat the
  # second.
  @redeclare_attempts 2

  @doc """
  Make `operative_id`'s declared child spec carry the plugins its registry row
  carries, so a restart-driven start builds the operative from its current
  plugin set rather than the one it was created with.

  The row is the source, not the caller. `ProcessRegistry.update/4` applies its
  transform inside the registry, so concurrent plugin writes for one operative
  merge there atomically; building the new list in the caller instead would let
  two concurrent enables each declare a list computed before the other's and
  silently drop one, which is the defect this whole change exists to fix. The
  write is re-checked for the same reason: whoever finishes last observes the
  settled row and declares it.

  A declared entry that already matches is left alone, so a no-op write never
  reaches the leader.

  `:ok` when durable starts are off: that mode declares nothing at start either,
  so there is no entry to keep current and none is invented.
  """
  @spec redeclare_plugins(binary(), binary()) :: :ok | {:error, term()}
  def redeclare_plugins(namespace, operative_id) do
    if HubManager.durable_starts?() do
      sync_declared_plugins(namespace, operative_id, @redeclare_attempts)
    else
      :ok
    end
  end

  defp sync_declared_plugins(_namespace, _operative_id, 0), do: :ok

  defp sync_declared_plugins(namespace, operative_id, attempts) do
    hub_id = Identificator.hub(namespace)

    with {:ok, hub} <- hub_state(hub_id),
         {:ok, spec} <- declared_spec(hub, operative_id),
         {:ok, row_plugins} <- HubManager.operative_plugins(namespace, operative_id) do
      case HubManager.rewrite_cspec_plugins(spec, fn _ -> row_plugins end) do
        ^spec ->
          :ok

        declared ->
          with :ok <- declare(hub, hub_id, declared) do
            sync_declared_plugins(namespace, operative_id, attempts - 1)
          end
      end
    end
  end

  defp hub_state(hub_id) do
    {:ok, GenServer.call(hub_id, :get_state, Durations.get(:hub_declared_mutate_ms))}
  catch
    :exit, _ -> {:error, :hub_unavailable}
  end

  # An upsert would declare an operative that was never declared, which would
  # let a plugin write resurrect one a stop removed for good.
  defp declared_spec(hub, operative_id) do
    case DeclaredChildren.manifest(hub) do
      %{entries: %{^operative_id => spec}} -> {:ok, spec}
      _ -> {:error, :operative_not_declared}
    end
  end

  # `{:add, [spec]}` is an upsert keyed on child id (`DeclaredChildren`'s
  # `mutate_entries/2`), so re-declaring replaces that one entry in place and
  # starts, stops or moves nothing. It must run on the leader, the one node that
  # may bump the list version, and it shares the batch that coalesces every
  # concurrent declared write into one write and one sync.
  #
  # `{:declared_mutate, _}` is process_hub's own leader-side entry point and is
  # not part of its public API; `declared_mutate_contract_test.exs` fails loudly
  # if a release moves it, and the tidy public wrapper replaces this the moment
  # one ships.
  defp declare(hub, hub_id, spec) do
    case DeclaredChildren.leader(hub) do
      leader when leader == node() -> call_declared_mutate(hub_id, spec)
      leader -> remote_declared_mutate(leader, hub_id, spec)
    end
  catch
    :exit, reason -> {:error, {:declared_mutate_failed, reason}}
  end

  @doc false
  def call_declared_mutate(hub_id, spec) do
    GenServer.call(
      hub_id,
      {:declared_mutate, {:add, [spec]}},
      Durations.get(:hub_declared_mutate_ms)
    )
  end

  # `:erpc.call/5` raises rather than exits when the node is gone, so this arm
  # carries the extra rescue the local one does not need.
  defp remote_declared_mutate(leader, hub_id, spec) do
    :erpc.call(
      leader,
      __MODULE__,
      :call_declared_mutate,
      [hub_id, spec],
      Durations.get(:hub_declared_mutate_rpc_ms)
    )
  rescue
    error -> {:error, {:declared_mutate_failed, error}}
  end

  def init_operative(exist?, namespace, operative, attempt \\ 0) do
    result = start_child(namespace, operative)

    # `:no_response` surfaces when ProcessHub's `with_partition_check`
    # short-circuits `execute/2` without sending a response — i.e. the
    # local hub's `dist_sup` was momentarily missing or dead, so
    # `Service.State.is_partitioned?/1` returned `true`. The dist_sup
    # restarts on its own; the only thing the caller can do is wait it
    # out, on the `:no_response` row of the backoff table, so a transient
    # partition during controller/cluster startup, a snapshot/restore
    # taking down `dist_sup`, or parallel-suite test churn doesn't surface
    # as a hard failure. A steady-state `:no_response` still propagates
    # once the row gives up.
    cond do
      no_response?(result) and not Backoff.give_up?(:no_response, attempt) ->
        Backoff.wait(:no_response, attempt + 1, operative.id)
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
      timeout: Durations.get(:hub_start_child_ms),
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
    # Started, then exited cleanly before we could read its binding — a
    # run-once workflow operative that already completed, or a concurrent
    # stop. A clean exit is a successful start, not a failure; no live
    # binding remains, so `runner_id` is nil.
    on_exit = fn
      {reason, _} when reason in [:normal, :noproc] ->
        {:ok, %{operative_id: operative.id, runner_id: nil, node: operative_node, pid: pid}}

      reason ->
        Logger.warning(
          "Operative #{operative.id} appeared in `started` but died during " <>
            "post-init continue (reason=#{inspect(reason)})"
        )

        _ = stop_child_quietly(namespace, operative.id)
        {:error, %{operative_id: operative.id, node: node(), reason: reason}}
    end

    case SafeCall.bounded(pid, :registry_runner_set, Durations.get(:overwatch_call_timeout_ms),
           on_error: on_exit
         ) do
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

      answered ->
        answered
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

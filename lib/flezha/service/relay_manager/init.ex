defmodule Flezha.Service.RelayManager.Init do
  @moduledoc """
  Creating the relay under the hub and settling what the start returned —
  the `:no_response` retry ladder, the claim-loss concession, and the quiet
  teardown of a child that lost its claim.
  """

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.Identificator
  alias Flezha.Process.Hub
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager.Teardown
  alias ProcessHub.Service.DeclaredChildren
  alias ProcessHub.Service.ProcessRegistry

  # ProcessHub's own answer for a row it does not hold (yet).
  @no_child "No child found"

  # Losing the id claim is not a failed start: another path already runs this
  # relay. Conceding is the WHOLE remedy — the winner is never stopped,
  # restarted, or rebound, which is exactly what the generic failure path
  # (`stop_child_quietly/2`) would have done to it.
  #
  # A claim refused with no live winner behind it is a different thing — a
  # stale registry row — and stays an error so it is not silently accepted.
  def claim_lost(exist?, namespace, relay_id) do
    if exist?.(namespace, relay_id) do
      Logger.debug("Relay #{relay_id} start conceded: id already claimed by a live instance")

      :duplicate
    else
      {:error, {:id_in_use, relay_id}}
    end
  end

  # The declared set is written at start and at stop and has nowhere else to
  # learn anything, so a setting changed while a relay runs never reaches
  # the record a restart rebuilds it from. This is that update, and it lives
  # here because `durable-relay-intent` puts every declared-set write behind
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
  Make `child_id`'s declared child spec carry the plugins its registry row
  carries — a relay's bindings, or a plugin child's own entry — so a
  restart-driven start builds it from its current set rather than the one it
  was created with.

  The row is the source, not the caller. `ProcessRegistry.update/4` applies its
  transform inside the registry, so concurrent plugin writes for one relay
  merge there atomically; building the new list in the caller instead would let
  two concurrent binds each declare a list computed before the other's and
  silently drop one. The write is re-checked for the same reason: whoever
  finishes last observes the settled row and declares it.

  A declared entry that already matches is left alone, so a no-op write never
  reaches the leader.

  `:ok` when durable starts are off: that mode declares nothing at start either,
  so there is no entry to keep current and none is invented.
  """
  @spec redeclare(binary(), term()) :: :ok | {:error, term()}
  def redeclare(namespace, child_id) do
    if HubManager.durable_starts?() do
      sync_declared(namespace, child_id, @redeclare_attempts)
    else
      :ok
    end
  end

  @doc """
  Apply `transform` to `child_id`'s registry row on every node, then make its
  declared entry carry the result (`redeclare/2`): two records, one mutation,
  and every plugin write goes through here. The row is what `flz relay info`
  and the convergence backstop read while the child runs; the declared set is
  what a restart rebuilds it from, and it is the ONLY one a restart reads.

  The row goes first and is the source the declared write copies from: its
  transform runs inside the registry, so concurrent writes for one child merge
  there atomically rather than in two callers that cannot see each other.
  """
  @spec write_plugins(binary(), term(), function()) :: :ok | {:error, term()}
  def write_plugins(namespace, child_id, transform) do
    with :ok <- write_row(namespace, child_id, transform, 0), do: redeclare(namespace, child_id)
  end

  defp write_row(namespace, child_id, transform, attempt) do
    hub_id = Identificator.hub(namespace)

    if Hub.alive?(hub_id) do
      # propagate: true: every node's row must carry the change, because the
      # readers answer from whichever node is asked.
      case update_row(hub_id, child_id, transform) do
        :ok ->
          :ok

        {:error, :no_response} = failed ->
          rewrite_or_give_up(namespace, child_id, transform, attempt, failed)

        # The row may live on a peer whose synchronization has not reached us.
        {:error, @no_child} = failed ->
          rewrite_or_give_up(namespace, child_id, transform, attempt, failed)

        {:error, :not_found} ->
          row_not_found(namespace, child_id)

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, :registry_unavailable}
    end
  end

  defp update_row(hub_id, child_id, transform) do
    ProcessRegistry.update(hub_id, child_id, transform, propagate: true)
  catch
    :exit, _ -> {:error, :no_response}
  end

  defp rewrite_or_give_up(namespace, child_id, transform, attempt, failed) do
    cond do
      not Backoff.give_up?(:no_response, attempt) ->
        Backoff.wait(:no_response, attempt + 1, child_id)
        write_row(namespace, child_id, transform, attempt + 1)

      failed == {:error, @no_child} ->
        row_not_found(namespace, child_id)

      true ->
        failed
    end
  end

  defp row_not_found(_namespace, {:plugin, name}), do: {:error, {:plugin_not_found, name}}

  defp row_not_found(namespace, relay_id),
    do: {:error, {:relay_not_found, namespace, relay_id}}

  defp sync_declared(_namespace, _child_id, 0), do: :ok

  defp sync_declared(namespace, child_id, attempts) do
    hub_id = Identificator.hub(namespace)

    with {:ok, hub} <- hub_state(hub_id),
         {:ok, spec} <- declared_spec(hub_id, child_id),
         {:ok, row_plugins} <- row_plugins(namespace, child_id) do
      case HubManager.rewrite_cspec_plugins(spec, fn _ -> row_plugins end) do
        ^spec ->
          :ok

        declared ->
          with :ok <- declare(hub, hub_id, declared) do
            sync_declared(namespace, child_id, attempts - 1)
          end
      end
    end
  end

  defp row_plugins(namespace, relay_id) when is_binary(relay_id),
    do: HubManager.relay_bindings(namespace, relay_id)

  defp row_plugins(namespace, {:plugin, name}) do
    case HubManager.plugin_row(namespace, name) do
      {:ok, plugin} -> {:ok, [{name, plugin}]}
      :error -> {:error, {:plugin_not_found, namespace, name}}
    end
  end

  defp hub_state(hub_id) do
    case ProcessHub.Hub.get(hub_id) do
      nil -> {:error, :hub_unavailable}
      hub -> {:ok, hub}
    end
  end

  # An upsert would declare a child that was never declared, which would let a
  # plugin write resurrect one a stop removed for good.
  defp declared_spec(hub_id, child_id) do
    case DeclaredChildren.declared_child(hub_id, child_id) do
      nil -> {:error, :relay_not_declared}
      spec -> {:ok, spec}
    end
  end

  # `{:add, [spec]}` is an upsert keyed on child id
  # (`DeclaredChildren.apply_mutation/2`), so re-declaring replaces that one
  # entry in place and starts, stops or moves nothing. It must run on the
  # leader, the one node that may bump the list version, and it shares the
  # batch that coalesces every concurrent declared write into one write and
  # one sync.
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

  def init_relay(exist?, namespace, relay, announce? \\ true) do
    child_spec = %{
      id: relay.id,
      start: {Flezha.Relay.Overwatch, :start_link, [{namespace, relay}]},
      restart: :transient
    }

    result = start_declared(namespace, child_spec, start_metadata(relay), relay.id)
    finalize_init_relay(exist?, namespace, relay, result, announce?)
  end

  @doc """
  Start `child_spec` under `namespace`'s hub as declared intent: the start
  records the child in the hub's declared set BEFORE anything runs, so a
  whole-cluster cold start restores it and a deliberate stop (hub stop path)
  removes it for good. Off in lockstep with auto-recovery — a hub without the
  reconcile refuses durable starts. Answers the hub's start result.

  `:no_response` surfaces when ProcessHub's `with_partition_check`
  short-circuits `execute/2` without sending a response — i.e. the local
  hub's `dist_sup` was momentarily missing or dead. The dist_sup restarts on
  its own; the only thing the caller can do is wait it out, on the
  `:no_response` row of the backoff table, so a transient partition during
  startup doesn't surface as a hard failure. A steady-state `:no_response`
  still propagates once the row gives up.
  """
  @spec start_declared(binary(), map(), map(), term(), non_neg_integer()) :: term()
  def start_declared(namespace, child_spec, metadata, label, attempt \\ 0) do
    result =
      Identificator.hub(namespace)
      |> ProcessHub.start_child(child_spec,
        awaitable: true,
        # We will get the errors from the start result.
        disable_logging: true,
        timeout: Durations.get(:hub_start_child_ms),
        metadata: metadata,
        durable: HubManager.durable_starts?()
      )
      |> ProcessHub.Future.await()

    if no_response?(result) and not Backoff.give_up?(:no_response, attempt) do
      Backoff.wait(:no_response, attempt + 1, label)
      start_declared(namespace, child_spec, metadata, label, attempt + 1)
    else
      result
    end
  end

  # A partition member's identity is on its row from the first registration,
  # so `PartitionGroup.members/2` sees it before it binds. The factory is the
  # group the hub places the relay by, so it travels with the start request
  # and lands on the row.
  defp start_metadata(relay) do
    metadata =
      case Flezha.Partition.identity(relay.opts) do
        nil -> %{}
        partition -> %{partition: partition}
      end

    case relay.factory_id do
      nil -> metadata
      factory_id -> Map.put(metadata, :group, factory_id)
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

  defp finalize_init_relay(exist?, namespace, relay, result, announce?) do
    case result do
      %ProcessHub.StartResult{} ->
        case Enum.find(result.started || [], fn {cid, _} -> cid == relay.id end) do
          {_cid, [{relay_node, pid}]} ->
            bind_started(namespace, relay, {relay_node, pid}, announce?)

          _ ->
            refuse_start(exist?, namespace, relay, result)
        end

      {:error, reason} ->
        settle_claim(exist?, namespace, relay, reason)
    end
  end

  defp bind_started(namespace, relay, {relay_node, pid}, announce?) do
    # ProcessHub reports `started` as soon as `start_link/1`
    # returns; the relay's `handle_continue(:bind_runner, …)`
    # surfaces a plugin-chain failure via the `halt_reason` field
    # on Overwatch state — `:registry_runner_set` returns
    # `{:error, _}` instead of crashing the GenServer, as it does for a
    # call the start carried that the instance refuses. The
    # `:exit` catch covers older failure modes (timeout, kill).
    # Started, then exited cleanly before we could read its binding — a
    # run-once steps relay that already completed, or a concurrent
    # stop. A clean exit is a successful start, not a failure; no live
    # binding remains, so `runner_id` is nil.
    on_exit = fn
      {reason, _} when reason in [:normal, :noproc] ->
        {:ok, %{relay_id: relay.id, runner_id: nil, node: relay_node, pid: pid}}

      reason ->
        Logger.warning(
          "Relay #{relay.id} appeared in `started` but died during " <>
            "post-init continue (reason=#{inspect(reason)})"
        )

        _ = stop_child_quietly(namespace, relay.id)
        {:error, start_failure(relay.id, {:relay_exited, reason})}
    end

    case SafeCall.bounded(
           pid,
           {:registry_runner_set, announce?},
           Durations.get(:overwatch_call_timeout_ms),
           on_error: on_exit
         ) do
      {:ok, runner_id} ->
        {:ok,
         %{
           relay_id: relay.id,
           runner_id: runner_id,
           node: relay_node,
           pid: pid
         }}

      {:error, reason} ->
        _ = stop_child_quietly(namespace, relay.id)
        {:error, start_failure(relay.id, reason)}

      answered ->
        answered
    end
  end

  defp refuse_start(exist?, namespace, relay, result) do
    reason =
      case Enum.find(result.errors || [], fn {cid, _} -> cid == relay.id end) do
        {_cid, {:error, reason}} -> reason
        {_cid, {reason, _child_spec}} -> reason
        {_cid, reason} -> reason
        _ -> :unknown
      end

    if claim_loss?(reason) do
      claim_lost(exist?, namespace, relay.id)
    else
      Logger.warning(
        "Relay #{relay.id} failed to start. " <>
          "Started: #{inspect(result.started)}, Errors: #{inspect(result.errors)}, " <>
          "Status: #{inspect(result.status)}"
      )

      _ = stop_child_quietly(namespace, relay.id)
      {:error, start_failure(relay.id, reason)}
    end
  end

  defp settle_claim(exist?, namespace, relay, reason) do
    if claim_loss?(reason),
      do: claim_lost(exist?, namespace, relay.id),
      else: {:error, start_failure(relay.id, reason)}
  end

  # A failure the runner already answered with its own error is that error;
  # anything else is the cause the start stopped at, kept for the details.
  defp start_failure(_relay_id, %Flezha.Error{} = error), do: error
  defp start_failure(relay_id, reason), do: {:start_failed, relay_id, reason}

  # The registry refused the id: a live instance already holds it. Taking the
  # failure path on that verdict would call `stop_child_quietly/2` on the id —
  # killing the winner — which is precisely the start-authority violation the
  # claim exists to prevent.
  def claim_loss?(:already_started), do: true
  def claim_loss?({:already_started, _ids}), do: true
  def claim_loss?({:id_in_use, _id}), do: true
  def claim_loss?(_reason), do: false

  defp stop_child_quietly(namespace, relay_id) do
    _ = Teardown.hub_stop_children(namespace, [relay_id], 5_000)
    :ok
  end
end

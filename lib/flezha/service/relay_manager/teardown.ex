defmodule Flezha.Service.RelayManager.Teardown do
  @moduledoc """
  What a relay stop owes, as the `:relay` steps of the shutdown plan:
  the runner's stream channels flushed, the child stopped through the hub, the
  state directory reclaimed, and the quota released — each once, in that
  order. The `:namespace` walk stops every child of a namespace through the
  same seam.
  """

  alias Flezha.AuthZ.Quotas
  alias Flezha.Identificator
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal

  @behaviour Flezha.Shutdown.Step

  @typedoc """
  The `:relay` walk's target: the relay, the node its Overwatch ran
  on, the runner it is bound to (or `nil`), whether its state stays on disk (a
  rescale moving it, `:sealed` for a steps relay survivor), whether controller
  plugins are told of the stop and why it stops (`:stopped` or `:completed`),
  and the Cell count its start allocated.
  """
  @type target :: %{
          namespace: binary(),
          relay_id: binary(),
          node: node(),
          runner_id: binary() | nil,
          preserve_state?: boolean() | :sealed,
          announce: boolean(),
          reason: :stopped | :completed,
          cell_count: non_neg_integer()
        }

  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [
      %{scope: :relay, order: 10, name: :stream_flush, run: &stream_flush_step/2},
      %{scope: :relay, order: 20, name: :hub_stop, run: &hub_stop_step/2},
      %{scope: :relay, order: 30, name: :state_dir, run: &state_dir_step/2},
      %{scope: :relay, order: 40, name: :quota, run: &quota_step/2},
      %{scope: :relay, order: 50, name: :announce, run: &announce_step/2},
      %{scope: :namespace, order: 10, name: :relays, run: &relays_step/2}
    ]
  end

  # Flush the runner's stream channels before the child is deregistered: a
  # frame already queued there is routed, never counted, so nothing the runner
  # wrote is lost to the teardown (`fix-steps-teardown-drain`).
  defp stream_flush_step(%{runner_id: nil}, _budget), do: :ok

  defp stream_flush_step(%{runner_id: runner_id}, _budget) do
    _ = StreamPool.flush_channels(runner_id)
    :ok
  catch
    # A runner whose channels are already gone has nothing left to flush.
    _kind, _reason -> :ok
  end

  defp hub_stop_step(%{namespace: namespace, relay_id: relay_id}, budget) do
    case hub_stop_children(namespace, [relay_id], budget) do
      :ok ->
        :ok

      {:error, errors} when is_list(errors) ->
        case List.keyfind(errors, relay_id, 0) do
          {_cid, reason} -> {:error, reason}
          nil -> {:error, :stop_failed}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The state and its writers are on the node the Overwatch ran on, which a
  # stop issued from another controller is not.
  defp state_dir_step(target, budget) do
    :erpc.call(
      target.node,
      __MODULE__,
      :reclaim_state_dir,
      [target.relay_id, target.preserve_state?],
      budget
    )
  end

  @doc """
  What a stop owes the relay's state on this node. A stop that is a move —
  a rescale taking the group down to redistribute its Cells — keeps the files:
  they are the state about to be handed to another member, not garbage. Its
  writers close all the same, so the member restarted under this id opens the
  files where the move left them. `:sealed` first cuts a steps relay's journal to
  its latest committed iteration (`TaskJournal.cut/2`), the history boundary a
  surviving member of a rescale resumes from.
  """
  @spec reclaim_state_dir(binary(), boolean() | :sealed) :: :ok
  def reclaim_state_dir(relay_id, :sealed) do
    :ok = TaskJournal.cut(relay_id, seal: true)
    reclaim_state_dir(relay_id, true)
  end

  def reclaim_state_dir(relay_id, true),
    do: Flezha.State.Supervisor.close_writers(relay_id)

  def reclaim_state_dir(relay_id, false), do: delete_relay_state_dir(relay_id)

  # Released symmetrically with the start: the relay, and the Cells its
  # class declared.
  defp quota_step(%{namespace: namespace, cell_count: count}, _budget) do
    _ = Quotas.record_deallocation(namespace, :max_relays, 1)

    if count > 0,
      do: _ = Quotas.record_deallocation(namespace, :max_cells_per_relay, count)

    :ok
  end

  # Once every step before it ran: a deliberate stop or a completion is told to
  # controller plugins.
  defp announce_step(%{announce: true} = target, _budget) do
    :telemetry.execute([:flezha, :relay, :stopped], %{count: 1}, %{
      namespace: target.namespace,
      relay: target.relay_id,
      reason: target.reason
    })
  end

  defp announce_step(_not_told, _budget), do: :ok

  defp relays_step(%{namespace: namespace}, budget), do: stop_all(namespace, budget)

  @doc """
  Stop every relay in `namespace` through the hub, in one call, and
  reclaim each one's state directory. Quotas are not released: the namespace
  is going away with its ledger.
  """
  @spec stop_all(binary(), non_neg_integer()) :: :ok
  def stop_all(namespace, budget) do
    relay_ids =
      namespace |> HubManager.get_all_relays() |> Enum.map(fn {id, _pid} -> id end)

    if relay_ids != [] do
      _ = hub_stop_children(namespace, relay_ids, budget)
      Enum.each(relay_ids, &delete_relay_state_dir/1)
    end

    :ok
  end

  # The ONE seam flezha stops children through: the hub's precommit removes
  # each declared-set entry BEFORE anything terminates (remove-before-terminate),
  # so a stop is remembered as list absence and the reconcile can never
  # resurrect it. The Overwatch's teardown — telling the runner to drop the
  # relay, plugin `:on_terminate`, table teardown — runs in its
  # `terminate/2` under the supervisor's shutdown. Best-effort callers discard
  # the verdict; a dead or mid-restart hub maps to an error, never a raise.
  def hub_stop_children(namespace, relay_ids, timeout) do
    result =
      try do
        Identificator.hub(namespace)
        |> ProcessHub.stop_children(relay_ids, awaitable: true, timeout: timeout)
        |> ProcessHub.Future.await()
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      %ProcessHub.StopResult{status: :ok} -> :ok
      %ProcessHub.StopResult{errors: errors} -> {:error, errors || []}
      {:error, reason} -> {:error, reason}
    end
  end

  # On explicit stop, delete the on-disk state directories owned by the
  # relay. The relay_id is used as the relay_class identifier — the SDK
  # convention is that each relay's cell frames carry the relay_id
  # as relay_class. A finished steps relay keeps its state and
  # completion record for the retention window so the outcome stays
  # inspectable; `Flezha.State.Retention` reclaims it once the window elapses.
  #
  # `child_unregistered_hook` deliberately does NOT call this — HotSwap
  # source-side and redistribution-away preserve files so the
  # destination (or next host) can pull them via WAL-follows-relay.
  def delete_relay_state_dir(relay_id) do
    # Retained or deleted, the writer processes go with the relay.
    Flezha.State.Supervisor.close_writers(relay_id)

    if Completion.exists?(relay_id), do: :ok, else: Paths.delete_class_dir(relay_id)
  end
end

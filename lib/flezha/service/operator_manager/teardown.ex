defmodule Flezha.Service.OperatorManager.Teardown do
  @moduledoc """
  What an operator stop owes, as the `:operator` steps of the shutdown plan:
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
  The `:operator` walk's target: the operator, the node its Overwatch ran
  on, the runner it is bound to (or `nil`), whether its state stays on disk (a
  rescale moving it, `:sealed` for a steps operator survivor), and the Cell count its
  start allocated.
  """
  @type target :: %{
          namespace: binary(),
          operator_id: binary(),
          node: node(),
          runner_id: binary() | nil,
          preserve_state?: boolean() | :sealed,
          cell_count: non_neg_integer()
        }

  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [
      %{scope: :operator, order: 10, name: :stream_flush, run: &stream_flush_step/2},
      %{scope: :operator, order: 20, name: :hub_stop, run: &hub_stop_step/2},
      %{scope: :operator, order: 30, name: :state_dir, run: &state_dir_step/2},
      %{scope: :operator, order: 40, name: :quota, run: &quota_step/2},
      %{scope: :namespace, order: 10, name: :operators, run: &operators_step/2}
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

  defp hub_stop_step(%{namespace: namespace, operator_id: operator_id}, budget) do
    case hub_stop_children(namespace, [operator_id], budget) do
      :ok ->
        :ok

      {:error, errors} when is_list(errors) ->
        case List.keyfind(errors, operator_id, 0) do
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
      [target.operator_id, target.preserve_state?],
      budget
    )
  end

  @doc """
  What a stop owes the operator's state on this node. A stop that is a move —
  a rescale taking the group down to redistribute its Cells — keeps the files:
  they are the state about to be handed to another member, not garbage. Its
  writers close all the same, so the member restarted under this id opens the
  files where the move left them. `:sealed` first cuts a steps operator's journal to
  its latest committed iteration (`TaskJournal.cut/2`), the history boundary a
  surviving member of a rescale resumes from.
  """
  @spec reclaim_state_dir(binary(), boolean() | :sealed) :: :ok
  def reclaim_state_dir(operator_id, :sealed) do
    :ok = TaskJournal.cut(operator_id, seal: true)
    reclaim_state_dir(operator_id, true)
  end

  def reclaim_state_dir(operator_id, true),
    do: Flezha.State.Supervisor.close_writers(operator_id)

  def reclaim_state_dir(operator_id, false), do: delete_operator_state_dir(operator_id)

  # Released symmetrically with the start: the operator, and the Cells its
  # class declared.
  defp quota_step(%{namespace: namespace, cell_count: count}, _budget) do
    _ = Quotas.record_deallocation(namespace, :max_operators, 1)

    if count > 0,
      do: _ = Quotas.record_deallocation(namespace, :max_cells_per_operator, count)

    :ok
  end

  defp operators_step(%{namespace: namespace}, budget), do: stop_all(namespace, budget)

  @doc """
  Stop every operator in `namespace` through the hub, in one call, and
  reclaim each one's state directory. Quotas are not released: the namespace
  is going away with its ledger.
  """
  @spec stop_all(binary(), non_neg_integer()) :: :ok
  def stop_all(namespace, budget) do
    operator_ids =
      namespace |> HubManager.get_all_operators() |> Enum.map(fn {id, _pid} -> id end)

    if operator_ids != [] do
      _ = hub_stop_children(namespace, operator_ids, budget)
      Enum.each(operator_ids, &delete_operator_state_dir/1)
    end

    :ok
  end

  # The ONE seam flezha stops children through: the hub's precommit removes
  # each declared-set entry BEFORE anything terminates (remove-before-terminate),
  # so a stop is remembered as list absence and the reconcile can never
  # resurrect it. The Overwatch's teardown — telling the runner to drop the
  # operator, plugin `:on_terminate`, table teardown — runs in its
  # `terminate/2` under the supervisor's shutdown. Best-effort callers discard
  # the verdict; a dead or mid-restart hub maps to an error, never a raise.
  def hub_stop_children(namespace, operator_ids, timeout) do
    result =
      try do
        Identificator.hub(namespace)
        |> ProcessHub.stop_children(operator_ids, awaitable: true, timeout: timeout)
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
  # operator. The operator_id is used as the operator_class identifier — the SDK
  # convention is that each operator's cell frames carry the operator_id
  # as operator_class. A finished steps operator keeps its state and
  # completion record for the retention window so the outcome stays
  # inspectable; `Flezha.State.Retention` reclaims it once the window elapses.
  #
  # `child_unregistered_hook` deliberately does NOT call this — HotSwap
  # source-side and redistribution-away preserve files so the
  # destination (or next host) can pull them via WAL-follows-operator.
  def delete_operator_state_dir(operator_id) do
    # Retained or deleted, the writer processes go with the operator.
    Flezha.State.Supervisor.close_writers(operator_id)

    if Completion.exists?(operator_id), do: :ok, else: Paths.delete_class_dir(operator_id)
  end
end

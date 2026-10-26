defmodule Flezha.Service.RelayManager.Teardown do
  @moduledoc """
  What a relay stop owes, as the `:relay` steps of the shutdown plan:
  the runner's stream channels flushed, the child stopped through the hub, the
  state directory reclaimed, the quota released and the stop announced — each
  once, in that order, as far as its cause owes them. The `:namespace` walk
  stops every child of a namespace through the same seam. `request_stop/2` and `stop_relay/3` run the stop verbs there.
  """

  alias Flezha.AuthZ.Quotas
  alias Flezha.Config.Backoff
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Process.Hub
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Admission
  alias Flezha.Shutdown
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal

  @behaviour Flezha.Shutdown.Step

  @typedoc """
  The `:relay` walk's target: the relay, its Overwatch (`nil` for a start that
  never ran one), the runner it is bound to (or `nil`), whether its state stays
  on disk (a rescale moving it, `:sealed` for a steps relay survivor), why it
  stops, and the Cell count its start allocated.
  """
  @type target :: %{
          namespace: binary(),
          relay_id: binary(),
          overwatch: pid() | nil,
          runner_id: binary() | nil,
          preserve_state?: boolean() | :sealed,
          cause: :stop | :complete | :rescale | :failed_start,
          cell_count: non_neg_integer()
        }

  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [
      %{scope: :relay, order: 10, name: :stream_flush, run: &stream_flush_step/2},
      %{scope: :relay, order: 20, name: :hub_stop, run: &hub_stop_step/2},
      %{scope: :relay, order: 25, name: :final_flush, run: &stream_flush_step/2},
      %{scope: :relay, order: 30, name: :state_dir, run: &state_dir_step/2},
      %{scope: :relay, order: 40, name: :quota, run: &quota_step/2},
      %{scope: :relay, order: 50, name: :announce, run: &announce_step/2},
      %{scope: :namespace, order: 10, name: :relays, run: &relays_step/2}
    ]
  end

  # The steps a stop's cause runs: a rescale is no stop to announce.
  defp acts?(:failed_start, step), do: step == :hub_stop
  defp acts?(:rescale, step), do: step != :announce
  defp acts?(cause, _step) when cause in [:stop, :complete], do: true

  # Flush the runner's stream channels before the child is deregistered: a
  # frame already queued there is routed, never counted, so nothing the runner
  # wrote is lost to the teardown. Again once the runner answered the stop:
  # the Cells' last changes it shipped then land before the state step, so a
  # deleted relay's files are not written again for the next start of its id.
  # Bounded to 1 s, so the hub stop keeps the rest of the budget for the
  # Overwatch's own shutdown bound.
  defp stream_flush_step(%{runner_id: nil}, _budget), do: :ok

  defp stream_flush_step(%{runner_id: runner_id}, budget) do
    _ = StreamPool.flush_channels(runner_id, min(budget, 1_000))
    :ok
  catch
    # A runner whose channels are already gone has nothing left to flush.
    _kind, _reason -> :ok
  end

  defp hub_stop_step(%{namespace: namespace, relay_id: relay_id} = target, budget) do
    with {:error, failure} <- hub_stop_children(namespace, [relay_id], budget) do
      # Still running, the relay must not end later as stopped by this stop.
      if target.overwatch, do: RelayManager.call_overwatch(target.overwatch, {:cause, nil})

      case is_list(failure) && List.keyfind(failure, relay_id, 0) do
        {_cid, reason} -> {:error, reason}
        nil -> {:error, :stop_failed}
        false -> {:error, failure}
      end
    end
  end

  # The state and its writers are on the node the Overwatch ran on, which a
  # stop issued from another controller is not.
  defp state_dir_step(target, budget) do
    :erpc.call(
      node(target.overwatch),
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
  defp announce_step(target, _budget) do
    :telemetry.execute([:flezha, :relay, :stopped], %{count: 1}, %{
      namespace: target.namespace,
      relay: target.relay_id,
      reason: if(target.cause == :complete, do: :completed, else: :stopped)
    })
  end

  defp relays_step(%{namespace: namespace}, budget), do: stop_all(namespace, budget)

  @doc """
  Stop every relay in `namespace` through the hub, in one call after each
  runner's stream channels are flushed, and reclaim each one's state directory.
  Quotas are not released: the namespace is going away with its ledger.
  """
  @spec stop_all(binary(), non_neg_integer()) :: :ok
  def stop_all(namespace, budget) do
    deadline = System.monotonic_time(:millisecond) + budget
    relay_ids = namespace |> HubManager.get_all_relays() |> Enum.map(&elem(&1, 0))

    if relay_ids != [] do
      relay_ids
      |> Enum.map(&HubManager.relay_runner_tag(namespace, &1))
      |> Enum.uniq()
      |> Enum.each(&stream_flush_step(%{runner_id: &1}, remaining(deadline)))

      _ = hub_stop_children(namespace, relay_ids, remaining(deadline))
      Enum.each(relay_ids, &delete_relay_state_dir/1)
    end

    :ok
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  # The ONE seam flezha stops children through: the hub's precommit removes
  # each declared-set entry BEFORE anything terminates (remove-before-terminate),
  # so a stop is remembered as list absence and the reconcile can never
  # resurrect it. The Overwatch's teardown — telling the runner to drop the
  # relay, plugin `:on_terminate`, table teardown — runs in its
  # `terminate/2` under the supervisor's shutdown. Best-effort callers discard
  # the verdict; a dead or mid-restart hub maps to an error, never a raise.
  # A stop that got no answer is asked again on the `:no_response` row within
  # `timeout`, and a child a retry no longer finds was stopped by an earlier ask.
  def hub_stop_children(namespace, relay_ids, timeout),
    do: stop_children(namespace, relay_ids, System.monotonic_time(:millisecond) + timeout, 0)

  defp stop_children(namespace, ids, deadline, attempt) do
    with {:error, [_ | _] = errors} <- stop_once(namespace, ids, remaining(deadline)) do
      left =
        if attempt > 0,
          do: Enum.reject(errors, &match?({_, {:error, :not_found}}, &1)),
          else: errors

      silent = for {id, {:error, :no_response}} <- left, do: id

      cond do
        left == [] ->
          :ok

        length(silent) == length(left) and
            Backoff.again?(:no_response, attempt, namespace, deadline) ->
          stop_children(namespace, silent, deadline, attempt + 1)

        true ->
          {:error, left}
      end
    end
  end

  defp stop_once(namespace, ids, timeout) do
    result =
      try do
        Identificator.hub(namespace)
        |> ProcessHub.stop_children(ids, awaitable: true, timeout: timeout)
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

  @doc """
  `hub_stop_children/3` for one child, answering `:ok` also when the stop got
  no answer or found nothing and the child has no row left: the stop's
  precommit took it out of the declared set first, so it runs nowhere.
  """
  @spec hub_stop_child(binary(), term(), timeout()) :: :ok | {:error, term()}
  def hub_stop_child(namespace, id, timeout) do
    with {:error, [_ | _] = errors} <- hub_stop_children(namespace, [id], timeout) do
      gone? =
        Enum.all?(errors, &(elem(&1, 1) in [{:error, :no_response}, {:error, :not_found}])) and
          Hub.with_live(Identificator.hub(namespace), :hub_down, fn hub_id ->
            ProcessHub.child_lookup(hub_id, id, include_empty: true)
          end) == nil

      if gone?, do: :ok, else: {:error, errors}
    end
  end

  # On explicit stop, delete the on-disk state directories owned by the
  # relay. The relay_id is used as the relay_class identifier — the SDK
  # convention is that each relay's cell frames carry the relay_id
  # as relay_class. A finished steps relay keeps its state and
  # completion record for the retention window so the outcome stays
  # inspectable; `Flezha.State.Retention` reclaims it once the window elapses.
  def delete_relay_state_dir(relay_id) do
    # Retained or deleted, the writer processes go with the relay.
    Flezha.State.Supervisor.close_writers(relay_id)

    if Completion.exists?(relay_id), do: :ok, else: Paths.delete_class_dir(relay_id)
  end

  @doc "`RelayManager.request_stop/3` on the hosting node."
  @spec request_stop(binary(), binary()) :: :ok | {:ok, :cooperative} | {:error, term()}
  def request_stop(namespace, relay_id), do: request_stop(namespace, relay_id, true)

  defp request_stop(namespace, relay_id, wake?) do
    case RelayManager.get_overwatch_state(namespace, relay_id) do
      {:ok, %{builder: %{run: :steps}, phase: :suspended}} when wake? ->
        case RelayManager.ask_overwatch(namespace, relay_id, {:wake, then: :stop}) do
          :ok -> {:ok, :cooperative}
          # Woken meanwhile: stopped as an awake one, once.
          {:error, :awake} -> request_stop(namespace, relay_id, false)
          {:error, _no_runner} -> RelayManager.stop_relay(namespace, relay_id)
        end

      {:ok, %{builder: %{run: :steps}, runner_id: runner_id}}
      when is_binary(runner_id) ->
        case ControlUnit.steps_stop(runner_id, relay_id) do
          :ok -> {:ok, :cooperative}
          # No control unit to cooperate with — fall back to the hard teardown.
          {:error, _} -> RelayManager.stop_relay(namespace, relay_id)
        end

      _ ->
        RelayManager.stop_relay(namespace, relay_id)
    end
  end

  @doc "`RelayManager.stop_relay/3` on the hosting node: the `:relay` walk."
  @spec stop_relay(binary(), binary(), keyword()) :: :ok | {:error, term()}
  def stop_relay(namespace, relay_id, opts) do
    # A hub without quorum here stopped its relays already; the stop belongs
    # to the side of the split that runs them.
    with :ok <- HubManager.quorum(Identificator.hub(namespace)),
         do: stop_hosted(namespace, relay_id, opts)
  end

  defp stop_hosted(namespace, relay_id, opts) do
    cause = Keyword.get(opts, :cause, :stop)

    with {:ok, relay_pid} <- stop_pid(namespace, relay_id, cause, Keyword.get(opts, :overwatch)) do
      # Told why it ends and read in one call: the flush needs its runner, the
      # quota step the Cells its start allocated.
      {runner_id, cell_count} = stop_facts(namespace, relay_id, relay_pid, cause)

      target = %{
        namespace: namespace,
        relay_id: relay_id,
        overwatch: relay_pid,
        runner_id: runner_id,
        preserve_state?: Keyword.get(opts, :preserve_state, false),
        cause: cause,
        cell_count: cell_count
      }

      steps = Enum.filter(Shutdown.steps(:relay), &acts?(cause, &1.name))

      case Shutdown.run(:relay, target, Shutdown.stop_timeout_ms(), steps) do
        {:ok, _report} ->
          :ok

        {:partial, %{errors: [{step, reason} | _]}} ->
          {:error, {:stop_failed, relay_id, step, reason}}

        {:partial, %{skipped: skipped}} ->
          {:error, {:stop_timeout, relay_id, skipped}}
      end
    end
  end

  # A failed start's child may never have registered: its start's pid, if any.
  defp stop_pid(_namespace, _relay_id, :failed_start, overwatch), do: {:ok, overwatch}

  defp stop_pid(namespace, relay_id, _cause, overwatch) do
    case RelayManager.get_relay_pid(namespace, relay_id) do
      {:error, {:relay_not_found, _, _}} when is_pid(overwatch) -> {:ok, overwatch}
      {:error, {:relay_not_found, _, _}} = missing -> peer_pid(namespace, relay_id, missing)
      found_or_error -> found_or_error
    end
  end

  # A start answers once the relay's own node registered it, before this
  # node's copy of the hub has the row: the other members are asked in one
  # bounded call, and only a miss on all of them is final.
  defp peer_pid(namespace, relay_id, missing) do
    namespace
    |> HubManager.get_hub_nodes()
    |> :erpc.multicall(
      HubManager,
      :get_pid,
      [namespace, relay_id],
      Durations.get(:hub_peer_lookup_ms)
    )
    |> Enum.find_value(missing, fn
      {:ok, {:ok, pid}} -> {:ok, pid}
      _miss -> nil
    end)
  end

  defp stop_facts(_namespace, _relay_id, nil, _cause), do: {nil, 0}

  defp stop_facts(namespace, relay_id, relay_pid, cause) do
    {runner_id, relay} =
      case RelayManager.call_overwatch(relay_pid, {:cause, cause}) do
        {:ok, %{builder: builder} = ow} ->
          {Map.get(ow, :runner_id), builder}

        # Unreadable: the row its start wrote, which the Cell allocation counted.
        _unreadable ->
          case HubManager.relay_spec(namespace, relay_id) do
            {:ok, relay, %{runner: runner_id}} -> {runner_id, relay}
            {:error, _} -> {nil, %{}}
          end
      end

    {if(is_binary(runner_id), do: runner_id), Admission.cell_count(relay)}
  end
end

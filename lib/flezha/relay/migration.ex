defmodule Flezha.Relay.Migration do
  @moduledoc """
  Everything that moves a relay off the runner it is on: suspension, the
  bind task every move runs (an un-park, a wake, a `runner_migrate`, a
  restarted runner), a reconnect's session swap, a loop rewind, and the
  terminal teardown.

  It sits above `Flezha.Relay.Lifecycle` in the call graph — it binds,
  completes an init and parks through that module and is never called back by
  it — so it is a sibling of `Handover`, `Ingest`, `Plugins` and `Replay`,
  reached from `Overwatch`.

  Invariant: a suspended relay holds no runner resources. The wake timer it
  leaves behind is the only thing that outlives the eviction, and it lives in
  `Lifecycle` with the rest of the bind machinery.
  """

  alias Flezha.Identificator
  alias Flezha.Relay.Builder
  alias Flezha.Relay.Calls
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Plugins
  alias Flezha.Relay.Replay
  alias Flezha.Payloader
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Shutdown
  alias Flezha.State.TaskJournal
  alias ProcessHub.Service.Migration, as: HubMigration

  require Logger

  @behaviour Flezha.Shutdown.Step

  @doc """
  Suspend an instance of either kind that asked to sleep (a steps relay at the
  committed boundary of iteration `iter`, a flow relay at `0`): write its park
  record, evict it from its runner (`RemRelay`), release the registry's
  runner binding, and arm the recorded wake deadline. The registry entry (and
  the Overwatch process) stay, so a new input still has somewhere to land; it
  is the **runner** that holds zero resources for a suspended relay.

  With no live Overwatch on this node (already migrated, torn down, or dead
  while its ownership record remains) the record is written under the
  instance's own key, so it is rediscovered as suspended
  whenever it is next started, unless another namespace owns its state.

  Called by `ControlChannel` on the runner's `IterPark`, with the Overwatch the
  frame was admitted against as `bound` (see `overwatch/3`).
  """
  @spec suspend(binary(), binary(), non_neg_integer(), ClassRegistry.park(), pid() | nil) :: :ok
  def suspend(namespace, relay_id, iter, park, bound) do
    case overwatch(namespace, relay_id, bound) do
      {:ok, pid} when is_pid(pid) -> GenServer.cast(pid, {:suspend, iter, park})
      _ -> _ = ClassRegistry.put_parked(relay_id, park, namespace)
    end

    :ok
  end

  @doc """
  The Overwatch a runner's frame about `relay_id` goes to: `bound`, the one the
  frame was admitted against (`Flezha.Runner.StreamChannel.Records.admit_to/3`),
  while it is alive, else the hub's when no Overwatch was recorded. A dead
  `bound` is `{:error, :overwatch_down}`: the hub may still list that pid or a
  departing copy, and a cast to either is lost.
  """
  @spec overwatch(binary(), binary(), pid() | nil) :: {:ok, pid()} | {:error, term()}
  def overwatch(_namespace, _relay_id, bound) when is_pid(bound),
    do: if(Process.alive?(bound), do: {:ok, bound}, else: {:error, :overwatch_down})

  def overwatch(namespace, relay_id, nil), do: HubManager.get_pid(namespace, relay_id)

  @doc """
  Rewind a steps loop to the committed boundary of iteration `to_iter`: journal
  the rewind — which bumps the epoch and un-commits every iteration above it —
  then evict the instance from its runner and rehydrate it.

  The rehydrating bind ships the folded journal back, so the driver undoes the
  unwound tasklets through the ordinary compensation planner and resumes the
  loop at `to_iter + 1`, waiting for its next input: the inputs the unwound
  iterations consumed stay released, since a rewind never lowers the
  instance's floor in its input log. The marker is durable before the
  eviction, so a crash mid-rewind resumes as a rewind rather than as a
  partially unwound loop.

  The caller (`Flezha.Service.Steps.rewind/4`) has already refused an uncommitted target and an
  iteration whose plugin output cannot be compensated.
  """
  @spec rewind(pid(), non_neg_integer()) :: :ok | {:error, term()}
  def rewind(overwatch_pid, to_iter) when is_pid(overwatch_pid),
    do: GenServer.call(overwatch_pid, {:rewind, to_iter}, 30_000)

  # ---------------------------------------------------------------
  # Bind
  # ---------------------------------------------------------------

  @doc """
  The relay a bind onto another runner starts: the builder, seeded from
  the final state a graceful stop shipped when one is stored, else from the
  init state this Overwatch already resolved, so a move never runs the
  `:on_relay_init` chain again. The kind and the partition identity ride
  the builder, so a moved member keeps its mis-route guard.
  """
  @spec bind_relay(Overwatch.t()) :: Flezha.Relay.Builder.t()
  def bind_relay(%Overwatch{snapshot: nil, init_values: resolved, builder: builder})
      when resolved == %{},
      do: builder

  def bind_relay(%Overwatch{
        snapshot: nil,
        init_values: resolved,
        builder: %Flezha.Relay.Builder{} = builder
      }),
      do: %Flezha.Relay.Builder{builder | init_values: resolved}

  def bind_relay(%Overwatch{builder: %Flezha.Relay.Builder{} = builder} = state),
    do: %Flezha.Relay.Builder{builder | init_values: Payloader.json_to_map(state.snapshot)}

  @doc """
  The convergence loop's `{:runner_migrate, target}`, answered as the reply
  tuple the Overwatch's call returns.

  Every move is the full bind an un-park runs, off the Overwatch so a slow
  bind never blocks it: the resume state rides `AddRelay`, the durable
  Cells are restored and the input log's undelivered tail is re-forwarded,
  whether the relay was parked, suspended or running on a runner that
  died. A failed bind parks the relay on the target.
  """
  @spec runner_migrate(Overwatch.t(), binary()) ::
          {:reply, :ok | {:error, term()}, Overwatch.t()}
  def runner_migrate(%Overwatch{migrate_task: task} = state, _new_runner_id) when task != nil,
    do: {:reply, {:error, :busy}, state}

  def runner_migrate(%Overwatch{} = state, new_runner_id),
    do: {:reply, :ok, start_bind(state, new_runner_id, nil)}

  @doc """
  The runner this relay is bound to connected again as `new_session` with
  the same stable identity. A reconnect of the same process still runs the
  relay, so the binding is only re-pointed: the Overwatch, the registry
  tag and the route move together. A restarted process runs nothing, so the
  relay is bound onto it afresh. A binding that has already moved away
  from `old_session` is left alone.
  """
  @spec session_replaced(Overwatch.t(), binary(), binary(), :reconnected | :restarted) ::
          Overwatch.t()
  def session_replaced(%Overwatch{runner_id: old} = state, old, new_session, continuity) do
    case continuity do
      :reconnected -> Lifecycle.commit_binding(state, new_session)
      :restarted when state.migrate_task == nil -> start_bind(state, new_session, nil)
      :restarted -> state
    end
  end

  def session_replaced(%Overwatch{} = state, _old_session, _new_session, _continuity), do: state

  # Refuse a rewind before it changes anything: an uncommitted target, or an
  # unwound tasklet whose plugin output cannot be undone.
  @spec rewind_guard(Overwatch.t(), non_neg_integer()) :: :ok | {:error, term()}
  def rewind_guard(%Overwatch{} = state, to_iter) do
    committed = TaskJournal.committed_iterations(state.id)

    cond do
      to_iter not in committed ->
        {:error, {:uncommitted_iteration, to_iter, committed}}

      (uncovered = uncovered_channels(state, to_iter)) != [] ->
        {:error, {:uncovered, to_iter, uncovered}}

      true ->
        :ok
    end
  end

  # A steps relay's staged plugin output carries no natural key, so a
  # re-run's output cannot be relied on to overwrite the original at every sink.
  # The only undo the engine can guarantee is the tasklet's own `compensate`
  # hook — so an unwound tasklet that emitted and declares none blocks the
  # rewind, naming the channels it wrote. A tasklet that emitted nothing needs
  # no coverage.
  def uncovered_channels(%Overwatch{} = state, to_iter) do
    compensable = ClassRegistry.compensable_tasks(Overwatch.relay_class(state)) || []

    for {task_idx, channel} <- TaskJournal.staged_channels_above(state.id, to_iter),
        task_idx not in compensable,
        uniq: true,
        do: channel
  end

  @doc """
  Bind state for a suspended instance: no runner, no stream channel, the
  wake timer armed from the park record's deadline.
  """
  @spec enter_suspended(Overwatch.t(), ClassRegistry.park()) :: Overwatch.t()
  def enter_suspended(%Overwatch{} = state, park) do
    :telemetry.execute(
      [:flezha, :steps, :suspended],
      %{count: 1},
      %{namespace: state.namespace, relay_id: state.id}
    )

    # A sleeping instance holds no pass in flight: the migration-safe barrier.
    signal_migration_ready(state)
    evict_runner(state, park.wake_deadline_ms)
  end

  # Release every runner-side resource but keep the process and the registry
  # entry, so a new input still has somewhere to land. Shared by suspension (a
  # park, with its wake timer) and rewind (a rebind, with none).
  @spec evict_runner(Overwatch.t(), non_neg_integer() | nil) :: Overwatch.t()
  def evict_runner(%Overwatch{} = state, deadline) do
    %Overwatch{
      Lifecycle.cancel_wake_timer(state)
      | suspended: true,
        runner_id: nil,
        channel: nil
    }
    |> Lifecycle.arm_wake_timer(deadline)
  end

  # Drop the relay from its runner and release the registry's runner tag —
  # the half a suspension and a rewind both perform before they diverge.
  def release_runner(%Overwatch{} = state) do
    if state.runner_id do
      ControlUnit.stop_relay_async(state.runner_id, state.id)
    end

    _ = HubManager.relay_set_runner(state.namespace, state.id, nil)
    :ok
  end

  # The invariant that keeps a turn from being stranded: an instance with
  # undelivered input, or whose wake deadline has elapsed, is never left
  # suspended. Applied wherever suspension is entered — at the park, and at
  # boot from durable state.
  @doc """
  Rehydrate a suspended instance whose input log still holds input above its
  floor, or whose recorded wake deadline has elapsed.
  """
  @spec rehydrate_if_pending(Overwatch.t()) :: Overwatch.t()
  def rehydrate_if_pending(%Overwatch{suspended: false} = state), do: state

  def rehydrate_if_pending(%Overwatch{} = state) do
    if pending?(Overwatch.relay_class(state)),
      do: rehydrate(state),
      else: state
  end

  defp pending?(relay_class),
    do: Replay.owed_count(relay_class) > 0 or wake_due?(relay_class)

  # The park's deadline is the earliest timer the instance's Cells held: once
  # it has passed, the instance has work waiting as surely as undelivered
  # input.
  defp wake_due?(relay_class) do
    case ClassRegistry.parked(relay_class) do
      %{wake_deadline_ms: deadline} when is_integer(deadline) ->
        deadline <= System.system_time(:millisecond)

      _ ->
        false
    end
  end

  @doc """
  Rehydrate a suspended instance through the normal bind path (the same
  `complete_init/5` an un-park drives), which restores journal + snapshots and
  re-forwards the input log's undelivered tail.

  Idempotent: an instance already running, or with a rehydrate already in
  flight, no-ops — so two wake sources racing (an input-log append and the
  wake deadline) rehydrate exactly once.
  """
  @spec rehydrate(Overwatch.t()) :: Overwatch.t()
  def rehydrate(%Overwatch{} = state), do: state |> wake(nil) |> elem(1)

  @doc """
  `rehydrate/1` with what the wake carries: `then: :stop` sends `StepsStop`
  to the runner the un-park binds, so a sleeping steps relay stopped by a
  user ends exactly as one stopped awake at the same boundary. A wake
  already in flight takes the stop on. `{:error, :no_runner}` when no runner
  can take the instance; it stays suspended.
  """
  @spec wake(Overwatch.t(), :stop | nil) :: {:ok | {:error, :no_runner}, Overwatch.t()}
  def wake(%Overwatch{suspended: false} = state, _then), do: {:ok, state}

  def wake(%Overwatch{migrate_task: %{} = task} = state, then),
    do: {:ok, %Overwatch{state | migrate_task: Map.put(task, :then, then || task[:then])}}

  def wake(%Overwatch{} = state, then) do
    case RelayManager.assign_runner(state.namespace, state.factory_id, state.id) do
      nil ->
        # No runner to rehydrate onto: stay suspended. The next input (or the
        # convergence tick that brings a runner up) tries again.
        Logger.warning("[Overwatch] no active runner to rehydrate suspended relay #{state.id}")

        {{:error, :no_runner}, state}

      runner_id ->
        :telemetry.execute(
          [:flezha, :steps, :rehydrating],
          %{count: 1},
          %{namespace: state.namespace, relay_id: state.id, runner_id: runner_id}
        )

        {:ok, start_bind(state, runner_id, then)}
    end
  end

  @doc """
  A durable steps class: the one that owns a task-journal and a durable
  input log. An opted-out class keeps only its run's in-memory queue and
  re-runs from the top.
  """
  @spec durable_steps?(Overwatch.t(), map()) :: boolean()
  def durable_steps?(%Overwatch{} = state, relay) do
    Builder.journals?(state.builder) and
      ClassRegistry.persistent?(Keyword.get(relay.opts, :relay_class, relay.id))
  end

  @doc """
  Suspended per durable state alone: a park record exists, the input log owes
  nothing above the instance's floor and the park's wake deadline has not
  passed. Undelivered input or an elapsed deadline outranks the park: the
  instance has work waiting, so it binds instead.
  """
  @spec suspended_on_disk?(binary()) :: boolean()
  def suspended_on_disk?(relay_class) do
    ClassRegistry.parked(relay_class) != nil and not pending?(relay_class)
  end

  @doc """
  Tell a deferred migration it may retry now. It re-queries consent on its own
  tick; this only makes the move prompt. Any failure — hub down mid-shutdown,
  nothing deferred — is a no-op.
  """
  @spec signal_migration_ready(Overwatch.t()) :: :ok
  def signal_migration_ready(%Overwatch{} = state) do
    _ =
      HubMigration.migration_ready(
        Identificator.hub(state.namespace),
        state.id
      )

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  @doc """
  Process teardown — the `:process` walk of the shutdown plan, deliberate stops
  included: the hub stop path terminates the Overwatch through its supervisor,
  so everything a stop owes runs here, as the steps this module declares.
  Evicts the in-memory class entry and, after the final flush, this node's
  adoption of the class's remote lineage; the durable sidecar stays for the
  process-less retention sweep over any retained finished-job state.
  """
  @spec terminate(Overwatch.t(), term()) :: :ok
  def terminate(%Overwatch{} = state, reason) do
    _ = Shutdown.run(:process, {state, reason}, Shutdown.process_budget_ms())

    Logger.info(
      "Terminating relay #{state.id} in namespace #{state.namespace}: #{inspect(reason)}"
    )

    :ok
  end

  # The `:process` steps, in order: the in-flight bind task FIRST (left
  # running, it would bind a runner for a relay that is being torn down),
  # the class entry, the runner's copy of the relay, and the plugin table
  # ONCE — the on-terminate hooks of a bound relay, then every subscriber
  # and its registration, bound or not, so a prepared provider's pid never
  # outlives its host.
  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [
      %{scope: :process, order: 5, name: :calls, run: &calls_step/2},
      %{scope: :process, order: 10, name: :migrate_task, run: &migrate_task_step/2},
      %{scope: :process, order: 20, name: :class_registry, run: &class_registry_step/2},
      %{scope: :process, order: 30, name: :control_unit, run: &control_unit_step/2},
      %{scope: :process, order: 35, name: :writers, run: &writers_step/2},
      %{scope: :process, order: 37, name: :lineage, run: &lineage_step/2},
      %{scope: :process, order: 40, name: :plugin_table, run: &plugin_table_step/2}
    ]
  end

  # Every caller still waiting learns at once that no answer will come: the
  # steps run's terminal status when it ended as one, `stopped` otherwise.
  # An ended run never reads its undelivered input, so the relays that
  # called into it are answered too. A stopped one keeps its input for a start:
  # when it journals its input, the relays' calls are in that log and are
  # answered when it runs again, or by their callers' deadlines.
  defp calls_step({state, _reason}, _budget) do
    callee = Overwatch.callee(state)

    case RelayManager.completion(Overwatch.relay_class(state)) do
      {:ok, %{status: status}} ->
        Calls.end_all(state.calls, callee, status, Ingest.undelivered_calls(state))

      _no_terminal_record ->
        calls = if Replay.floor_reporter(state), do: Calls.outside(state.calls), else: state.calls
        Calls.end_all(calls, callee, "stopped")
    end

    :ok
  end

  defp migrate_task_step({state, _reason}, _budget) do
    _ = terminate_migrate_task(state)
    :ok
  end

  defp class_registry_step({state, _reason}, _budget) do
    _ = ClassRegistry.delete(Overwatch.relay_class(state))
    :ok
  end

  # The runner has been told to drop the relay, so it is no longer bound
  # here: its writers close, flushed, and its files stay. A relay that
  # moved away leaves nothing open for its return to find (`Flezha.State.Adoption`).
  defp writers_step({state, _reason}, _budget),
    do: RelayManager.close_writers(Overwatch.relay_class(state))

  defp control_unit_step({%Overwatch{runner_id: nil}, _reason}, _budget), do: :ok

  # A runner whose control unit has already gone holds nothing of this
  # relay left to release, so there is nothing to tell it — and the walk
  # must go on to the plugin table rather than halt with the subscribers'
  # owned pids still alive.
  defp control_unit_step({state, _reason}, _budget) do
    case ControlUnit.stop_relay_async(state.runner_id, state.id) do
      {:error, :no_control_unit} -> :ok
      other -> other
    end
  end

  # After the writers' final flush, so the last turns of a relay stopped here
  # still ship under the adoption that entitles them.
  defp lineage_step({state, _reason}, _budget),
    do: Lifecycle.release_lineage(Overwatch.relay_class(state), departure(state))

  # A deliberate stop removed the relay's declared entry before it
  # terminated: a shared plugin it was the last one on stops at once. A move or
  # a restart leaves the entry, and the plugin waits out its idle stop.
  defp plugin_table_step({state, reason}, _budget) do
    unless is_nil(state.runner_id), do: Plugins.fire_on_terminate(state, reason)
    idle_or_now = if departure(state) == :moved, do: :idle, else: :now
    _ = Table.teardown_all(state.extensions, idle_or_now)
    :ok
  end

  defp departure(state) do
    declared? = Enum.any?(HubManager.declared_relays(state.namespace), &(&1.id == state.id))
    if declared?, do: :moved, else: :stopped
  end

  @doc """
  The runner asked to sleep: write the park record, durable before the
  eviction so a controller crash in between still rediscovers the sleep, then
  evict the instance from its runner and keep it suspended. Not a terminal
  outcome: no completion record, no state deletion.
  """
  @spec suspend_at(Overwatch.t(), non_neg_integer(), ClassRegistry.park()) :: Overwatch.t()
  def suspend_at(%Overwatch{} = state, iter, park) do
    case ClassRegistry.put_parked(Overwatch.relay_class(state), park) do
      :ok ->
        evict_parked(state, iter, park)

      # The park is not durable, so the instance must NOT be evicted: it stays
      # bound, and resumes from its durable state when it is next rebound.
      {:error, reason} ->
        Logger.warning(
          "[Overwatch] park record failed for #{state.id} iter=#{iter}: " <>
            "#{inspect(reason)}; instance left bound"
        )

        state
    end
  end

  defp evict_parked(%Overwatch{} = state, iter, park) do
    release_runner(state)

    Logger.info(
      "[Overwatch] relay #{state.id} suspended at iteration #{iter}" <>
        if(park.wake_deadline_ms, do: " (wakes at #{park.wake_deadline_ms})", else: "")
    )

    # An input that landed while the loop was parking — durable, but never
    # drained by the exiting driver — is undelivered work: rehydrate straight
    # away rather than leaving the instance suspended until the NEXT message
    # arrives. The park/rehydrate cycle costs a bind; stranding the turn would
    # cost correctness. `RemRelay` is already enqueued on the control
    # stream, so the rehydrating `AddRelay` cannot overtake it.
    # Parked while an un-park was still binding: the task's result must not
    # resurrect a binding the runner has just torn down.
    state = %Overwatch{state | parked_in_flight: state.migrate_task != nil}

    state
    |> enter_suspended(park)
    |> rehydrate_if_pending()
  end

  @doc """
  Session `runner_id` reports that the process of `relay_id` exited with
  `reason`: tell its Overwatch, which `park_exited/3` decides for. With no
  Overwatch on this node there is nothing to rebind.

  Called by `ControlChannel` on the runner's `RelayExited`, with the Overwatch
  the frame was admitted against as `bound` (see `overwatch/3`).
  """
  @spec relay_exited(binary(), binary(), binary(), binary(), pid() | nil) :: :ok
  def relay_exited(namespace, relay_id, runner_id, reason, bound) do
    case overwatch(namespace, relay_id, bound) do
      {:ok, pid} when is_pid(pid) -> GenServer.cast(pid, {:relay_exited, runner_id, reason})
      _ -> :ok
    end
  end

  @doc """
  Session `runner_id` reports that this instance's process exited with
  `reason` and that it no longer hosts it. A current report parks the
  instance, and the convergence loop's un-park rebinds it: it is bound to that
  session, no bind is in flight, it is neither suspended nor parked already,
  and no completion is recorded for it. Any other report is stale, or already
  answered by what is under way, and changes nothing.

  A report that lands while a bind is in flight may be about the incarnation
  that bind started, which can die before the bind reports, so it is held and
  judged against the binding `apply_migrate_result/2` adopts.
  """
  @spec park_exited(Overwatch.t(), binary(), binary()) :: Overwatch.t()
  def park_exited(%Overwatch{migrate_task: %{}} = state, runner_id, reason),
    do: %Overwatch{state | exited_in_flight: {runner_id, reason}}

  def park_exited(
        %Overwatch{runner_id: runner_id, migrate_task: nil, suspended: false, init_error: nil} =
          state,
        runner_id,
        reason
      ) do
    if RelayManager.completed?(Overwatch.relay_class(state)) do
      state
    else
      Logger.warning(
        "[Overwatch] relay #{state.id} exited on runner #{runner_id}: #{reason}; rebinding"
      )

      :telemetry.execute(
        [:flezha, :relay, :exited],
        %{count: 1},
        %{namespace: state.namespace, relay_id: state.id, runner_id: runner_id, reason: reason}
      )

      Lifecycle.park(state, {:relay_exited, reason})
    end
  end

  def park_exited(%Overwatch{} = state, _runner_id, _reason), do: state

  @doc """
  Journal the rewind marker, evict, and rehydrate. A rewind is a rebind, not a
  park: no park record, no wake timer, and the rehydrate is unconditional:
  the loop must come back up to undo the iterations the marker unwound even with
  no input waiting on it.
  """
  @spec rewind_to(Overwatch.t(), non_neg_integer()) :: {:ok, Overwatch.t()} | {:error, term()}
  def rewind_to(%Overwatch{} = state, to_iter) do
    with :ok <- rewind_guard(state, to_iter),
         :ok <- TaskJournal.append_rewound(state.id, to_iter) do
      release_runner(state)

      :telemetry.execute(
        [:flezha, :steps, :rewind],
        %{count: 1, epoch: TaskJournal.current_epoch(state.id)},
        %{namespace: state.namespace, relay_id: state.id, to_iter: to_iter}
      )

      Logger.info("[Overwatch] relay #{state.id} rewound to iteration #{to_iter}")

      {:ok, rehydrate(evict_runner(state, nil))}
    end
  end

  # ---------------------------------------------------------------
  # Teardown
  # ---------------------------------------------------------------

  # ---------------------------------------------------------------
  # Async migrate task plumbing. Kept together so the task-spawn /
  # terminate / result-application surface is reviewable as a single unit.
  # ---------------------------------------------------------------

  # A graceful stop ends the Overwatch with a reason its linked bind task
  # would ignore, so teardown kills the task rather than wait on its bind.
  @spec terminate_migrate_task(Overwatch.t()) :: Overwatch.t()
  def terminate_migrate_task(%Overwatch{migrate_task: nil} = state), do: state

  def terminate_migrate_task(%Overwatch{migrate_task: %{pid: pid}} = state) do
    Process.unlink(pid)
    Process.exit(pid, :kill)
    %{state | migrate_task: nil}
  end

  # Run `complete_init/5` on `runner_id` in a task so a slow bind never blocks
  # the Overwatch. The task is linked to the Overwatch, so a killed Overwatch
  # takes its bind with it and no second bind outlives it. It reports its
  # outcome and decides nothing: a failure is a park the Overwatch declares.
  # `then` is what a wake carries (`apply_migrate_result/2`).
  defp start_bind(%Overwatch{} = state, runner_id, then) do
    relay = bind_relay(state)
    parent = self()

    pid =
      spawn_link(fn ->
        result =
          case Lifecycle.complete_init(state, runner_id, relay, parent, :pinned) do
            {:ok, recovered} -> {:bound, recovered}
            {:error, err, parked} -> {:park, parked, err}
          end

        send(parent, {:migrate_task_result, self(), result})
      end)

    %Overwatch{state | migrate_task: %{pid: pid, then: then}}
  end

  @doc """
  The bind task ended before it reported: its bind failed with `reason`. A
  task that reported is no longer in flight, so its exit reaches here only
  when it died first.
  """
  @spec bind_task_exited(Overwatch.t(), term()) :: Overwatch.t()
  def bind_task_exited(%Overwatch{} = state, reason),
    do: apply_migrate_result(state, {:park, state, {:bind_task_exit, reason}})

  @doc """
  Fold a bind task's result back into the state.

  `complete_init/5` ran inside the task, whose `self()` is not the Overwatch
  process, so the binding is published here — the one point every move
  funnels through: an un-park, a wake, a `runner_migrate` and a restarted
  runner.
  """
  @spec apply_migrate_result(Overwatch.t(), tuple()) :: Overwatch.t()
  def apply_migrate_result(%Overwatch{parked_in_flight: true} = state, {:bound, _recovered}) do
    # The instance parked before this bind reported, so the bound state is
    # stale; the suspension stands, and the tail that was journaled meanwhile
    # (or a stop the wake carried) wakes it again.
    then = carried(state)
    state = %Overwatch{state | migrate_task: nil, parked_in_flight: false, exited_in_flight: nil}

    case then do
      :stop -> state |> wake(:stop) |> elem(1)
      nil -> rehydrate_if_pending(state)
    end
  end

  def apply_migrate_result(state, {:bound, recovered}) do
    Logger.info("[Overwatch] relay #{state.id} un-parked onto runner #{recovered.runner_id}")

    Lifecycle.publish_binding(recovered, state.runner_id)

    if carried(state) == :stop, do: ControlUnit.steps_stop(recovered.runner_id, state.id)

    # The bound runner's state supersedes a snapshot stored before the bind.
    %{recovered | migrate_task: nil, snapshot: nil, bound_at: System.system_time(:millisecond)}
    |> judge_held_exit(state.exited_in_flight)
  end

  # A failed un-park with input journaled meanwhile tries again at once, as
  # any bind that ends with work owed does.
  def apply_migrate_result(_state, {:park, parked, err}) do
    %{
      Lifecycle.park(parked, err)
      | migrate_task: nil,
        parked_in_flight: false,
        exited_in_flight: nil
    }
    |> rehydrate_if_pending()
  end

  defp judge_held_exit(state, nil), do: state
  defp judge_held_exit(state, {runner_id, reason}), do: park_exited(state, runner_id, reason)

  defp carried(%Overwatch{migrate_task: %{then: then}}), do: then
  defp carried(%Overwatch{}), do: nil

  # ---------------------------------------------------------------
  # SDK round-trips
  # ---------------------------------------------------------------
end

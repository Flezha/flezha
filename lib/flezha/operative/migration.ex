defmodule Flezha.Operative.Migration do
  @moduledoc """
  Everything that moves an operative off the runner it is on: suspension, the
  un-park task, a running migrate, a reconnect's session swap, a loop rewind,
  and the terminal teardown.

  It sits above `Flezha.Operative.Lifecycle` in the call graph — it binds,
  completes an init and parks through that module and is never called back by
  it — so it is a sibling of `Handover`, `Ingest`, `Plugins` and `Replay`,
  reached from `Overwatch`.

  Invariant: a suspended operative holds no runner resources. The wake timer it
  leaves behind is the only thing that outlives the eviction, and it lives in
  `Lifecycle` with the rest of the bind machinery.
  """

  alias Flezha.Identificator
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Lifecycle
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Plugins
  alias Flezha.Operative.Replay
  alias Flezha.Payloader
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Shutdown
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.TaskJournal
  alias ProcessHub.Service.Migration, as: HubMigration

  require Logger

  @behaviour Flezha.Shutdown.Step

  @doc """
  Suspend an instance of either kind that asked to sleep (a workflow at the
  committed boundary of iteration `iter`, a stream at `0`): write its park
  record, evict it from its runner (`RemOperative`), release the registry's
  runner binding, and arm the recorded wake deadline. The registry entry (and
  the Overwatch process) stay, so a new input still has somewhere to land; it
  is the **runner** that holds zero resources for a suspended operative.

  With no Overwatch on this node (already migrated or torn down) the record is
  written under the instance's own key, so it is rediscovered as suspended
  whenever it is next started, unless another namespace owns its state.

  Called by `ControlChannel` on the runner's `IterPark`.
  """
  @spec suspend(binary(), binary(), non_neg_integer(), ClassRegistry.park()) :: :ok
  def suspend(namespace, operative_id, iter, park) do
    case HubManager.get_pid(namespace, operative_id) do
      {:ok, pid} when is_pid(pid) -> GenServer.cast(pid, {:suspend, iter, park})
      _ -> _ = ClassRegistry.put_parked(operative_id, park, namespace)
    end

    :ok
  end

  @doc """
  Rewind a workflow loop to the committed boundary of iteration `to_iter`: journal
  the rewind — which bumps the epoch and un-commits every iteration above it —
  then evict the instance from its runner and rehydrate it.

  The rehydrating bind ships the folded journal back, so the driver undoes the
  unwound tasklets through the ordinary compensation planner and resumes the
  loop at `to_iter + 1`, waiting for its next input: the inputs the unwound
  iterations consumed stay released, since a rewind never lowers the
  instance's floor in its input log. The marker is durable before the
  eviction, so a crash mid-rewind resumes as a rewind rather than as a
  partially unwound loop.

  The caller (`CliManager`) has already refused an uncommitted target and an
  iteration whose plugin output cannot be compensated.
  """
  @spec rewind(pid(), non_neg_integer()) :: :ok | {:error, term()}
  def rewind(overwatch_pid, to_iter) when is_pid(overwatch_pid),
    do: GenServer.call(overwatch_pid, {:rewind, to_iter}, 30_000)

  # ---------------------------------------------------------------
  # Bind
  # ---------------------------------------------------------------

  @doc """
  The minimal SDK operative for a running-operative migrate: the factory + the
  current (snapshot-derived) init_values, no plugins (the controller-side
  plugin table is untouched on a running migrate).

  Partition identity is the one thing that must survive: it arms the SDK's
  mis-route guard, and a member that migrates without it starts accepting keys
  it does not own.
  """
  @spec migrate_operative(Overwatch.t()) :: Flezha.Operative.Builder.t()
  def migrate_operative(%Overwatch{} = state) do
    init_values =
      if state.snapshot != nil,
        do: Payloader.json_to_map(state.snapshot),
        else: state.init_values

    %Flezha.Operative.Builder{
      id: state.id,
      factory_id: state.factory_id,
      kind: state.builder.kind,
      init_values: init_values,
      opts: Keyword.take(state.builder.opts, Flezha.Partition.opt_keys())
    }
  end

  @doc """
  The convergence loop's `{:runner_migrate, target}`, answered as the reply
  tuple the Overwatch's call returns.

  Parked (a failed init) and suspended (a durable loop park) both come back
  through the un-park task: it re-binds, restores journal and cells, and
  re-forwards the input log's undelivered tail, off the Overwatch so a slow
  bind never blocks it. A running operative migrates in place: one ping and start
  round-trip, kept synchronous for the HotSwap handover invariants that depend
  on the state being applied before the call returns.
  """
  @spec runner_migrate(Overwatch.t(), binary()) ::
          {:reply, :ok | {:error, term()}, Overwatch.t()}
  def runner_migrate(%Overwatch{migrate_task: task} = state, _new_runner_id) when task != nil,
    do: {:reply, {:error, :busy}, state}

  def runner_migrate(%Overwatch{init_error: reason, suspended: suspended} = state, new_runner_id)
      when reason != nil or suspended do
    {ref, pid} = spawn_migrate_task({:unpark, state, new_runner_id})
    {:reply, :ok, %{state | migrate_task: %{ref: ref, pid: pid, mode: :unpark}}}
  end

  # The binding is committed only once the runner has answered. Committing it
  # first leaves a failed migrate naming a runner this process cannot reach —
  # the operative then reads as bound to a LIVE runner, so `Convergence.Diff`
  # stops classifying it misbound and the divergence hides itself permanently.
  # The runner now holds the stored snapshot it was seeded from, so it is not kept.
  #
  # While the attempt runs the target's identity is pending on the ownership
  # record, so the target's first output and the current runner's frames are
  # both accepted. A failed attempt clears it and drops any route the target's
  # channels warmed meanwhile.
  def runner_migrate(%Overwatch{} = state, new_runner_id) do
    class = Overwatch.operative_class(state)
    _ = ClassRegistry.set_pending(class, Marks.identity_of(new_runner_id))

    case Lifecycle.attempt_ping_and_start(migrate_operative(state), new_runner_id) do
      {:ok, _} ->
        {:reply, :ok, Lifecycle.commit_binding(%{state | snapshot: nil}, new_runner_id)}

      {:error, reason} ->
        _ = ClassRegistry.set_pending(class, nil)
        _ = StreamPool.drop_route(new_runner_id, state.id)
        {:reply, {:error, reason}, state}
    end
  end

  @doc """
  The runner this operative is bound to reconnected as `new_session` with the
  same stable identity: re-point the binding and publish it, so the Overwatch,
  the registry tag and the route move together while the ownership record
  keeps naming the same identity. A binding that has already moved away from
  `old_session` is left alone.
  """
  @spec session_replaced(Overwatch.t(), binary(), binary()) :: Overwatch.t()
  def session_replaced(%Overwatch{runner_id: old_session} = state, old_session, new_session),
    do: Lifecycle.commit_binding(state, new_session)

  def session_replaced(%Overwatch{} = state, _old_session, _new_session), do: state

  # Refuse a rewind before it changes anything: an uncommitted target, or an
  # unwound tasklet whose plugin output cannot be undone.
  @spec rewind_guard(Overwatch.t(), non_neg_integer()) :: :ok | {:error, term()}
  def rewind_guard(%Overwatch{} = state, to_iter) do
    committed = TaskJournal.committed_iterations(state.id)

    cond do
      to_iter not in committed ->
        {:error, {:uncommitted_iteration, committed}}

      (uncovered = uncovered_channels(state, to_iter)) != [] ->
        {:error, {:uncovered, uncovered}}

      true ->
        :ok
    end
  end

  # A workflow operative's staged plugin output carries no natural key, so a
  # re-run's output cannot be relied on to overwrite the original at every sink.
  # The only undo the engine can guarantee is the tasklet's own `compensate`
  # hook — so an unwound tasklet that emitted and declares none blocks the
  # rewind, naming the channels it wrote. A tasklet that emitted nothing needs
  # no coverage.
  def uncovered_channels(%Overwatch{} = state, to_iter) do
    compensable = ClassRegistry.compensable_tasks(Overwatch.operative_class(state)) || []

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
      [:flezha, :workflow, :suspended],
      %{count: 1},
      %{namespace: state.namespace, operative_id: state.id}
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

  # Drop the operative from its runner and release the registry's runner tag —
  # the half a suspension and a rewind both perform before they diverge.
  def release_runner(%Overwatch{} = state) do
    if state.runner_id do
      ControlUnit.stop_operative_async(state.runner_id, state.id)
    end

    _ = HubManager.operative_set_runner(state.namespace, state.id, nil)
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
    if pending?(Overwatch.operative_class(state)),
      do: rehydrate(state),
      else: state
  end

  defp pending?(operative_class),
    do: Replay.owed_count(operative_class) > 0 or wake_due?(operative_class)

  # The park's deadline is the earliest timer the instance's Cells held: once
  # it has passed, the instance has work waiting as surely as undelivered
  # input.
  defp wake_due?(operative_class) do
    case ClassRegistry.parked(operative_class) do
      %{wake_deadline_ms: deadline} when is_integer(deadline) ->
        deadline <= System.system_time(:millisecond)

      _ ->
        false
    end
  end

  @doc """
  Rehydrate a suspended instance through the normal bind path (the same
  `complete_init/4` an un-park drives), which restores journal + snapshots and
  re-forwards the input log's undelivered tail.

  Idempotent: an instance already running, or with a rehydrate already in
  flight, no-ops — so two wake sources racing (an input-log append and the
  wake deadline) rehydrate exactly once.
  """
  @spec rehydrate(Overwatch.t()) :: Overwatch.t()
  def rehydrate(%Overwatch{} = state), do: state |> wake(nil) |> elem(1)

  @doc """
  `rehydrate/1` with what the wake carries: `then: :stop` sends `WorkflowStop`
  to the runner the un-park binds, so a sleeping workflow stopped by an
  operator ends exactly as one stopped awake at the same boundary. A wake
  already in flight takes the stop on. `{:error, :no_runner}` when no runner
  can take the instance; it stays suspended.
  """
  @spec wake(Overwatch.t(), :stop | nil) :: {:ok | {:error, :no_runner}, Overwatch.t()}
  def wake(%Overwatch{suspended: false} = state, _then), do: {:ok, state}

  def wake(%Overwatch{migrate_task: %{} = task} = state, then),
    do: {:ok, %Overwatch{state | migrate_task: Map.put(task, :then, then || task[:then])}}

  def wake(%Overwatch{} = state, then) do
    case OperativeManager.assign_runner(state.namespace, state.factory_id, state.id) do
      nil ->
        # No runner to rehydrate onto: stay suspended. The next input (or the
        # convergence tick that brings a runner up) tries again.
        Logger.warning(
          "[Overwatch] no active runner to rehydrate suspended operative #{state.id}"
        )

        {{:error, :no_runner}, state}

      runner_id ->
        :telemetry.execute(
          [:flezha, :workflow, :rehydrating],
          %{count: 1},
          %{namespace: state.namespace, operative_id: state.id, runner_id: runner_id}
        )

        {ref, pid} = spawn_migrate_task({:unpark, state, runner_id})
        {:ok, %Overwatch{state | migrate_task: %{ref: ref, pid: pid, mode: :unpark, then: then}}}
    end
  end

  @doc """
  A durable workflow class: the kind that owns a task-journal and a durable
  input log. An opted-out class keeps only its run's in-memory queue and
  re-runs from the top.
  """
  @spec durable_workflow?(Overwatch.t(), map()) :: boolean()
  def durable_workflow?(%Overwatch{} = state, operative) do
    state.builder.kind == :workflow and
      PersistenceResolver.persistence_enabled?(
        Keyword.get(operative.opts, :operative_class, operative.id)
      )
  end

  @doc """
  Suspended per durable state alone: a park record exists, the input log owes
  nothing above the instance's floor and the park's wake deadline has not
  passed. Undelivered input or an elapsed deadline outranks the park: the
  instance has work waiting, so it binds instead.
  """
  @spec suspended_on_disk?(binary()) :: boolean()
  def suspended_on_disk?(operative_class) do
    ClassRegistry.parked(operative_class) != nil and not pending?(operative_class)
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
  Evicts the in-memory class entry; the durable sidecar stays for the
  process-less retention sweep over any retained finished-job state.
  """
  @spec terminate(Overwatch.t(), term()) :: :ok
  def terminate(%Overwatch{} = state, reason) do
    _ = Shutdown.run(:process, {state, reason}, Shutdown.process_budget_ms())

    Logger.info(
      "Terminating operative #{state.id} in namespace #{state.namespace}: #{inspect(reason)}"
    )

    :ok
  end

  # The `:process` steps, in order: the in-flight un-park task FIRST (left
  # running, it would bind a runner for an operative that is being torn down),
  # the class entry, the runner's copy of the operative, and the plugin table
  # ONCE — the on-terminate hooks of a bound operative, then every subscriber
  # and its registration, bound or not, so a prepared provider's pid never
  # outlives its host.
  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [
      %{scope: :process, order: 10, name: :migrate_task, run: &migrate_task_step/2},
      %{scope: :process, order: 20, name: :class_registry, run: &class_registry_step/2},
      %{scope: :process, order: 30, name: :control_unit, run: &control_unit_step/2},
      %{scope: :process, order: 40, name: :plugin_table, run: &plugin_table_step/2}
    ]
  end

  defp migrate_task_step({state, _reason}, _budget) do
    _ = terminate_migrate_task(state)
    :ok
  end

  defp class_registry_step({state, _reason}, _budget) do
    _ = ClassRegistry.delete(Overwatch.operative_class(state))
    :ok
  end

  defp control_unit_step({%Overwatch{runner_id: nil}, _reason}, _budget), do: :ok

  # A runner whose control unit has already gone holds nothing of this
  # operative left to release, so there is nothing to tell it — and the walk
  # must go on to the plugin table rather than halt with the subscribers'
  # owned pids still alive.
  defp control_unit_step({state, _reason}, _budget) do
    case ControlUnit.stop_operative_async(state.runner_id, state.id) do
      {:error, :no_control_unit} -> :ok
      other -> other
    end
  end

  defp plugin_table_step({state, reason}, _budget) do
    unless is_nil(state.runner_id), do: Plugins.fire_on_terminate(state, reason)
    _ = Plugins.unregister(Table.teardown_all(state.extensions))
    :ok
  end

  @doc """
  The runner asked to sleep: write the park record, durable before the
  eviction so a controller crash in between still rediscovers the sleep, then
  evict the instance from its runner and keep it suspended. Not a terminal
  outcome: no completion record, no state deletion. A plain sleeping stream's
  record carries its forwarded floor, which is the one thing about its input a
  boot cannot read back.
  """
  @spec suspend_at(Overwatch.t(), non_neg_integer(), ClassRegistry.park()) :: Overwatch.t()
  def suspend_at(%Overwatch{} = state, iter, park) do
    park = Replay.floor_at_park(state, park)

    case ClassRegistry.put_parked(Overwatch.operative_class(state), park) do
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
      "[Overwatch] operative #{state.id} suspended at iteration #{iter}" <>
        if(park.wake_deadline_ms, do: " (wakes at #{park.wake_deadline_ms})", else: "")
    )

    # An input that landed while the loop was parking — durable, but never
    # drained by the exiting driver — is undelivered work: rehydrate straight
    # away rather than leaving the instance suspended until the NEXT message
    # arrives. The park/rehydrate cycle costs a bind; stranding the turn would
    # cost correctness. `RemOperative` is already enqueued on the control
    # stream, so the rehydrating `AddOperative` cannot overtake it.
    # Parked while an un-park was still binding: the task's result must not
    # resurrect a binding the runner has just torn down.
    state = %Overwatch{state | parked_in_flight: state.migrate_task != nil}

    state
    |> enter_suspended(park)
    |> rehydrate_if_pending()
  end

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
        [:flezha, :workflow, :rewind],
        %{count: 1, epoch: TaskJournal.current_epoch(state.id)},
        %{namespace: state.namespace, operative_id: state.id, to_iter: to_iter}
      )

      Logger.info("[Overwatch] operative #{state.id} rewound to iteration #{to_iter}")

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

  # Terminate any in-flight un-park task so teardown is never blocked by the
  # bind running inside it.
  @spec terminate_migrate_task(Overwatch.t()) :: Overwatch.t()
  def terminate_migrate_task(%Overwatch{migrate_task: nil} = state), do: state

  def terminate_migrate_task(%Overwatch{migrate_task: %{ref: ref, pid: pid}} = state) do
    _ = Process.demonitor(ref, [:flush])
    _ = Task.Supervisor.terminate_child(Flezha.Operative.MessageHandlerTaskSup, pid)
    %{state | migrate_task: nil}
  end

  @doc """
  Run `complete_init/4` in a task so a slow bind never blocks the Overwatch.
  The task reports its outcome and decides nothing: a failure is a park the
  Overwatch declares. Returns the task's `{ref, pid}`.
  """
  @spec spawn_migrate_task({:unpark, Overwatch.t(), binary()}) :: {reference(), pid()}
  def spawn_migrate_task({:unpark, state, new_runner_id}) do
    operative = state.builder
    parent = self()

    task =
      Task.Supervisor.async_nolink(Flezha.Operative.MessageHandlerTaskSup, fn ->
        case Lifecycle.complete_init(state, new_runner_id, operative, parent) do
          {:ok, recovered} ->
            send(parent, {:migrate_task_result, self(), {:unparked, recovered}})
            :ok

          {:error, err, parked} ->
            send(parent, {:migrate_task_result, self(), {:park, parked, err}})
            :ok
        end
      end)

    {task.ref, task.pid}
  end

  @doc """
  Fold an un-park task's result back into the state.

  `complete_init/4` ran inside the task, whose `self()` is not the Overwatch
  process, so the binding is published here — the one point every un-park caller
  funnels through, `rehydrate/1` included.
  """
  @spec apply_migrate_result(Overwatch.t(), tuple()) :: Overwatch.t()
  def apply_migrate_result(%Overwatch{parked_in_flight: true} = state, {:unparked, _recovered}) do
    # The instance parked before this bind reported, so the bound state is
    # stale; the suspension stands, and the tail that was journaled meanwhile
    # (or a stop the wake carried) wakes it again.
    then = carried(state)
    state = %Overwatch{state | migrate_task: nil, parked_in_flight: false}

    case then do
      :stop -> state |> wake(:stop) |> elem(1)
      nil -> rehydrate_if_pending(state)
    end
  end

  def apply_migrate_result(state, {:unparked, recovered}) do
    Logger.info("[Overwatch] operative #{state.id} un-parked onto runner #{recovered.runner_id}")

    Lifecycle.publish_binding(recovered, state.runner_id)

    if carried(state) == :stop, do: ControlUnit.workflow_stop(recovered.runner_id, state.id)

    # The bound runner's state supersedes a snapshot stored before the bind.
    %{recovered | migrate_task: nil, snapshot: nil}
  end

  def apply_migrate_result(_state, {:park, parked, err}) do
    %{Lifecycle.park(parked, err) | migrate_task: nil, parked_in_flight: false}
  end

  defp carried(%Overwatch{migrate_task: %{then: then}}), do: then
  defp carried(%Overwatch{}), do: nil

  # ---------------------------------------------------------------
  # SDK round-trips
  # ---------------------------------------------------------------
end

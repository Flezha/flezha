defmodule Flezha.Relay.Suspend do
  @moduledoc """
  A relay asleep, waking and rewinding: the sleep record, the eviction, the
  wake timer, the rehydrate through `Flezha.Relay.Rebind`, and a steps loop's
  rewind, which rides the same evict and rehydrate path.

  Invariant: a suspended relay holds no runner resources. The wake timer it
  leaves behind is the only thing that outlives the eviction.
  """

  alias Flezha.Config.Durations
  alias Flezha.Process.Timer
  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Rebind
  alias Flezha.Relay.Replay
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.TaskJournal

  require Logger

  # The ETS key and the sidecar's name on disk: durable state, never renamed.
  @sleep {:parked, "parked"}

  @typedoc """
  An instance asleep: the durable wake deadline it parked with. What it
  consumed lives in its own records, never here.
  """
  @type sleep_record :: %{wake_deadline_ms: non_neg_integer() | nil}

  @doc """
  Record that `relay_class`'s instance is asleep, or clear it with `nil`.
  Written durably before the instance is evicted, so a controller restart
  rediscovers the sleep, and a write that did not land is answered as an error
  so the caller leaves the instance bound; cleared by the bind that wakes it,
  before the runner is told to start the instance. Given a `namespace`, refused
  when another namespace owns the instance's durable state.
  """
  @spec put_sleep(binary(), sleep_record() | nil, binary() | nil) :: :ok | {:error, term()}
  def put_sleep(relay_class, record, namespace \\ nil)

  def put_sleep(relay_class, nil, namespace),
    do: ClassRegistry.put_record(relay_class, @sleep, nil, namespace)

  def put_sleep(relay_class, %{wake_deadline_ms: deadline} = record, namespace),
    do: ClassRegistry.put_record(relay_class, @sleep, {record, "#{deadline || "-"}"}, namespace)

  @doc "The sleep record of `relay_class`'s instance, `nil` when awake."
  @spec sleep_record(binary()) :: sleep_record() | nil
  def sleep_record(relay_class) when is_binary(relay_class) do
    {key, sidecar} = @sleep
    ClassRegistry.declaration(relay_class, key, sidecar, &parse_sleep/1)
  end

  # A record written before the park stopped carrying an offset has one more
  # field, which is not read.
  defp parse_sleep(text) do
    [deadline | _offset] = String.split(text, ",")
    %{wake_deadline_ms: if(deadline == "-", do: nil, else: String.to_integer(deadline))}
  end

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
  @spec suspend(binary(), binary(), non_neg_integer(), sleep_record(), pid() | nil) :: :ok
  def suspend(namespace, relay_id, iter, park, bound) do
    case overwatch(namespace, relay_id, bound) do
      {:ok, pid} when is_pid(pid) -> GenServer.cast(pid, {:suspend, iter, park})
      _ -> _ = put_sleep(relay_id, park, namespace)
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
    do: GenServer.call(overwatch_pid, {:rewind, to_iter}, Durations.get(:rewind_call_ms))

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
  @spec enter_suspended(Overwatch.t(), sleep_record()) :: Overwatch.t()
  def enter_suspended(%Overwatch{} = state, park) do
    :telemetry.execute(
      [:flezha, :steps, :suspended],
      %{count: 1},
      %{namespace: state.namespace, relay_id: state.id}
    )

    # A sleeping instance holds no pass in flight: the migration-safe barrier.
    Rebind.signal_migration_ready(state)
    evict_runner(state, park.wake_deadline_ms)
  end

  # Release every runner-side resource but keep the process and the registry
  # entry, so a new input still has somewhere to land. Shared by suspension (a
  # park, with its wake timer) and rewind (a rebind, with none).
  @spec evict_runner(Overwatch.t(), non_neg_integer() | nil) :: Overwatch.t()
  def evict_runner(%Overwatch{} = state, deadline) do
    %Overwatch{
      cancel_wake_timer(state)
      | phase: :suspended,
        runner_id: nil,
        channel: nil
    }
    |> arm_wake_timer(deadline)
  end

  # Drop the relay from its runner and release the registry's runner tag —
  # the half a suspension and a rewind both perform before they diverge.
  def release_runner(%Overwatch{} = state) do
    if state.runner_id, do: ControlUnit.stop_relay_async(state.runner_id, state.id)

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
  def rehydrate_if_pending(%Overwatch{phase: :suspended} = state) do
    if pending?(Overwatch.relay_class(state)), do: rehydrate(state), else: state
  end

  def rehydrate_if_pending(%Overwatch{} = state), do: state

  @doc """
  The Overwatch's answer to a runner of its factory connecting: a suspended
  relay owing work wakes, and one that stays asleep hibernates again.
  """
  @spec runner_available(Overwatch.t()) ::
          {:noreply, Overwatch.t()} | {:noreply, Overwatch.t(), :hibernate}
  def runner_available(%Overwatch{} = state) do
    case rehydrate_if_pending(state) do
      %Overwatch{phase: :suspended, bind_task: nil} = asleep -> {:noreply, asleep, :hibernate}
      state -> {:noreply, state}
    end
  end

  defp pending?(relay_class),
    do: Replay.owed_count(relay_class) > 0 or wake_due?(relay_class)

  # The park's deadline is the earliest timer the instance's Cells held: once
  # it has passed, the instance has work waiting as surely as undelivered
  # input.
  defp wake_due?(relay_class) do
    case sleep_record(relay_class) do
      %{wake_deadline_ms: deadline} when is_integer(deadline) ->
        deadline <= System.system_time(:millisecond)

      _ ->
        false
    end
  end

  @doc """
  Rehydrate a suspended instance through the normal bind path (the same
  `complete_init/5` a rebind drives), which restores journal + snapshots and
  re-forwards the input log's undelivered tail.

  Idempotent: an instance already running, or with a rehydrate already in
  flight, no-ops — so two wake sources racing (an input-log append and the
  wake deadline) rehydrate exactly once.
  """
  @spec rehydrate(Overwatch.t()) :: Overwatch.t()
  def rehydrate(%Overwatch{} = state), do: state |> wake(nil) |> elem(1)

  @doc """
  `rehydrate/1` with what the wake carries: `then: :stop` sends `StepsStop`
  to the runner the rebind binds, so a sleeping steps relay stopped by a
  user ends exactly as one stopped awake at the same boundary. A wake
  already in flight takes the stop on. `{:error, :no_runner}` when no runner
  can take the instance; it stays suspended.
  """
  @spec wake(Overwatch.t(), :stop | nil) :: {:ok | {:error, :no_runner}, Overwatch.t()}
  def wake(%Overwatch{phase: phase} = state, _then) when phase != :suspended, do: {:ok, state}

  def wake(%Overwatch{bind_task: %{} = task} = state, then),
    do: {:ok, %Overwatch{state | bind_task: Map.put(task, :then, then || task[:then])}}

  def wake(%Overwatch{} = state, then) do
    case RelayManager.assign_runner(state.namespace, state.factory_id, state.id) do
      nil ->
        # No runner to rehydrate onto: stay suspended. The next input, the
        # wake timer, or a runner of its factory connecting tries again.
        Logger.warning("[Overwatch] no active runner to rehydrate suspended relay #{state.id}")

        {{:error, :no_runner}, state}

      runner_id ->
        :telemetry.execute(
          [:flezha, :steps, :rehydrating],
          %{count: 1},
          %{namespace: state.namespace, relay_id: state.id, runner_id: runner_id}
        )

        {:ok, Rebind.start_bind(state, runner_id, then)}
    end
  end

  @doc """
  A stop of a sleeping steps relay: woken onto a runner with the stop carried,
  so it ends as it would stopped awake. `{:error, :awake}` when it is not
  asleep, and the caller stops it as an awake one.
  """
  @spec wake_to_stop(Overwatch.t()) :: {:reply, :ok | {:error, term()}, Overwatch.t()}
  def wake_to_stop(%Overwatch{phase: :suspended} = state) do
    {reply, state} = wake(state, :stop)
    {:reply, reply, state}
  end

  def wake_to_stop(%Overwatch{} = state), do: {:reply, {:error, :awake}, state}

  @doc """
  The recorded wake deadline elapsed (`durable-suspension`): it is
  the earliest timer the instance's Cells held when it parked, so the
  rehydrate alone wakes it: the restored Cells re-arm the timer and its
  firing opens the pass. Nothing is appended. A racing message collapses
  into the same rehydration, which is a no-op on a rehydrating instance.
  With no runner to wake on, the deadline is retried a convergence tick
  later, since no input will arrive to wake it.
  """
  @spec wake_deadline(Overwatch.t()) :: {:noreply, Overwatch.t()}
  def wake_deadline(%Overwatch{} = state) do
    case wake(state, nil) do
      {:ok, state} ->
        {:noreply, state}

      {{:error, :no_runner}, state} ->
        retry_at = System.system_time(:millisecond) + Durations.get(:convergence_tick_ms)
        {:noreply, arm_wake_timer(state, retry_at)}
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
    sleep_record(relay_class) != nil and not pending?(relay_class)
  end

  @doc """
  The runner asked to sleep: write the park record, durable before the
  eviction so a controller crash in between still rediscovers the sleep, then
  evict the instance from its runner and keep it suspended. Not a terminal
  outcome: no completion record, no state deletion.
  """
  @spec suspend_at(Overwatch.t(), non_neg_integer(), sleep_record()) :: Overwatch.t()
  def suspend_at(%Overwatch{} = state, iter, park) do
    case put_sleep(Overwatch.relay_class(state), park) do
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
    # Parked while a bind was still binding: its result must not resurrect a
    # binding the runner has just torn down.
    state
    |> Rebind.void_bind()
    |> enter_suspended(park)
    |> rehydrate_if_pending()
  end

  @doc """
  The Overwatch's `{:rewind, to_iter}` call: journal the rewind marker, evict,
  and rehydrate. A rewind is a rebind, not a park: no park record, no wake
  timer, and the rehydrate is unconditional: the loop must come back up to
  undo the iterations the marker unwound even with no input waiting on it.
  """
  @spec rewind_to(Overwatch.t(), non_neg_integer()) ::
          {:reply, :ok | {:error, term()}, Overwatch.t()}
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

      {:reply, :ok, rehydrate(evict_runner(state, nil))}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # The durable timer: a park record's wake deadline, re-armed from the record
  # on every boot, so it survives a controller redeploy. Armed here on
  # the Overwatch's behalf, under the `:wake` key that module declares.
  @spec arm_wake_timer(Overwatch.t(), non_neg_integer() | nil) :: Overwatch.t()
  def arm_wake_timer(%Overwatch{} = state, nil), do: state

  def arm_wake_timer(%Overwatch{} = state, deadline_ms) do
    delay = max(deadline_ms - System.system_time(:millisecond), 0)
    %Overwatch{state | timers: Timer.arm(state.timers, :wake, delay, :wake_deadline)}
  end

  # Cancel the recorded wake timer, if one is armed.
  @spec cancel_wake_timer(Overwatch.t()) :: Overwatch.t()
  def cancel_wake_timer(%Overwatch{} = state),
    do: %Overwatch{state | timers: Timer.cancel(state.timers, :wake)}
end

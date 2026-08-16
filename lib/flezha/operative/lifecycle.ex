defmodule Flezha.Operative.Lifecycle do
  @moduledoc """
  The operative lifecycle state machine: unbound → binding → bound → suspended →
  rehydrating → bound, and back out through teardown.

  Bind, suspension and the un-park task are one module because they are one
  cycle, not three concerns: a bind cancels the wake timer and delivers the
  inbox tail a suspension left behind, a suspension rehydrates through the
  migrate task, and the migrate task re-enters `complete_init/4`. Cutting
  between them produces a circular dependency whichever way it is cut, so the
  cut is drawn around them instead. `Flezha.Operative.Handover` is a genuine
  leaf of this and does live apart.

  Every function here runs in the `Flezha.Operative.Overwatch` process and takes
  its state, returning the next state (or a GenServer reply tuple for the phases
  a callback delegates wholesale). The Overwatch owns the struct, its accessor
  and its callbacks; this owns what happens between them.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Plugins
  alias Flezha.Operative.Replay
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.StreamSup
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.Payloader
  alias Flezha.State.Commands
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.ProcInbox
  alias Flezha.State.ProcJournal

  require Logger

  # Defaults for the bounded-retry governor. Tuned to ~19 s aggregate so a
  # transient control-channel rebind (raspberrypi5 cold-start hits ~5–10 s)
  # recovers within one burst without spinning the supervisor.
  @default_retry_backoff_ms [200, 500, 1_000, 2_000, 5_000, 10_000]

  # ---------------------------------------------------------------
  # Process API — the lifecycle messages the Overwatch answers to
  # ---------------------------------------------------------------

  @doc """
  Suspend a workflow instance parked at the committed boundary of iteration
  `iter`: evict it from its runner (`RemOperative`), release the registry's
  runner binding, and arm the journaled wake deadline. The registry entry —
  and the Overwatch process — stay, so a new input still has somewhere to land;
  it is the **runner** that holds zero resources for a suspended operative.

  Called by `ControlChannel` after the `parked` journal entry is durable.
  """
  @spec suspend(pid(), non_neg_integer(), non_neg_integer() | nil) :: :ok
  def suspend(overwatch_pid, iter, wake_deadline_ms) when is_pid(overwatch_pid),
    do: GenServer.cast(overwatch_pid, {:suspend, iter, wake_deadline_ms})

  @doc """
  Rewind a workflow loop to the committed boundary of iteration `to_iter`: journal
  the rewind — which bumps the epoch and un-commits every iteration above it —
  then evict the instance from its runner and rehydrate it.

  The rehydrating bind ships the folded journal back, so the driver undoes the
  unwound procedures through the ordinary compensation planner and resumes the
  loop at `to_iter + 1`, with the inbox items those iterations consumed
  undelivered again. The marker is durable before the eviction, so a crash
  mid-rewind resumes as a rewind rather than as a partially unwound loop.

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
  Resolve a live runner for the operative's factory and bring it up on that
  runner. Returns the `handle_continue/2` reply the bind continue answers with.
  """
  @spec bind(Overwatch.t(), binary() | nil, map()) ::
          {:noreply, Overwatch.t()} | {:stop, :runner_not_ready, Overwatch.t()}
  def bind(%Overwatch{} = state, stale_runner_id, operative) do
    # Wait for an active runner in this namespace advertising
    # `operative.factory_id`. The DETS-baked runner_id is usually stale
    # after a restart (Identificator.unique_id/1 generates a fresh id
    # per connect), so we re-resolve via OperativeManager.await_runner/5
    # — the same bounded lookup HubManager.alter_child_data/2 performs.
    case Overwatch.with_proc_span(:await_runner, operative.id, fn ->
           OperativeManager.await_runner(
             state.namespace,
             operative.factory_id,
             operative.id,
             runner_wait_ms(),
             &runner_ready?/1
           )
         end) do
      {:ok, runner_id} ->
        case complete_init(state, runner_id, operative) do
          # `complete_init` re-resolves the runner on every start attempt, so the
          # binding to publish is the one it ended on, not the one awaited.
          {:ok, new_state} ->
            {:noreply, publish_rebind(new_state, stale_runner_id)}

          {:error, reason, parked} ->
            {:noreply, park(parked, reason)}
        end

      # `:unavailable` (FleetManager never answered) defers exactly like
      # `:timeout` — the operative has no runner either way — but must be
      # distinguishable in the log, since only one of the two blames the fleet.
      {:error, reason} ->
        Logger.warning(
          "[Overwatch] no active runner in namespace #{inspect(state.namespace)} for factory #{inspect(operative.factory_id)} within #{runner_wait_ms()} ms (#{reason}); deferring operative #{operative.id} startup"
        )

        {:stop, :runner_not_ready, state}
    end
  end

  defp publish_rebind(%Overwatch{runner_id: runner_id} = state, runner_id), do: state

  # Binding to a runner other than the recorded one leaves the durable row naming
  # the old one and the live runner's StreamChannels with no route to this pid, so
  # every output frame is dropped while the operative still reads as healthy.
  defp publish_rebind(%Overwatch{} = state, stale_runner_id) do
    Logger.info(
      "[Overwatch] operative #{state.id} rebound from stale runner " <>
        "#{inspect(stale_runner_id)} to live #{inspect(state.runner_id)}"
    )

    publish_binding(state)
  end

  # Publish the binding: registry tag + the live runner's `operative_mapping`
  # route. Same pair `Convergence.Heal.rebind_operative/3` publishes.
  #
  # Must run in the Overwatch process, because the route points at `self()`. Both
  # halves read the bound state, so neither can name a runner this process is not
  # on.
  @spec publish_binding(Overwatch.t()) :: Overwatch.t()
  defp publish_binding(%Overwatch{id: operative_id, runner_id: runner_id} = state) do
    case HubManager.operative_set_runner(state.namespace, operative_id, runner_id) do
      :ok ->
        :ok

      err ->
        Logger.warning(
          "[Overwatch] could not publish binding of #{operative_id} to #{inspect(runner_id)}: " <>
            "#{inspect(err)}"
        )
    end

    _ = HubManager.hook_insert_local_pid(runner_id, operative_id, self())

    state
  end

  # Bring the operative up on `runner_id`: prepare plugins, resolve init state,
  # start the SDK operative under the bounded retry ladder, apply the class's
  # declarations, restore durable state, activate plugins, and deliver whatever
  # the durable inbox still owes. Both ways in — a fresh bind and an un-park —
  # funnel through here, which is what keeps them one path.
  #
  # `owner` is the Overwatch process itself. It is passed explicitly because the
  # un-park path runs this inside a task (so the heavy bounded-retry never blocks
  # `:stop_operative`), and `self()` there is the TASK — registering it as the
  # operative's Overwatch would point the journal's outbox flush at a process that
  # exits moments later, silently dropping the output.
  @spec complete_init(Overwatch.t(), binary(), map(), pid() | nil) ::
          {:ok, Overwatch.t()} | {:error, term(), Overwatch.t()}
  defp complete_init(%Overwatch{} = state, runner_id, operative, owner \\ nil) do
    owner = owner || self()
    state = bind_to_runner(state, operative.id, runner_id)
    plugins = Keyword.get(operative.opts, :plugins, [])

    with {:ok, state} <- Plugins.prepare(state, plugins),
         {:ok, state, operative} <- Plugins.resolve_init_state(state, operative),
         operative =
           with_resume_state(operative, state.builder.kind, Overwatch.operative_class(state)),
         {:ok, final_runner_id, add_operative_raw, state} <-
           start_operative_sdk_with_retry(state, operative) do
      # The class's SDK-side declarations ride the AddOperative response; resolve
      # them BEFORE plugin activation so source adapters pick the tier ingest mode
      # (blocking call) from the env.
      %{effectively_once: effectively_once, correction_window_ms: correction_window_ms} =
        declarations = ClassRegistry.decode_declarations(add_operative_raw)

      state =
        %{state | effectively_once: effectively_once, correction_window_ms: correction_window_ms}
        |> bind_to_runner(operative.id, final_runner_id)
        |> Map.put(:init_error, nil)

      # Tier: restore every persisted Entity (awaited) and re-forward the
      # journaled input tail BEFORE sources activate, so replayed offsets
      # reach the runner ahead of fresh ones (fold order at the SDK).
      state = if effectively_once, do: Replay.restore_and_replay_journal(state), else: state

      state = Plugins.activate(state)

      # Record this class's kind so the State subsystem can gate stream-only
      # behaviours (retention/watchdog/dynamic-evict/time-travel) for a durable
      # workflow operative — plus every SDK declaration and this pid (the
      # fsync-gated outbox flush dispatches `:on_output` through it).
      operative_class = Overwatch.operative_class(state)
      _ = ClassRegistry.put(operative_class, state.builder.kind)
      _ = ClassRegistry.put_declarations(operative_class, declarations)
      # Partitionability is a class-code fact, so it is recorded against the
      # factory: a later `--partitions N` start of a known-keyless class is
      # then refused before a runner is touched.
      _ = ClassRegistry.put_keyless(operative.factory_id, declarations.keyless)
      _ = ClassRegistry.put_overwatch(operative_class, owner)

      # The declaration is durable now, so the retention floors it needs resolve
      # from it. Announce any it had to raise — a one-line declaration must not
      # commit the controller to disk silently.
      if correction_window_ms do
        ClassRegistry.announce_derived_retention(operative_class, correction_window_ms)
      end

      _ = Replay.restore_on_bind(state, operative)

      # A bind is also a rehydrate: whatever the durable inbox still owes is
      # re-forwarded from the recorded consumed offset, so input that arrived
      # while the instance was suspended, crashed, or migrating is delivered
      # exactly here — one path for every way an instance comes back.
      state = Replay.deliver_inbox_tail(cancel_wake_timer(%{state | suspended: false}))
      {:ok, state}
    else
      {:error, reason, %Overwatch{} = parked} -> {:error, reason, parked}
      {:error, reason} -> {:error, reason, state}
    end
  end

  @doc """
  Point the state at `runner_id` and the stream channel that serves it.
  """
  @spec bind_to_runner(Overwatch.t(), binary(), binary()) :: Overwatch.t()
  def bind_to_runner(%Overwatch{} = state, operative_id, runner_id) do
    %Overwatch{
      state
      | runner_id: runner_id,
        channel: attach_channel(operative_id, runner_id)
    }
  end

  # Stamp everything a resuming bind carries onto the operative's opts so
  # `control_unit` can set the `AddOperative` fields: how many Entity restores
  # to wait for, and the proc-journal the driver resumes its iteration from. Only
  # a workflow instance has a journal, and the kind check is what keeps an
  # stream bind from reading for a file that cannot exist.
  defp with_resume_state(operative, kind, operative_class) do
    opts =
      operative.opts
      |> Keyword.put(:resume_entity_count, Replay.resume_entity_count(kind, operative_class))
      |> Keyword.put(
        :proc_journal,
        if(kind == :workflow, do: Commands.proc_journal_state(operative_class))
      )

    %{operative | opts: opts}
  end

  # Returns `{:ok, runner_id, add_operative_raw_data, state}` on success or `{:error, reason,
  # state}` on exhaustion. On every attempt the runner is re-resolved
  # so a fresh runner that came online during a backoff window is
  # picked up.
  defp start_operative_sdk_with_retry(%Overwatch{} = state, operative) do
    do_start_with_retry(state, operative, retry_backoff_ms(), 1)
  end

  defp do_start_with_retry(state, _operative, [], _attempt) do
    {:error, :exhausted, state}
  end

  defp do_start_with_retry(%Overwatch{} = state, operative, [delay | rest], attempt) do
    runner_id =
      resolve_runner(state.namespace, operative.factory_id, operative.id, state.runner_id)

    state = %Overwatch{state | runner_id: runner_id}

    case attempt_ping_and_start(operative, runner_id) do
      {:ok, raw_data} ->
        {:ok, runner_id, raw_data, state}

      {:error, reason} when rest == [] ->
        {:error, reason, state}

      {:error, reason} ->
        Logger.warning(
          "[Overwatch] operative #{operative.id} startup attempt #{attempt} failed: " <>
            "#{inspect(reason)} — retrying within #{delay} ms"
        )

        wait_with_jitter(delay)
        do_start_with_retry(state, operative, rest, attempt + 1)
    end
  end

  # On ping failure, reset the underlying `ControlChannel`'s `stream_ref`
  # so the SDK's next data stream can claim it via the existing rebind
  # path (`runner-controller-connection-healing`). `:start_operative` is
  # only issued AFTER the ping confirms the round-trip works.
  @doc false
  def attempt_ping_and_start(operative, runner_id) do
    case ping_runner(runner_id) do
      :ok ->
        case start_operative_sdk(runner_id, operative) do
          {:ok, _} = ok -> ok
          {:error, reason} -> {:error, {:start_operative_failed, reason}}
        end

      {:error, reason} ->
        _ = reset_control_channel(runner_id)
        {:error, {:ping_failed, reason}}
    end
  end

  # `:overwatch_ping_fn` / `:overwatch_reset_channel_fn` are test-only
  # function-injection hooks (Mimic cannot stub across the dist_sup-spawned
  # Overwatch process boundary from a test pid).
  defp ping_runner(runner_id) do
    cond do
      not Application.get_env(:flezha, :overwatch_ping_enabled, true) ->
        :ok

      fun = test_hook(:overwatch_ping_fn) ->
        normalize_ping(fun.(runner_id))

      true ->
        normalize_ping(ControlUnit.ping_runner(runner_id))
    end
  end

  defp normalize_ping({:ok, :pong}), do: :ok
  defp normalize_ping({:error, _} = err), do: err

  defp reset_control_channel(runner_id) do
    case test_hook(:overwatch_reset_channel_fn) do
      nil -> FleetManager.reset_control_channel(runner_id)
      fun -> fun.(runner_id)
    end
  end

  defp test_hook(key) do
    case Application.get_env(:flezha, key) do
      fun when is_function(fun, 1) -> fun
      _ -> nil
    end
  end

  defp resolve_runner(namespace, factory_id, operative_id, fallback) do
    case OperativeManager.assign_runner(namespace, factory_id, operative_id) do
      id when is_binary(id) and byte_size(id) > 0 -> id
      _ -> fallback
    end
  end

  defp retry_backoff_ms,
    do: Application.get_env(:flezha, :overwatch_retry_backoff_ms, @default_retry_backoff_ms)

  # Idle the GenServer for `1..delay` ms. The empty receive consumes no
  # mailbox messages — it only fires the `after` branch on timeout, so
  # casts and infos arriving during the wait stay queued for the next
  # dispatch. Equivalent to `Process.sleep/1` minus the dedicated
  # sleep primitive.
  defp wait_with_jitter(0), do: :ok

  defp wait_with_jitter(delay) when is_integer(delay) and delay > 0 do
    receive do
    after
      :rand.uniform(delay) -> :ok
    end
  end

  # Transition the Overwatch into the parked-error state: store the failure
  # reason and stay alive (no `:stop`, no supervisor restart).
  #
  # Plugins remain in `:prepared` so no upstream I/O fires while parked. The
  # reconciler drives the event-driven un-park (`runner_migrate`) once a healthy
  # runner for the operative's factory is available.
  @spec park(Overwatch.t(), term()) :: Overwatch.t()
  defp park(%Overwatch{} = state, reason) do
    Map.put(state, :init_error, reason)
  end

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
      init_values: init_values,
      opts: Keyword.take(state.builder.opts, Flezha.Partition.opt_keys())
    }
  end

  defp runner_wait_ms,
    do: Application.get_env(:flezha, :overwatch_runner_wait_ms, 30_000)

  # The runner's control unit GenServer registers itself in
  # `Flezha.Service.Identificator` as the first child of its supervision
  # tree (`RunnerSupervisor.init/1` → `ControlUnit.start_link/1`).
  # Registration alone is NOT sufficient: a SIGKILL'd runner can leave
  # the cu registered as a zombie (the `{:flz_closed}` disconnect never
  # arrived), so we additionally require a fresh end-to-end ping when
  # `:overwatch_ping_enabled` is on. Skipped in the unit suites that drive
  # synthetic runners with no real SDK behind the cu.
  defp runner_ready?(runner_id) do
    if OperativeManager.runner_registered?(runner_id) do
      if Application.get_env(:flezha, :overwatch_ping_enabled, true),
        do: ping_runner(runner_id) == :ok,
        else: true
    else
      false
    end
  end

  # ---------------------------------------------------------------
  # Suspension (`workflow-durable-suspension`)
  # ---------------------------------------------------------------

  # Refuse a rewind before it changes anything: an uncommitted target, or an
  # unwound procedure whose plugin output cannot be undone.
  @spec rewind_guard(Overwatch.t(), non_neg_integer()) :: :ok | {:error, term()}
  defp rewind_guard(%Overwatch{} = state, to_iter) do
    committed = ProcJournal.committed_iterations(state.id)

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
  # The only undo the engine can guarantee is the procedure's own `compensate`
  # hook — so an unwound procedure that emitted and declares none blocks the
  # rewind, naming the channels it wrote. A procedure that emitted nothing needs
  # no coverage.
  defp uncovered_channels(%Overwatch{} = state, to_iter) do
    compensable = ClassRegistry.compensable_procs(Overwatch.operative_class(state)) || []

    for {proc_idx, channel} <- ProcJournal.staged_channels_above(state.id, to_iter),
        proc_idx not in compensable,
        uniq: true,
        do: channel
  end

  @doc """
  Bind state for a suspended instance: no runner, no stream channel, the
  wake timer armed from the journaled deadline.
  """
  @spec enter_suspended(Overwatch.t(), map() | nil) :: Overwatch.t()
  def enter_suspended(%Overwatch{} = state, journal) do
    :telemetry.execute(
      [:flezha, :workflow, :suspended],
      %{count: 1},
      %{
        namespace: state.namespace,
        operative_id: state.id,
        iter: (journal && journal.iter) || 0
      }
    )

    # A committed iteration boundary is the migration-safe barrier.
    signal_migration_ready(state)
    evict_runner(state, journal && journal.wake_deadline_ms)
  end

  # Release every runner-side resource but keep the process and the registry
  # entry, so a new input still has somewhere to land. Shared by suspension (a
  # park, with its wake timer) and rewind (a rebind, with none).
  @spec evict_runner(Overwatch.t(), non_neg_integer() | nil) :: Overwatch.t()
  defp evict_runner(%Overwatch{} = state, deadline) do
    %Overwatch{
      cancel_wake_timer(state)
      | suspended: true,
        runner_id: nil,
        channel: nil,
        wake_timer: arm_wake_timer(deadline)
    }
  end

  # The durable timer: a `parked` record's wake deadline, re-armed from the
  # journal on every boot, so it survives a controller redeploy.
  defp arm_wake_timer(nil), do: nil

  defp arm_wake_timer(deadline_ms) do
    Process.send_after(
      self(),
      :wake_deadline,
      max(deadline_ms - System.system_time(:millisecond), 0)
    )
  end

  # Drop the operative from its runner and release the registry's runner tag —
  # the half a suspension and a rewind both perform before they diverge.
  defp release_runner(%Overwatch{} = state) do
    if state.runner_id do
      ControlUnit.stop_operative_async(state.runner_id, state.id)
    end

    _ = HubManager.operative_set_runner(state.namespace, state.id, nil)
    :ok
  end

  # Cancel the journaled wake timer, if one is armed.
  @spec cancel_wake_timer(Overwatch.t()) :: Overwatch.t()
  defp cancel_wake_timer(%Overwatch{wake_timer: nil} = state), do: state

  defp cancel_wake_timer(%Overwatch{wake_timer: ref} = state) do
    _ = Process.cancel_timer(ref)
    %Overwatch{state | wake_timer: nil}
  end

  # The invariant that keeps a turn from being stranded: an instance with
  # undelivered input is never left suspended. Applied wherever suspension is
  # entered — at the park, and at boot from durable state.
  @spec rehydrate_if_pending(Overwatch.t()) :: Overwatch.t()
  defp rehydrate_if_pending(%Overwatch{} = state) do
    if ProcInbox.undelivered(state.id) == [], do: state, else: rehydrate(state)
  end

  @doc """
  Rehydrate a suspended instance through the normal bind path (the same
  `complete_init/4` an un-park drives), which restores journal + snapshots and
  re-delivers the undelivered inbox tail.

  Idempotent: an instance already running, or with a rehydrate already in
  flight, no-ops — so two wake sources racing (an inbox append and the wake
  deadline) rehydrate exactly once.
  """
  @spec rehydrate(Overwatch.t()) :: Overwatch.t()
  def rehydrate(%Overwatch{suspended: false} = state), do: state
  def rehydrate(%Overwatch{migrate_task: task} = state) when task != nil, do: state

  def rehydrate(%Overwatch{} = state) do
    case OperativeManager.assign_runner(state.namespace, state.factory_id, state.id) do
      nil ->
        # No runner to rehydrate onto: stay suspended. The next input (or the
        # convergence tick that brings a runner up) tries again.
        Logger.warning(
          "[Overwatch] no active runner to rehydrate suspended operative #{state.id}"
        )

        state

      runner_id ->
        :telemetry.execute(
          [:flezha, :workflow, :rehydrating],
          %{count: 1},
          %{namespace: state.namespace, operative_id: state.id, runner_id: runner_id}
        )

        {ref, pid} = spawn_migrate_task({:unpark, state, runner_id})
        %Overwatch{state | migrate_task: %{ref: ref, pid: pid, mode: :unpark}}
    end
  end

  @doc """
  A durable workflow class: the kind that owns a proc-journal and an inbox. An
  opted-out class keeps the memory-only inbox and re-runs from the top.
  """
  @spec durable_workflow?(Overwatch.t(), map()) :: boolean()
  def durable_workflow?(%Overwatch{} = state, operative) do
    state.builder.kind == :workflow and
      PersistenceResolver.persistent_state?(
        :workflow,
        Keyword.get(operative.opts, :operative_class, operative.id)
      )
  end

  @doc """
  Suspended per durable state alone: the journal's last entry is a `parked`
  record AND no input has arrived since. Undelivered input outranks the park —
  the instance has work waiting, so it binds instead.
  """
  @spec suspended_on_disk?(binary()) :: boolean()
  def suspended_on_disk?(instance_id) do
    case ProcJournal.restore_state(instance_id) do
      %{parked: true} -> ProcInbox.undelivered(instance_id) == []
      _ -> false
    end
  end

  @doc """
  Tell a deferred migration it may retry now. It re-queries consent on its own
  tick; this only makes the move prompt. Any failure — hub down mid-shutdown,
  nothing deferred — is a no-op.
  """
  @spec signal_migration_ready(Overwatch.t()) :: :ok
  def signal_migration_ready(%Overwatch{} = state) do
    _ =
      ProcessHub.Service.Migration.migration_ready(
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
  The synchronous `:stop_operative` sequence: terminate any in-flight un-park
  task BEFORE the stop so `flz operative stop` always returns within its normal
  call window even mid-retry, tell the runner to drop the operative, fire
  `:on_terminate`, and tear the plugin table down. Answers the state to stop with.
  """
  @spec stop(Overwatch.t()) :: Overwatch.t()
  def stop(%Overwatch{} = state) do
    state = terminate_migrate_task(state)

    if state.runner_id do
      Overwatch.with_proc_span(:stop_operative, state.id, fn ->
        GenServer.call(
          Identificator.control_unit(state.runner_id),
          {:stop_operative, state.id}
        )
      end)
    end

    Plugins.fire_on_terminate(state, :stopped)
    %{state | runner_id: nil, extensions: Table.teardown_all(state.extensions)}
  end

  @doc """
  Process teardown. Evicts the in-memory class entry; the durable sidecar stays
  for the process-less retention sweep over any retained finished-job state.
  """
  @spec terminate(Overwatch.t(), term()) :: :ok
  def terminate(%Overwatch{} = state, reason) do
    _ = ClassRegistry.delete(Overwatch.operative_class(state))

    unless is_nil(state.runner_id) do
      ControlUnit.stop_operative_async(state.runner_id, state.id)
      Plugins.fire_on_terminate(state, reason)
      _ = Table.teardown_all(state.extensions)
    end

    Logger.info(
      "Terminating operative #{state.id} in namespace #{state.namespace}: #{inspect(reason)}"
    )

    :ok
  end

  @doc """
  The SDK parked at a committed iteration boundary and the `parked` journal
  entry is durable: evict the instance from its runner and keep it suspended.
  Not a terminal outcome — no completion record, no state deletion.
  """
  @spec suspend_at(Overwatch.t(), non_neg_integer(), non_neg_integer() | nil) :: Overwatch.t()
  def suspend_at(%Overwatch{} = state, iter, wake_deadline_ms) do
    release_runner(state)

    Logger.info(
      "[Overwatch] operative #{state.id} suspended at iteration #{iter}" <>
        if(wake_deadline_ms, do: " (wakes at #{wake_deadline_ms})", else: "")
    )

    # An input that landed while the loop was parking — durable, but never
    # drained by the exiting driver — is undelivered work: rehydrate straight
    # away rather than leaving the instance suspended until the NEXT message
    # arrives. The park/rehydrate cycle costs a bind; stranding the turn would
    # cost correctness. `RemOperative` is already enqueued on the control
    # stream, so the rehydrating `AddOperative` cannot overtake it.
    state
    |> enter_suspended(%{iter: iter, wake_deadline_ms: wake_deadline_ms, parked: true})
    |> rehydrate_if_pending()
  end

  @doc """
  Journal the rewind marker, evict, and rehydrate. A rewind is a rebind, not a
  park: no `parked` entry, no wake timer, and the rehydrate is unconditional —
  the loop must come back up to undo the iterations the marker unwound even with
  no input waiting on it.
  """
  @spec rewind_to(Overwatch.t(), non_neg_integer()) :: {:ok, Overwatch.t()} | {:error, term()}
  def rewind_to(%Overwatch{} = state, to_iter) do
    with :ok <- rewind_guard(state, to_iter),
         :ok <- ProcJournal.append_rewound(state.id, to_iter) do
      release_runner(state)

      :telemetry.execute(
        [:flezha, :workflow, :rewind],
        %{count: 1, epoch: ProcJournal.current_epoch(state.id)},
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

  # Terminate any in-flight un-park task so `:stop_operative` is never blocked by
  # the bounded-retry ladder running inside it.
  @spec terminate_migrate_task(Overwatch.t()) :: Overwatch.t()
  defp terminate_migrate_task(%Overwatch{migrate_task: nil} = state), do: state

  defp terminate_migrate_task(%Overwatch{migrate_task: %{ref: ref, pid: pid}} = state) do
    _ = Process.demonitor(ref, [:flush])
    _ = Task.Supervisor.terminate_child(Flezha.Operative.MessageHandlerTaskSup, pid)
    %{state | migrate_task: nil}
  end

  @doc """
  Run `complete_init/4` in a task so the heavy bounded-retry path never blocks
  `:stop_operative`. Returns the task's `{ref, pid}`.
  """
  @spec spawn_migrate_task({:unpark, Overwatch.t(), binary()}) :: {reference(), pid()}
  def spawn_migrate_task({:unpark, state, new_runner_id}) do
    operative = state.builder
    parent = self()

    task =
      Task.Supervisor.async_nolink(Flezha.Operative.MessageHandlerTaskSup, fn ->
        case complete_init(state, new_runner_id, operative, parent) do
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
  def apply_migrate_result(state, {:unparked, recovered}) do
    Logger.info("[Overwatch] operative #{state.id} un-parked onto runner #{recovered.runner_id}")

    publish_binding(recovered)

    %{recovered | migrate_task: nil}
  end

  def apply_migrate_result(_state, {:park, parked, err}) do
    %{park(parked, err) | migrate_task: nil}
  end

  # ---------------------------------------------------------------
  # SDK round-trips
  # ---------------------------------------------------------------

  defp start_operative_sdk(runner_id, operative) do
    Overwatch.with_proc_span(:start_operative, operative.id, fn ->
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_operative, operative},
        # 8s wasn't enough when the SDK is on the Java side — Spring/JPA
        # warmup plus the AddOperative control-channel round-trip easily
        # exceeds that during a cold start. 30s gives Spring time to breathe
        # without masking genuine stalls.
        30_000
      )
    end)
  end

  defp attach_channel(operative_id, runner_id) do
    operative_int = String.to_charlist(operative_id) |> Enum.sum()
    count = ConfigManager.concurrent_channel_count()
    channel_index = rem(operative_int, count)

    StreamSup.stream_ids(runner_id, :primary) |> Enum.at(channel_index)
  end
end

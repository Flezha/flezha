defmodule Flezha.Operator.Lifecycle do
  @moduledoc """
  The operator bind state machine: unbound → binding → bound, or parked.

  A bind is ONE attempt: a failure parks the operator with the attempt's
  reason, and the park is declared to the convergence loop, which drives the
  un-park on its own table. Bind and the SDK start are one module because they
  are one cycle: a bind cancels the wake timer a suspension left behind and
  re-forwards its input log's tail, and the bind task every move runs re-enters
  `complete_init/5`.

  Everything that takes an operator OFF its runner — suspend, rehydrate,
  rewind, terminate, migrate — is `Flezha.Operator.Migration`, a sibling above
  this module in the call graph.

  Invariant: every function runs in the `Overwatch` process and takes its state,
  returning the next state or a GenServer reply tuple.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Identificator
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Operator.Overwatch
  alias Flezha.Operator.Plugins
  alias Flezha.Operator.Replay
  alias Flezha.Process.Timer
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Commands
  alias Flezha.State.DurabilityTier
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Restore

  require Logger

  # ---------------------------------------------------------------
  # Process API — the lifecycle messages the Overwatch answers to
  # ---------------------------------------------------------------

  @doc """
  Resolve a live runner for the operator's factory and bring it up on that
  runner. Returns the `handle_continue/2` reply the bind continue answers with.
  """
  @spec bind(Overwatch.t(), binary() | nil, map()) :: {:noreply, Overwatch.t()}
  def bind(%Overwatch{} = state, stale_runner_id, operator) do
    # Wait for an active runner in this namespace advertising
    # `operator.factory_id`. The DETS-baked runner_id is usually stale
    # after a restart (Identificator.unique_id/1 generates a fresh id
    # per connect), so we re-resolve via OperatorManager.await_runner/5
    # — the same bounded lookup HubManager.alter_child_data/2 performs.
    case Overwatch.with_task_span(:await_runner, operator.id, fn ->
           OperatorManager.await_runner(
             state.namespace,
             operator.factory_id,
             operator.id,
             runner_wait_ms(),
             &runner_ready?/1
           )
         end) do
      {:ok, runner_id} ->
        case complete_init(state, runner_id, operator) do
          # `complete_init` re-resolves the runner before its one attempt, so
          # the binding to publish is the one it ended on, not the one awaited.
          {:ok, new_state} ->
            {:noreply, publish_rebind(new_state, stale_runner_id)}

          {:error, reason, parked} ->
            {:noreply, park(parked, reason)}
        end

      # `:unavailable` (FleetManager never answered) defers exactly like
      # `:timeout` — the operator has no runner either way — but must be
      # distinguishable in the log, since only one of the two blames the fleet.
      {:error, reason} ->
        Logger.warning(
          "[Overwatch] no active runner in namespace #{inspect(state.namespace)} for factory #{inspect(operator.factory_id)} within #{runner_wait_ms()} ms (#{reason}); holding operator #{operator.id} unbound"
        )

        {:noreply, hold_unbound(state, reason)}
    end
  end

  defp publish_rebind(%Overwatch{runner_id: runner_id} = state, runner_id), do: state

  # Binding to a runner other than the recorded one leaves the durable row naming
  # the old one and the live runner's StreamChannels with no route to this pid, so
  # every output frame is dropped while the operator still reads as healthy.
  defp publish_rebind(%Overwatch{} = state, stale_runner_id) do
    Logger.info(
      "[Overwatch] operator #{state.id} rebound from stale runner " <>
        "#{inspect(stale_runner_id)} to live #{inspect(state.runner_id)}"
    )

    publish_binding(state, stale_runner_id)
  end

  # Publish the binding: the registry tag, the bound session's route, and the
  # route dropped from `previous`, the session bound before, so a route exists
  # only in the channels of the session that owns the operator. The Overwatch
  # is the one publisher of both.
  #
  # Must run in the Overwatch process, because the route points at `self()`. Every
  # half reads the bound state, so none can name a runner this process is not on.
  @spec publish_binding(Overwatch.t(), binary() | nil) :: Overwatch.t()
  def publish_binding(%Overwatch{id: operator_id, runner_id: runner_id} = state, previous) do
    case HubManager.operator_set_runner(state.namespace, operator_id, runner_id) do
      :ok ->
        :ok

      err ->
        Logger.warning(
          "[Overwatch] could not publish binding of #{operator_id} to #{inspect(runner_id)}: " <>
            "#{inspect(err)}"
        )
    end

    if previous not in [nil, runner_id], do: StreamPool.drop_route(previous, operator_id)
    _ = StreamPool.put_route(runner_id, operator_id, self())

    state
  end

  @doc """
  Move the binding to `runner_id` and publish it: the ownership record, the
  registry tag and the route follow together, and the route leaves the session
  the operator was bound to before.
  """
  @spec commit_binding(Overwatch.t(), binary()) :: Overwatch.t()
  def commit_binding(%Overwatch{} = state, runner_id) do
    state
    |> bind_to_runner(state.id, runner_id)
    |> publish_binding(state.runner_id)
  end

  # Bring the operator up on `runner_id`: prepare plugins, resolve init state,
  # make the one SDK start attempt, apply the class's declarations, restore
  # durable state, re-forward whatever the input log still owes, and activate
  # plugins. Both ways in — a fresh bind and an un-park — funnel through here,
  # which is what keeps them one path.
  #
  # `owner` is the Overwatch process itself. It is passed explicitly because the
  # un-park path runs this inside a task (so a slow start never blocks the
  # Overwatch), and `self()` there is the TASK — registering it as the
  # operator's Overwatch would point the journal's outbox flush at a process that
  # exits moments later, silently dropping the output.
  #
  # `placement` is `:resolve` for a bind that awaited any runner, which asks
  # again just before the start, and `:pinned` for a move whose caller chose
  # `runner_id`.
  @spec complete_init(Overwatch.t(), binary(), map(), pid(), :resolve | :pinned) ::
          {:ok, Overwatch.t()} | {:error, term(), Overwatch.t()}
  def complete_init(
        %Overwatch{} = state,
        runner_id,
        operator,
        owner \\ self(),
        placement \\ :resolve
      ) do
    state = bind_to_runner(state, operator.id, runner_id, owner)

    with {:ok, state} <- Plugins.prepare(state, declared_plugins(state, operator)),
         {:ok, state, operator} <- Plugins.resolve_init_state(state, operator),
         # Before the resume state is read: a bounded operator resumes from the
         # journal, restore count and log tail it finds here.
         :ok <- Replay.adopt_foreign_state(operator.opts, Overwatch.operator_class(state)),
         operator =
           with_resume_state(operator, Overwatch.operator_class(state)),
         # Before the runner starts: its Cell writers learn their commit unit
         # from the kind, and a live one drops what the journal never named.
         :ok <- ClassRegistry.put(Overwatch.operator_class(state), state.builder.kind),
         :ok <- Replay.seed_bounded_outboxes(state),
         # A park landing after the runner starts the instance writes a record
         # that stays; a failed start leaves none for a boot to take for a sleep.
         :ok <- ClassRegistry.put_parked(Overwatch.operator_class(state), nil),
         {:ok, add_operator_raw, state} <- start_once(state, operator, owner, placement),
         # The class's SDK-side declarations ride the AddOperator response;
         # resolve them BEFORE plugin activation so source adapters pick the
         # tier ingest mode (blocking call) from the env.
         %{
           effectively_once: effectively_once,
           correction_window_ms: correction_window_ms,
           sleep_when_idle: sleeps
         } = declarations = ClassRegistry.decode_declarations(add_operator_raw),
         state =
           %{
             state
             | effectively_once: effectively_once,
               correction_window_ms: correction_window_ms,
               sleeps: sleeps
           }
           |> Map.put(:init_error, nil),
         :ok <- await_stream_bound(state),
         # BEFORE the restore: a refused restore parks the operator, so a
         # correction running after it would never run at all — the factory
         # would park every attempt at its wrong language forever.
         _ =
           Restore.establish_sdk_lang_from_state(
             state.namespace,
             operator.factory_id,
             Overwatch.operator_class(state)
           ),
         # The tier and a sleeping unbounded operator: restore every persisted Cell
         # (awaited) before the tail is re-forwarded onto them.
         {:ok, state} <- awaited_restore(state),
         # Once per bind, for either kind, BEFORE sources activate: the log's
         # tail above the floor reaches the runner ahead of fresh input (fold
         # order at the SDK), whether this bind is a fresh start, a crash
         # resume, a migration or a rehydrate.
         state = Replay.reforward_undelivered_tail(state) do
      record_and_replay(state, operator, declarations, correction_window_ms)
    else
      {:error, reason, %Overwatch{} = parked} -> {:error, reason, parked}
      {:error, reason} -> {:error, reason, state}
    end
  end

  # The operator's registry row, which every enable and disable rewrites, so a
  # restart of this process after a runtime enable (a listener) brings that
  # plugin up too; the start arguments when the row cannot be read yet.
  defp declared_plugins(%Overwatch{namespace: namespace, id: id}, operator) do
    case HubManager.operator_plugins(namespace, id) do
      {:ok, plugins} -> plugins
      {:error, _unreadable} -> Keyword.get(operator.opts, :plugins, [])
    end
  end

  # Everything a started operator owes once its state is restored and its tail
  # re-forwarded: its plugins, the class facts the State subsystem reads and
  # the retention floors the declaration raised.
  defp record_and_replay(state, operator, declarations, correction_window_ms) do
    # A plugin that fails to come up stays `:prepared`, which the convergence
    # loop diffs as missing and heals under backoff, so the bind does not fail
    # on it.
    {state, _not_running} = Plugins.bring_up(state)

    # Record this class's kind and every SDK declaration for the paths that
    # hold only the class string (the Overwatch pid the fsync-gated outbox
    # flush dispatches `:on_output` through was recorded before the SDK
    # started).
    operator_class = Overwatch.operator_class(state)
    _ = ClassRegistry.put_declarations(operator_class, declarations)
    # Partitionability is a class-code fact, so it is recorded against the
    # factory: a later `--partitions N` start of a known-keyless class, or of
    # a bounded operator that reads one message, is then refused before a runner is
    # touched.
    _ = ClassRegistry.put_partitioning(operator.factory_id, declarations)
    _ = ClassRegistry.put_state_keys(operator.id, operator_class)

    # The declaration is durable now, so the retention floors it needs resolve
    # from it. Announce any it had to raise — a one-line declaration must not
    # commit the controller to disk silently.
    if correction_window_ms do
      PersistenceResolver.announce_derived_retention(operator_class, correction_window_ms)
    end

    # This node runs the operator now, which is what entitles it to take over
    # the reference's remote lineage. It has to be said here rather than
    # inferred from shipping: a migration leaves the source node holding a
    # full copy of the state, so "has the files" would let the node that just
    # lost the operator claim it straight back.
    _ = DurabilityTier.adopt_operator(operator_class)

    _ = Replay.restore_on_bind(state, operator)

    {:ok, cancel_wake_timer(%{state | suspended: false})}
  end

  # A refused restore is not a cold start: the durable state never reached the
  # runner, so the operator parks rather than folding new inputs onto empty
  # state. Rides the existing parked-error contract, so `bind/3` needs no new
  # failure arm.
  defp awaited_restore(state) do
    with true <- Replay.awaited_restore?(state),
         {:error, reason} <- Replay.restore_cells_awaited(state) do
      {:error, {:restore_refused, reason}, state}
    else
      false -> {:ok, state}
      {:ok, state} -> {:ok, state}
    end
  end

  # An operator binds to a runner over the CONTROL channel; the data stream that
  # carries its input is a separate connection the SDK opens afterwards.
  # Everything below this point forwards on that stream — the input log's
  # re-forwarded tail, the plugins' first input, and every ingest
  # waiting in this process's mailbox — so the bind waits for it rather than
  # issuing onto a stream that would drop what it is given.
  #
  # No caller is parked by the wait. `start_operator/2` returned when this
  # process started; an ingest cast simply stays queued, which is the queue an
  # unbound stream needs and the reason no second one is built. A runner with no
  # channel pool at all has nothing to wait for and passes straight through.
  defp await_stream_bound(%Overwatch{} = state) do
    case Gate.await_bind(state.channel, stream_bind_wait_ms()) do
      :ok -> :ok
      {:error, :stream_unbound} -> {:error, {:stream_unbound, state.channel}}
    end
  end

  defp stream_bind_wait_ms,
    do: Durations.get(:stream_bind_wait_ms)

  @doc """
  Point the state at `runner_id` and the stream channel that serves it, and
  record `runner_id`'s stable identity as the operator's owner.

  Every bind runs this before the runner is told to start the operator: a
  fast chain's first output can reach the stream channel before the publish
  seeds its route, and the channel judges that miss by the record. `owner` is
  the Overwatch process, named because an un-park binds inside a task.
  """
  @spec bind_to_runner(Overwatch.t(), binary(), binary(), pid()) :: Overwatch.t()
  def bind_to_runner(%Overwatch{} = state, operator_id, runner_id, owner \\ self()) do
    _ =
      ClassRegistry.put_overwatch(
        Overwatch.operator_class(state),
        owner,
        Marks.identity_of(runner_id)
      )

    %Overwatch{
      state
      | runner_id: runner_id,
        channel: attach_channel(operator_id, runner_id)
    }
  end

  # Stamp everything a resuming bind carries onto the operator's opts so
  # `control_unit` can set the `AddOperator` fields: how many Cell restores
  # to wait for, the task-journal the run resumes its iteration from, and how
  # many inputs the bind's re-forward owes it, for any instance whose input is
  # journaled. A class without a journal or an input log reads `nil` and `0`.
  defp with_resume_state(operator, operator_class) do
    opts =
      operator.opts
      |> Keyword.put(:resume_cell_count, Replay.resume_cell_count(operator_class))
      |> Keyword.put(:task_journal, Commands.task_journal_state(operator_class))
      |> Keyword.put(:inbox_tail_count, Replay.owed_count(operator_class))

    %{operator | opts: opts}
  end

  # The one attempt. An awaited runner is re-resolved first so a runner that
  # came online since the bind was awaited is the one used, and bound before the
  # attempt so the ownership record names the runner told to start; the state
  # carries the runner the attempt was made on either way, so a park names it.
  defp start_once(%Overwatch{} = state, operator, owner, placement) do
    runner_id =
      case placement do
        :resolve ->
          resolve_runner(state.namespace, operator.factory_id, operator.id, state.runner_id)

        :pinned ->
          state.runner_id
      end

    state = bind_to_runner(state, operator.id, runner_id, owner)

    case attempt_ping_and_start(operator, runner_id) do
      {:ok, raw_data} -> {:ok, raw_data, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  # `:start_operator` is only issued AFTER the ping confirms the round trip
  # works. A ping failure parks the operator with its reason; whether the
  # runner's channel is then cut is the loop's decision, read off the reason
  # through `transport_verdict/1`.
  @doc false
  def attempt_ping_and_start(operator, runner_id) do
    case ping_runner(runner_id) do
      :ok ->
        case start_operator_sdk(runner_id, operator) do
          {:ok, _} = ok -> ok
          {:error, reason} -> {:error, {:start_operator_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:ping_failed, reason}}
    end
  end

  @doc """
  What a park's reason says about the runner's transport: `:dead` when the
  bind's ping reported the transport gone, `:busy` when it merely timed out,
  `nil` when the reason is not a ping failure at all. A busy runner shows up
  as a timeout in three shapes: the ping's own, the ControlUnit's call into a
  channel that is busy, and the ControlUnit having exited on such a call.
  None of them says the transport is dead.
  """
  @spec transport_verdict(term()) :: :dead | :busy | nil
  def transport_verdict({:ping_failed, reason}),
    do: if(timeout_class?(reason), do: :busy, else: :dead)

  def transport_verdict(_reason), do: nil

  defp timeout_class?(:timeout), do: true
  defp timeout_class?({:control_unit_exit, {{:timeout, _}, _}}), do: true
  defp timeout_class?({:control_unit_exit, {:timeout, _}}), do: true
  defp timeout_class?(_), do: false

  # `:overwatch_ping_fn` is a test-only function-injection hook (Mimic cannot
  # stub across the dist_sup-spawned Overwatch process boundary from a test
  # pid).
  defp ping_runner(runner_id) do
    cond do
      not Config.get(:overwatch_ping_enabled) ->
        :ok

      fun = test_hook(:overwatch_ping_fn) ->
        normalize_ping(fun.(runner_id))

      true ->
        normalize_ping(ControlUnit.ping_runner(runner_id))
    end
  end

  defp normalize_ping({:ok, :pong}), do: :ok
  defp normalize_ping({:error, _} = err), do: err

  defp test_hook(name) do
    case Config.get(name) do
      fun when is_function(fun, 1) -> fun
      _ -> nil
    end
  end

  defp resolve_runner(namespace, factory_id, operator_id, fallback) do
    case OperatorManager.assign_runner(namespace, factory_id, operator_id) do
      id when is_binary(id) and byte_size(id) > 0 -> id
      _ -> fallback
    end
  end

  # Transition the Overwatch into the parked-error state: store the failure
  # reason and stay alive (no `:stop`, no supervisor restart).
  #
  # Plugins remain in `:prepared` so no upstream I/O fires while parked. A park
  # is an outcome, not a remedy: it is declared to the convergence loop, which
  # drives the un-park (`runner_migrate`) on the `{:parked, id}` ladder of its
  # own table and cuts the runner's channel first when the reason says the
  # transport is dead.
  #
  # Every park funnels through here, so it is the one point that can report and
  # declare one. The reason rides the event whole: a plugin park's already names
  # both the plugin and its provider module.
  @spec park(Overwatch.t(), term()) :: Overwatch.t()
  def park(%Overwatch{} = state, reason) do
    log_missing_provider(state, reason)

    Facts.declare(
      {:operator_parked, {state.namespace, state.id},
       %{reason: reason, transport: transport_verdict(reason)}}
    )

    :telemetry.execute(
      [:flezha, :operator, :parked],
      %{count: 1},
      %{
        namespace: state.namespace,
        operator_id: state.id,
        reason: reason,
        plugin_id: parking_plugin(reason)
      }
    )

    Map.put(state, :init_error, reason)
  end

  # A provider this controller did not load is the park a user repairs on this
  # node, by installing or fixing a package, so its reason is logged; once, not
  # on every retry that parks again for the same reason.
  defp log_missing_provider(state, {:plugin_prepare_failed, plugin_id, {tag, _} = why} = reason)
       when tag == :module_not_loaded,
       do: log_park_once(state, reason, plugin_id, why)

  defp log_missing_provider(state, {:plugin_prepare_failed, plugin_id, why} = reason)
       when elem(why, 0) == :provider_unavailable,
       do: log_park_once(state, reason, plugin_id, why)

  defp log_missing_provider(_state, _reason), do: :ok

  defp log_park_once(state, reason, plugin_id, why) do
    if Map.get(state, :init_error) != reason do
      Logger.error(
        "operator #{state.id} parked: plugin #{plugin_id}: " <>
          Flezha.Error.from_reason(why, nil).message
      )
    end
  end

  # No runner has connected yet, which is not a fault: hold the operator alive
  # with the outcome on its state. Stopping here would put a supervisor restart
  # beside the convergence loop's own rebind ladder, and a runnerless namespace
  # would crash-loop every `overwatch_runner_wait_ms` for ever.
  #
  # Not `park/2`: a park declares an `{:operator_parked, …}` fact the Diff never
  # consumes for an operator it classifies unbound, and would report the
  # operator parked while every health surface calls it unbound.
  #
  # The recorded runner goes with it. A runner id baked into the cspec by a
  # previous boot is not a binding, and keeping one makes the judge call the
  # operator failed where the phase calls it unbound.
  defp hold_unbound(%Overwatch{} = state, reason),
    do: %{state | runner_id: nil, init_error: {:runner_not_ready, reason}}

  # A park a plugin caused names that plugin; every other reason carries `nil`
  # rather than claim a plugin id it does not have.
  defp parking_plugin({:plugin_prepare_failed, plugin_id, _reason}), do: plugin_id
  defp parking_plugin(_reason), do: nil

  defp runner_wait_ms,
    do: Durations.get(:overwatch_runner_wait_ms)

  # The runner's control unit GenServer registers itself in
  # `Flezha.Identificator` as the first child of its supervision
  # tree (`RunnerSupervisor.init/1` → `ControlUnit.start_link/1`).
  # Registration alone is NOT sufficient: a SIGKILL'd runner can leave
  # the cu registered as a zombie (the `{:flz_closed}` disconnect never
  # arrived), so we additionally require a fresh end-to-end ping when
  # `:overwatch_ping_enabled` is on. Skipped in the unit suites that drive
  # synthetic runners with no real SDK behind the cu.
  defp runner_ready?(runner_id) do
    if OperatorManager.runner_registered?(runner_id) do
      if Config.get(:overwatch_ping_enabled),
        do: ping_runner(runner_id) == :ok,
        else: true
    else
      false
    end
  end

  # ---------------------------------------------------------------
  # Suspension (`bounded-durable-suspension`)
  # ---------------------------------------------------------------

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

  defp start_operator_sdk(runner_id, operator) do
    Overwatch.with_task_span(:start_operator, operator.id, fn ->
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_operator, operator},
        # 8s wasn't enough when the SDK is on the Java side — Spring/JPA
        # warmup plus the AddOperator control-channel round-trip easily
        # exceeds that during a cold start. 30s gives Spring time to breathe
        # without masking genuine stalls.
        30_000
      )
    end)
  end

  defp attach_channel(operator_id, runner_id) do
    operator_int = String.to_charlist(operator_id) |> Enum.sum()
    count = Config.get(:concurrent_channel_count)
    channel_index = rem(operator_int, count)

    StreamPool.stream_ids(runner_id, :primary) |> Enum.at(channel_index)
  end
end

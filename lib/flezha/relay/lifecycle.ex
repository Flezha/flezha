defmodule Flezha.Relay.Lifecycle do
  @moduledoc """
  The relay bind state machine: unbound → binding → bound, or halted.

  A bind is ONE attempt: a failure halts the relay with the attempt's
  reason, and the halt is declared to the convergence loop, which drives the
  rebind on its own table. Bind and the SDK start are one module because they
  are one cycle: a bind cancels the wake timer a suspension left behind and
  re-forwards its input log's tail, and the bind task every move runs
  (`Flezha.Relay.Rebind`) re-enters `complete_init/5`. The process's end is
  here too: `terminate/2` walks the `:process` steps of the shutdown plan.

  A relay asleep, waking or rewinding is `Flezha.Relay.Suspend`.

  Invariant: every function runs in the `Overwatch` process and takes its state,
  returning the next state or a GenServer reply tuple.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Identificator
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Relay.Calls.Inbound
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Plugins
  alias Flezha.Relay.Rebind
  alias Flezha.Relay.Replay
  alias Flezha.Relay.Suspend
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Shutdown
  alias Flezha.State.Commands
  alias Flezha.State.DurabilityTier
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Restore

  require Logger

  @behaviour Flezha.Shutdown.Step

  @named_stops [:stop, :complete, :rescale, :rescale_leave, :failed_start]

  # ---------------------------------------------------------------
  # Process API — the lifecycle messages the Overwatch answers to
  # ---------------------------------------------------------------

  @doc "The Overwatch's bind continue: claim the class's state, then sleep or bind."
  @spec bind_continue(Overwatch.t(), binary() | nil, map()) ::
          {:noreply, Overwatch.t()}
          | {:noreply, Overwatch.t(), :hibernate}
          | {:stop, term(), Overwatch.t()}
  def bind_continue(%Overwatch{} = state, stale_runner_id, relay) do
    # Claim the class's durable-state namespace BEFORE any path below
    # (journal probe, restore, WAL open) resolves: writes must land under
    # this relay's namespace, and a class owned by another namespace
    # must never be silently read or overwritten.
    with :ok <- one_state_key(state),
         :ok <- Paths.claim_namespace(state.id, state.namespace) do
      continue_bind(stale_runner_id, relay, state)
    else
      {:error, {:namespace_conflict, _class, owner}} ->
        Logger.error(
          "[overwatch] refusing to bind #{state.id} in namespace #{state.namespace}: " <>
            "its durable state belongs to namespace #{owner}"
        )

        {:stop, {:shutdown, {:namespace_conflict, state.id, owner}}, state}

      {:error, {:split_state_key, class}} ->
        Logger.error(
          "[overwatch] refusing to bind steps #{state.id}: its state key is its " <>
            "instance id, not #{inspect(class)}"
        )

        {:stop, {:shutdown, {:split_state_key, state.id, class}}, state}
    end
  end

  # A steps relay's journal, Cells and input log are all keyed by its instance id,
  # so an `:relay_class` naming anything else would split them: it is
  # refused, not ignored.
  defp one_state_key(%Overwatch{builder: %{run: :steps}} = state) do
    case Overwatch.relay_class(state) do
      id when id == state.id -> :ok
      class -> {:error, {:split_state_key, class}}
    end
  end

  defp one_state_key(%Overwatch{}), do: :ok

  defp continue_bind(stale_runner_id, relay, %Overwatch{} = state) do
    class = Overwatch.relay_class(state)
    # A move brings the source's journal, input log and Cells over before
    # anything below reads them.
    relay = Replay.adopt_moved_state(state, relay)

    state = %Overwatch{state | builder: relay}

    state = %Overwatch{
      state
      | durable_steps: Suspend.durable_steps?(state, relay),
        sleeps: ClassRegistry.sleeps?(class)
    }

    # Sleeping instances are rediscovered from durable state alone (the park
    # record and the input log), so a controller redeploy neither binds them to
    # a runner nor loses their sleep (`durable-suspension`). An
    # instance with undelivered input is not asleep any more: it binds and
    # consumes it. The floor is seeded first, since the log answers that
    # question against it.
    :ok = Replay.seed_floor(state)

    if Suspend.suspended_on_disk?(class) do
      {:noreply, Suspend.enter_suspended(state, Suspend.sleep_record(class)), :hibernate}
    else
      bind(state, stale_runner_id, relay)
    end
    |> seed_input(relay)
  end

  # The input a start carries, taken in the bind continue or held for the
  # start's own `:registry_runner_set` call (`Ingest.seed_or_hold/2`).
  defp seed_input(result, relay),
    do: put_elem(result, 1, Ingest.seed_or_hold(elem(result, 1), relay))

  # Resolve a live runner for the relay's factory and bring it up on that runner.
  defp bind(%Overwatch{} = state, stale_runner_id, relay) do
    # Wait for an active runner in this namespace advertising
    # `relay.factory_id`. The DETS-baked runner_id is usually stale
    # after a restart (Identificator.unique_id/1 generates a fresh id
    # per connect), so we re-resolve via RelayManager.await_runner/5.
    case Overwatch.with_task_span(:await_runner, relay.id, fn ->
           RelayManager.await_runner(
             state.namespace,
             relay.factory_id,
             relay.id,
             runner_wait_ms(),
             &runner_ready?/1
           )
         end) do
      {:ok, runner_id} ->
        case complete_init(state, runner_id, relay) do
          # `complete_init` re-resolves the runner before its one attempt, so
          # the binding to publish is the one it ended on, not the one awaited.
          {:ok, new_state} ->
            {:noreply, publish_rebind(new_state, stale_runner_id)}

          {:error, reason, halted} ->
            {:noreply, halt(halted, reason)}
        end

      # `:unavailable` (FleetManager never answered) defers exactly like
      # `:timeout` — the relay has no runner either way — but must be
      # distinguishable in the log, since only one of the two blames the fleet.
      {:error, reason} ->
        Logger.warning(
          "[Overwatch] no active runner in namespace #{inspect(state.namespace)} for factory #{inspect(relay.factory_id)} within #{runner_wait_ms()} ms (#{reason}); holding relay #{relay.id} unbound"
        )

        {:noreply, hold_unbound(state, reason)}
    end
  end

  defp publish_rebind(%Overwatch{runner_id: runner_id} = state, runner_id), do: state

  # Binding to a runner other than the recorded one leaves the durable row naming
  # the old one and the live runner's StreamChannels with no route to this pid, so
  # every output frame is dropped while the relay still reads as healthy.
  defp publish_rebind(%Overwatch{} = state, stale_runner_id) do
    Logger.info(
      "[Overwatch] relay #{state.id} rebound from stale runner " <>
        "#{inspect(stale_runner_id)} to live #{inspect(state.runner_id)}"
    )

    publish_binding(state, stale_runner_id)
  end

  # Publish the binding: the registry tag, the bound session's route, and the
  # route dropped from `previous`, the session bound before, so a route exists
  # only in the channels of the session that owns the relay. The Overwatch
  # is the one publisher of both.
  #
  # Must run in the Overwatch process, because the route points at `self()`. Every
  # half reads the bound state, so none can name a runner this process is not on.
  @spec publish_binding(Overwatch.t(), binary() | nil) :: Overwatch.t()
  def publish_binding(%Overwatch{id: relay_id, runner_id: runner_id} = state, previous) do
    case HubManager.relay_set_runner(state.namespace, relay_id, runner_id) do
      :ok ->
        :ok

      err ->
        Logger.warning(
          "[Overwatch] could not publish binding of #{relay_id} to #{inspect(runner_id)}: " <>
            "#{inspect(err)}"
        )
    end

    if previous not in [nil, runner_id], do: StreamPool.drop_route(previous, relay_id)
    _ = StreamPool.put_route(runner_id, relay_id, self())

    state
  end

  @doc """
  Move the binding to `runner_id` and publish it: the ownership record, the
  registry tag and the route follow together, and the route leaves the session
  the relay was bound to before.
  """
  @spec commit_binding(Overwatch.t(), binary()) :: Overwatch.t()
  def commit_binding(%Overwatch{} = state, runner_id) do
    state
    |> bind_to_runner(state.id, runner_id)
    |> publish_binding(state.runner_id)
  end

  # Bring the relay up on `runner_id`: prepare plugins, resolve init state,
  # make the one SDK start attempt, apply the class's declarations, restore
  # durable state, re-forward whatever the input log still owes, and activate
  # plugins. Both ways in — a fresh bind and a rebind — funnel through here,
  # which is what keeps them one path.
  #
  # `owner` is the Overwatch process itself. It is passed explicitly because the
  # rebind path runs this inside a task (so a slow start never blocks the
  # Overwatch), and `self()` there is the TASK — registering it as the
  # relay's Overwatch would point the journal's outbox flush at a process that
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
        relay,
        owner \\ self(),
        placement \\ :resolve
      ) do
    state = bind_to_runner(state, relay.id, runner_id, owner)
    relay = Replay.adopt_unlanded_state(state, relay, owner)
    state = %{state | builder: %{state.builder | opts: relay.opts}}

    with {:ok, state} <- Plugins.prepare(state, declared_bindings(state, relay)),
         {:ok, state, relay} <- Plugins.resolve_init_state(state, relay),
         relay =
           with_resume_state(relay, Overwatch.relay_class(state), state.namespace),
         # Before the runner starts: its Cell writers learn their commit unit
         # from the run, and a live one drops what the journal never named.
         :ok <- ClassRegistry.put(Overwatch.relay_class(state), state.builder.run || :flow),
         :ok <- Replay.seed_steps_outboxes(state),
         # A park landing after the runner starts the instance writes a record
         # that stays; a failed start leaves none for a boot to take for a sleep.
         :ok <- Suspend.put_sleep(Overwatch.relay_class(state), nil),
         {:ok, add_relay_raw, state} <- start_once(state, relay, owner, placement),
         # The class's SDK-side declarations ride the AddRelay response;
         # resolve them BEFORE plugin activation so source adapters pick the
         # tier ingest mode (blocking call) from the env.
         declarations = declarations(add_relay_raw, state.namespace, relay.factory_id),
         :ok <- refuse_remote_without_backend(declarations, relay.factory_id),
         state =
           %{
             state
             | effectively_once: declarations.effectively_once,
               correction_window_ms: declarations.correction_window_ms,
               sleeps: declarations.sleep_after_ms != nil,
               settings:
                 Map.take(declarations, [:run, :each, :on_failure, :durable, :sleep_after_ms]),
               halt_reason: nil
           },
         :ok <- await_stream_bound(state),
         # Before anything re-runs: the kept effect results reach the runner
         # ahead of the restores and the input tail.
         :ok <- Replay.ship_effect_results(state, Keyword.get(relay.opts, :effect_results, [])),
         # BEFORE the restore: a refused restore halts the relay, so a
         # correction running after it would never run at all — the factory
         # would halt every attempt at its wrong language forever.
         _ =
           Restore.establish_sdk_lang_from_state(
             state.namespace,
             relay.factory_id,
             Overwatch.relay_class(state)
           ),
         # The tier and a sleeping flow relay: restore every persisted Cell
         # (awaited) before the tail is re-forwarded onto them.
         {:ok, state} <- awaited_restore(state),
         # Once per bind, for either kind, BEFORE sources activate: the log's
         # tail above the floor reaches the runner ahead of fresh input (fold
         # order at the SDK), whether this bind is a fresh start, a crash
         # resume, a migration or a rehydrate.
         state = Replay.reforward_undelivered_tail(state) do
      record_and_replay(state, relay, declarations)
    else
      {:error, reason, %Overwatch{} = copy} -> {:error, reason, Plugins.unprepare(copy, state)}
      {:error, reason} -> {:error, reason, state}
    end
  end

  # The run, loop shape and failure mode are read as placement read them, so
  # the bind runs what was placed.
  defp declarations(add_relay_raw, namespace, factory_id) do
    established = FleetManager.established(namespace, factory_id, [:run, :each, :on_failure])
    ClassRegistry.decode_declarations(add_relay_raw, established)
  end

  # `durable: remote` is a promise the machine keeps only with a durability
  # backend: where the class meets the controller, the start is refused.
  defp refuse_remote_without_backend(%{durable: :remote}, factory_id) do
    if DurabilityTier.enabled?(), do: :ok, else: {:error, {:remote_without_backend, factory_id}}
  end

  defp refuse_remote_without_backend(_declarations, _factory_id), do: :ok

  @doc """
  The relay's bindings as its registry row holds them, which every bind and
  unbind rewrites, so a restart of this process after a runtime bind (a
  listener) brings that binding up too; the start arguments when the row
  cannot be read yet.
  """
  @spec declared_bindings(Overwatch.t(), map()) :: list()
  def declared_bindings(%Overwatch{namespace: namespace, id: id}, relay) do
    case HubManager.relay_bindings(namespace, id) do
      {:ok, bindings} -> bindings
      {:error, _unreadable} -> Keyword.get(relay.opts, :bindings, [])
    end
  end

  # Everything a started relay owes once its state is restored and its tail
  # re-forwarded: its plugins, the class facts the State subsystem reads and
  # the retention floors the declaration raised.
  defp record_and_replay(state, relay, %{correction_window_ms: window} = declarations) do
    # A plugin that fails to come up stays `:prepared`, which the convergence
    # loop diffs as missing and heals under backoff, so the bind does not fail
    # on it.
    {state, _not_running} = Plugins.bring_up(state)

    # Record this class's kind and every SDK declaration for the paths that
    # hold only the class string (the Overwatch pid the fsync-gated outbox
    # flush dispatches `:on_output` through was recorded before the SDK
    # started).
    relay_class = Overwatch.relay_class(state)
    _ = ClassRegistry.put_declarations(relay_class, declarations)
    _ = RelayManager.stamp_window_end(state.namespace, relay, declarations.max_duration_ms)
    # Partitionability is a class-code fact, so it is recorded against the
    # factory: a later `--partitions N` start of a known-keyless class, or of
    # a steps relay that reads one message, is then refused before a runner is
    # touched.
    _ = ClassRegistry.put_partitioning(relay.factory_id, declarations)
    _ = ClassRegistry.put_state_keys(relay.id, relay_class)

    # The declaration is durable now, so the retention floors it needs resolve
    # from it. Announce any it had to raise — a one-line declaration must not
    # commit the controller to disk silently.
    if window do
      PersistenceResolver.announce_derived_retention(relay_class, window)
    end

    # This node runs the relay now, which is what entitles it to take over
    # the reference's remote lineage. It has to be said here rather than
    # inferred from shipping: a migration leaves the source node holding a
    # full copy of the state, so "has the files" would let the node that just
    # lost the relay claim it straight back.
    _ = DurabilityTier.adopt_relay(relay_class)

    _ = Replay.restore_on_bind(state, relay)

    {:ok, Suspend.cancel_wake_timer(%{state | phase: :running})}
  end

  # A refused restore is not a cold start: the durable state never reached the
  # runner, so the relay halts rather than folding new inputs onto empty
  # state. Rides the existing halted-error contract, so `bind/3` needs no new
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

  # A relay binds to a runner over the CONTROL channel; the data stream that
  # carries its input is a separate connection the SDK opens afterwards.
  # Everything below this point forwards on that stream — the input log's
  # re-forwarded tail, the plugins' first input, and every ingest
  # waiting in this process's mailbox — so the bind waits for it rather than
  # issuing onto a stream that would drop what it is given.
  #
  # No caller is parked by the wait. `start_relay/2` returned when this
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
  record `runner_id`'s stable identity as the relay's owner.

  Every bind runs this before the runner is told to start the relay: a
  fast chain's first output can reach the stream channel before the publish
  seeds its route, and the channel judges that miss by the record. `owner` is
  the Overwatch process, named because a rebind binds inside a task.
  """
  @spec bind_to_runner(Overwatch.t(), binary(), binary(), pid()) :: Overwatch.t()
  def bind_to_runner(%Overwatch{} = state, relay_id, runner_id, owner \\ self()) do
    :ok = Replay.redrive_outboxes(state, owner)

    _ =
      ClassRegistry.put_overwatch(
        Overwatch.relay_class(state),
        owner,
        Marks.identity_of(runner_id)
      )

    %Overwatch{
      state
      | runner_id: runner_id,
        channel: attach_channel(relay_id, runner_id),
        owes_outputs: false
    }
  end

  # Stamp everything a resuming bind carries onto the relay's opts so
  # `control_unit` can set the `AddRelay` fields: the Cell restores to wait
  # for, the task-journal the run resumes from, the inputs the re-forward owes
  # it, the kept effect results it ships and when its run window ends. A class
  # without a journal or an input log reads `nil` and `0`.
  defp with_resume_state(relay, relay_class, namespace) do
    opts =
      relay.opts
      |> Keyword.put(:resume_cell_count, Replay.resume_cell_count(relay_class))
      |> Keyword.put(:task_journal, Commands.task_journal_state(relay_class))
      |> Keyword.put(:inbox_tail_count, Replay.owed_count(relay_class))
      |> Keyword.put(:effect_results, Commands.effect_results(relay_class))
      |> Keyword.put(:max_duration_ends_at, RelayManager.window_end(namespace, relay.id))

    %{relay | opts: opts}
  end

  # The one attempt. An awaited runner is re-resolved first so a runner that
  # came online since the bind was awaited is the one used, and bound before the
  # attempt so the ownership record names the runner told to start; the state
  # carries the runner the attempt was made on either way, so a halt names it.
  defp start_once(%Overwatch{} = state, relay, owner, placement) do
    runner_id =
      case placement do
        :resolve ->
          resolve_runner(state.namespace, relay.factory_id, relay.id, state.runner_id)

        :pinned ->
          state.runner_id
      end

    state = bind_to_runner(state, relay.id, runner_id, owner)

    case attempt_ping_and_start(relay, runner_id) do
      {:ok, raw_data} -> {:ok, raw_data, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  # `:start_relay` is only issued AFTER the ping confirms the round trip
  # works. A ping failure halts the relay with its reason; whether the
  # runner's channel is then cut is the loop's decision, read off the reason
  # through `transport_verdict/1`.
  @doc false
  def attempt_ping_and_start(relay, runner_id) do
    case ping_runner(runner_id) do
      :ok ->
        case start_relay_sdk(runner_id, relay) do
          {:ok, _} = ok -> ok
          {:error, reason} -> {:error, {:start_relay_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:ping_failed, reason}}
    end
  end

  @doc """
  What a halt's reason says about the runner's transport: `:dead` when the
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

  defp resolve_runner(namespace, factory_id, relay_id, fallback) do
    case RelayManager.assign_runner(namespace, factory_id, relay_id) do
      id when is_binary(id) and byte_size(id) > 0 -> id
      _ -> fallback
    end
  end

  @doc """
  The deliberate start's read of the binding it brought up
  (`{:registry_runner_set, announce?}`, which only `Init.bind_started/4` asks,
  never a restart, a move or a wake): the runner is written to the relay's
  row and, unless the start is a rescale's (`announce?` false), the start is
  announced here, on the relay's own controller, to the controller plugins
  that hear it.
  """
  @spec started(Overwatch.t(), boolean()) :: :ok
  def started(%Overwatch{} = state, announce?) do
    HubManager.relay_set_runner(state.namespace, state.id, state.runner_id)

    if announce? do
      :telemetry.execute([:flezha, :relay, :started], %{count: 1}, %{
        namespace: to_string(state.namespace),
        relay: state.id,
        factory: state.factory_id
      })
    end

    :ok
  end

  # Transition the Overwatch into the halted-error state: store the failure
  # reason and stay alive (no `:stop`, no supervisor restart).
  #
  # Plugins remain in `:prepared` so no upstream I/O fires while halted. A halt
  # is an outcome, not a remedy: it is declared to the convergence loop, which
  # drives the rebind (`rebind`) on the `{:halted, id}` ladder of its
  # own table and cuts the runner's channel first when the reason says the
  # transport is dead.
  #
  # Every halt funnels through here, so it is the one point that can report and
  # declare one. The reason rides the event whole: a plugin halt's already names
  # both the plugin and its provider module. A bind in flight is voided, so its
  # result cannot overwrite the halt.
  @spec halt(Overwatch.t(), term()) :: Overwatch.t()
  def halt(%Overwatch{} = state, reason) do
    Facts.declare(
      {:relay_halted, {state.namespace, state.id},
       %{reason: reason, transport: transport_verdict(reason)}}
    )

    :telemetry.execute(
      [:flezha, :relay, :halted],
      %{count: 1},
      %{
        namespace: state.namespace,
        relay_id: state.id,
        reason: reason,
        plugin_id: halting_plugin(reason)
      }
    )

    %Overwatch{Rebind.void_bind(state) | halt_reason: reason, phase: halted_phase(state)}
  end

  # The order health is derived in: a sleeper stays suspended, and a relay
  # holding no binding stays unbound.
  defp halted_phase(%Overwatch{phase: :suspended}), do: :suspended
  defp halted_phase(%Overwatch{runner_id: nil}), do: :unbound
  defp halted_phase(%Overwatch{}), do: :halted

  # No runner has connected yet, which is not a fault: hold the relay alive
  # with the outcome on its state. Stopping here would put a supervisor restart
  # beside the convergence loop's own rebind ladder, and a runnerless namespace
  # would crash-loop every `overwatch_runner_wait_ms` for ever.
  #
  # Not `halt/2`: a halt declares an `{:relay_halted, …}` fact the Diff never
  # consumes for a relay it classifies unbound, and would report the
  # relay halted while every health surface calls it unbound.
  #
  # The recorded runner goes with it. A runner id baked into the cspec by a
  # previous boot is not a binding, and keeping one makes the judge call the
  # relay failed where the phase calls it unbound.
  defp hold_unbound(%Overwatch{} = state, reason),
    do: %{state | runner_id: nil, halt_reason: {:runner_not_ready, reason}, phase: :unbound}

  # A halt a plugin caused names that plugin; every other reason carries `nil`
  # rather than claim a plugin id it does not have.
  defp halting_plugin({:plugin_prepare_failed, plugin_id, _reason}), do: plugin_id
  defp halting_plugin(_reason), do: nil

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
    if RelayManager.runner_registered?(runner_id) do
      if Config.get(:overwatch_ping_enabled),
        do: ping_runner(runner_id) == :ok,
        else: true
    else
      false
    end
  end

  defp start_relay_sdk(runner_id, relay) do
    Overwatch.with_task_span(:start_relay, relay.id, fn ->
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_relay, relay},
        Durations.get(:relay_start_call_ms)
      )
    end)
  end

  defp attach_channel(relay_id, runner_id) do
    relay_int = String.to_charlist(relay_id) |> Enum.sum()
    count = Config.get(:concurrent_channel_count)
    channel_index = rem(relay_int, count)

    StreamPool.stream_ids(runner_id, :primary) |> Enum.at(channel_index)
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
      %{scope: :process, order: 10, name: :bind_task, run: &bind_task_step/2},
      %{scope: :process, order: 20, name: :class_registry, run: &class_registry_step/2},
      %{scope: :process, order: 30, name: :control_unit, run: &control_unit_step/2},
      %{scope: :process, order: 33, name: :journal, run: &journal_step/2},
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
        Inbound.end_all(state.calls, callee, status, Ingest.undelivered_calls(state))

      _no_terminal_record ->
        calls =
          if Replay.floor_reporter(state), do: Inbound.outside(state.calls), else: state.calls

        Inbound.end_all(calls, callee, "stopped")
    end

    :ok
  end

  defp bind_task_step({state, _reason}, _budget) do
    _ = Rebind.terminate_bind_task(state)
    :ok
  end

  defp class_registry_step({state, _reason}, _budget),
    do: ClassRegistry.delete(Overwatch.relay_class(state))

  # The runner has been told to drop the relay, so it is no longer bound
  # here: its writers close, flushed, and its files stay. A relay that
  # moved away leaves nothing open for its return to find (`Flezha.State.Adoption`).
  # Before the writers close: the log still answers the appends in flight.
  defp journal_step({state, _reason}, budget), do: Ingest.settle_journaling(state, budget)

  defp writers_step({state, _reason}, _budget),
    do: RelayManager.close_writers(Overwatch.relay_class(state))

  defp control_unit_step({%Overwatch{runner_id: nil}, _reason}, _budget), do: :ok

  # Awaited for half the budget: the runner ships its Cells' last changes as it
  # removes the relay, and the stop flushes them in before its files go; with no
  # input log to re-forward what it has not applied, the stop rides behind the
  # last input. A runner that is gone or does not answer has nothing to wait for.
  defp control_unit_step({state, _reason}, budget) do
    stop = {"", %{"flz.control" => "stop"}}
    behind? = Replay.floor_reporter(state) == nil and Overwatch.deliver(state, stop) == :ok
    _answer = ControlUnit.stop_relay(state.runner_id, state.id, div(budget, 2), drain: behind?)
    :ok
  end

  # After the writers' final flush, so the last turns of a relay stopped here
  # still ship under the adoption that entitles them.
  defp lineage_step({state, _reason}, _budget), do: release_lineage(state)

  @doc """
  The relay's class stops running on this node, the counterpart of the adoption
  a bind records, as `departure/1` reads it: at a move's start and again, a
  no-op then, as its Overwatch ends.
  """
  @spec release_lineage(Overwatch.t()) :: :ok
  def release_lineage(%Overwatch{} = state),
    do: DurabilityTier.release_relay(Overwatch.relay_class(state), departure(state))

  # A deliberate stop removed the relay's declared entry before it
  # terminated: a shared plugin it was the last one on stops at once. A move or
  # a restart leaves the entry (a move also hands over, which counts without
  # one: auto-restore off declares nothing), and the plugin waits out its idle
  # stop.
  defp plugin_table_step({state, reason}, _budget) do
    unless is_nil(state.runner_id), do: Plugins.fire_on_terminate(state, reason)
    idle_or_now = if departure(state) == :moved, do: :idle, else: :now
    _ = Table.teardown_all(state.extensions, idle_or_now)
    :ok
  end

  @doc "How this process leaves its relay; with no stop named, by its declared entry."
  @spec departure(Overwatch.t()) :: :moved | :stopped
  def departure(%Overwatch{handed_over?: true}), do: :moved
  def departure(%Overwatch{cause: :move}), do: :moved
  def departure(%Overwatch{cause: cause}) when cause in @named_stops, do: :stopped

  def departure(state) do
    declared? = Enum.any?(HubManager.declared_relays(state.namespace), &(&1.id == state.id))
    if declared?, do: :moved, else: :stopped
  end
end

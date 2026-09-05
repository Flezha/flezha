defmodule Flezha.Operative.Lifecycle do
  @moduledoc """
  The operative bind state machine: unbound → binding → bound, or parked.

  A bind is ONE attempt: a failure parks the operative with the attempt's
  reason, and the park is declared to the convergence loop, which drives the
  un-park on its own table. Bind and the SDK start are one module because they
  are one cycle: a bind cancels the wake timer a suspension left behind and
  delivers its inbox tail, and the un-park task re-enters `complete_init/4`.

  Everything that takes an operative OFF its runner — suspend, rehydrate,
  rewind, terminate, migrate — is `Flezha.Operative.Migration`, a sibling above
  this module in the call graph.

  Invariant: every function runs in the `Overwatch` process and takes its state,
  returning the next state or a GenServer reply tuple.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Identificator
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Plugins
  alias Flezha.Operative.Replay
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Commands
  alias Flezha.State.DurabilityTier
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.ProcInbox
  alias Flezha.State.Restore

  require Logger

  # ---------------------------------------------------------------
  # Process API — the lifecycle messages the Overwatch answers to
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
          # `complete_init` re-resolves the runner before its one attempt, so
          # the binding to publish is the one it ended on, not the one awaited.
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
  def publish_binding(%Overwatch{id: operative_id, runner_id: runner_id} = state) do
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
  # make the one SDK start attempt, apply the class's declarations, restore
  # durable state, activate plugins, and deliver whatever the durable inbox
  # still owes. Both ways in — a fresh bind and an un-park — funnel through
  # here, which is what keeps them one path.
  #
  # `owner` is the Overwatch process itself. It is passed explicitly because the
  # un-park path runs this inside a task (so a slow start never blocks the
  # Overwatch), and `self()` there is the TASK — registering it as the
  # operative's Overwatch would point the journal's outbox flush at a process that
  # exits moments later, silently dropping the output.
  @spec complete_init(Overwatch.t(), binary(), map(), pid() | nil) ::
          {:ok, Overwatch.t()} | {:error, term(), Overwatch.t()}
  def complete_init(%Overwatch{} = state, runner_id, operative, owner \\ nil) do
    owner = owner || self()
    state = bind_to_runner(state, operative.id, runner_id)
    # Before the SDK starts: a fast chain's first output can reach the stream
    # channel ahead of the registry hook that seeds its routing, and the
    # channel resolves that miss through this row.
    _ = ClassRegistry.put_overwatch(Overwatch.operative_class(state), owner)
    plugins = Keyword.get(operative.opts, :plugins, [])

    with {:ok, state} <- Plugins.prepare(state, plugins),
         {:ok, state, operative} <- Plugins.resolve_init_state(state, operative),
         operative =
           with_resume_state(operative, state.builder.kind, Overwatch.operative_class(state)),
         {:ok, final_runner_id, add_operative_raw, state} <- start_once(state, operative),
         # The class's SDK-side declarations ride the AddOperative response;
         # resolve them BEFORE plugin activation so source adapters pick the
         # tier ingest mode (blocking call) from the env.
         %{effectively_once: effectively_once, correction_window_ms: correction_window_ms} =
           declarations = ClassRegistry.decode_declarations(add_operative_raw),
         state =
           %{
             state
             | effectively_once: effectively_once,
               correction_window_ms: correction_window_ms
           }
           |> bind_to_runner(operative.id, final_runner_id)
           |> Map.put(:init_error, nil),
         :ok <- await_stream_bound(state),
         # BEFORE the restore: a refused restore parks the operative, so a
         # correction running after it would never run at all — the factory
         # would park every attempt at its wrong language forever.
         _ =
           Restore.establish_sdk_lang_from_state(
             state.namespace,
             operative.factory_id,
             Overwatch.operative_class(state)
           ),
         # Tier: restore every persisted Entity (awaited) and re-forward the
         # journaled input tail BEFORE sources activate, so replayed offsets
         # reach the runner ahead of fresh ones (fold order at the SDK).
         {:ok, state} <- tier_restore(state, effectively_once) do
      record_and_replay(state, operative, declarations, correction_window_ms)
    else
      {:error, reason, %Overwatch{} = parked} -> {:error, reason, parked}
      {:error, reason} -> {:error, reason, state}
    end
  end

  # Everything a started operative owes once its state is restored: the class
  # facts the State subsystem reads, the retention floors the declaration
  # raised, and the replay of whatever the durable inbox still owes.
  defp record_and_replay(state, operative, declarations, correction_window_ms) do
    # A plugin that fails to come up stays `:prepared`, which the convergence
    # loop diffs as missing and heals under backoff, so the bind does not fail
    # on it.
    {state, _not_running} = Plugins.bring_up(state)

    # Record this class's kind so the State subsystem can gate stream-only
    # behaviours (retention/watchdog/dynamic-evict/time-travel) for a durable
    # workflow operative — plus every SDK declaration (the Overwatch pid the
    # fsync-gated outbox flush dispatches `:on_output` through was recorded
    # before the SDK started).
    operative_class = Overwatch.operative_class(state)
    _ = ClassRegistry.put(operative_class, state.builder.kind)
    _ = ClassRegistry.put_declarations(operative_class, declarations)
    # Partitionability is a class-code fact, so it is recorded against the
    # factory: a later `--partitions N` start of a known-keyless class is
    # then refused before a runner is touched.
    _ = ClassRegistry.put_keyless(operative.factory_id, declarations.keyless)
    _ = ClassRegistry.put_state_keys(operative.id, operative_class)

    # The declaration is durable now, so the retention floors it needs resolve
    # from it. Announce any it had to raise — a one-line declaration must not
    # commit the controller to disk silently.
    if correction_window_ms do
      PersistenceResolver.announce_derived_retention(operative_class, correction_window_ms)
    end

    # This node runs the operative now, which is what entitles it to take over
    # the reference's remote lineage. It has to be said here rather than
    # inferred from shipping: a migration leaves the source node holding a
    # full copy of the state, so "has the files" would let the node that just
    # lost the operative claim it straight back.
    _ = DurabilityTier.adopt_operative(operative_class)

    _ = Replay.restore_on_bind(state, operative)

    # A bind is also a rehydrate: whatever the durable inbox still owes is
    # re-forwarded from the recorded consumed offset, so input that arrived
    # while the instance was suspended, crashed, or migrating is delivered
    # exactly here — one path for every way an instance comes back.
    {:ok, Replay.deliver_inbox_tail(cancel_wake_timer(%{state | suspended: false}))}
  end

  # A refused restore is not a cold start: the durable state never reached the
  # runner, so the operative parks rather than folding new inputs onto empty
  # state. Rides the existing parked-error contract, so `bind/3` needs no new
  # failure arm.
  defp tier_restore(state, false), do: {:ok, state}

  defp tier_restore(state, true) do
    case Replay.restore_and_replay_journal(state) do
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:error, {:restore_refused, reason}, state}
    end
  end

  # An operative binds to a runner over the CONTROL channel; the data stream that
  # carries its input is a separate connection the SDK opens afterwards.
  # Everything below this point forwards on that stream — the durable restore's
  # re-forward, the inbox tail, the plugins' first input, and every ingest
  # waiting in this process's mailbox — so the bind waits for it rather than
  # issuing onto a stream that would drop what it is given.
  #
  # No caller is parked by the wait. `start_operative/2` returned when this
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
      |> Keyword.put(
        :inbox_tail_count,
        if(kind == :workflow, do: length(ProcInbox.undelivered(operative_class)), else: 0)
      )

    %{operative | opts: opts}
  end

  # The one attempt. The runner is re-resolved first so a runner that came
  # online since the bind was awaited is the one used; the state carries the
  # runner the attempt was made on either way, so a park names it.
  defp start_once(%Overwatch{} = state, operative) do
    runner_id =
      resolve_runner(state.namespace, operative.factory_id, operative.id, state.runner_id)

    state = %Overwatch{state | runner_id: runner_id}

    case attempt_ping_and_start(operative, runner_id) do
      {:ok, raw_data} -> {:ok, runner_id, raw_data, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  # `:start_operative` is only issued AFTER the ping confirms the round trip
  # works. A ping failure parks the operative with its reason; whether the
  # runner's channel is then cut is the loop's decision, read off the reason
  # through `transport_verdict/1`.
  @doc false
  def attempt_ping_and_start(operative, runner_id) do
    case ping_runner(runner_id) do
      :ok ->
        case start_operative_sdk(runner_id, operative) do
          {:ok, _} = ok -> ok
          {:error, reason} -> {:error, {:start_operative_failed, reason}}
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

  defp resolve_runner(namespace, factory_id, operative_id, fallback) do
    case OperativeManager.assign_runner(namespace, factory_id, operative_id) do
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
    Facts.declare(
      {:operative_parked, {state.namespace, state.id},
       %{reason: reason, transport: transport_verdict(reason)}}
    )

    :telemetry.execute(
      [:flezha, :operative, :parked],
      %{count: 1},
      %{
        namespace: state.namespace,
        operative_id: state.id,
        reason: reason,
        plugin_id: parking_plugin(reason)
      }
    )

    Map.put(state, :init_error, reason)
  end

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
    if OperativeManager.runner_registered?(runner_id) do
      if Config.get(:overwatch_ping_enabled),
        do: ping_runner(runner_id) == :ok,
        else: true
    else
      false
    end
  end

  # ---------------------------------------------------------------
  # Suspension (`workflow-durable-suspension`)
  # ---------------------------------------------------------------

  # The durable timer: a `parked` record's wake deadline, re-armed from the
  # journal on every boot, so it survives a controller redeploy.
  def arm_wake_timer(nil), do: nil

  def arm_wake_timer(deadline_ms) do
    Process.send_after(
      self(),
      :wake_deadline,
      max(deadline_ms - System.system_time(:millisecond), 0)
    )
  end

  # Cancel the journaled wake timer, if one is armed.
  @spec cancel_wake_timer(Overwatch.t()) :: Overwatch.t()
  def cancel_wake_timer(%Overwatch{wake_timer: nil} = state), do: state

  def cancel_wake_timer(%Overwatch{wake_timer: ref} = state) do
    _ = Process.cancel_timer(ref)
    %Overwatch{state | wake_timer: nil}
  end

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
    count = Config.get(:concurrent_channel_count)
    channel_index = rem(operative_int, count)

    StreamPool.stream_ids(runner_id, :primary) |> Enum.at(channel_index)
  end
end

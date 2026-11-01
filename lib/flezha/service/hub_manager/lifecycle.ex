defmodule Flezha.Service.HubManager.Lifecycle do
  @moduledoc """
  What a hub does around its own start and stop: the registry backend, recovery
  and migration settings it starts with, the post-start hook wiring, the
  aliveness wait, the drain, and the registry directory a stopped hub leaves
  behind. The drain and the stops are the hub's steps of the shutdown
  plan — the `:node` drain and hub stop, and the `:namespace` hub stop — and
  nothing else reaches `drain_hub/2`.
  """

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Boot.RecoveryHooks
  alias Flezha.Cluster.Directory
  alias Flezha.Cluster.Orchestrator
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Config.Keys
  alias Flezha.Convergence.Snapshot
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Parallel
  alias Flezha.Plugin.Shared
  alias Flezha.Process.Hub
  alias Flezha.Process.Poll
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.HubManager.Rows
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.DurabilityTier
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.Migration
  alias ProcessHub.Service.Recovery, as: HubRecovery

  alias ProcessHub.Strategy.PartitionTolerance.{Divergence, DynamicQuorum, StaticQuorum}
  alias ProcessHub.Worker.WorkerQueue

  require Logger

  @behaviour Flezha.Shutdown.Step

  @drain_delivered :flezha_drain_delivered

  # A force pass fires AT the drain deadline and its handover is async, so this
  # window is carved out of the drain timeout to let it land.

  @start_failed [:flezha, :hub, :start_failed]

  @doc """
  The one wait every hub-creating path takes before it creates a hub: a
  singleton hub started on a not-yet-clustered node diverges from its peers.
  Bounded by the degrade-to-solo window, so a genuinely single-node deployment
  still proceeds.

  A plain function and not a process, because the on-demand path runs inside
  the fleet manager's own call and has to answer that caller rather than defer
  it. The boot-time starter waits once for its whole batch: cluster formation
  is a property of the node, not of a namespace.
  """
  @spec await_cluster_gate() :: :ok | :timeout
  def await_cluster_gate(), do: Orchestrator.await_cluster_ready()

  @doc """
  Start `namespace`'s hub on this node unless one is alive here, behind the
  cluster gate every hub-creating path takes: a hub created during the boot
  window must not be a singleton before the node has joined the cluster. One a
  concurrent caller started counts as started.
  """
  @spec ensure_hub(binary()) :: :ok | {:error, term()}
  def ensure_hub(namespace) do
    if Rows.hub_alive?(namespace) do
      :ok
    else
      _ = await_cluster_gate()

      case HubManager.start_hub(namespace) do
        {:error, {:already_started, _pid}} -> :ok
        {:error, reason} -> {:error, reason}
        _started -> :ok
      end
    end
  end

  @doc """
  One attempt at starting a hub, for the owners that keep retrying one. `:ok`
  once it is up; on any other answer `@start_failed` is emitted naming the
  target and the reason, the failure is logged, and `:error` comes back.

  The owner decides what to do with the verdict, because the retry timer is
  the only part of that loop the owners do not share. The event is named here
  rather than passed in: one failure, one event, whichever hub it was, with
  `target` saying which — and an event resolved at the site that emits it is
  one the bridge seam can find.
  """
  @spec attempt_start(term(), (-> term())) :: :ok | :error
  def attempt_start(target, start) when is_function(start, 0) do
    case start.() do
      {:ok, _pid} -> :ok
      {:ok, _pid, _info} -> :ok
      {:error, reason} -> start_failed(target, reason)
      other -> start_failed(target, other)
    end
  end

  defp start_failed(target, reason) do
    :telemetry.execute(@start_failed, %{count: 1}, %{
      target: to_string(target),
      reason: inspect(reason)
    })

    :error
  end

  def on_hub_started(hub_id, pid, namespace) do
    wait_until_hub_alive(hub_id)
    # Advertise placement cluster-wide; membership dies with the hub pid,
    # so stop/crash/nodedown need no leave call.
    Directory.join_hub(pid, namespace)

    # A hub's group table dies with the hub, so this node declares again which
    # of the namespace's factories its fleet can run. A cast, because this
    # often runs inside `FleetManager`'s own call.
    FleetManager.hub_started(namespace)

    # Everything below reaches into the hub, and this runs inside
    # `FleetManager`'s own `handle_call`: a hub that dies in the window between
    # the wait above and the last of these calls must be an answer here rather
    # than an exit, or one namespace's teardown takes the node's runner roster
    # and its acceptor with it. The guard is the caller's because `service` may
    # name `boot` and `boot` may not name `service`.
    Rows.with_live_hub_id(hub_id, :ok, fn id ->
      RecoveryHooks.register(id)

      # Seed the snapshot cache with the coordinator's current state. The
      # recovery telemetry fired during recovery overwrites this entry as
      # the lifecycle progresses; the seed only matters when a hub boots
      # straight into `:normal` (auto-recovery off) and no telemetry event
      # will follow.
      _ = Snapshot.observe_recovery(id, HubRecovery.recovery_state(id))
      :ok
    end)
  end

  @doc """
  Last known recovery state for `hub_id`. Returns
  `{:ok, :recovery_pending | :recovering | :normal | :skipped | :timeout}`,
  or `{:error, :unknown_hub}` if the hub is not registered with this
  supervisor.
  """
  @spec recovery_state(atom()) :: {:ok, atom()} | {:error, :unknown_hub}
  def recovery_state(hub_id) when is_atom(hub_id) do
    cond do
      not Hub.alive?(hub_id) ->
        {:error, :unknown_hub}

      true ->
        case Snapshot.recovery_state(hub_id) do
          :unknown -> {:ok, HubRecovery.recovery_state(hub_id)}
          state -> {:ok, state}
        end
    end
  end

  # Supervisor.start_child returns once the hub's top supervisor is up,
  # but ProcessHub registers its named coordinator GenServer asynchronously
  # afterwards. Anything calling into the named hub_id (register_hook_handlers,
  # alter_child_data callbacks) before that
  # registration completes raises :noproc and cascades the FleetManager
  # crash. Spin briefly until ProcessHub.is_alive? confirms the name is
  # claimed.
  def wait_until_hub_alive(hub_id, deadline_ms \\ Durations.get(:hub_alive_wait_ms)) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    alive = fn -> if Hub.alive?(hub_id), do: {:ok, :alive}, else: {:error, :timeout} end

    case Poll.until(deadline, Durations.get(:hub_alive_poll_ms), alive) do
      {:ok, :alive} -> :ok
      {:error, :timeout} -> :timeout
    end
  end

  # The hub's steps: the node drain — every running namespace's hub drained
  # concurrently under the budget the coordinator derived, one
  # `:drain_progress` per hub — the node's hub stop after the data rows, and
  # the namespace stop, which stops the hub and wipes its registry directory.
  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [
      %{scope: :node, order: 10, name: :drain, run: &drain_step/2},
      %{scope: :node, order: 40, name: :hubs, run: &__MODULE__.hubs_step/2},
      %{scope: :namespace, order: 40, name: :hub, run: &hub_step/2}
    ]
  end

  # Terminated, not stopped: its supervisor would restart a stopped child.
  @doc false
  def hubs_step(%{supervisor: sup}, _budget), do: Supervisor.terminate_child(sup, HubManager)

  defp hub_step(%{namespace: namespace}, _budget), do: HubManager.stop_hub(namespace)

  defp drain_step(%{namespaces: namespaces, phase_start_ms: phase_start_ms}, budget) do
    total = length(namespaces)

    {migrated, forced} =
      namespaces
      |> Parallel.results(fn ns -> drain_hub(ns, budget_ms: budget) end,
        max_concurrency: max(total, 1),
        timeout: budget + Durations.get(:forced_handover_grace_ms)
      )
      |> Enum.with_index(1)
      |> Enum.reduce({0, 0}, fn {{ns, task_result}, idx}, {m_acc, f_acc} ->
        # A task killed by the async_stream timeout degrades like any other
        # drain error.
        result =
          case task_result do
            {:ok, drain_result} -> drain_result
            {:exit, reason} -> {:error, {:task_exit, reason}}
          end

        {m, f} = fold_drain_outcome(ns, result)

        :telemetry.execute(
          [:flezha, :shutdown, :drain_progress],
          %{
            migrated: m,
            forced: f,
            remaining_hubs: total - idx,
            elapsed_ms: System.monotonic_time(:millisecond) - phase_start_ms
          },
          %{namespace: ns}
        )

        {m_acc + m, f_acc + f}
      end)

    {:ok, %{migrated: migrated, forced: forced}}
  end

  # One hub's drain verdict → `{migrated, forced}`. Every error degrades to
  # the no-peer path: the relays terminate with the hub supervisor and
  # the next boot restores them from on-disk WAL/snapshot — never a wedge.
  defp fold_drain_outcome(ns, {:ok, %{migrated: migrated, forced: forced}}) do
    if forced > 0 do
      Logger.warning("[shutdown] drain ns=#{ns} force-migrated #{forced} deferred relay(s)")
    end

    {migrated, forced}
  end

  defp fold_drain_outcome(_ns, {:error, :no_target_nodes}) do
    Logger.info(
      "[shutdown] drain skipped (no peers); state will be restored on next boot via on-disk WAL/snapshot"
    )

    {0, 0}
  end

  defp fold_drain_outcome(ns, {:error, reason}) do
    Logger.warning(
      "[shutdown] drain ns=#{ns} degraded (#{inspect(reason)}); " <>
        "local relays terminate and restore on next boot"
    )

    {0, 0}
  end

  @doc """
  Migrate every locally-hosted relay of `namespace` to a peer controller
  through the migration-consent gate and the namespace's migration strategy,
  then wait for the local children, and the plugin copies they left retiring,
  to actually be gone, and for each moved child to have arrived.

  The wait is load-bearing: `Migration.drain/2` returns once the deferred list
  is empty, but the migration start requests are async. A hot swap stops a
  local child only AFTER the remote start succeeds; a cold swap stops it first,
  and its new process then reads its handover from this node's hub and pulls
  the relay's durable files from this node. So the drain also waits for each
  child whose handover is still undelivered once the children are gone, and
  for its new process to have run its arrival; a child that ended without
  moving is not waited for. Both waits are read from the children's exits and
  the hub's deliveries, never polled. `migrated` counts the delivered
  handovers less the ones `forced` counts.
  `:budget_ms` is required and bounds the whole call: the caller always hands
  the drain its slice of the one shutdown budget. `:handover_grace_ms` is
  carved off it so the deadline force pass can land before the caller moves on.
  A retiring copy whose successor's upstream never opens holds the drain until
  that budget runs out.

  The one drain seam, reached by the `:node` step alone.
  """
  @spec drain_hub(binary(), keyword()) :: {:ok, map()} | {:error, term()} | :deadline
  def drain_hub(namespace, opts) do
    budget = Keyword.fetch!(opts, :budget_ms)
    grace = Keyword.get(opts, :handover_grace_ms, Durations.get(:forced_handover_grace_ms))
    deadline = System.monotonic_time(:millisecond) + budget

    Rows.with_live_hub(namespace, {:error, :no_target_nodes}, fn hub_id ->
      local = Rows.local_children(namespace, :all)
      handler = %HookManager{id: @drain_delivered, m: Kernel, f: :send, a: [self(), :_]}
      :ok = ProcessHub.register_hook_handlers(hub_id, Hook.handover_delivered(), [handler])

      try do
        # A hub with work still queued, as a membership change leaves it,
        # is drained behind that work rather than refused.
        result = Migration.drain(hub_id, timeout: max(budget - grace, 1_000), when_busy: :wait)

        with {:ok, %{forced: forced} = summary} <- result do
          moving =
            case await_local_children_gone(local, namespace, deadline) do
              :ok -> moving(hub_id, local, deadline)
              :deadline -> []
            end

          delivered = await_arrivals(namespace, moving, 0, deadline)
          {:ok, %{summary | migrated: max(delivered - forced, 0)}}
        end
      after
        ProcessHub.cancel_hook_handlers(hub_id, Hook.handover_delivered(), [@drain_delivered])
      end
    end)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # The local children still on their way: a handover stored and undelivered,
  # and a row binding them nowhere here (a move's placeholder or their new
  # home); a child that ended keeps its binding here or loses its row. Read on
  # the worker queue, behind the moves that unbind what they stop. The hub
  # removes a handover and announces its delivery in one coordinator callback,
  # so once it answers, a delivery the read missed is in this mailbox. A queue
  # or coordinator busy past the budget leaves the deliveries already received.
  defp moving(hub_id, local, deadline) do
    read = fn ->
      for child_id <- Hub.pending_handovers(hub_id, include_expired: true),
          List.keymember?(local, child_id, 0),
          {_cspec, nodes} <- [ProcessHub.child_lookup(hub_id, child_id, include_empty: true)],
          not List.keymember?(nodes, node(), 0),
          do: child_id
    end

    moving =
      WorkerQueue.run_on(ProcessHub.Hub.get(hub_id).procs.worker_queue, read, left(deadline))

    :sys.get_state(hub_id, left(deadline))
    moving
  catch
    :exit, _busy -> []
  end

  # Each delivery names the children whose handover reached their new process
  # on `target_node`. One call to each new process returns once its arrival,
  # which pulls the relay's files from this node, has run. Answers how many
  # were delivered; once none is waited for, it settles only those received.
  defp await_arrivals(namespace, waiting, count, deadline) do
    wait = if waiting == [], do: 0, else: left(deadline)

    receive do
      %{child_ids: delivered, target_node: target} ->
        settle(target, namespace, delivered, deadline)
        await_arrivals(namespace, waiting -- delivered, count + length(delivered), deadline)
    after
      wait -> count
    end
  end

  # A target that went away or answers late holds the drain no longer.
  defp settle(target, namespace, child_ids, deadline) do
    left = left(deadline)
    :erpc.call(target, __MODULE__, :arrived, [namespace, child_ids, left], left)
  catch
    _kind, _reason -> :ok
  end

  defp left(deadline), do: max(deadline - System.monotonic_time(:millisecond), 1)

  @doc false
  # On a drain's target: answers once each of `child_ids`' local processes has
  # taken every message sent to it before, its start's continuation included.
  def arrived(namespace, child_ids, timeout) do
    for {child_id, pid} <- Rows.local_children(namespace, :all), child_id in child_ids do
      :sys.get_state(pid, timeout)
    end

    :ok
  catch
    :exit, _gone_or_slow -> :ok
  end

  # Monitor every child local when the drain began and wait for each exit,
  # then for the plugin copies those children left retiring: a moved `single`
  # plugin's hand-over ends when its old copy closes, after the new one opened.
  # The node is quiesced, so none is started meanwhile. Answers `:deadline`
  # with the monitors flushed when the budget runs out first.
  defp await_local_children_gone(local, namespace, deadline) do
    monitors = for {_child_id, pid} <- local, do: Process.monitor(pid)

    with :ok <- await_downs(monitors, deadline),
         do:
           await_downs(
             Enum.map(Shared.retiring(namespace, deadline), &Process.monitor/1),
             deadline
           )
  catch
    _kind, _reason -> :ok
  end

  defp await_downs([], _deadline), do: :ok

  defp await_downs([ref | rest], deadline) do
    receive do
      {:DOWN, ^ref, :process, _pid, _reason} -> await_downs(rest, deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        Enum.each([ref | rest], &Process.demonitor(&1, [:flush]))
        :deadline
    end
  end

  def wipe_registry_dir(hub_id) do
    File.rm_rf(Layout.file(:process_hub, Atom.to_string(hub_id)))
  end

  @doc false
  def auto_recovery_config do
    # Orphan reconcile: each round converges the cluster toward the hub's
    # declared set, starting declared children observed running nowhere and
    # stopping running durable children whose entry was removed. The hub is
    # `:recovering`, which holds readiness, until its first round; that opens
    # once the settle window has passed and every connected peer has sent its
    # registry, and the grace only caps a peer that never does. No runner is
    # waited for, so an orphan placed first is held unbound until the rebind;
    # only a node returning to a running cluster holds its first round's starts
    # until it serves a factory (`GroupedHashing.handle_first_round/1`).
    if Rows.durable_starts?() do
      [
        reconcile_grace_ms: Durations.get(:process_hub_reconcile_grace_ms),
        reconcile_interval_ms: Durations.get(:process_hub_reconcile_interval_ms),
        cluster_settle_ms: Durations.get(:process_hub_cluster_settle_ms),
        remote_manifest: remote_manifest_config()
      ]
    else
      false
    end
  end

  # The declared set's off-cluster copy rides the durability tier the user
  # already configured: one remote-storage path, every tier backend
  # (`:postgres`, `:s3`, `:local_path`, custom) works unchanged. No tier, no
  # remote copy: `nil` keeps the feature local-only, exactly as before the
  # tier existed.
  defp remote_manifest_config do
    if DurabilityTier.enabled?() do
      {Flezha.State.DurabilityTier.DeclaredManifest, []}
    else
      nil
    end
  end

  @doc false
  def registry_backend_config(hub_id) do
    # When auto-restore is disabled (test mode + any user who flips
    # the kill-switch), there is no point persisting the registry to
    # disk — the DETS file would only become test-isolation noise. ETS
    # gives us identical in-memory semantics with no on-disk residue.
    if Rows.durable_starts?() do
      # `:durable_ets` serves reads from ETS and mirrors writes to disk
      # synchronously, so the reconcile's per-round registry reads stay
      # in-memory while the file remains the record a cold cluster
      # recovers from. Same on-disk format as `:dets`.
      {:durable_ets, [path: hub_file(hub_id, Layout.hub_registry_file())]}
    else
      :ets
    end
  end

  @doc """
  The partition tolerance strategy `FLEZHA_SPLIT_BEHAVIOUR` picks for every hub.
  Raises, naming the variable, on a setting that cannot work; the boot builds
  it once before any hub starts, so a bad setting stops the boot.
  """
  @spec partition_tolerance_strategy() :: struct()
  def partition_tolerance_strategy do
    nodes = Config.get(:quorum_nodes)

    case Config.get(:split_behaviour) do
      :divergence ->
        %Divergence{}

      :static ->
        unless nodes, do: refuse(:quorum_nodes, "is required when FLEZHA_SPLIT_BEHAVIOUR=static")
        %StaticQuorum{quorum_size: nodes, startup_confirm: true}

      :dynamic ->
        percent = Config.get(:quorum_percent)

        unless percent in 51..100,
          do:
            refuse(
              :quorum_percent,
              "is #{percent}; FLEZHA_SPLIT_BEHAVIOUR=dynamic needs 51 to 100"
            )

        %DynamicQuorum{
          quorum_size: percent,
          threshold_time: div(Durations.get(:quorum_window_ms) + 999, 1_000),
          startup_nodes: nodes
        }
    end
  end

  @doc """
  `{:error, :no_quorum}` when `hub_id` runs on this node without quorum: a
  quorum split behaviour is set and the hub stopped its relays here; `:ok`
  otherwise. Read from the hub's own state, never through its coordinator, so
  a busy hub answers at once; a hub that is not running has no quorum to lack.
  """
  @spec quorum(atom()) :: :ok | {:error, :no_quorum}
  def quorum(hub_id) do
    if Config.get(:split_behaviour) != :divergence and partitioned?(hub_id),
      do: {:error, :no_quorum},
      else: :ok
  end

  defp partitioned?(hub_id) do
    case ProcessHub.Hub.get(hub_id) do
      nil -> false
      hub -> ProcessHub.Service.State.is_partitioned?(hub)
    end
  rescue
    # A hub going down can still be stored after its registry has gone.
    ArgumentError -> false
  end

  defp refuse(name, why), do: raise(ArgumentError, "#{Keys.fetch!(name).env} #{why}")

  @doc false
  def migration_strategy(namespace) do
    struct(Hub.strategy(NamespaceStore.migration(namespace)),
      handover: true,
      state_query_timeout: Durations.get(:migration_state_query_ms),
      state_ttl: Durations.get(:migration_state_ttl_ms),
      consent_settings: consent_settings()
    )
  end

  # `max_defer_ms` nil = derive from the shutdown budget, so a deferring
  # relay can never outlive the graceful-shutdown window.
  @doc false
  def consent_settings do
    %ProcessHub.Strategy.Migration.MigrationConsent{
      consent_timeout: Durations.get(:migration_consent_timeout_ms),
      retry_interval: Durations.get(:migration_retry_interval_ms),
      max_defer_time:
        Durations.get(:migration_max_defer_ms) || Coordinator.configured_timeout_ms()
    }
  end

  defp hub_file(hub_id, name) do
    path = Layout.file(:process_hub, [Atom.to_string(hub_id), name])
    File.mkdir_p!(Path.dirname(path))
    path
  end
end

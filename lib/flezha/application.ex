defmodule Flezha.Application do
  @moduledoc false

  alias Flezha.Boot.Loader
  alias Flezha.Boot.Readiness
  alias Flezha.Boot.StatePathMigrator
  alias Flezha.Boot.StateRootLock
  alias Flezha.Config
  alias Flezha.Diagnostics.MemoryReporter
  alias Flezha.Plugin.Installer.Loader, as: PackageLoader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.DurabilityTier

  use Application

  @impl true
  def start(_type, _args) do
    # The legacy `:backends` Logger config is deprecated in newer Elixir.
    # We register the file backend at runtime via `:logger_backends`,
    # which silently no-ops if the dep isn't loaded so consumers that
    # only want the default :console handler still boot cleanly.
    if Code.ensure_loaded?(LoggerBackends) and
         function_exported?(LoggerBackends, :add, 1) do
      _ = LoggerBackends.add({LoggerFileBackend, :file_log})
    end

    # Runner-only boot: a standalone SDK node depends on flezha as a library
    # without becoming a controller. It starts an EMPTY tree — no transport
    # servers, no HubManager, no convergence, no state-path migration, no
    # boot-ready gate — leaving the host app to run SDK processes via
    # `Flezha.SDK.child_spec/1` under its own supervisor.
    if Config.get(:controller_enabled) do
      start_controller()
    else
      require Logger
      Logger.info("[flezha] controller disabled — runner-only boot (no controller tree)")
      Supervisor.start_link([], strategy: :one_for_one, name: Flezha.Supervisor)
    end
  end

  defp start_controller() do
    # An Elixir SDK runner in this VM reaches the controller through the
    # in-node transport, which the SDK finds registered here.
    :ok = Flezha.SDK.Connect.register(Flezha.Transport.Server)

    # Before any state path is opened: a second controller over this state root
    # is two unsynchronised writers on one cell WAL tree, and the claim is
    # what makes that a refusal rather than corruption.
    _ = StateRootLock.claim!()

    _ = StatePathMigrator.migrate()

    # Before the children are computed: a package's providers are rows of the
    # provider table, whose worker families the tree supervises.
    _ = PackageLoader.load_all()

    children = controller_children()

    opts = [strategy: :one_for_one, name: Flezha.Supervisor]

    # A child that refused to start already carries the reason a user
    # needs — a claimed socket directory names the peer and the variable. Only
    # a tree that came up is asked whether it reached ready; asking a
    # supervisor that never started replaces that reason with `:noproc`.
    with {:ok, _pid} = result <- Supervisor.start_link(children, opts) do
      :ok = PackageLoader.announce()
      # Here and nowhere earlier: `start_link/2` has returned, so every child
      # started, and this call blocks on the Loader until its boot continuation
      # finished. Publishing from the continuation itself would claim a
      # complete boot over a hub set that is empty only because HubManager has
      # not started yet.
      :ok = Loader.await_sequence()
      Readiness.publish_sequence_complete()
      result
    end
  end

  @impl true
  def prep_stop(state) do
    cond do
      not Config.get(:controller_enabled) ->
        state

      Config.get(:graceful_shutdown_enabled) ->
        drain_on_prep_stop()

      true ->
        safe_legacy_stop()
    end

    state
  end

  defp drain_on_prep_stop() do
    # The Coordinator owns the drain — it dispatches ProcessHub
    # `cluster_leave` (via HubManager.stop_hub_manager/0) inside its
    # drain phase and waits for peers to claim our Operators before
    # finalising. Bounded by `:flezha, :graceful_shutdown_timeout_ms`.
    _ = Coordinator.shutdown(:prep_stop)
  catch
    :exit, reason ->
      require Logger
      Logger.warning("[shutdown] coordinator unavailable in prep_stop: #{inspect(reason)}")
      # Coordinator isn't running (e.g. early-boot crash) — fall
      # back to the legacy direct call so the BEAM still tears
      # the hub down before halting.
      safe_legacy_stop()
  end

  defp safe_legacy_stop() do
    HubManager.stop_hub_manager()
  catch
    :exit, _ -> :ok
  end

  # The controller tree, in start order. Every entry's position is load-bearing;
  # the ordered id list is pinned by `Flezha.Boot.ApplicationChildrenTest`.
  @doc false
  def controller_children() do
    children =
      core_children() ++
        DurabilityTier.children() ++
        service_children() ++
        authz_children() ++
        hub_starter_children() ++
        transport_children() ++
        plugin_children() ++
        Flezha.State.Supervisor.application_children() ++
        state_children() ++
        convergence_loop_children() ++
        schedule_children() ++
        healthz_children() ++
        memory_reporter_children()

    prepend_testnet(children)
  end

  # SignalHandler is started FIRST so OS signal handlers are installed before any
  # other service can observe a delivered SIGTERM.
  defp core_children do
    [
      Flezha.Shutdown.SignalHandler,
      Flezha.Service.EventManager,
      Flezha.Boot.Loader,
      Flezha.Identificator,
      # Owns the recovery-state cache + the published convergence
      # snapshot; must be up before any hub fires recovery telemetry.
      Flezha.Convergence.Snapshot,
      # The cluster directory scope must be up before its writers:
      # HubManager joins hub pids, FleetManager joins capability groups.
      Flezha.Cluster.Directory
    ]
  end

  # After `DurabilityTier.children()`, whose boot-time cspec hydration must land
  # before ProcessHub opens `registry.dets` at hub start.
  defp service_children do
    [
      Flezha.Service.HubManager,
      Flezha.Observability.Supervisor,
      # Right after HubManager, because the drain queries the hub.
      Flezha.Shutdown.Coordinator,
      Flezha.Runner.FleetSupervisor,
      FleetManager,
      # Cluster formation must start BEFORE the transport accepts runner
      # connects, so a hub is never created on a not-yet-clustered node.
      # `Flezha.Convergence.Loop` (started after the State trio below)
      # owns all bind/rebind/plugin-restart healing.
      Flezha.Cluster.Orchestrator
    ]
  end

  defp plugin_children do
    worker_families =
      Enum.map(
        Flezha.Plugin.Providers.worker_families(),
        &{Flezha.Plugin.Provider.WorkerSup, name: &1}
      )

    [
      Flezha.Plugin.Provider.Kafka.WorkerRegistry,
      Flezha.Service.Calls,
      Flezha.Observability.HookSink
    ] ++
      worker_families ++
      [
        Flezha.Service.PortPilot,
        {Task.Supervisor, name: Flezha.Operator.MessageHandlerTaskSup}
      ]
  end

  defp state_children do
    [
      Flezha.State.FsyncBatcher,
      Flezha.State.Retention,
      Flezha.State.Watchdog,
      Flezha.Dlq.Store
    ]
  end

  # Flezha.Testnet only compiles in :test (test/helpers/); ensure_loaded?
  # gates the child spec so prod boots without the module. Prepended, never
  # appended: the harness must be listening before anything connects.
  defp prepend_testnet(children) do
    if Config.get(:testnet_enabled) and
         Code.ensure_loaded?(Flezha.Testnet) do
      [{Flezha.Testnet, []} | children]
    else
      children
    end
  end

  # Authn/authz supervisor is omitted from the global application
  # tree when `:authn_authz, :application_managed` is `false` so
  # unit tests can bring up isolated AuthState/RoleStore/etc.
  # against a per-test tmp `auth_dir`. The flag defaults `true` so
  # production and dev boot the subsystem unconditionally.
  defp authz_children do
    if Config.get(:authz_application_managed) do
      [Flezha.AuthZ.Supervisor]
    else
      []
    end
  end

  # The declared set's hubs, given an owner off the boot path. After
  # `authz_children()` because the namespace catalog is one of the two sources
  # the declared set is derived from. Rides the auto-restore kill-switch: a
  # node told not to bring things back by itself creates no hub at boot, and
  # the readiness row reads the same switch.
  defp hub_starter_children do
    if Config.get(:auto_restore_enabled) do
      [Flezha.Service.HubManager.Starter]
    else
      []
    end
  end

  defp memory_reporter_children do
    case MemoryReporter.configured_interval_ms() do
      {:ok, _ms} -> [Flezha.Diagnostics.MemoryReporter]
      :disabled -> []
    end
  end

  # Opt the convergence Loop into the supervision tree. Default `true`
  # in prod; tests turn it off so the global app boot does not tick
  # across suites. The `Snapshot` it publishes to is always started.
  defp convergence_loop_children do
    if Config.get(:convergence_health_enabled) do
      [Flezha.Convergence.Loop]
    else
      []
    end
  end

  # Like authz: omitted when `:schedules, :application_managed` is `false`
  # so tests can bring up isolated Store/Hub singletons against a per-test
  # tmp dir. The Hub owner starts the `:flz_schedules` hub off the boot path.
  defp schedule_children do
    if Config.get(:schedule_application_managed) do
      [Flezha.Schedule.Store, Flezha.Schedule.Hub]
    else
      []
    end
  end

  # `convergence-self-healing` §4.6-4.7: opt-in `/healthz`
  # listener. Off by default — only flezha_market opts in via its
  # ConfigMap so bare-metal users keep their own probe shape.
  defp healthz_children do
    if Config.get(:healthz_enabled) do
      [Flezha.Healthz.Server]
    else
      []
    end
  end

  defp transport_children do
    transports = Config.get(:transports)

    Enum.map(transports, fn transport ->
      Supervisor.child_spec(
        {Flezha.Transport.Server, [transport: transport]},
        id: {Flezha.Transport.Server, transport}
      )
    end)
  end
end

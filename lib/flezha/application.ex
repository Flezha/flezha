defmodule Flezha.Application do
  @moduledoc false

  alias Flezha.Runner.FleetManager

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
    if Application.get_env(:flezha, :controller_enabled, true) do
      start_controller()
    else
      require Logger
      Logger.info("[flezha] controller disabled — runner-only boot (no controller tree)")
      Supervisor.start_link([], strategy: :one_for_one, name: Flezha.Supervisor)
    end
  end

  defp start_controller() do
    # Before any state path is opened: a second controller over this state root
    # is two unsynchronised writers on one entity WAL tree, and the claim is
    # what makes that a refusal rather than corruption.
    _ = Flezha.Boot.StateRootLock.claim!()

    _ = Flezha.Boot.StatePathMigrator.migrate()

    # `[]` unless a remote durability backend is configured. Ahead of
    # HubManager because the boot-time cspec hydration among these children
    # must land before ProcessHub opens `registry.dets` at hub start.
    children =
      [
        # SignalHandler is started FIRST so OS signal handlers are
        # installed before any other service can observe a delivered
        # SIGTERM. The Coordinator is inserted right after HubManager
        # because drain queries the hub.
        Flezha.Shutdown.SignalHandler,
        Flezha.Service.EventManager,
        Flezha.BootLoader,
        Flezha.Service.Identificator,
        # Owns the recovery-state cache + the published convergence
        # snapshot; must be up before any hub fires recovery telemetry.
        Flezha.Convergence.Snapshot,
        # The cluster directory scope must be up before its writers:
        # HubManager joins hub pids, FleetManager joins capability groups.
        Flezha.Cluster.Directory
      ] ++
        Flezha.State.DurabilityTier.children() ++
        [
          Flezha.Service.HubManager,
          Flezha.Observability.Supervisor,
          Flezha.Shutdown.Coordinator,
          Flezha.Runner.FleetSupervisor,
          FleetManager,
          # Cluster formation must start BEFORE the transport accepts runner
          # connects, so a hub is never created on a not-yet-clustered node.
          # The transport must not accept connects before the cluster is
          # formed, so a hub is never created on a not-yet-clustered node.
          # `Flezha.Convergence.Loop` (started after the State trio below)
          # owns all bind/rebind/plugin-restart healing.
          Flezha.Cluster.Orchestrator
        ] ++
        authz_children() ++
        installer_children() ++
        transport_children() ++
        [
          Flezha.Service.PluginManager,
          Flezha.Plugin.Provider.Kafka.WorkerRegistry,
          Flezha.Plugin.Provider.Kafka.ProducerSup,
          Flezha.Plugin.Provider.Postgres.SinkSup,
          Flezha.Plugin.Provider.OperativeForward.ForwardSup,
          Flezha.Service.PortPilot,
          {Task.Supervisor, name: Flezha.Operative.MessageHandlerTaskSup}
        ] ++
        Flezha.State.Supervisor.application_children() ++
        [
          Flezha.State.FsyncBatcher,
          Flezha.State.Retention,
          Flezha.State.Watchdog,
          Flezha.Dlq.Store
        ] ++
        convergence_loop_children() ++
        schedule_children() ++
        healthz_children() ++
        memory_reporter_children()

    # Flezha.Testnet only compiles in :test (test/support/); ensure_loaded?
    # gates the child spec so prod boots without the module.
    children =
      if Application.get_env(:flezha, :testnet_enabled) and
           Code.ensure_loaded?(Flezha.Testnet) do
        [{Flezha.Testnet, []} | children]
      else
        children
      end

    opts = [strategy: :one_for_one, name: Flezha.Supervisor]

    # A child that refused to start already carries the reason an operator
    # needs — a claimed socket directory names the peer and the variable. Only
    # a tree that came up is asked whether it reached ready; asking a
    # supervisor that never started replaces that reason with `:noproc`.
    with {:ok, _pid} = result <- Supervisor.start_link(children, opts) do
      validate_boot_ready()
      result
    end
  end

  @impl true
  def prep_stop(state) do
    cond do
      not Application.get_env(:flezha, :controller_enabled, true) ->
        state

      Application.get_env(:flezha, :graceful_shutdown_enabled, true) ->
        drain_on_prep_stop()

      true ->
        safe_legacy_stop()
    end

    state
  end

  defp drain_on_prep_stop() do
    # The Coordinator owns the drain — it dispatches ProcessHub
    # `cluster_leave` (via HubManager.stop_hub_manager/0) inside its
    # drain phase and waits for peers to claim our Operatives before
    # finalising. Bounded by `:flezha, :graceful_shutdown_timeout_ms`.
    try do
      _ = Flezha.Shutdown.Coordinator.shutdown(:prep_stop)
    catch
      :exit, reason ->
        require Logger
        Logger.warning("[shutdown] coordinator unavailable in prep_stop: #{inspect(reason)}")
        # Coordinator isn't running (e.g. early-boot crash) — fall
        # back to the legacy direct call so the BEAM still tears
        # the hub down before halting.
        safe_legacy_stop()
    end
  end

  defp safe_legacy_stop() do
    try do
      Flezha.Service.HubManager.stop_hub_manager()
    catch
      :exit, _ -> :ok
    end
  end

  defp installer_children do
    if Application.get_env(:flezha, :plugin_installer_enabled, false) do
      [Flezha.Plugin.Installer.Supervisor]
    else
      []
    end
  end

  # Authn/authz supervisor is omitted from the global application
  # tree when `:authn_authz, :application_managed` is `false` so
  # unit tests can bring up isolated AuthState/RoleStore/etc.
  # against a per-test tmp `auth_dir`. The flag defaults `true` so
  # production and dev boot the subsystem unconditionally.
  defp authz_children do
    cfg = Application.get_env(:flezha, :authn_authz, [])

    if Keyword.get(cfg, :application_managed, true) do
      [Flezha.AuthZ.Supervisor]
    else
      []
    end
  end

  defp memory_reporter_children do
    case Flezha.Diagnostics.MemoryReporter.configured_interval_ms() do
      {:ok, _ms} -> [Flezha.Diagnostics.MemoryReporter]
      :disabled -> []
    end
  end

  # Opt the convergence Loop into the supervision tree. Default `true`
  # in prod; tests turn it off so the global app boot does not tick
  # across suites. The `Snapshot` it publishes to is always started.
  defp convergence_loop_children do
    if Application.get_env(:flezha, :convergence_health_enabled, true) do
      [Flezha.Convergence.Loop]
    else
      []
    end
  end

  # Like authz: omitted when `:schedules, :application_managed` is `false`
  # so tests can bring up isolated Store/Hub singletons against a per-test
  # tmp dir. The Hub owner starts the `:flz_schedules` hub off the boot path.
  defp schedule_children do
    cfg = Application.get_env(:flezha, :schedules, [])

    if Keyword.get(cfg, :application_managed, true) do
      [Flezha.Schedule.Store, Flezha.Schedule.Hub]
    else
      []
    end
  end

  # `convergence-self-healing` §4.6-4.7: opt-in `/healthz`
  # listener. Off by default — only flezha_market opts in via its
  # ConfigMap so bare-metal users keep their own probe shape.
  defp healthz_children do
    if Application.get_env(:flezha, :healthz_enabled, false) do
      [Flezha.Healthz.Server]
    else
      []
    end
  end

  defp transport_children do
    transports = Application.get_env(:flezha, :transports, [:uds])

    Enum.map(transports, fn transport ->
      Supervisor.child_spec(
        {Flezha.Transport.Server, [transport: transport]},
        id: {Flezha.Transport.Server, transport}
      )
    end)
  end

  defp validate_boot_ready() do
    case GenServer.call(Flezha.BootLoader, :boot_ready?, 30_000) do
      true -> :ok
      false -> raise "Boot not ready"
    end
  end
end

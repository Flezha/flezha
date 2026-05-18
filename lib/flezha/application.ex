defmodule Flezha.Application do
  @moduledoc false

  alias Flezha.Executor.FleetManager

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

    _ = Flezha.Boot.StatePathMigrator.migrate()

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
        Flezha.Service.HubManager,
        Flezha.Observability.Supervisor,
        Flezha.Shutdown.Coordinator,
        Flezha.Executor.FleetSupervisor,
        FleetManager
      ] ++
        authz_children() ++
        installer_children() ++
        transport_children() ++
        [
          Flezha.Service.PluginManager,
          Flezha.Plugin.Provider.Kafka.WorkerRegistry,
          Flezha.Plugin.Provider.Kafka.ProducerSup,
          Flezha.Service.PortPilot,
          Flezha.Cluster.Orchestrator,
          {Task.Supervisor, name: Flezha.Relay.MessageHandlerTaskSup}
        ] ++
        Flezha.State.Supervisor.application_children() ++
        [
          Flezha.State.FsyncBatcher,
          Flezha.State.Retention
        ] ++
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
    result = Supervisor.start_link(children, opts)

    validate_boot_ready()

    result
  end

  @impl true
  def prep_stop(state) do
    if Application.get_env(:flezha, :graceful_shutdown_enabled, true) do
      # The Coordinator owns the drain — it dispatches ProcessHub
      # `cluster_leave` (via HubManager.stop_hub_manager/0) inside its
      # drain phase and waits for peers to claim our Relays before
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
    else
      safe_legacy_stop()
    end

    state
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

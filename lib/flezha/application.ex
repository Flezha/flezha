defmodule Flezha.Application do
  @moduledoc false

  alias Flezha.Boot.Loader
  alias Flezha.Boot.Readiness
  alias Flezha.Boot.StatePathMigrator
  alias Flezha.Boot.StateRootLock
  alias Flezha.Config
  alias Flezha.Config.Environment
  alias Flezha.Process.DurableTasks
  alias Flezha.Plugin.Installer.Loader, as: PackageLoader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager.Lifecycle
  alias Flezha.Shutdown
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.DurabilityTier
  alias Flezha.State.Supervisor, as: StateSupervisor

  use Application

  @impl true
  def start(_type, _args) do
    restore_user_env()
    :ok = Environment.check!()

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
    # Before any state path is opened: a second controller over this state root
    # is two unsynchronised writers on one cell WAL tree, and the claim is
    # what makes that a refusal rather than corruption.
    _ = StateRootLock.claim!()

    _ = preload_code()

    _ = StatePathMigrator.migrate()

    # Every hub is built with this strategy later, off the boot path; built
    # here once, a split setting that cannot work stops the boot instead.
    _ = Lifecycle.partition_tolerance_strategy()

    # Before the children are computed: a package's providers are rows of the
    # provider table, whose worker families the tree supervises.
    _ = PackageLoader.load_all()

    # Owned here, for the application's life: a coordinator restart must not reopen admission.
    :ok = Shutdown.create_table()
    # Likewise: a writer restart storm that takes its supervisor down must leave
    # the tables the data plane reads in place.
    :ok = StateSupervisor.ensure_tables()

    children = controller_children()

    opts = [strategy: :one_for_one, name: Flezha.Supervisor]

    # A child that refused to start already carries the reason a user
    # needs — a claimed socket directory names the peer and the variable. Only
    # a tree that came up is asked whether it reached ready; asking a
    # supervisor that never started replaces that reason with `:noproc`.
    with {:ok, _pid} = result <- Supervisor.start_link(children, opts) do
      :ok = PackageLoader.announce()
      # Discovery loads every module; paid here rather than by the first stop or start.
      _ = Shutdown.all_steps()
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
        Coordinator.finalise()
    end

    state
  end

  defp drain_on_prep_stop() do
    _ = Coordinator.shutdown(:prep_stop)
  catch
    :exit, reason ->
      require Logger
      Logger.warning("[shutdown] coordinator unavailable in prep_stop: #{inspect(reason)}")
      Coordinator.finalise()
  end

  # The controller tree, in start order and stopped in reverse. A row is
  # `{child | (-> [child]), gate}`, gate `:always`, a config key or a predicate;
  # the comment above a row says why it sits there. The id list is pinned by
  # `Flezha.Boot.ApplicationChildrenTest`.
  @doc false
  def controller_children() do
    [
      # Listening before anything connects; the module compiles in :test only.
      {{Flezha.Testnet, []}, &testnet?/0},
      # Before any other service can observe a SIGTERM.
      {Flezha.Shutdown.SignalHandler, :always},
      {Loader, :always},
      {Flezha.Identificator, :always},
      # Owns the recovery-state cache, up before any hub fires recovery telemetry.
      {Flezha.Convergence.Snapshot, :always},
      # Before its writers: HubManager joins hub pids, FleetManager capability groups.
      {Flezha.Cluster.Directory, :always},
      # Its boot-time cspec hydration lands before a hub opens its `registry.dets`.
      {&DurabilityTier.children/0, :always},
      {Flezha.Service.HubManager, :always},
      {{Phoenix.PubSub, name: Flezha.Observability.PubSub}, :always},
      {Flezha.Observability.Supervisor, :always},
      {Flezha.Logs.Supervisor, :always},
      # After HubManager, because the drain queries the hub.
      {Coordinator, :always},
      {Flezha.Runner.FleetSupervisor, :always},
      {FleetManager, :always},
      # Before the transports, so no hub is created on a node not yet clustered.
      {Flezha.Cluster.Orchestrator, :always},
      {Flezha.AuthZ.Supervisor, :authz_application_managed},
      {Flezha.Service.Calls, :always},
      {Flezha.Observability.HookSink, :always},
      {&worker_families/0, :always},
      {{Task.Supervisor, name: Flezha.Relay.MessageHandlerTaskSup}, :always},
      {&StateSupervisor.application_children/0, :always},
      {Flezha.State.FsyncBatcher, :always},
      {Flezha.State.Retention, :always},
      {Flezha.State.Watchdog, :always},
      {Flezha.Dlq.Store, :always},
      # Stopped before the state side, so durable work ends while the writers are open.
      {DurableTasks, :always},
      # No hub or runner may reach a writer before the state side exists; the starter also
      # follows AuthZ, whose catalog is one source of the declared set.
      {Flezha.Service.HubManager.Starter, :auto_restore_enabled},
      {&transport_children/0, :always},
      # Launches runners, which dial the transports.
      {Flezha.Service.PortPilot, :always},
      # After the state side it heals.
      {Flezha.Convergence.Loop, :convergence_health_enabled},
      {Flezha.Schedule.Store, :schedule_application_managed},
      {Flezha.Schedule.Hub, :schedule_application_managed},
      {Flezha.Healthz.Server, :healthz_enabled}
    ]
    |> Enum.flat_map(&row_children/1)
  end

  defp row_children({child, gate}) do
    cond do
      !open?(gate) -> []
      is_function(child, 0) -> child.()
      true -> [child]
    end
  end

  defp open?(:always), do: true
  defp open?(gate) when is_function(gate, 0), do: gate.()
  defp open?(key), do: Config.get(key)

  # The launcher runs the VM with HOME at a private directory, only so OTP
  # reads the distribution cookie there (`rel/env.sh.eex`); processes the
  # controller spawns get the user's HOME back, and not the cookie, which
  # nothing past the boot reads. `:init.get_argument(:home)`, and so
  # `System.user_home/0`, keeps naming the private directory the emulator was
  # started with.
  defp restore_user_env() do
    case System.get_env("RELEASE_USER_HOME") do
      nil ->
        :ok

      home ->
        if home == "", do: System.delete_env("HOME"), else: System.put_env("HOME", home)
        Enum.each(~w(RELEASE_USER_HOME RELEASE_COOKIE), &System.delete_env/1)
    end
  end

  # The boot walks every module of the application (shutdown steps, declared
  # observability sources, stores, providers), and loaded lazily each one waits
  # alone for every scheduler to take its code, which a host short of CPU turns
  # into most of the boot. One batch prepares them in parallel and commits them
  # together. A release boots with its code loaded, so there it finds nothing.
  defp preload_code() do
    :code.ensure_modules_loaded(
      for app <- [:flezha, :process_hub],
          module <- Application.spec(app, :modules) || [],
          do: module
    )
  end

  defp testnet?(), do: Config.get(:testnet_enabled) and Code.ensure_loaded?(Flezha.Testnet)

  defp worker_families() do
    Enum.map(
      Flezha.Plugin.Providers.worker_families(),
      &{Flezha.Plugin.Provider.WorkerSup, name: &1}
    )
  end

  # The request supervisor first, because each UDS server hands it every `flz`
  # request.
  defp transport_children do
    servers =
      Enum.map(Flezha.Transport.Server.transports(), fn transport ->
        Supervisor.child_spec(
          {Flezha.Transport.Server,
           [transport: transport, cli_request: &Flezha.Cli.Socket.start/2]},
          id: {Flezha.Transport.Server, transport}
        )
      end)

    [{Task.Supervisor, name: Flezha.Cli.Socket.supervisor()} | servers]
  end
end

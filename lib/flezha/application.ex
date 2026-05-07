defmodule Flezha.Application do
  @moduledoc false

  alias Flezha.ComputeEngine.FleetManager

  require Logger

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

    children =
      [
        Flezha.Service.EventManager,
        Flezha.BootLoader,
        Flezha.Service.Identificator,
        Flezha.Service.HubManager,
        Flezha.ComputeEngine.FleetSupervisor,
        FleetManager
      ] ++
        installer_children() ++
        transport_children() ++
        [
          Flezha.Service.PluginManager,
          Flezha.Plugin.Provider.Kafka.WorkerRegistry,
          Flezha.Plugin.Provider.Kafka.ProducerSup,
          Flezha.Service.PortPilot,
          Flezha.Cluster.Orchestrator,
          {Task.Supervisor, name: Flezha.Processor.MessageHandlerTaskSup}
        ] ++
        Flezha.State.Supervisor.application_children() ++
        [
          Flezha.State.FsyncBatcher,
          Flezha.State.Retention
        ]

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

    log_overwatch_state_snapshot_config()
    validate_boot_ready()

    result
  end

  # Boot log for the Overwatch periodic state-snapshot task (design §D8).
  # Emits exactly once per agent boot.
  defp log_overwatch_state_snapshot_config do
    case Application.get_env(:flezha, :overwatch_state_snapshot_interval_ms, 60_000) do
      0 ->
        Logger.info("[overwatch] state_snapshot=disabled source_of_truth=postgres")

      n when is_integer(n) and n > 0 ->
        Logger.info("[overwatch] state_snapshot=interval_ms=#{n}")

      other ->
        Logger.info("[overwatch] state_snapshot=unrecognised(#{inspect(other)})")
    end
  end

  @impl true
  def prep_stop(_state) do
    # We need to stop the hub manager first to keep the compute_engines
    # running while processors are stopped.
    Flezha.Service.HubManager.stop_hub_manager()
  end

  defp installer_children do
    if Application.get_env(:flezha, :plugin_installer_enabled, false) do
      [Flezha.Plugin.Installer.Supervisor]
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

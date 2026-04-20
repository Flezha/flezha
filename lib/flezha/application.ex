defmodule Flezha.Application do
  @moduledoc false

  alias Flezha.ComputeEngine.FleetManager

  require Logger

  use Application

  @impl true
  def start(_type, _args) do
    children =
      [
        Flezha.Service.EventManager,
        Flezha.BootLoader
      ] ++
        recovery_children() ++
        [
          Flezha.Service.Identificator,
          Flezha.Service.HubManager,
          Flezha.ComputeEngine.FleetSupervisor,
          FleetManager
        ] ++
        transport_children() ++
        [
          Flezha.Service.PluginManager,
          Flezha.Service.PortPilot,
          Flezha.Cluster.Orchestrator
        ]

    # If Testnet is enabled, we start the TestnetSupervisor
    children =
      if Application.get_env(:flezha, :testnet_enabled) do
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
  def prep_stop(_state) do
    # We need to stop the hub manager first to keep the compute_engines
    # running while processors are stopped.
    Flezha.Service.HubManager.stop_hub_manager()
  end

  # Recovery subsystem children — Store → SysMode → Coordinator. Autostart is
  # controlled by `:flezha, :recovery_autostart`, which defaults to true and
  # is overridden to false in test env so each test manages its own isolated
  # instances under a `tmp_dir` fixture.
  defp recovery_children do
    if Application.get_env(:flezha, :recovery_autostart, true) do
      [
        Flezha.Recovery.Store,
        Flezha.Recovery.PluginStore,
        Flezha.Recovery.SysMode,
        Flezha.Recovery.Coordinator
      ]
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

defmodule Flezha.Boot.ApplicationChildrenTest do
  @moduledoc """
  The controller's start order, pinned.

  `Flezha.Application.controller_children/0` decides the whole boot sequence,
  and the comment on a row says why it sits where it does, which the compiler
  cannot see. A silent reorder surfaces as an
  intermittent boot failure, never as a test failure, so the ordered id list is
  asserted here literally.
  """

  # Pins `:flezha` application env for the duration of a test.
  use FlezhaTest.Case, fixtures: [:env], async: false

  @pinned [
    transports: [:uds],
    authn_authz: [application_managed: true],
    convergence_health_enabled: true,
    schedules: [application_managed: true],
    auto_restore_enabled: true,
    healthz_enabled: true,
    testnet_enabled: false
  ]

  @order [
    Flezha.Shutdown.SignalHandler,
    Flezha.Boot.Loader,
    Flezha.Identificator,
    Flezha.Convergence.Snapshot,
    :flz_directory,
    Flezha.Service.HubManager,
    Phoenix.PubSub.Supervisor,
    Flezha.Observability.Supervisor,
    Flezha.Logs.Supervisor,
    Flezha.Shutdown.Coordinator,
    Flezha.Runner.FleetSupervisor,
    Flezha.Runner.FleetManager,
    Flezha.Cluster.Orchestrator,
    Flezha.AuthZ.Supervisor,
    Flezha.Service.Calls,
    Flezha.Observability.HookSink,
    # One worker family per provider that declares a worker, from the provider
    # table, in its order.
    Flezha.Plugin.Provider.HookWebhook.WorkerSup,
    Flezha.Plugin.Provider.MonitoredIOForward.WorkerSup,
    Flezha.Plugin.Provider.File.Source.WorkerSup,
    Flezha.Plugin.Provider.File.Sink.WorkerSup,
    Flezha.Plugin.Provider.RelayForward.WorkerSup,
    Flezha.Plugin.Provider.HttpPoll.WorkerSup,
    Flezha.Plugin.Provider.HttpPush.WorkerSup,
    Flezha.Plugin.Provider.Postgres.WorkerSup,
    Flezha.Plugin.Provider.PostgresPoll.WorkerSup,
    Flezha.Plugin.Provider.Websocket.WorkerSup,
    # The running copies of shareable plugins, one per plugin on this controller.
    Flezha.Plugin.Shared.WorkerSup,
    Flezha.Relay.MessageHandlerTaskSup,
    Flezha.State.WriterRegistry,
    Flezha.State.Supervisor,
    Flezha.State.FsyncBatcher,
    Flezha.State.Retention,
    Flezha.State.Watchdog,
    Flezha.Dlq.Store,
    Flezha.Process.DurableTasks,
    Flezha.Service.HubManager.Starter,
    Flezha.Cli.Socket.Supervisor,
    {Flezha.Transport.Server, :uds},
    Flezha.Service.PortPilot,
    Flezha.Convergence.Loop,
    Flezha.Schedule.Store,
    Flezha.Schedule.Hub,
    Flezha.Healthz.Server
  ]

  setup do
    Enum.each(@pinned, fn {k, v} -> Application.put_env(:flezha, k, v) end)
    :ok
  end

  defp ids,
    do: Enum.map(Flezha.Application.controller_children(), &Supervisor.child_spec(&1, []).id)

  test "the controller starts its children in exactly this order" do
    assert ids() == @order
  end

  test "the hub starter is absent when auto-restore is off" do
    Application.put_env(:flezha, :auto_restore_enabled, false)
    refute Flezha.Service.HubManager.Starter in ids()
  end

  test "the testnet child is prepended, never appended" do
    Application.put_env(:flezha, :testnet_enabled, true)
    assert hd(ids()) == Flezha.Testnet
  end

  # fails when: the boot leaves the application's or its hub library's code to
  # load one module at a time on first use.
  test "a started controller has its own and its hub library's code loaded" do
    lazy =
      for app <- [:flezha, :process_hub],
          module <- Application.spec(app, :modules),
          !:code.is_loaded(module),
          do: module

    assert lazy == []
  end
end

defmodule FlezhaTest.Runner.FleetManagerHubTeardownRaceTest do
  @moduledoc """
  KI-69 reproduction: a runner's `connect_control_channel` frame lands while its
  namespace's hub is being torn down.

  Everything here is real — a real ProcessHub coordinator, the real
  `HubManager.stop_hub/1` teardown, the real `Transport.Server` acceptor callback
  body, the real node-wide `FleetManager`. The only test seam is `:sys.suspend/1`
  on the coordinator, which holds it in the state the sighting caught it in: a
  coordinator with a `$gen_call` in its mailbox that its supervisor is about to
  shut down. Nothing is stubbed and no exit is injected.
  """
  # async: false: the live controller, its fleet, and a real named hub.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  import ExUnit.CaptureLog

  alias Flezha.Boot.RecoveryHooks
  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Transport.Server
  alias FlezhaTest.Helper.MessageTestHelper

  test "a connect racing the hub teardown must not take the acceptor or the fleet manager down" do
    ns = "ki69_race_#{System.unique_integer([:positive])}"
    hub_id = Identificator.hub(ns)

    {:ok, _} = HubManager.start_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    hub_pid = Process.whereis(hub_id)
    assert is_pid(hub_pid)

    fleet_pid = Process.whereis(FleetManager)
    fleet_ref = Process.monitor(fleet_pid)
    MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)

    # The coordinator is occupied — exactly as it is mid-drain, when the sighting
    # happened 30 ms after `[shutdown] drain ns=default force-migrated 1 …`.
    :ok = :sys.suspend(hub_pid)

    test = self()

    log =
      capture_log(fn ->
        {acceptor, acceptor_ref} =
          spawn_monitor(fn ->
            state = %{transport: :uds, pending_streams: %{7 => make_ref()}}

            frame =
              {:flz_data, 7, "connect_control_channel",
               Jason.encode!(%{"namespace" => ns, "factories" => []}), %{}}

            send(test, {:handled, Server.handle_info(frame, state)})
          end)

        # The connect's `register_hook_handlers` call is now queued on the coordinator.
        assert wait_for_queued_call(hub_pid), "the connect never reached the hub coordinator"

        # The real teardown path: what `runner_cleanup` and `stop_namespace` run.
        HubManager.stop_hub(ns)

        receive do
          {:DOWN, ^acceptor_ref, :process, ^acceptor, reason} ->
            flunk("""
            the acceptor died on a connect racing the hub teardown:
              #{inspect(reason)}
            fleet_manager alive?: #{inspect(Process.alive?(fleet_pid))}
            """)

          {:handled, {:noreply, _state}} ->
            :ok
        after
          10_000 -> flunk("the connect neither completed nor died")
        end
      end)

    refute_received {:DOWN, ^fleet_ref, :process, _, _}
    assert Process.whereis(FleetManager) == fleet_pid, "the fleet manager was restarted"
    Process.demonitor(fleet_ref, [:flush])

    # The refusal is loud and names the namespace at every layer that reports it.
    assert log =~ "could not register its operative-insertion hook"
    assert log =~ ns
    assert log =~ "Rejected runner connect"

    assert_received {:rejected, %{count: 1}, %{reason: {:hub_unavailable, ^ns}, transport: :uds}}
  end

  test "recovery-hook registration racing the hub teardown refuses instead of exiting its caller" do
    ns = "ki178_race_#{System.unique_integer([:positive])}"
    hub_id = Identificator.hub(ns)

    {:ok, _} = HubManager.start_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    # Armed only now: `config/test.exs` holds auto-restore off, and a hub started
    # with it on boots into `:recovering` and holds there. `register/1` is a
    # no-op while it is off, so the call under test needs it on.
    previous = Application.get_env(:flezha, :auto_restore_enabled)
    Application.put_env(:flezha, :auto_restore_enabled, true)
    on_exit(fn -> Application.put_env(:flezha, :auto_restore_enabled, previous) end)

    hub_pid = Process.whereis(hub_id)
    assert is_pid(hub_pid)

    # `register/1` runs inside `FleetManager`'s own `handle_call`, so an exit
    # here takes the node's runner roster and its acceptor with it. The stand-in
    # is a monitored caller: what must survive is the process that called.
    :ok = :sys.suspend(hub_pid)

    test = self()
    MessageTestHelper.forward_telemetry([:flezha, :hub, :unavailable], :hub_unavailable)

    {caller, caller_ref} =
      spawn_monitor(fn -> send(test, {:registered, RecoveryHooks.register(hub_id)}) end)

    assert wait_for_queued_call(hub_pid), "the registration never reached the hub coordinator"

    HubManager.stop_hub(ns)

    receive do
      {:DOWN, ^caller_ref, :process, ^caller, reason} ->
        flunk("register/1 exited its caller on a dying hub: #{inspect(reason)}")

      {:registered, answer} ->
        assert answer == {:error, {:hub_unavailable, ns}},
               "a dying hub must be the caller's answer, got #{inspect(answer)}"
    after
      10_000 -> flunk("register/1 neither answered nor died")
    end

    # A containment is loud: the namespace's own failure is reported as one, so
    # it reads apart from a node-wide fault.
    assert_receive {:hub_unavailable, %{count: 1}, %{hub_id: ^hub_id}}, 1_000
  end

  defp wait_for_queued_call(pid, attempts \\ 200)
  defp wait_for_queued_call(_pid, 0), do: false

  defp wait_for_queued_call(pid, attempts) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, len} when len > 0 ->
        true

      _ ->
        Process.sleep(5)
        wait_for_queued_call(pid, attempts - 1)
    end
  end
end

defmodule FlezhaTest.Runner.FleetManagerHubTeardownRaceTest do
  @moduledoc """
  KI-178 reproduction: a hub's recovery-hook registration lands while the hub is
  being torn down.

  Everything here is real — a real ProcessHub coordinator and the real
  `HubManager.stop_hub/1` teardown. The only test seam is `:sys.suspend/1` on
  the coordinator, which holds it in the state the sighting caught it in: a
  coordinator with a `$gen_call` in its mailbox that its supervisor is about to
  shut down. Nothing is stubbed and no exit is injected.

  A runner's connect makes no call to a hub coordinator, so it has no share in
  this race.
  """
  # async: false: the live controller, its fleet, and a real named hub.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Flezha.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.HubManager.Lifecycle
  alias FlezhaTest.Helper.MessageTestHelper

  test "recovery-hook registration racing the hub teardown refuses instead of exiting its caller" do
    ns = "ki178_race_#{System.unique_integer([:positive])}"
    hub_id = Identificator.hub(ns)

    {:ok, _} = HubManager.start_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    # Armed only now: `config/test.exs` holds auto-restore off, and a hub started
    # with it on boots into `:recovering` and holds there. Hook registration is
    # a no-op while it is off, so the call under test needs it on.
    previous = Application.get_env(:flezha, :auto_restore_enabled)
    Application.put_env(:flezha, :auto_restore_enabled, true)
    on_exit(fn -> Application.put_env(:flezha, :auto_restore_enabled, previous) end)

    hub_pid = Process.whereis(hub_id)
    assert is_pid(hub_pid)

    # `on_hub_started/3` runs inside `FleetManager`'s own `handle_call`, so an
    # exit here takes the node's runner roster and its acceptor with it. The
    # stand-in is a monitored caller: what must survive is the process that
    # called. Driven through the post-start hook the hub's own start path runs,
    # not through the hook registrar it reaches — the registrar carries no
    # guard of its own, because `boot` may not name `service`.
    :ok = :sys.suspend(hub_pid)

    test = self()
    MessageTestHelper.forward_telemetry([:flezha, :hub, :unavailable], :hub_unavailable)

    {caller, caller_ref} =
      spawn_monitor(fn ->
        send(test, {:started, Lifecycle.on_hub_started(hub_id, hub_pid, ns)})
      end)

    assert wait_for_queued_call(hub_pid), "the registration never reached the hub coordinator"

    HubManager.stop_hub(ns)

    receive do
      {:DOWN, ^caller_ref, :process, ^caller, reason} ->
        flunk("on_hub_started/3 exited its caller on a dying hub: #{inspect(reason)}")

      {:started, answer} ->
        assert answer == :ok,
               "a dying hub must be the caller's answer, got #{inspect(answer)}"
    after
      10_000 -> flunk("on_hub_started/3 neither answered nor died")
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

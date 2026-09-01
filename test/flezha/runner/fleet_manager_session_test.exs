defmodule FlezhaTest.Runner.FleetManagerSessionTest do
  @moduledoc """
  Reconnect binding continuity (runner-controller-connection-healing, Phase 2).

  A runner that restarts on the same persistence path re-presents the same
  durable STABLE IDENTITY. The controller keys operatives on the per-connection
  session token, so on reconnect it SWAPS the prior session's bindings onto the
  new session token (no mint-and-evict, no lost operative) — exactly the
  recurring-outage fix. This drives a real Rust runner through the proven
  stop → restart pattern (matrix 18.7) with a fixed `FLEZHA_RUNNER_ID_PATH`,
  and asserts the operative follows the runner onto its new session.
  """
  # async: false: Mimic global mode; Mimic stubs; a shared on-disk root.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  @moduletag :capture_log

  setup :set_mimic_global_until_exit

  alias Flezha.Identificator
  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Service.ProcessRegistry

  @namespace "reconnect-binding-test"
  @factory "do_nothing"
  @stale_runner "runner_from_a_previous_boot"

  setup do
    id_path =
      Path.join(System.tmp_dir!(), "flezha-stable-id-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      FleetManager.stop_namespace(@namespace)
      File.rm_rf(id_path)
    end)

    {:ok, id_path: id_path}
  end

  test "an operative survives a runner restart via the stable-id binding swap", %{
    id_path: id_path
  } do
    env = [{"FLEZHA_RUNNER_ID_PATH", id_path}]

    # First boot: the SDK derives + persists a stable id; the operative binds to
    # this connection's session token (session1).
    session1 = NodeTestHelper.start_runner(@namespace, env: env)
    assert session1 in FleetManager.active_runners(@namespace)

    operative = %Flezha.Operative.Builder{id: "reconnect_binding_op", factory_id: @factory}
    {:ok, _} = OperativeManager.start_operative(@namespace, operative)
    assert bound_to?(session1, operative.id)

    # Restart on the SAME persistence path → the fresh process re-presents the
    # SAME stable id. The controller mints session2 and swaps the operative's
    # binding onto it rather than minting-and-evicting.
    NodeTestHelper.stop_runner(@namespace, session1)

    session2 = NodeTestHelper.start_runner(@namespace, env: env)
    assert session2 != session1, "restart must mint a fresh session token"
    assert session2 in FleetManager.active_runners(@namespace)

    # The operative now belongs to the new session, and the old session is gone.
    assert NodeTestHelper.wait_until(fn -> bound_to?(session2, operative.id) end, 1_000, 25),
           "operative must follow the runner onto its new session token"

    refute session1 in FleetManager.active_runners(@namespace)
  end

  # fails when: a warm-restarted operative binds to a live runner but its durable
  # row keeps naming the previous boot's runner and the live runner's
  # StreamChannels never learn its pid — every output frame is dropped while the
  # operative still lists as placed and healthy, and no reconciler sees it.
  test "an operative binding away from its recorded runner republishes the binding" do
    session = NodeTestHelper.start_runner(@namespace)
    operative = %Builder{id: "rebind_repair_op", factory_id: @factory}

    # The durable row a degraded recovery replay leaves behind: cspec args and
    # tag both name a runner whose boot is gone.
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)

    HubRegistryTestHelper.inject_operative(@namespace, operative.id, [{node(), self()}], %{
      id: operative.id,
      start: {Overwatch, :start_link, [{@namespace, @stale_runner, operative}]}
    })

    :ok = HubManager.operative_set_runner(@namespace, operative.id, @stale_runner)
    refute bound_to?(session, operative.id), "setup must start from the stale tag"

    # `dist_sup` starts persisted children from exactly this cspec on a warm
    # restart, with no `alter_child_data` in between.
    # Linked, so it is torn down with the test process.
    {:ok, overwatch} = Overwatch.start_link({@namespace, @stale_runner, operative})

    assert NodeTestHelper.wait_until(fn -> bound_to?(session, operative.id) end, 10_000, 25),
           "the registry tag still names the dead runner, so nothing can reconcile the binding"

    assert NodeTestHelper.wait_until(
             fn -> mapped?(session, operative.id, overwatch) end,
             10_000,
             25
           ),
           "the live runner's StreamChannels never learned the operative pid, so every output frame hits the `[] -> :ok` drop"
  end

  # fails when: the rebind publishes the runner `await_runner` handed back rather
  # than the one `complete_init` re-resolved and actually started the operative
  # on, so the durable row and the StreamChannel route both name a runner the
  # operative does not run on — the same silent output drop, one layer down.
  test "the rebind publishes the runner the operative actually started on" do
    session = NodeTestHelper.start_runner(@namespace)
    operative = %Builder{id: "retry_rebind_op", factory_id: @factory}

    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)

    HubRegistryTestHelper.inject_operative(@namespace, operative.id, [{node(), self()}], %{
      id: operative.id,
      start: {Overwatch, :start_link, [{@namespace, @stale_runner, operative}]}
    })

    :ok = HubManager.operative_set_runner(@namespace, operative.id, @stale_runner)

    # The awaited runner goes away before the SDK start, so `complete_init`'s own
    # re-resolve moves the operative onto `session` — the runner it ends up on.
    stub(OperativeManager, :await_runner, fn _, _, _, _, _ -> {:ok, @stale_runner} end)
    stub(OperativeManager, :assign_runner, fn _, _, _ -> session end)

    {:ok, overwatch} = Overwatch.start_link({@namespace, @stale_runner, operative})
    assert :sys.get_state(overwatch).runner_id == session

    assert NodeTestHelper.wait_until(fn -> bound_to?(session, operative.id) end, 10_000, 25),
           "the tag names the awaited runner, not the one the operative started on"

    assert NodeTestHelper.wait_until(
             fn -> mapped?(session, operative.id, overwatch) end,
             10_000,
             25
           ),
           "the route was seeded into the awaited runner's channels, not the live one's"
  end

  # fails when: the start request's `update_registry` lands after the Overwatch
  # published its rebind and clobbers `metadata.tag` back to the dead runner.
  # Silent and permanent: `Probe` reads the Overwatch's own `runner_id`, never
  # the tag, so `Diff` never classifies it and nothing repairs it.
  test "a registry write landing after a rebind does not strand the stale tag" do
    session = NodeTestHelper.start_runner(@namespace)
    operative = %Builder{id: "reassert_tag_op", factory_id: @factory}
    {:ok, _} = OperativeManager.start_operative(@namespace, operative)
    assert NodeTestHelper.wait_until(fn -> bound_to?(session, operative.id) end, 1_000, 25)

    hub_id = Identificator.hub(@namespace)
    hub = ProcessHub.Coordinator.get_hub(hub_id)
    {cspec, node_pids, _meta} = ProcessRegistry.lookup(hub_id, operative.id, with_metadata: true)

    # Byte-for-byte what `update_registry` writes when it wins the race: the
    # altered child_data's metadata, carrying the runner alter resolved.
    :ok =
      ProcessRegistry.insert(hub_id, cspec, node_pids,
        metadata: %{tag: @stale_runner},
        hook_storage: hub.storage.hook
      )

    assert NodeTestHelper.wait_until(fn -> bound_to?(session, operative.id) end, 10_000, 25),
           "the late registry write left the tag on the dead runner, where no reconciler can see it"
  end

  # True when one of `session`'s primary StreamChannels routes `operative_id` to
  # `pid` — the mapping whose absence makes `stream_channel.ex` drop frames.
  defp mapped?(session, operative_id, pid) do
    Identificator.stream_sup_primary(session)
    |> Supervisor.which_children()
    |> Enum.any?(fn
      {_, channel, _, _} when is_pid(channel) ->
        :sys.get_state(channel)[:operative_mapping][operative_id] == [pid]

      _ ->
        false
    end)
  catch
    :exit, _ -> false
  end

  defp bound_to?(session_token, operative_id) do
    HubManager.runner_operatives(@namespace, session_token)
    |> Enum.any?(fn {oid, _node_pids} -> oid == operative_id end)
  end
end

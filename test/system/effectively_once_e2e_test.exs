defmodule FlezhaTest.System.EffectivelyOnceE2ETest do
  @moduledoc """
  §6.1 end-to-end proof for `effectively-once-output` against a live Rust
  runner: a source drives a tier relay (`tier_store`, declared
  `effectively_once()`) whose keyed outputs land on a conforming changelog
  sink (BeamMsgForward with metadata → upsert by `flz.key`, dedup ledger by
  the durable identity). The runner is SIGKILLed mid-stream while inputs
  keep flowing (journaled, re-forwarded on rebind), and the controller's
  state layer is crashed with a staled flush cursor (forcing a full outbox
  re-drive). The sink's final state must equal the no-crash run's final
  state: last value per key, every duplicate byte-identical under an
  identical durable identity.

  Tagged `:system` (boots real subprocesses); run with
  `mix test --include system test/system/effectively_once_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :effectively_once

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness
  import FlezhaTest.Helper.StepsResumeHarness, only: [outboxes_settled?: 1]

  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.InputLog
  alias FlezhaTest.Fixtures.RelayFixture

  @moduletag timeout: 300_000

  @factory "tier_store"
  @tasklet "tier_store_tasklet"

  test "kills at each phase boundary converge the changelog sink to the no-crash state" do
    namespace = unique("eff_once")
    relay_id = unique("tier_store")
    id_path = unique_id_path()
    cell_id = {relay_id, @tasklet, ""}

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    # --- Phase 0: boot, bind, sink ------------------------------------------
    {port_a, exec_a} = boot_runner(namespace, id_path)

    # The sink rides the builder opts so every Overwatch restart (the
    # controller-crash phases) re-prepares it from the cspec.
    sink = %Flezha.Plugin{
      id: "tier_sink",
      namespace: namespace,
      relay_id: relay_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    builder = %Builder{
      id: relay_id,
      factory_id: @factory,
      init_values: %{},
      opts: [bindings: RelayFixture.bindings(namespace, relay_id, [sink])]
    }

    assert {:ok, info} = RelayManager.start_relay(namespace, builder)

    assert info.runner_id == exec_a

    # The SDK-declared tier must have been resolved at bind.
    assert wait_until(
             fn -> ClassRegistry.effectively_once?(relay_id) end,
             30_000
           ),
           "the tier declaration never reached the controller"

    # --- Phase 1: clean stream ------------------------------------------------
    send_inputs(relay_id, namespace, ["k1:a", "k2:a", "k1:b"])
    assert await_changelog(%{"k1" => "k1=b", "k2" => "k2=a"}, 30_000)

    # --- Phase 2: runner SIGKILL mid-stream ---------------------------------
    sigkill(os_pid(port_a))

    # Inputs while no runner is alive: journaled, undeliverable — the
    # rebind's journal replay must recover them.
    send_inputs(relay_id, namespace, ["k2:b", "k3:a"])

    {_port_b, exec_b} = boot_runner(namespace, id_path)
    assert exec_b != exec_a

    assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end, 30_000)

    assert await_changelog(
             %{"k1" => "k1=b", "k2" => "k2=b", "k3" => "k3=a"},
             60_000
           ),
           "journaled inputs were not replayed after the runner kill"

    # --- Phase 3: controller state-layer crash with a staled cursor ----------
    # Brutally kill the relay's writers (bypassing terminate/2) and stale
    # the flush cursor, then force a rebind: the restart must RE-DRIVE every
    # durable output with its original identity, and the sink must converge
    # to the same changelog regardless.
    stale_cursor_and_crash_writers(relay_id, cell_id)
    force_fresh_rebind(namespace, relay_id)

    send_inputs(relay_id, namespace, ["k1:c"])

    expected_final = %{"k1" => "k1=c", "k2" => "k2=b", "k3" => "k3=a"}
    assert await_changelog(expected_final, 60_000)

    # Once the outbox owes nothing, run the conformance audit over EVERYTHING
    # the sink ever received (including re-driven duplicates).
    assert await_outbox_drained(relay_id), "the outbox never drained"
    outputs = drain_outputs([])

    assert changelog(outputs) == expected_final,
           "changelog upsert diverged from the no-crash final state"

    for {msg, md} <- outputs do
      assert md["flz.epoch"], "tier output missing its epoch stamp: #{inspect(md)}"
      assert md["flz.cell"], "tier output missing its cell stamp"
      assert md["flz.seq"], "tier output missing its outbox seq"
      assert md["flz.key"], "tier output missing its natural key: #{inspect(msg)}"
    end

    # Exact dedup contract: one identity never carries two different
    # payloads, and every logical output appears under at least one identity.
    assert_identities_are_consistent(outputs)

    duplicates = Enum.count(by_identity(outputs), fn {_id, payloads} -> length(payloads) > 1 end)

    assert duplicates > 0,
           "the staled cursor must force at least one identity-identical re-drive"

    # And the input-log watermark advanced past every consumed offset (all
    # six inputs durably consumed → retention can truncate).
    assert wait_until(fn -> (InputLog.watermark(relay_id) || -1) >= 5 end, 30_000),
           "min watermark never caught up to the consumed inputs"
  end

  # The pass's record for `k:hold` is durable and the tally's is not: the
  # runner dies with the tally blocked on it. The forward skips the re-forwarded
  # input, so the tally has to receive the value from the pass's outbox.
  test "a value lost from the consumer's mailbox is delivered again" do
    namespace = unique("eff_once_edge")
    relay_id = unique("tier_chain")
    id_path = unique_id_path()
    forward = {relay_id, "tier_pass", ""}

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    {port_a, _exec_a} = boot_runner(namespace, id_path)

    sink = %Flezha.Plugin{
      id: "tier_sink",
      namespace: namespace,
      relay_id: relay_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    builder = %Builder{
      id: relay_id,
      factory_id: "tier_chain",
      init_values: %{},
      opts: [bindings: RelayFixture.bindings(namespace, relay_id, [sink])]
    }

    assert {:ok, _info} = RelayManager.start_relay(namespace, builder)
    send_inputs(relay_id, namespace, ["k:a"])
    assert await_changelog(%{"k" => "k=1"}, 30_000)

    send_inputs(relay_id, namespace, ["k:hold"])

    assert wait_until(fn -> {forward, 1} in InputLog.cell_offsets(relay_id) end, 30_000),
           "the pass's record for the held input never became durable: " <>
             inspect({InputLog.cell_offsets(relay_id), InputLog.tail(relay_id), peek_outputs()})

    sigkill(os_pid(port_a))
    boot_runner(namespace, id_path, env: [{"FLEZHA_STEPS_E2E_RELEASE", "1"}])

    assert await_changelog(%{"k" => "k=2"}, 60_000),
           "the tally never took the value the pass had committed: " <>
             inspect(changelog(peek_outputs()))

    # A later input is tallied behind anything the crash left to deliver, so
    # once it is consumed and its count delivered, a value tallied twice shows.
    send_inputs(relay_id, namespace, ["k:a"])
    assert wait_until(fn -> InputLog.watermark(relay_id) == 2 end, 30_000)
    assert outboxes_settled?(relay_id), "a Cell writer still owes output"
    assert changelog(peek_outputs()) == %{"k" => "k=3"}
  end
end

defmodule FlezhaTest.System.EffectivelyOnceE2ETest do
  @moduledoc """
  §6.1 end-to-end proof for `effectively-once-output` against a live Rust
  executor: a source drives a tier operative (`tier_store`, declared
  `effectively_once()`) whose keyed outputs land on a conforming changelog
  sink (BeamMsgForward with metadata → upsert by `flz.key`, dedup ledger by
  the durable identity). The executor is SIGKILLed mid-stream while inputs
  keep flowing (journaled, re-forwarded on rebind), and the controller's
  state layer is crashed with a staled flush cursor (forcing a full outbox
  re-drive). The sink's final state must equal the no-crash run's final
  state: last value per key, every duplicate byte-identical under an
  identical durable identity.

  Tagged `:system` (boots real subprocesses); run with
  `mix test --include system test/system/effectively_once_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Executor.FleetManager
  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager
  alias Flezha.State.InputLog

  @moduletag :system
  @moduletag :effectively_once_e2e
  @moduletag timeout: 300_000

  @factory "tier_store"
  @operation "tier_store_operation"

  test "kills at each phase boundary converge the changelog sink to the no-crash state" do
    namespace = unique("eff_once")
    operative_id = unique("tier_store")
    id_path = unique_id_path()
    segment_id = {operative_id, @operation, ""}

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    # --- Phase 0: boot, bind, sink ------------------------------------------
    {port_a, exec_a} = boot_executor(namespace, id_path)

    # The sink rides the builder opts so every Overwatch restart (the
    # controller-crash phases) re-prepares it from the cspec.
    sink = %Flezha.Plugin{
      id: "tier_sink",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      subscriptions: [{:on_output, []}]
    }

    builder = %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{},
      opts: [plugins: [sink]]
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)
    assert info.executor_id == exec_a

    # The SDK-declared tier must have been resolved at bind.
    assert wait_until(
             fn -> Flezha.Operative.ClassRegistry.effectively_once?(operative_id) end,
             30_000
           ),
           "the tier declaration never reached the controller"

    # --- Phase 1: clean stream ------------------------------------------------
    send_inputs(operative_id, namespace, ["k1:a", "k2:a", "k1:b"])
    assert await_changelog(%{"k1" => "k1=b", "k2" => "k2=a"}, 30_000)

    # --- Phase 2: executor SIGKILL mid-stream ---------------------------------
    sigkill(os_pid(port_a))

    # Inputs while no executor is alive: journaled, undeliverable — the
    # rebind's journal replay must recover them.
    send_inputs(operative_id, namespace, ["k2:b", "k3:a"])

    {_port_b, exec_b} = boot_executor(namespace, id_path)
    assert exec_b != exec_a

    assert wait_until(fn -> exec_a not in FleetManager.active_executors(namespace) end, 30_000)
    force_fresh_rebind(namespace, operative_id)

    assert await_changelog(
             %{"k1" => "k1=b", "k2" => "k2=b", "k3" => "k3=a"},
             60_000
           ),
           "journaled inputs were not replayed after the executor kill"

    # --- Phase 3: controller state-layer crash with a staled cursor ----------
    # Brutally kill the operative's writers (bypassing terminate/2) and stale
    # the flush cursor, then force a rebind: the restart must RE-DRIVE every
    # durable output with its original identity, and the sink must converge
    # to the same changelog regardless.
    stale_cursor_and_crash_writers(operative_id, segment_id)
    force_fresh_rebind(namespace, operative_id)

    send_inputs(operative_id, namespace, ["k1:c"])

    expected_final = %{"k1" => "k1=c", "k2" => "k2=b", "k3" => "k3=a"}
    assert await_changelog(expected_final, 60_000)

    # Drain the quiet window, then run the conformance audit over EVERYTHING
    # the sink ever received (including re-driven duplicates).
    Process.sleep(1_000)
    outputs = drain_outputs([])

    assert changelog(outputs) == expected_final,
           "changelog upsert diverged from the no-crash final state"

    for {msg, md} <- outputs do
      assert md["flz.epoch"], "tier output missing its epoch stamp: #{inspect(md)}"
      assert md["flz.segment"], "tier output missing its segment stamp"
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
    assert wait_until(fn -> (InputLog.watermark(operative_id) || -1) >= 5 end, 30_000),
           "min watermark never caught up to the consumed inputs"
  end
end

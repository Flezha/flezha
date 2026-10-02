defmodule FlezhaTest.System.EffectivelyOnceCheckpointE2ETest do
  @moduledoc """
  End-to-end proof for `effectively-once-checkpoint-granularity` against a live
  Rust runner: a source drives `checkpoint_store` (declared
  `effectively_once()` + `ack_checkpoint()`), whose keyed outputs land on a
  conforming changelog sink. Every input folds into the Cell's state, but
  only a `commit:` input closes the interval — so the durable consumed-offset
  stands still for a whole interval while the outputs of that interval are
  already flowing.

  The runner is SIGKILLed mid-interval. Recovery must restore the state to the
  last checkpoint and re-forward the interval's inputs, and the sink must
  converge to the same changelog as a no-crash run — the interval's re-emitted
  outputs absorbed by upsert on `flz.key`, and the offset never resuming past an
  input the restored state did not fold in.

  Tagged `:system`; run with
  `mix test --include system test/system/effectively_once_checkpoint_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :effectively_once

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Operator.Builder
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.InputLog
  alias FlezhaTest.Fixtures.OperatorFixture

  @moduletag timeout: 300_000

  @factory "checkpoint_store"

  test "the offset holds for the interval, and a mid-interval kill replays it" do
    namespace = unique("cp_ack")
    operator_id = unique("checkpoint_store")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operator_id, id_path) end)

    {port_a, exec_a} = boot_runner(namespace, id_path)

    sink = %Flezha.Plugin{
      id: "tier_sink",
      namespace: namespace,
      operator_id: operator_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    builder = %Builder{
      id: operator_id,
      factory_id: @factory,
      init_values: %{},
      opts: [bindings: OperatorFixture.bindings(namespace, operator_id, [sink])]
    }

    assert {:ok, info} = OperatorManager.start_operator(namespace, builder)

    assert info.runner_id == exec_a

    assert wait_until(fn -> ClassRegistry.effectively_once?(operator_id) end, 30_000),
           "the tier declaration never reached the controller"

    assert wait_until(fn -> ClassRegistry.ack_checkpoint?(operator_id) end, 30_000),
           "the checkpoint ack-granularity declaration never reached the controller"

    # --- Phase 1: an open interval acknowledges nothing --------------------
    # Offsets 0 and 1. Their outputs flush as they happen (checkpointing moves
    # the acknowledgement, not the output) — but nothing is durably consumed.
    send_inputs(operator_id, namespace, ["k1:a", "k2:a"])
    assert await_changelog(%{"k1" => "k1=a", "k2" => "k2=a"}, 30_000)

    # The outputs are out, and the watermark has still never moved.
    refute InputLog.watermark(operator_id),
           "an open interval must not advance the durable consumed-offset"

    # --- Phase 2: the checkpoint closes the interval ------------------------
    # Offset 2. One record carries the interval's accumulated state AND the
    # offset that acknowledges every input folded into it.
    send_inputs(operator_id, namespace, ["commit:1"])

    assert wait_until(fn -> InputLog.watermark(operator_id) == 2 end, 30_000),
           "the checkpoint never advanced the watermark to the interval's closing offset"

    # --- Phase 3: SIGKILL mid-interval --------------------------------------
    # Offsets 3 and 4 fold into a NEW interval and are never checkpointed. Their
    # state is not durable; their outputs already are. The kill abandons the
    # open interval.
    send_inputs(operator_id, namespace, ["k3:a", "k1:b"])
    assert await_changelog(%{"k3" => "k3=a", "k1" => "k1=b"}, 30_000)

    sigkill(os_pid(port_a))

    # An input arriving with no runner alive: journaled, undeliverable.
    send_inputs(operator_id, namespace, ["k4:a"])

    {_port_b, exec_b} = boot_runner(namespace, id_path)
    assert exec_b != exec_a
    assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end, 30_000)

    # Recovery restored the 2-checkpoint and re-forwarded 3..5. The re-run
    # re-emits the interval's outputs; upsert by key absorbs them.
    assert await_changelog(%{"k4" => "k4=a"}, 60_000),
           "the interval's inputs were not re-forwarded after the kill"

    # --- Phase 4: the replayed interval checkpoints -------------------------
    send_inputs(operator_id, namespace, ["commit:2"])

    assert wait_until(fn -> InputLog.watermark(operator_id) == 6 end, 60_000),
           "the replayed interval never acknowledged its inputs"

    # --- Conformance over EVERYTHING the sink ever saw ----------------------
    assert await_outbox_drained(operator_id), "the outbox never drained"
    outputs = drain_outputs([])

    expected_final = %{"k1" => "k1=b", "k2" => "k2=a", "k3" => "k3=a", "k4" => "k4=a"}

    assert changelog(outputs) == expected_final,
           "changelog upsert diverged from the no-crash final state"

    for {msg, md} <- outputs do
      assert md["flz.key"], "tier output missing its natural key: #{inspect(msg)}"
      assert md["flz.epoch"], "tier output missing its epoch stamp"
      assert md["flz.seq"], "tier output missing its outbox seq"
    end

    # A crash replay keeps the epoch — only a deliberate rewind bumps it.
    assert outputs |> Enum.map(fn {_msg, md} -> md["flz.epoch"] end) |> Enum.uniq() == ["0"],
           "a crash replay must not bump the epoch"

    # One identity never carries two different payloads.
    assert_identities_are_consistent(outputs)

    # The abandoned interval WAS re-emitted — that is the price of the mode, and
    # the whole reason it demands keyed sinks. A re-run is a fresh invocation, so
    # it lands under a NEW seq; what proves the replay is the same (key, payload)
    # arriving twice, which upsert-by-key absorbs into one row.
    duplicates =
      outputs
      |> Enum.frequencies_by(fn {msg, md} -> {md["flz.key"], msg} end)
      |> Enum.filter(fn {_pair, count} -> count > 1 end)

    assert duplicates != [],
           "the abandoned interval was never re-emitted — nothing was actually replayed"
  end
end

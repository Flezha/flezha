defmodule FlezhaTest.System.CheckpointBatchedOutputsE2ETest do
  @moduledoc """
  End-to-end proof for the `outputs: :at_checkpoint` opt-in against a live Rust
  runner: `checkpoint_batched_store` declares `effectively_once()` +
  `checkpoint_ack()` + `outputs_at_checkpoint(1_500)`, so an open interval holds
  its outputs with its changes instead of flushing each as it is produced.

  Three declared behaviours, each its own phase: no output reaches a plugin
  before the interval closes; the whole interval's outputs then arrive together
  with sequential ordinals off ONE record seq; and a quiet Cell's interval
  closes on its `max_delay_ms` alone, so a held output is never held
  indefinitely.

  Replay semantics are unchanged by the option and are proved by
  `effectively_once_checkpoint_e2e_test.exs`; the controller's batching of a
  checkpoint record's outputs is proved by `wal_writer_outbox_test.exs`.

  Tagged `:system`; run with
  `mix test --include system test/system/checkpoint_batched_outputs_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :effectively_once

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Service.OperativeManager
  alias Flezha.State.InputLog

  @moduletag timeout: 300_000

  @factory "checkpoint_batched_store"
  # Mirrors `outputs_at_checkpoint(1_500)` in the runner's fixture.
  @max_delay_ms 1_500

  test "an interval's outputs are held until it closes, and a quiet one closes on the delay" do
    namespace = unique("cp_batched")
    operative_id = unique("checkpoint_batched_store")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port, runner} = boot_runner(namespace, id_path)

    sink = %Flezha.Plugin{
      id: "tier_sink",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    builder = %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{},
      opts: [plugins: [sink]]
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)

    assert info.runner_id == runner

    assert wait_until(fn -> ClassRegistry.checkpoint_ack?(operative_id) end, 30_000),
           "the checkpoint ack-granularity declaration never reached the controller"

    # --- Phase 1: an open interval holds its outputs ------------------------
    # Offsets 0..2. Under the default these three emits would already be at the
    # sink; declared `:at_checkpoint`, none of them may be. The controller hears
    # nothing from an open interval, so the hold is proved by what arrives:
    # phase 2's batch rides ONE record seq (an early flush would ride its own)
    # and phase 3's held output lands no earlier than the delay.
    send_inputs(operative_id, namespace, ["k1:a", "k2:a", "k3:a"])

    refute InputLog.watermark(operative_id),
           "an open interval acknowledges nothing, exactly as under the default"

    # --- Phase 2: the checkpoint ships the whole interval as one batch -------
    # Offset 3 closes it. One record carries the interval's changes, all three
    # held outputs, and the offset that acknowledges every input folded in.
    send_inputs(operative_id, namespace, ["commit:1"])

    assert await_changelog(%{"k1" => "k1=a", "k2" => "k2=a", "k3" => "k3=a"}, 30_000),
           "the checkpoint never flushed the interval's held outputs"

    assert wait_until(fn -> InputLog.watermark(operative_id) == 3 end, 30_000),
           "the closing record never acknowledged the interval's inputs"

    batch = drain_outputs()
    assert length(batch) == 3

    # Every output of the interval rides ONE record, so the identities are that
    # record's seq with sequential ordinals — which is what makes them one
    # dispatch batch on the controller rather than three.
    seqs = Enum.map(batch, fn {_msg, md} -> md["flz.seq"] end)
    record_seqs = seqs |> Enum.map(&(&1 |> String.split(".") |> hd())) |> Enum.uniq()

    assert length(record_seqs) == 1,
           "a held interval must ship as ONE record, got seqs #{inspect(seqs)}"

    assert Enum.map(seqs, &(&1 |> String.split(".") |> List.last())) == ["0", "1", "2"],
           "ordinals must be sequential across the batch, got #{inspect(seqs)}"

    for {msg, md} <- batch do
      assert md["flz.key"], "tier output missing its natural key: #{inspect(msg)}"
      assert md["flz.epoch"] == "0"
    end

    assert_identities_are_consistent(batch)

    # --- Phase 3: a quiet Cell closes on the delay alone -------------------
    # One input, then silence. Nothing will ever call `ctx.checkpoint()` again,
    # so only `max_delay_ms` can close this interval — and it must, or the
    # output waits for an input that never comes.
    sent_at = System.monotonic_time(:millisecond)
    send_inputs(operative_id, namespace, ["k4:a"])

    assert_receive {:message_forward, msg, md}, @max_delay_ms * 4
    held_ms = System.monotonic_time(:millisecond) - sent_at

    assert held_ms >= @max_delay_ms - 500,
           "the held output shipped #{held_ms} ms after its input, before the delay could close"

    assert to_string(msg) == "k4=a"
    assert md["flz.key"] == "k4"

    assert wait_until(fn -> InputLog.watermark(operative_id) == 4 end, 30_000),
           "the delay closed the interval without acknowledging its input"
  end
end

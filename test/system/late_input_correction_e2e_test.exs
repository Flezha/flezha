defmodule FlezhaTest.System.LateInputCorrectionE2ETest do
  @moduledoc """
  End-to-end proof for `unbounded-late-input-correction` against a live Rust
  runner.

  The `candle` fixture declares the tier, an `EventTimeFrom` clock, a keyed sink
  and a 10-minute `correction_window`. Its state records the event times in the
  order they were folded — which is the assertion surface, because the whole
  claim of this change is about *order*, not about values.

  A trade at 200 arrives after trades at 100 and 300. Without correction the
  fold order is `[100, 300, 200]` and the candle is permanently wrong. With it,
  the engine rewinds behind 200, replays the retained input log ordered by event
  time, and the fold order becomes `[100, 200, 300]` — while the sink's row for
  the affected bucket is overwritten under a bumped epoch rather than duplicated.

  Tagged `:system` (boots real subprocesses); run with
  `mix test --include system test/system/late_input_correction_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager

  @moduletag :system
  @moduletag :correction_e2e
  @moduletag timeout: 300_000

  @factory "candle"
  @procedure "candle_procedure"

  # No retention setup. The class's declared `correction_window` derives its own
  # input-log hold window and snapshot age floor — that IS the test. If the
  # derivation ever regresses, this suite is the first thing that breaks: the
  # replayed slice would have been truncated away.

  test "a late trade inside the window is folded back into event-time order" do
    namespace = unique("correction")
    operative_id = unique("candle")
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port, _exec} = boot_runner(namespace, id_path)

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

    assert {:ok, _info} = OperativeManager.start_operative(namespace, builder)

    assert wait_until(
             fn -> Flezha.Operative.ClassRegistry.effectively_once?(operative_id) end,
             30_000
           ),
           "the tier declaration never reached the controller"

    # --- Two on-time trades ---------------------------------------------------
    # Event times are epoch-ms; 60_000 and 120_000 fall in different candle
    # buckets, 61_000 in the same bucket as 60_000.
    send_inputs(operative_id, namespace, ["60000:a", "120000:b"])
    assert wait_until(fn -> folded(namespace, operative_id) == [60_000, 120_000] end, 30_000)

    # --- The late trade -------------------------------------------------------
    # 61_000 is behind the high-water (120_000) by 59 s — well inside the
    # 10-minute window. It must NOT be folded at the tail.
    send_inputs(operative_id, namespace, ["61000:late"])

    assert wait_until(
             fn -> folded(namespace, operative_id) == [60_000, 61_000, 120_000] end,
             60_000
           ),
           "the late trade was not folded back into event-time order; got #{inspect(folded(namespace, operative_id))}"

    # --- The sink converged, it did not accumulate ----------------------------
    Process.sleep(1_000)
    outputs = drain_outputs([])

    changelog =
      Enum.reduce(outputs, %{}, fn {msg, md}, acc -> Map.put(acc, md["flz.key"], msg) end)

    # Bucket 60 was recomputed by the replay (its last write is the late trade's
    # re-run of 61000); bucket 120 was re-emitted unchanged by the same replay.
    assert Map.has_key?(changelog, "candle/60")
    assert Map.has_key?(changelog, "candle/120")

    # The correction bumped the epoch: outputs regenerated above the rewind
    # boundary carry a higher one than the pre-correction run, which is exactly
    # what lets a conforming sink overwrite rather than dedup them away.
    epochs = outputs |> Enum.map(fn {_, md} -> String.to_integer(md["flz.epoch"]) end)
    assert Enum.max(epochs) > Enum.min(epochs), "a correction must bump the epoch"

    for {_msg, md} <- outputs do
      assert md["flz.key"], "a correction-capable operative must emit keyed output"
    end
  end

  test "a trade beyond the window is processed as an arrival, not dropped" do
    namespace = unique("correction_bw")
    operative_id = unique("candle_bw")
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port, _exec} = boot_runner(namespace, id_path)

    builder = %Builder{id: operative_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, _info} = OperativeManager.start_operative(namespace, builder)

    assert wait_until(
             fn -> Flezha.Operative.ClassRegistry.effectively_once?(operative_id) end,
             30_000
           )

    # 1_000_000 ms high-water; the straggler at 1_000 is 999 s late, well past
    # the 600 s window. The default policy is `process_as_arrival` — the
    # engine's behaviour with no window declared at all, so declaring one never
    # silently starts discarding data.
    send_inputs(operative_id, namespace, ["1000000:a", "1000:straggler"])

    assert wait_until(fn -> folded(namespace, operative_id) == [1_000_000, 1_000] end, 30_000),
           "a beyond-window input must be processed as an arrival, not dropped"
  end

  # The Entity's fold order, read straight out of the live state. This is the
  # assertion surface: the change is about ORDER, so the test looks at order.
  defp folded(namespace, operative_id) do
    with {:ok, json} <- OperativeManager.get_entity_state(namespace, operative_id),
         %{@procedure => %{"folded" => folded}} <- Flezha.Service.Payloader.json_to_map(json) do
      folded
    else
      _ -> nil
    end
  end
end

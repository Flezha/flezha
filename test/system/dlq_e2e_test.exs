defmodule FlezhaTest.System.DlqE2ETest do
  @moduledoc """
  End-to-end proof for `dead-letter-queue` against live runners.

  Tier-off (`poison_probe`, on whichever SDK the run selects): a poison
  payload exhausts the retry budget, parks durably (park frame →
  fsync → `ParkPayloadAck`), the cell continues, the entry is
  inspectable, and — after the "antidote" input fixes the tasklet — a
  `flz dlq replay` re-dispatches the payload through the ordinary input
  path, succeeds, and removes the entry.

  Tier-on (`tier_store`): the poison payload freezes the relay's
  min-watermark while it retries; the atomic park (`{no state change, dlq
  entry, consumed offset}`) un-freezes it, so input-log retention proceeds
  — the same watermark signal the source credit loop grants from (credit
  behaviour is covered by the credit suites).

  Tagged `:system` (boots real subprocesses); the Java case additionally
  Run with `mix test --include system test/system/dlq_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :retry_ladder

  import ExUnit.CaptureIO
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, wait_until: 3]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Dlq.Store
  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Service.Cli
  alias Flezha.Service.RelayManager
  alias Flezha.State.InputLog
  alias FlezhaTest.Fixtures.RelayFixture

  @moduletag timeout: 300_000

  setup do
    dir = Path.join(System.tmp_dir!(), "flezha_dlq_e2e_#{System.unique_integer([:positive])}")
    original = Application.get_env(:flezha, :dlq, [])

    Application.put_env(
      :flezha,
      :dlq,
      Keyword.merge(original, dir: dir, replay_grace_ms: 2_000)
    )

    on_exit(fn ->
      Application.put_env(:flezha, :dlq, original)
      File.rm_rf!(dir)
    end)

    :ok
  end

  test "tier-off poison payload parks durably, cell continues, replay works" do
    namespace = unique("dlq")
    relay_id = unique("probe")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    {_port, _exec} = boot_runner(namespace, id_path)

    sink = %Flezha.Plugin{
      id: "probe_sink",
      namespace: namespace,
      relay_id: relay_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    builder = %Builder{
      id: relay_id,
      factory_id: "poison_probe",
      init_values: %{},
      opts: [bindings: RelayFixture.bindings(namespace, relay_id, [sink])]
    }

    assert {:ok, _info} = RelayManager.start_relay(namespace, builder)

    # Healthy payload flows end to end.
    send_inputs(relay_id, namespace, ["a"])
    assert await_output("ok:a", 30_000), "healthy payload never reached the sink"

    # Poison, then a healthy payload BEHIND it: the cell may only
    # process "b" after the poison payload is durably parked
    # (park-then-advance), so ok:b proves both the retries and the park.
    send_inputs(relay_id, namespace, ["poison", "b"])
    assert await_output("ok:b", 60_000), "cell never continued past the poison payload"

    assert wait_until(fn -> Store.list(relay_id) != [] end, 30_000),
           "durable DLQ entry never appeared"

    assert [entry] = Store.list(relay_id)
    assert entry["tasklet_id"] == "poison_probe_tasklet"
    assert entry["channel"] == "input"
    assert entry["attempts"] == 3, "the default retry budget is 3 total attempts"
    assert entry["error"] =~ "poison"
    assert entry["generation"] == 1

    ref = "#{entry["relay_class"]}/#{entry["tasklet_id"]}/#{entry["id"]}"

    # Entry inspectable: payload bytes round-tripped exactly.
    assert {:ok, _meta, "poison"} = Store.get(ref)
    shown = capture_io(fn -> Cli.call("Dlq.get", Jason.encode!([ref]), []) end)
    assert shown =~ "poison"

    # Fix the tasklet, then replay through the normal input path.
    send_inputs(relay_id, namespace, ["antidote"])
    assert await_output("ok:antidote", 30_000)

    output = capture_io(fn -> Cli.call("Dlq.replay", Jason.encode!([ref]), []) end)
    assert output =~ "replay succeeded"
    assert await_output("ok:poison", 30_000), "replayed payload never processed"
    assert {:error, :not_found} = Store.get(ref)
  end

  @tag capability: :effectively_once, skip: FlezhaTest.SystemCase.deferral(:effectively_once)
  test "tier-on poison payload freezes the min-watermark; the park un-freezes it" do
    namespace = unique("dlq_tier")
    relay_id = unique("tier_poison")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    {_port, _exec} = boot_runner(namespace, id_path)

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
      factory_id: "tier_store",
      init_values: %{},
      opts: [bindings: RelayFixture.bindings(namespace, relay_id, [sink])]
    }

    assert {:ok, _info} = RelayManager.start_relay(namespace, builder)

    assert wait_until(
             fn -> ClassRegistry.effectively_once?(relay_id) end,
             30_000
           )

    # Offset 0 consumed: the watermark reaches 0.
    send_inputs(relay_id, namespace, ["k1:a"])
    assert await_output("k1=a", 30_000)
    assert wait_until(fn -> InputLog.watermark(relay_id) == 0 end, 30_000)

    # Offset 1 = poison (fails every attempt, 300 ms backoff base → park
    # after ~1 s), offset 2 = healthy but queued BEHIND the poison.
    send_inputs(relay_id, namespace, ["kp:poison", "k2:b"])

    # Frozen: while the poison payload retries, nothing past offset 0 can
    # durably consume. Sampled across the whole retry window rather than once
    # after a fixed delay, which a sample taken before the poison was ever
    # dispatched would satisfy without proving anything. The watermark is read
    # BEFORE the store, so a store still empty at the later read proves the
    # sample predates the park.
    assert wait_until(
             fn ->
               watermark = InputLog.watermark(relay_id)

               if Store.list(relay_id) == [] do
                 assert watermark == 0,
                        "min-watermark advanced to #{inspect(watermark)} past an unparked poison payload"

                 false
               else
                 true
               end
             end,
             30_000,
             10
           ),
           "the poison payload never parked"

    # The park advances the cell past the poison payload: k2 processes
    # and the watermark resumes to its offset — input-log retention (and
    # the source credit loop granting off it) proceed.
    assert await_output("k2=b", 60_000), "cell never continued past the tier-on park"

    assert wait_until(fn -> (InputLog.watermark(relay_id) || -1) >= 2 end, 30_000),
           "min-watermark never resumed after the park"

    assert wait_until(fn -> Store.list(relay_id) != [] end, 30_000)
    assert [entry] = Store.list(relay_id)
    assert entry["origin_offset"] == 1, "the park must carry the poison input's offset"
    assert entry["channel"] == "input"
    assert entry["error"] =~ "poison value"
    assert {:ok, _meta, "kp:poison"} = Store.get(entry_ref(entry))
  end

  # fails when: inputs queued for a relay that was stopped or moved off its
  # runner run there anyway, every emit fails on the gone relay, and each is
  # parked as poison — on a moved relay a park the controller never acks, so
  # the runner re-sends it forever.
  @tag capability: :pool_wedge
  test "work queued for a relay that left its runner is dropped, not parked" do
    namespace = unique("dlq_gone")
    relay_id = unique("slow")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    {_port, _exec} = boot_runner(namespace, id_path)

    builder = %Builder{id: relay_id, factory_id: "wedge_probe", init_values: %{}, opts: []}
    assert {:ok, _info} = RelayManager.start_relay(namespace, builder)

    # "slow" holds the Cell for two seconds; the two behind it are still
    # queued when the relay stops.
    for input <- ["slow", "x1", "x2"] do
      assert :ok =
               RelayManager.send_io_confirming(relay_id, namespace, input, %{"input_type" => "2"})
    end

    assert :ok = RelayManager.stop_relay(namespace, relay_id)

    refute wait_until(fn -> Store.list(relay_id) != [] end, 6_000),
           "a stopped relay's queued input was parked: #{inspect(Store.list(relay_id))}"
  end

  # --- helpers ----------------------------------------------------------------

  defp entry_ref(entry),
    do: "#{entry["relay_class"]}/#{entry["tasklet_id"]}/#{entry["id"]}"

  # Wait until `expected` shows up among everything received so far,
  # without consuming the mailbox (later assertions re-scan).
  defp await_output(expected, timeout) do
    wait_until(
      fn ->
        {:messages, messages} = Process.info(self(), :messages)

        Enum.any?(messages, fn
          {:message_forward, msg, _md} -> to_string(msg) == expected
          _ -> false
        end)
      end,
      timeout
    )
  end
end

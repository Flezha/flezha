defmodule FlezhaTest.System.DlqE2ETest do
  @moduledoc """
  End-to-end proof for `platform-dead-letter-queue` against live executors.

  Tier-off (`poison_probe` / `java_poison_probe`, Rust AND Java): a poison
  payload exhausts the retry budget, parks durably (park frame →
  fsync → `ParkPayloadAck`), the segment continues, the entry is
  inspectable, and — after the "antidote" input fixes the operation — a
  `flz dlq replay` re-dispatches the payload through the ordinary input
  path, succeeds, and removes the entry.

  Tier-on (`tier_store`): the poison payload freezes the operative's
  min-watermark while it retries; the atomic park (`{no state change, dlq
  entry, consumed offset}`) un-freezes it, so input-log retention proceeds
  — the same watermark signal the source credit loop grants from (credit
  behaviour is covered by the credit suites).

  Tagged `:system` (boots real subprocesses); the Java case additionally
  needs the committed fat jar (`:java_sdk`). Run with
  `mix test --include system [--include java_sdk] test/system/dlq_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Dlq.Store
  alias Flezha.Operative.Builder
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.InputLog

  @moduletag :system
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

  test "tier-off poison payload parks durably, segment continues, replay works (Rust)" do
    run_poison_probe_case(:rust, "poison_probe", "poison_probe_operation")
  end

  @tag :java_sdk
  test "tier-off poison payload parks durably, segment continues, replay works (Java)" do
    run_poison_probe_case(:java, "java_poison_probe", "java_poison_probe_operation")
  end

  defp run_poison_probe_case(runtime, factory, operation) do
    namespace = unique("dlq_#{runtime}")
    operative_id = unique("probe_#{runtime}")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port, _exec} = boot_executor(namespace, id_path, runtime: runtime)

    sink = %Flezha.Plugin{
      id: "probe_sink",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      subscriptions: [{:on_output, []}]
    }

    builder = %Builder{
      id: operative_id,
      factory_id: factory,
      init_values: %{},
      opts: [plugins: [sink]]
    }

    assert {:ok, _info} = OperativeManager.start_operative(namespace, builder)

    # Healthy payload flows end to end.
    send_inputs(operative_id, namespace, ["a"])
    assert await_output("ok:a", 30_000), "healthy payload never reached the sink"

    # Poison, then a healthy payload BEHIND it: the segment may only
    # process "b" after the poison payload is durably parked
    # (park-then-advance), so ok:b proves both the retries and the park.
    send_inputs(operative_id, namespace, ["poison", "b"])
    assert await_output("ok:b", 60_000), "segment never continued past the poison payload"

    assert wait_until(fn -> Store.list(operative_id) != [] end, 30_000),
           "durable DLQ entry never appeared"

    assert [entry] = Store.list(operative_id)
    assert entry["operation_id"] == operation
    assert entry["channel"] == "input"
    assert entry["attempts"] == 3, "the default retry budget is 3 total attempts"
    assert entry["error"] =~ "poison"
    assert entry["generation"] == 1

    ref = "#{entry["operative_class"]}/#{entry["operation_id"]}/#{entry["id"]}"

    # Entry inspectable: payload bytes round-tripped exactly.
    assert {:ok, _meta, "poison"} = Store.get(ref)
    shown = capture_io(fn -> CliManager.dlq_show(ref) end)
    assert shown =~ "poison"

    # Fix the operation, then replay through the normal input path.
    send_inputs(operative_id, namespace, ["antidote"])
    assert await_output("ok:antidote", 30_000)

    output = capture_io(fn -> CliManager.dlq_replay(ref) end)
    assert output =~ "replay succeeded"
    assert await_output("ok:poison", 30_000), "replayed payload never processed"
    assert {:error, :not_found} = Store.get(ref)
  end

  test "tier-on poison payload freezes the min-watermark; the park un-freezes it" do
    namespace = unique("dlq_tier")
    operative_id = unique("tier_poison")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port, _exec} = boot_executor(namespace, id_path)

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
      factory_id: "tier_store",
      init_values: %{},
      opts: [plugins: [sink]]
    }

    assert {:ok, _info} = OperativeManager.start_operative(namespace, builder)

    assert wait_until(
             fn -> Flezha.Operative.ClassRegistry.effectively_once?(operative_id) end,
             30_000
           )

    # Offset 0 consumed: the watermark reaches 0.
    send_inputs(operative_id, namespace, ["k1:a"])
    assert await_output("k1=a", 30_000)
    assert wait_until(fn -> InputLog.watermark(operative_id) == 0 end, 30_000)

    # Offset 1 = poison (fails every attempt, 300 ms backoff base → park
    # after ~1 s), offset 2 = healthy but queued BEHIND the poison.
    send_inputs(operative_id, namespace, ["kp:poison", "k2:b"])

    # Frozen: while the poison payload retries, nothing past offset 0 can
    # durably consume.
    Process.sleep(200)

    assert InputLog.watermark(operative_id) == 0,
           "min-watermark advanced past an unparked poison payload"

    # The park advances the segment past the poison payload: k2 processes
    # and the watermark resumes to its offset — input-log retention (and
    # the source credit loop granting off it) proceed.
    assert await_output("k2=b", 60_000), "segment never continued past the tier-on park"

    assert wait_until(fn -> (InputLog.watermark(operative_id) || -1) >= 2 end, 30_000),
           "min-watermark never resumed after the park"

    assert wait_until(fn -> Store.list(operative_id) != [] end, 30_000)
    assert [entry] = Store.list(operative_id)
    assert entry["origin_offset"] == 1, "the park must carry the poison input's offset"
    assert entry["channel"] == "input"
    assert entry["error"] =~ "poison value"
    assert {:ok, _meta, "kp:poison"} = Store.get(entry_ref(entry))
  end

  # --- helpers ----------------------------------------------------------------

  defp entry_ref(entry),
    do: "#{entry["operative_class"]}/#{entry["operation_id"]}/#{entry["id"]}"

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

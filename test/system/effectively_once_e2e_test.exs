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

  alias Flezha.Executor.FleetManager
  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager
  alias Flezha.State.{InputLog, Outbox}
  alias FlezhaTest.Helper.NodeTestHelper

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

    changelog =
      Enum.reduce(outputs, %{}, fn {msg, md}, acc -> Map.put(acc, md["flz.key"], msg) end)

    assert changelog == expected_final,
           "changelog upsert diverged from the no-crash final state"

    for {msg, md} <- outputs do
      assert md["flz.epoch"], "tier output missing its epoch stamp: #{inspect(md)}"
      assert md["flz.segment"], "tier output missing its segment stamp"
      assert md["flz.seq"], "tier output missing its outbox seq"
      assert md["flz.key"], "tier output missing its natural key: #{inspect(msg)}"
    end

    # Exact dedup contract: one identity never carries two different
    # payloads, and every logical output appears under at least one identity.
    by_identity =
      Enum.group_by(
        outputs,
        fn {_msg, md} -> {md["flz.segment"], md["flz.seq"], md["flz.epoch"]} end,
        fn {msg, _md} -> msg end
      )

    for {identity, payloads} <- by_identity do
      assert length(Enum.uniq(payloads)) == 1,
             "identity #{inspect(identity)} delivered divergent payloads: #{inspect(payloads)}"
    end

    duplicates = Enum.count(by_identity, fn {_id, payloads} -> length(payloads) > 1 end)

    assert duplicates > 0,
           "the staled cursor must force at least one identity-identical re-drive"

    # And the input-log watermark advanced past every consumed offset (all
    # six inputs durably consumed → retention can truncate).
    assert wait_until(fn -> (InputLog.watermark(operative_id) || -1) >= 5 end, 30_000),
           "min watermark never caught up to the consumed inputs"
  end

  # --- helpers ----------------------------------------------------------------

  defp send_inputs(operative_id, namespace, inputs) do
    for input <- inputs do
      OperativeManager.send_io(operative_id, namespace, input, %{"input_type" => "2"})
    end
  end

  # Wait until the changelog built from everything received SO FAR (without
  # consuming the mailbox) covers `expected`.
  defp await_changelog(expected, timeout) do
    wait_until(
      fn ->
        outputs = peek_outputs()

        changelog =
          Enum.reduce(outputs, %{}, fn {msg, md}, acc -> Map.put(acc, md["flz.key"], msg) end)

        Map.take(changelog, Map.keys(expected)) == expected
      end,
      timeout
    )
  end

  defp peek_outputs() do
    {:messages, messages} = Process.info(self(), :messages)

    for {:message_forward, msg, md} <- messages do
      {to_string(msg), md}
    end
  end

  defp drain_outputs(acc) do
    receive do
      {:message_forward, msg, md} -> drain_outputs([{to_string(msg), md} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp stale_cursor_and_crash_writers(operative_id, segment_id) do
    # Kill the writer brutally (no terminate/2 — the cursor persist at
    # shutdown must not run) and reset the durable cursor to "nothing
    # flushed", the crash-after-flush-before-cursor window at e2e scale.
    case Flezha.State.Supervisor.lookup(segment_id) do
      {:ok, pid} -> Process.exit(pid, :kill)
      :error -> :ok
    end

    case Flezha.State.Supervisor.lookup({operative_id, :input_log}) do
      {:ok, pid} -> Process.exit(pid, :kill)
      :error -> :ok
    end

    dedup = Outbox.read(segment_id)
    :ok = Outbox.write(segment_id, %{dedup | outbox_flushed_seq: -1})
  end

  defp boot_executor(namespace, id_path) do
    {port, executor_id} =
      NodeTestHelper.start_executor_port(namespace,
        runtime: :rust,
        transport: :uds,
        env: [{"FLEZHA_EXECUTOR_ID_PATH", id_path}],
        connect_timeout: 120_000
      )

    on_exit(fn ->
      if is_port(port) and Enum.member?(:erlang.ports(), port) do
        try do
          Port.close(port)
        catch
          _, _ -> :ok
        end
      end
    end)

    {port, executor_id}
  end

  # Kill the Overwatch and wait until its supervised replacement is live —
  # inputs sent before that would race the restart (a real source blocks or
  # retries; `send_io` is fire-and-forget).
  defp force_fresh_rebind(namespace, operative_id) do
    {:ok, old_pid} = OperativeManager.get_operative_pid(namespace, operative_id)
    Process.exit(old_pid, :kill)

    assert wait_until(
             fn ->
               case OperativeManager.get_operative_pid(namespace, operative_id) do
                 {:ok, pid} -> pid != old_pid and Process.alive?(pid)
                 _ -> false
               end
             end,
             60_000
           ),
           "no replacement Overwatch came up after the kill"
  end

  defp os_pid(port) do
    {:os_pid, pid} = :erlang.port_info(port, :os_pid)
    pid
  end

  defp sigkill(os_pid) do
    {_, 0} = System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)
  end

  defp unique(prefix), do: prefix <> "_" <> Integer.to_string(System.unique_integer([:positive]))

  defp unique_id_path() do
    Path.join(System.tmp_dir!(), "eff_once_id_#{System.unique_integer([:positive])}")
  end

  defp cleanup(namespace, operative_id, id_path) do
    try do
      OperativeManager.stop_all_operatives(namespace)
    catch
      _, _ -> :ok
    end

    try do
      FleetManager.stop_namespace(namespace)
    catch
      _, _ -> :ok
    end

    File.rm(id_path)
    File.rm_rf(Path.join(Flezha.State.Registry.state_root(), operative_id))
  end
end

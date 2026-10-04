defmodule FlezhaTest.System.RunnerLocalSendE2ETest do
  @moduledoc """
  `local_send`: application code on a runner sends to an operator bound on that
  runner with its SDK's `local` option, for each kind of operator. The runner is then SIGKILLed and a second one started: the operator
  rebinds and carries on, the local input is never replayed (it is in no input
  log), and input that came through the controller keeps its guarantee.

  The sends are made by the test app's own code (its local-send drill,
  `FLEZHA_LOCAL_SEND_SCRIPT`), each once the test creates its trigger file: the
  test first proves the operator's sink is live, which a send made at bind
  would race.

  Tagged `:system` (boots real runner subprocesses); run with
  `mix test --include system test/system/runner_local_send_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :local_send

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Operator.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperatorManager
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 300_000

  test "a memory-only flow operator" do
    round_trip("bench_keyed_nopersist", &keyed/1)
  end

  test "an at-least-once flow operator" do
    round_trip("store_input", & &1)
  end

  test "an effectively-once flow operator keeps its guarantee for controller input" do
    round_trip("tier_store", &keyed/1, journaled: "d:3")
  end

  test "a durable steps operator resumes after the step a local input committed" do
    round_trip("sugar_steps_per_input", & &1)
  end

  test "a local send to an operator bound on another runner is refused and sends nothing" do
    namespace = unique("local_away")
    operator_id = unique("away")
    go = trigger_path()
    on_exit(fn -> stop_namespace(namespace) end)

    {_port_b, exec_b} = boot_runner(namespace, unique_id_path())
    {:ok, info} = start(namespace, operator_id, "store_input")
    assert info.runner_id == exec_b

    {port_a, _exec_a} =
      boot_runner(namespace, unique_id_path(), env: drill(operator_id, "x", go))

    File.touch!(go)

    assert local_send_answer(port_a) == "err runtime.operator_not_local"
    refute_receive {:message_forward, _, _}, 1_000
  end

  # Prove the sink live with a controller input, send locally, kill the
  # runner, start another and prove the operator carries on; then the local
  # input's output must have arrived exactly once. `journaled:` is a
  # controller input sent while no runner is alive, which the operator's input
  # log must deliver after the rebind.
  defp round_trip(factory, output, opts \\ []) do
    namespace = unique("local_" <> factory)
    operator_id = unique(factory)
    id_path = unique_id_path()
    go = trigger_path()
    on_exit(fn -> cleanup(namespace, operator_id, id_path) end)

    {port_a, exec_a} =
      boot_runner(namespace, id_path, env: drill(operator_id, "l:1", go))

    {:ok, info} = start(namespace, operator_id, factory)
    assert info.runner_id == exec_a

    assert produces?(operator_id, namespace, "w:0", output.("w:0"), 30_000),
           "the operator's sink never went live"

    File.touch!(go)
    assert local_send_answer(port_a) == "ok"
    assert wait_until(fn -> seen(output.("l:1")) == 1 end, 30_000), "the local input was not handled"

    sigkill(os_pid(port_a))
    journaled = Keyword.get(opts, :journaled)
    if journaled, do: send_inputs(operator_id, namespace, [journaled])

    {_port_b, exec_b} = boot_runner(namespace, id_path)
    assert exec_b != exec_a
    assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end, 30_000)

    assert produces?(operator_id, namespace, "a:2", output.("a:2"), 60_000),
           "the operator did not carry on after the rebind"

    if journaled do
      assert wait_until(fn -> seen(output.(journaled)) >= 1 end, 60_000),
             "the controller input journaled while no runner was alive was not delivered"
    end

    assert seen(output.("l:1")) == 1, "the local input was replayed: #{inspect(peek_outputs())}"
  end

  defp start(namespace, operator_id, factory) do
    sink = %Flezha.Plugin{
      id: "local_send_sink",
      namespace: namespace,
      operator_id: operator_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    OperatorManager.start_operator(namespace, %Builder{
      id: operator_id,
      factory_id: factory,
      init_values: %{},
      opts: [bindings: OperatorFixture.bindings(namespace, operator_id, [sink])]
    })
  end

  defp drill(operator_id, message, go) do
    script = [%{operator: operator_id, channel: "input", message: message, when: go}]
    [{"FLEZHA_LOCAL_SEND_SCRIPT", Jason.encode!(script)}]
  end

  defp trigger_path do
    dir = NodeTestHelper.claim_tmp_dir("flz_local_send_go")
    on_exit(fn -> File.rm_rf(dir) end)
    Path.join(dir, "go")
  end

  # The drill's one answer line: `local_send <operator> ok | err <code>`.
  defp local_send_answer(port) do
    seen = NodeTestHelper.read_port(port, &(&1 =~ ~r/^local_send \S+ .+$/m), 60_000, "")
    [_, answer] = Regex.run(~r/^local_send \S+ (.+)$/m, seen) || [nil, "none in:\n#{seen}"]
    String.trim(answer)
  end

  defp keyed(input), do: String.replace(input, ":", "=", global: false)

  defp seen(payload), do: Enum.count(peek_outputs(), fn {msg, _md} -> msg == payload end)
end

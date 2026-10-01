defmodule FlezhaTest.System.CallResponseE2ETest do
  @moduledoc """
  An outside caller's call answered through `response()`, end to end on the
  runtime `FLEZHA_E2E_RUNTIME` selects, through the published
  `Flezha.Service.Operator.call/6`:

    * `call_echo`: one tasklet answers with its input.
    * `call_downstream`: the second tasklet of a chain answers `re:<input>`.
    * `call_split`: one tasklet feeds two filtered paths, each ending in
      `response()`; an input starting `no` is answered `refused:<input>`, every
      other `accepted:<input>`.
    * `call_turn`: a durable `:per_input` steps operator whose turn answers
      with its input.

      FLEZHA_E2E_RUNTIME=node mix test --include system test/system/call_response_e2e_test.exs
  """

  use FlezhaTest.SystemCase, capability: :call_response

  import FlezhaTest.Helper.TierE2EHarness, only: [unique: 1]

  alias Flezha.Operator.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{Operator, OperatorManager}
  alias Flezha.State.TaskJournal
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 300_000

  @context %{source: :ui, principal: "e2e@_system"}
  @deadline_ms 30_000

  setup do
    namespace = unique("call_response")
    on_exit(fn -> FleetManager.stop_namespace(namespace) end)
    NodeTestHelper.start_runner(namespace)
    {:ok, namespace: namespace}
  end

  test "a flow operator and a steps turn both answer the call", ctx do
    echo = start(ctx.namespace, "call_echo", :flow)
    assert {:ok, "hello", %{"channel" => _}} = call(ctx.namespace, echo, "hello")

    turn = start(ctx.namespace, "call_turn", :steps)
    assert {:ok, "hello", _} = call(ctx.namespace, turn, "hello")

    assert {:ok, entries} = TaskJournal.read(turn)

    # A durable steps answer leaves when the stage that produced it commits.
    assert Enum.any?(entries, &(&1.type == :task_committed)),
           "the turn answered before its stage committed: #{inspect(entries)}"
  end

  test "a downstream tasklet answers a call its input never reached", ctx do
    downstream = start(ctx.namespace, "call_downstream", :flow)
    assert {:ok, "re:x", _} = call(ctx.namespace, downstream, "x")
  end

  test "the path the input took answers, and nothing is dropped as answered", ctx do
    split = start(ctx.namespace, "call_split", :flow)
    test_pid = self()
    handler = "call-split-#{split}"

    :telemetry.attach(
      handler,
      [:flezha, :call, :answer_dropped],
      fn _event, _measurements, %{operator_id: ^split, reason: reason}, _ ->
        send(test_pid, {:dropped, reason})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:ok, "refused:no-thanks", _} = call(ctx.namespace, split, "no-thanks")
    assert {:ok, "accepted:yes", _} = call(ctx.namespace, split, "yes")
    refute_receive {:dropped, "answered"}, 1_000
  end

  defp call(namespace, operator_id, message),
    do: Operator.call(namespace, operator_id, message, "", [deadline_ms: @deadline_ms], @context)

  defp start(namespace, factory, kind) do
    operator_id = unique(factory)

    assert {:ok, _} =
             OperatorManager.start_operator(namespace, %Builder{
               id: operator_id,
               factory_id: factory,
               run: kind
             })

    operator_id
  end
end

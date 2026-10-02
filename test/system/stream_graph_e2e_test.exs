defmodule FlezhaTest.System.StreamGraphE2ETest do
  @moduledoc """
  The stream-graph shapes every SDK's builder authors, driven end to end on the
  runtime `FLEZHA_E2E_RUNTIME` selects:

    * `merged_input`: entries `ticks` and `quotes` merged into one tasklet,
      each source mapped to a value tagged with its channel and keyed on the
      symbol before the first `:`; the tasklet emits `<source>:<payload>` on
      the keyed sink `merged`.
    * `transform_chain`: entry `trades` through `filter` (drops `drop`),
      `flat_map` (splits on `,`), `map` (upper-cases) and `key_by` (the prefix
      before `:`) into a tasklet that emits its input on the keyed sink
      `transformed`.

  Both sinks are keyed, so `flz.cell` on each frame names the Cell the key
  routed the value to.

      FLEZHA_E2E_RUNTIME=node mix test --include system test/system/stream_graph_e2e_test.exs
  """

  use FlezhaTest.SystemCase, capability: :graph_builder

  import FlezhaTest.Helper.TierE2EHarness, only: [send_inputs: 4, unique: 1]

  alias Flezha.Operator.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperatorManager
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 300_000

  setup do
    namespace = unique("stream_graph")
    on_exit(fn -> FleetManager.stop_namespace(namespace) end)
    NodeTestHelper.start_runner(namespace)
    {:ok, namespace: namespace}
  end

  test "a frame on each merged entry reaches the tasklet under its own mapping", ctx do
    operator_id = start(ctx.namespace, "merged_input")

    send_inputs(operator_id, ctx.namespace, ["AAPL:1"], %{"channel" => "ticks"})
    send_inputs(operator_id, ctx.namespace, ["AAPL:2"], %{"channel" => "quotes"})

    assert Enum.sort(receive_lines(2)) == [
             {"quotes:AAPL:2", "#{operator_id}/merge_label_tasklet/AAPL"},
             {"ticks:AAPL:1", "#{operator_id}/merge_label_tasklet/AAPL"}
           ]
  end

  test "frames down one merged entry arrive in the order they were sent", ctx do
    operator_id = start(ctx.namespace, "merged_input")

    send_inputs(operator_id, ctx.namespace, ["MSFT:1", "MSFT:2"], %{"channel" => "ticks"})

    assert Enum.map(receive_lines(2), &elem(&1, 0)) == ["ticks:MSFT:1", "ticks:MSFT:2"]
  end

  test "filter, flat_map, map and key_by apply in order on one edge", ctx do
    operator_id = start(ctx.namespace, "transform_chain")

    send_inputs(operator_id, ctx.namespace, ["drop", "a:1,b:2"], %{"channel" => "trades"})

    assert Enum.sort(receive_lines(2)) == [
             {"A:1", "#{operator_id}/transform_label_tasklet/A"},
             {"B:2", "#{operator_id}/transform_label_tasklet/B"}
           ]

    refute_receive {:message_forward, _, _}, 1_000, "the filtered `drop` frame reached the sink"
  end

  defp start(namespace, factory) do
    operator_id = unique(factory)

    assert {:ok, _} =
             OperatorManager.start_operator(namespace, %Builder{
               id: operator_id,
               factory_id: factory,
               opts: [bindings: sink(namespace, operator_id)]
             })

    operator_id
  end

  # `{payload, flz.cell}` per frame, in arrival order.
  defp receive_lines(count) do
    for _ <- 1..count do
      assert_receive {:message_forward, payload, metadata}, 30_000
      {to_string(payload), metadata["flz.cell"]}
    end
  end

  defp sink(namespace, operator_id) do
    OperatorFixture.bindings(namespace, operator_id, [
      %Flezha.Plugin{
        id: "stream_graph_sink",
        provider: Flezha.Plugin.Provider.BeamMsgForward,
        opts: %{recv_pid: self(), include_metadata: true},
        hooks: [{:on_output, []}]
      }
    ])
  end
end

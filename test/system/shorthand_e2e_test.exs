defmodule FlezhaTest.System.ShorthandE2ETest do
  @moduledoc """
  The §5 short form, driven end to end on the runtime `FLEZHA_E2E_RUNTIME`
  selects. Six fixtures, one operative declaration each in every app — no
  hand-written operative stands beside them, which is what the `shorthand`
  capability asserts:

    * `sugar_do_nothing`: the default entry channel `input`, echoing onto
      `sugar_do_nothing_echo`;
    * `sugar_named_input`: the entry channel the author named, `prices`, onto
      `sugar_named_input_sink`;
    * `sugar_store_input`: the same shape with state, whose text inputs are
      read back off the live Cell;
    * `sugar_workflow`: the workflow short form, a `once` workflow run on its
      first message on `order`, whose output reaches `sugar_workflow_placed`
      and which then completes;
    * `sugar_workflow_per_input`: the workflow short form declaring the
      `per_input` loop mode, one pass per message onto
      `sugar_workflow_per_input_out`, which does not complete after a pass;
    * `sugar_state_only`: a short form with no sink, whose entry `ticks` feeds
      a tasklet that only keeps state, read back off the live Cell since
      no sink frame exists to observe.

  Text entries throughout, so the bytes a sink carries are equal across
  runtimes: a JSON round trip re-spaces the start values in some languages.
  Every start names only the factory; the kind is the one its runner announced.

      FLEZHA_E2E_RUNTIME=node mix test --include system test/system/shorthand_e2e_test.exs
  """

  use FlezhaTest.SystemCase, capability: :shorthand

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness, only: [send_inputs: 4, unique: 1]

  alias Flezha.Operative.Builder
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 300_000

  setup do
    namespace = unique("shorthand")
    on_exit(fn -> FleetManager.stop_namespace(namespace) end)
    NodeTestHelper.start_runner(namespace)
    {:ok, namespace: namespace}
  end

  test "a short form with no entry name is reached on `input`", ctx do
    operative_id = start(ctx.namespace, "sugar_do_nothing")

    send_inputs(operative_id, ctx.namespace, ["alpha", "beta"], %{"channel" => "input"})

    assert receive_payloads(2) == ["alpha", "beta"]
  end

  test "a short form is reached on the entry channel its author named", ctx do
    operative_id = start(ctx.namespace, "sugar_named_input")

    send_inputs(operative_id, ctx.namespace, ["AAPL:1"], %{"channel" => "prices"})

    assert receive_payloads(1) == ["AAPL:1"]
  end

  test "a stateful short form keeps its state and echoes its input", ctx do
    operative_id = start(ctx.namespace, "sugar_store_input")

    send_inputs(operative_id, ctx.namespace, ["one", "two"], %{"channel" => "input"})

    assert receive_payloads(2) == ["one", "two"]

    assert wait_until(
             fn -> stored(ctx.namespace, operative_id, "sugar_store_input") == ["one", "two"] end,
             30_000
           ),
           "the short form's state never came back; read " <>
             inspect(state_of(ctx.namespace, operative_id))
  end

  test "a short form with no sink keeps what it was sent in state", ctx do
    operative_id = start(ctx.namespace, "sugar_state_only")

    send_inputs(operative_id, ctx.namespace, ["one", "two"], %{"channel" => "ticks"})

    assert wait_until(
             fn -> stored(ctx.namespace, operative_id, "sugar_state_only") == ["one", "two"] end,
             30_000
           ),
           "the sinkless short form's state never came back; read " <>
             inspect(state_of(ctx.namespace, operative_id))

    refute_receive {:message_forward, _payload, _metadata}, 500
  end

  test "a workflow short form runs once on its first message and completes", ctx do
    operative_id = unique("sugar_workflow")

    assert {:ok, _} =
             OperativeManager.start_operative(ctx.namespace, %Builder{
               id: operative_id,
               factory_id: "sugar_workflow",
               opts: [plugins: [sink(ctx.namespace, operative_id)]]
             })

    # The instance waits for its message; the send on `order` is what runs the
    # one pass, so what reaches the sink is the message's own bytes.
    send_until_accepted(ctx.namespace, operative_id, "o-1", %{"channel" => "order"})
    assert [payload] = receive_payloads(1)
    assert payload =~ "o-1"

    # A `once` workflow self-terminates, and the controller deregisters it on
    # the completion it ships. The unit leaving the registry with no stop is
    # the whole loop having run.
    assert wait_until(fn -> not registered?(ctx.namespace, operative_id) end, 30_000),
           "the workflow short form never completed"
  end

  test "a workflow short form that declares its loop mode runs once per message", ctx do
    operative_id = start(ctx.namespace, "sugar_workflow_per_input")

    send_until_accepted(ctx.namespace, operative_id, "m-1", %{"channel" => "input"})
    assert receive_payloads(1) == ["m-1"]

    send_until_accepted(ctx.namespace, operative_id, "m-2", %{"channel" => "input"})
    assert receive_payloads(1) == ["m-2"]

    assert registered?(ctx.namespace, operative_id),
           "a per_input workflow completed after a pass, as a once workflow does"
  end

  # A send that lands before the bind is retryable; only an accepted one is
  # the instance's message.
  defp send_until_accepted(namespace, operative_id, message, metadata) do
    assert wait_until(
             fn ->
               not match?(
                 {:error, _},
                 OperativeManager.send_io_confirming(operative_id, namespace, message, metadata)
               )
             end,
             10_000
           ),
           "the message was never accepted"
  end

  defp start(namespace, factory) do
    operative_id = unique(factory)

    assert {:ok, _} =
             OperativeManager.start_operative(namespace, %Builder{
               id: operative_id,
               factory_id: factory,
               opts: [plugins: [sink(namespace, operative_id)]]
             })

    operative_id
  end

  defp receive_payloads(count) do
    for _ <- 1..count do
      assert_receive {:message_forward, payload, _metadata}, 30_000
      to_string(payload)
    end
  end

  defp state_of(namespace, operative_id) do
    case OperativeManager.get_cell_state(namespace, operative_id) do
      {:ok, json} -> Payloader.json_to_map(json)
      _ -> nil
    end
  end

  # The text inputs a stateful short form pushed, whatever the SDK spells the
  # field's container as: every runtime's state is one tasklet's, named after
  # the factory id, and its one list field is what the fixture accumulates.
  defp stored(namespace, operative_id, tasklet) do
    case state_of(namespace, operative_id) do
      %{^tasklet => cell} when is_map(cell) ->
        cell |> Map.values() |> Enum.find(&is_list/1)

      _ ->
        nil
    end
  end

  defp registered?(namespace, operative_id) do
    case OperativeManager.get_operative_pid(namespace, operative_id) do
      {:ok, _pid} -> true
      _ -> false
    end
  end

  defp sink(namespace, operative_id) do
    %Flezha.Plugin{
      id: "shorthand_sink_#{operative_id}",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }
  end
end

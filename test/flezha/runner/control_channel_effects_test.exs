defmodule FlezhaTest.Runner.ControlChannelEffectsTest do
  @moduledoc """
  SDK -> Controller `EffectResult` (`recorded-effects`): the result is kept in
  its instance's effect log and only then answered `EffectResultAck`; a second
  result for the identity is answered with the first; a result from a runner
  that does not own the relay keeps nothing; at `durable: remote` the ack waits
  for the effect log to reach the remote store.
  """
  # async: false: a shared on-disk state root and ETS registry; the remote case
  # installs a backend.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.ControlChannel
  alias Flezha.State.Wire
  alias Flezha.State.DurabilityTier.Manifest
  alias Flezha.State.{InputLog, TestHelpers}
  alias FlezhaTest.Helper.ControlFrameTestHelper, as: ControlFrame
  alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-chan-effects-test"

  setup do
    ClassRegistry.ensure_table()
    inst = "eff_inst_#{System.unique_integer([:positive])}"
    channel = start_channel()
    for i <- 0..4, do: {:ok, _} = InputLog.append(inst, "m#{i}", %{})

    on_exit(fn ->
      Stub.uninstall()
      TestHelpers.close_input_log(inst)
    end)

    %{inst: inst, channel: channel}
  end

  test "a result is kept before it is acked, and the first one wins", ctx do
    own(ctx)

    assert %RunnerCommands.EffectResultAck{error: nil, first: false} =
             keep(ctx, "ref-1", "answer")

    assert InputLog.effect_results(ctx.inst, -1) == [{4, "id-model", cell(ctx.inst), "answer"}]

    assert %RunnerCommands.EffectResultAck{error: nil, first: true, payload: "answer"} =
             keep(ctx, "ref-2", "another")
  end

  test "a result for a relay this node holds no record of keeps nothing", ctx do
    assert %RunnerCommands.EffectResultAck{error: %{code: "commit.not_owner"}} =
             keep(ctx, "ref-unrecorded", "answer")

    assert InputLog.effect_results(ctx.inst, -1) == []
  end

  test "a result naming another relay's Cell keeps nothing", ctx do
    own(ctx)

    assert %RunnerCommands.EffectResultAck{error: %{code: "commit.not_owner"}} =
             keep(ctx, "ref-other-cell", "answer", cell("another-relay"))

    assert InputLog.effect_results(ctx.inst, -1) == []
  end

  test "a result from a runner that does not own the relay keeps nothing", ctx do
    ClassRegistry.put_overwatch(ctx.inst, self(), "another-runner")

    assert %RunnerCommands.EffectResultAck{error: %{code: "commit.not_owner"}} =
             keep(ctx, "ref-foreign", "answer")

    assert InputLog.effect_results(ctx.inst, -1) == []
  end

  # Fails when: an internal failure's raw term rides to the runner, or its
  # reason reaches the log other than once.
  test "an internal failure is refused with its code, and the log keeps its reason", ctx do
    own(ctx)
    set_mimic_global_until_exit()
    reason = {:decode_error, "vault key s3cr3t-7f3a"}

    Mimic.stub(Flezha.Service.RelayManager, :keep_effect, fn _, _, _, _, _ -> {:error, reason} end)

    log =
      capture_log(fn ->
        assert %RunnerCommands.EffectResultAck{error: error} = keep(ctx, "ref-internal", "answer")
        assert error.code == "runner.bad_reply"
        assert error.details_json =~ ~s({"reason":"the controller's log names the cause (ref )
      end)

    assert length(String.split(log, "s3cr3t-7f3a")) == 2
  end

  describe "synchronous remote durability" do
    setup do
      Stub.install()
      {:ok, _pid} = start_supervised(Flezha.State.DurabilityTier.Shipper)
      :ok
    end

    test "the ack means the effect log is on the remote store", ctx do
      own(ctx)
      ClassRegistry.put_remote(ctx.inst, true)

      assert %RunnerCommands.EffectResultAck{error: nil} = keep(ctx, "ref-remote", "answer")
      assert {:ok, _blob} = Stub.get(Manifest.key({:effects_log, ctx.inst}), [])
    end
  end

  defp start_channel() do
    test_pid = self()
    runner_id = "#{@namespace}_#{System.unique_integer([:positive])}"

    write_fun = fn _ref, _payload_id, message, _metadata ->
      send(test_pid, {:written, message})
      :ok
    end

    {:ok, pid} = ControlChannel.start_link({@namespace, runner_id, make_ref(), write_fun})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000) end)
    pid
  end

  defp own(ctx),
    do: ClassRegistry.put_overwatch(ctx.inst, self(), :sys.get_state(ctx.channel).identity)

  defp cell(relay_id), do: Wire.encode_cell_id(relay_id, "task", "")

  # Send one result for offset 4 and wait for the ack answering it.
  defp keep(ctx, reference_id, payload, cell \\ nil) do
    result = %RunnerCommands.EffectResult{
      relay_id: ctx.inst,
      identity: "id-model",
      offset: 4,
      name: "model",
      payload: payload,
      cell_identity: cell || cell(ctx.inst)
    }

    frame = ControlFrame.encode_request(reference_id, {:effect_result, result})
    send(ctx.channel, {:flz_data, :stream_id, "control_command", frame, %{}})
    await_ack(reference_id)
  end

  defp await_ack(reference_id) do
    assert_receive {:written, message}, 5_000

    case ControlFrame.decode_request(message) do
      %{reference_id: ^reference_id, command: {:effect_result_ack, ack}} -> ack
      _other -> await_ack(reference_id)
    end
  end
end

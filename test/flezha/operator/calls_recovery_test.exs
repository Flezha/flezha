defmodule FlezhaTest.Operator.CallsRecoveryTest do
  @moduledoc """
  `operator-calls`, a durable caller's recovery: the windows a system test
  cannot aim at. A caller's Overwatch that restarted holds no row for a call,
  so the outbox re-drive of the call's output is how it learns of the call
  again, with the identity, the deadline and the request it was emitted with.
  The re-driven output resolves by what the caller's own input log retains
  and by the clock: a `Response` appended before the crash (its cursor write
  withheld) is found and no second one is appended; a deadline ahead sends
  the request again; one passed within the margin is answered `call.timeout`;
  past the margin the call settles with no `Response`, counted. An answer that
  arrives before the re-drive asks the Cell writers for the held calls at once
  and waits for them.
  """
  # async: false: writes Application env; Mimic global mode; telemetry handlers.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :state_tree], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  import FlezhaTest.Helper.CallHarness,
    only: [
      dispatch: 3,
      durable_caller: 1,
      now: 0,
      ok_outcome: 1,
      request_metadata: 1,
      response: 1
    ]

  alias Flezha.Operator.Calls
  alias Flezha.State.{InputLog, TestHelpers}
  alias FlezhaTest.Helper.CallHarness

  setup :set_mimic_global_until_exit

  setup do
    CallHarness.install()
    %{inst: "recovered_#{System.unique_integer([:positive])}"}
  end

  # A durable caller whose Overwatch has no row for the call, as after its
  # restart: what the outbox re-drive hands it is the call's output.
  defp redrive(inst, deadline_at) do
    host = durable_caller(inst)
    metadata = request_metadata(deadline_at)
    {id, _row} = Calls.request("o1", metadata, true)
    {host, id, metadata}
  end

  test "whose Response the log retains settles and sends nothing", %{inst: inst} do
    {host, id, metadata} = redrive(inst, now() + 30_000)
    {:ok, _offset} = InputLog.append(inst, "earlier", %{"flz.msg_id" => "r:" <> id})

    dispatch(host, "o1", metadata)
    assert_receive {:"$gen_cast", {:outbox_confirmed, {:cursor, 4}}}
    refute_received {:delivery_request, _, _, _}
    refute_receive {:channel_got, _, _}, 100
  end

  test "with its deadline ahead is sent again and waits", %{inst: inst} do
    {host, id, metadata} = redrive(inst, now() + 30_000)
    dispatch(host, "o1", metadata)
    assert_receive {:delivery_request, _, ^inst, %{id: ^id}}
    refute_received {:"$gen_cast", {:outbox_confirmed, _}}
    assert %{} = Calls.pending(:sys.get_state(host).calls, id)
  end

  test "past its deadline within the margin is answered call.timeout", %{inst: inst} do
    {host, id, metadata} = redrive(inst, now() - 1_000)
    dispatch(host, "o1", metadata)
    {%{"call" => ^id, "error" => %{"code" => "call.timeout"}}, _} = response(inst)
    assert_receive {:"$gen_cast", {:outbox_confirmed, {:cursor, 4}}}
    refute_received {:delivery_request, _, _, _}
  end

  test "past its deadline and the margin settles with no Response", %{inst: inst} do
    on_exit(TestHelpers.override_env(:call_retention_margin_ms, 1_000))
    {host, _id, metadata} = redrive(inst, now() - 5_000)
    dispatch(host, "o1", metadata)
    assert_receive {:"$gen_cast", {:outbox_confirmed, {:cursor, 4}}}
    assert_receive {:call_event, :recovered_unanswered, %{operator_id: ^inst}}
    refute_receive {:channel_got, _, _}, 100
  end

  test "an answer arriving before the re-drive waits for the call and makes one Response", %{
    inst: inst
  } do
    {host, id, metadata} = redrive(inst, now() + 30_000)
    writer = spawn_link(fn -> holding_writer(host, metadata) end)
    stub(Flezha.State.Supervisor, :live_writers, fn ^inst -> [writer] end)

    assert {:error, _retry} = GenServer.call(host, {:call_answer, id, ok_outcome("hi")})
    assert_receive {:delivery_request, _, ^inst, %{id: ^id}}
    assert :ok = GenServer.call(host, {:call_answer, id, ok_outcome("hi")})
    {%{"call" => ^id, "value" => _}, _} = response(inst)

    assert {:ok, :absorbed} = GenServer.call(host, {:call_answer, id, ok_outcome("hi")})
    refute_received {:channel_got, _, _}
  end

  # A Cell writer still holding the call's output: asked for its held calls, it
  # dispatches the output again and says how many it sent.
  defp holding_writer(host, metadata) do
    receive do
      {:"$gen_call", from, :redrive_calls} ->
        dispatch(host, "o1", metadata)
        GenServer.reply(from, 1)
        holding_writer(host, metadata)
    end
  end
end

defmodule FlezhaTest.Service.CallsAuthzTest do
  @moduledoc """
  An operator's call is judged as its namespace's runner identity, exactly as
  a tasklet's spawn: creating the callee against the `operator_start` row,
  which records it, and a delivery against `spawn_deliver`. A refusal ends the
  call with its `authz.*` code and starts nothing. The delivery runs the real
  router over `FakeStartRegistry`; the caller is the test process.
  """
  # async: false: writes Application env and a shared on-disk auth root;
  # Mimic global mode, because the delivery runs in a process of its own.
  use FlezhaTest.Case,
    fixtures: [
      :tmp_app_data,
      {:auth_stores, prefix: "flz_call_authz", claim: true, register_source: true}
    ],
    async: false

  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.RoleStore
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Process.CallDelivery
  alias Flezha.Service.OperatorManager
  alias FlezhaTest.Helper.FakeStartRegistry
  alias FlezhaTest.Helper.NodeTestHelper

  @ns "team-calls"
  @principal "runner@team-calls"

  setup :set_mimic_global_until_exit

  setup do
    registry = FakeStartRegistry.install(runners: ["runner-1"], run: :steps)
    test_pid = self()

    # The caller's Overwatch: every report of the delivery reaches the test.
    stub(OperatorManager, :get_operator_pid, fn
      @ns, "gateway" -> {:ok, test_pid}
      ns, id -> Mimic.call_original(OperatorManager, :get_operator_pid, [ns, id])
    end)

    %{registry: registry, session: "s-#{System.unique_integer([:positive])}"}
  end

  defp call_session(session) do
    CallDelivery.request(@ns, "gateway", %{
      id: "c-#{session}",
      target: "factory:worker",
      key: session,
      create: true,
      channel: nil,
      request: "turn",
      response: "ask.response",
      deadline_at: System.system_time(:millisecond) + 5_000,
      deadline_ms: 5_000,
      callee: nil,
      created: false
    })
  end

  # The outcome the delivery hands the caller, answered as a caller would.
  defp await_outcome do
    assert_receive {:"$gen_call", from, {:call_answer, _id, outcome}}, 5_000
    GenServer.reply(from, :ok)
    outcome
  end

  test "a runner identity with only the runner role cannot create a session", ctx do
    assert Flezha.AuthZ.enabled?()
    :ok = RoleStore.bind(@principal, @ns, :runner)
    call_session(ctx.session)

    assert %{result: {:error, %Flezha.Error{code: "authz.role_insufficient"}}, created: false} =
             await_outcome()

    assert FakeStartRegistry.started() == []
  end

  test "a spawner's call creates the session and records its start", ctx do
    :ok = RoleStore.bind(@principal, @ns, :spawner)
    boundary = System.monotonic_time(:microsecond)
    call_session(ctx.session)

    session = ctx.session
    assert_receive {:"$gen_cast", {:call_accepted, _id, ^session, true}}, 5_000
    assert [%{id: ^session}] = FakeStartRegistry.started()

    record =
      NodeTestHelper.wait_until(
        fn ->
          :operator_service
          |> RingBufferSink.query(boundary)
          |> Enum.find(&(&1.name == :start))
        end,
        2_000,
        25
      )

    assert record.metadata.action == :"operator.start"
    assert record.metadata.principal == @principal
    assert record.metadata.operator == ctx.session
    assert record.metadata.outcome == :ok
  end
end

defmodule FlezhaTest.Runner.ControlChannelSpawnAuthzTest do
  @moduledoc """
  A procedure-requested workflow spawn (`ctx.spawn_workflow`) is judged like
  any other act, as the identity its connection proved: a start against the
  `operative_start` row, which records it, and a delivery to a running instance
  against `spawn_deliver`, the `operative.send` right with no record of a
  permitted delivery. Every refusal is audited. `spawner` is the role that
  grants those two rights in its own namespace and nothing else.

  The frame is the one every SDK sends, written into a real control channel;
  the start runs the real `OperativeManager` path over `FakeStartRegistry`, so
  nothing between the frame and the registry claim is stubbed.
  """
  # async: false: writes Application env and a shared on-disk auth root;
  # Mimic global mode, because the spawn resolves in a process of its own.
  use FlezhaTest.Case,
    fixtures: [
      :tmp_app_data,
      {:auth_stores, prefix: "flz_spawn_authz", claim: true, register_source: true}
    ],
    async: false

  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.Verb
  alias Flezha.Cluster.Directory
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Observability.TestSinks
  alias Flezha.Operative.Builder
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Session
  alias Flezha.Service.Admin
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.ControlFrameTestHelper, as: ControlFrame
  alias FlezhaTest.Helper.FakeStartRegistry
  alias FlezhaTest.Helper.FakeStartRegistry.Instance
  alias FlezhaTest.Helper.NodeTestHelper

  @ns "team-spawns"
  @principal "runner@team-spawns"
  @factory "worker"

  setup :set_mimic_global_until_exit

  setup do
    registry = FakeStartRegistry.install(runners: ["runner-1"])

    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:written, ref, message})
      :ok
    end

    runner_id = "spawn_authz_runner_#{System.unique_integer([:positive])}"
    {:ok, channel} = ControlChannel.start_link({@ns, runner_id, make_ref(), write_fun})

    on_exit(fn ->
      try do
        NodeTestHelper.ensure_stopped(channel, :normal, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)

    %{
      channel: channel,
      registry: registry,
      instance_id: "spawned-#{System.unique_integer([:positive])}"
    }
  end

  test "an identity that may not start an operative is refused, and nothing starts", ctx do
    assert Flezha.AuthZ.enabled?()
    assert RoleStore.lookup(@principal) == :error
    since = System.monotonic_time(:microsecond)

    request_spawn(ctx)

    assert {"ref-spawn", %{outcome: "refused", error: error}} = await_response()
    assert error =~ "forbidden"

    # The reply is written after the resolution, so nothing can start behind it.
    assert FakeStartRegistry.started() == []
    refute OperativeManager.exist?(@ns, ctx.instance_id)

    assert [denial] = AuthTestHelper.await_platform_authz_events(:authorization_denied, since)
    assert denial.metadata.principal == @principal
    assert denial.metadata.action == :"operative.start"
    assert denial.metadata.reason == :role_insufficient
  end

  test "a spawner's spawn starts and writes its start record, naming the requester", ctx do
    :ok = RoleStore.bind(@principal, @ns, :spawner)
    boundary = System.monotonic_time(:microsecond)

    request_spawn(ctx)

    assert {"ref-spawn", %{outcome: "spawned", error: "", instance_id: answered}} =
             await_response()

    assert [%{id: id}] = FakeStartRegistry.started()
    assert id == ctx.instance_id
    assert answered == ctx.instance_id

    assert_start_recorded(boundary, ctx.instance_id)
  end

  test "a spawn without an id is recorded under the id it was answered with", ctx do
    :ok = RoleStore.bind(@principal, @ns, :spawner)
    boundary = System.monotonic_time(:microsecond)

    request_spawn(%{ctx | instance_id: ""})

    assert {"ref-spawn", %{outcome: "spawned", error: "", instance_id: generated}} =
             await_response()

    assert generated =~ ~r/\Aworker-[a-z2-7]{16}\z/
    assert [%{id: ^generated}] = FakeStartRegistry.started()
    assert_start_recorded(boundary, generated)
  end

  test "a spawn without an id refused by its act is recorded with no id", ctx do
    :ok = RoleStore.bind(@principal, @ns, :spawner)

    stub(Flezha.AuthZ.Quotas, :check_allocation, fn _ns, key, _n ->
      {:error, {:quota_exceeded, key, %{limit: 0}}}
    end)

    boundary = System.monotonic_time(:microsecond)

    request_spawn(%{ctx | instance_id: ""})

    assert {"ref-spawn", %{outcome: "refused", instance_id: ""}} = await_response()

    record = NodeTestHelper.wait_until(fn -> audit_record(:start, boundary) end, 2_000, 25)
    assert record.metadata.outcome == :error
    assert record.metadata.operative == nil
  end

  test "an identity that may not send is refused a delivery to a running instance", ctx do
    instance = run_instance(ctx)
    since = System.monotonic_time(:microsecond)

    request_spawn(ctx)

    assert {"ref-spawn", %{outcome: "refused", error: error}} = await_response()
    assert error =~ "forbidden"
    assert Instance.ingested(instance) == []

    assert [denial] = AuthTestHelper.await_platform_authz_events(:authorization_denied, since)
    assert denial.metadata.principal == @principal
    assert denial.metadata.action == :"operative.send"
    assert denial.metadata.reason == :role_insufficient
  end

  test "a spawner's delivery is made and leaves no send record", ctx do
    :ok = RoleStore.bind(@principal, @ns, :spawner)
    instance = run_instance(ctx)
    boundary = System.monotonic_time(:microsecond)

    request_spawn(ctx)

    assert {"ref-spawn", %{outcome: "delivered", error: ""}} = await_response()
    assert Instance.ingested(instance) == ["p1"]

    # The reply is written after the resolution emitted whatever it emits, so the
    # settled ring buffer already holds any record the delivery wrote.
    TestSinks.settle(RingBufferSink)
    refute audit_record(:send, boundary)
  end

  test "a spawner may not stop an operative or enable or disable a plugin" do
    :ok = RoleStore.bind(@principal, @ns, :spawner)
    assert Session.runner_caller_context(@ns).roles == [:spawner]

    for {verb, args} <- [
          {"Operative.stop", ~s(["o1"])},
          {"PluginManager.enable", ~s(["p1", {"id": "p1"}])},
          {"PluginManager.disable", ~s(["p1"])}
        ] do
      assert Admin.call(@ns, verb, args) == {:error, "forbidden: role_insufficient"}, verb
    end
  end

  test "a spawner's grant stops at its own namespace", ctx do
    :ok = RoleStore.bind(@principal, @ns, :spawner)
    start = fn _call -> flunk("a start in another namespace ran") end

    # The role check passes and the namespace check refuses: a grant the role
    # did not hold would answer `role_insufficient` instead.
    assert {:error, {:forbidden, :namespace_mismatch}} =
             Verb.run(
               :operative_start,
               Session.runner_caller_context(@ns),
               ["team-other", ctx.instance_id],
               start
             )
  end

  test "a spawner binding for another namespace spawns nothing here", ctx do
    :ok = RoleStore.bind(@principal, "team-other", :spawner)

    request_spawn(ctx)

    assert {"ref-spawn", %{outcome: "refused", error: error}} = await_response()
    assert error =~ "forbidden"
    assert FakeStartRegistry.started() == []
  end

  test "with authorization off, a spawn starts as before and its start is recorded", ctx do
    config = Application.get_env(:flezha, :authn_authz)
    Application.put_env(:flezha, :authn_authz, Keyword.put(config, :enabled, false))
    refute Flezha.AuthZ.enabled?()
    boundary = System.monotonic_time(:microsecond)

    request_spawn(ctx)

    assert {"ref-spawn", %{outcome: "spawned", error: ""}} = await_response()
    assert_start_recorded(boundary, ctx.instance_id)
  end

  test "a start another node would run is judged here, before anything is forwarded", ctx do
    # No runner for the factory on this node, one on a peer that is not there:
    # a forward would answer that the peer could not be reached.
    stub(FleetManager, :runners_for_factory, fn _ns, _factory -> [] end)
    stub(Directory, :factory_nodes, fn _ns, _factory -> [:"absent-peer@127.0.0.1"] end)

    request_spawn(ctx)

    assert {"ref-spawn", %{outcome: "refused", error: error}} = await_response()
    assert error =~ "forbidden"
    refute error =~ "routed_call_failed"
  end

  defp run_instance(%{registry: registry, instance_id: instance_id}) do
    builder = %Builder{id: instance_id, factory_id: @factory, kind: :workflow, init_values: %{}}
    FakeStartRegistry.insert(registry, self(), builder)
  end

  defp request_spawn(%{channel: channel, instance_id: instance_id}) do
    payload =
      ControlFrame.encode_request(
        "ref-spawn",
        {:spawn_workflow_request,
         %RunnerCommands.SpawnWorkflowRequest{
           instance_id: instance_id,
           factory_id: @factory,
           payload: "p1",
           if_running: "deliver"
         }}
      )

    send(channel, {:flz_data, :stream_id, "control_command", payload, %{}})
  end

  defp await_response do
    assert_receive {:written, _ref, framed}, 5_000

    %RunnerCommands.CommandRequest{
      reference_id: reference_id,
      command: {:spawn_workflow_response, response}
    } = ControlFrame.decode_request(framed)

    {reference_id, response}
  end

  defp assert_start_recorded(boundary, instance_id) do
    record = NodeTestHelper.wait_until(fn -> audit_record(:start, boundary) end, 2_000, 25)
    assert record, "the permitted start wrote no :start record on :operative_service"

    assert record.metadata.action == :"operative.start"
    assert record.metadata.namespace == @ns
    assert record.metadata.principal == @principal
    assert record.metadata.caller_source == :sdk
    assert record.metadata.operative == instance_id
    assert record.metadata.outcome == :ok
  end

  defp audit_record(name, boundary) do
    :operative_service
    |> RingBufferSink.query(boundary)
    |> Enum.find(&(&1.name == name))
  end
end

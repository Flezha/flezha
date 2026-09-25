defmodule Flezha.SDK.SpawnTest do
  @moduledoc """
  Tasklet-requested spawn in the SDK's own core, mirroring the reference's
  matrix in `sdk/rust/src/tasklet/tests/spawn.rs`: the outcome mapping, the
  call's options, the handle's two reads, the replay skip, the answers with no
  runner and with no control plane, and the runner server's waiter: its
  deadline, its claim and the one warning a failure nobody read gets.

  The stand-in runner is this test process. A request reaches it as the cast
  the runner server reads, and the server's own callbacks are handed that cast,
  so what is proved is the server's answer rather than a double's.
  """
  use ExUnit.Case, async: true

  import FlezhaTest.Helper.SdkOperatorHarness

  alias Flezha.SDK.{Codec, Counters, Ctx, Hold, RunnerServer, Spawn}
  alias Flezha.SDK.Operator.Inbox
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.SdkSpawnFixtures.{Spawner, SpawnerOp}

  # A request made with this process as its runner, and the cast it sent.
  defp requested(opts \\ [id: "w1"]) do
    handle = Spawn.request(self(), false, "worker", "payload", opts)
    assert_receive {:"$gen_cast", {:spawn, _caller, %Spawn{}, "payload", _policy} = cast}
    {handle, cast}
  end

  # A request answered as the runner server answers one, claimed first so the
  # mapping is read without the unread warning.
  defp resolve(outcome, error \\ "") do
    {handle, {:spawn, caller, spawn, _payload, _policy}} = requested()
    Spawn.answer(caller, %{spawn | claimed?: true}, {outcome, "w1"}, error)
    Spawn.await(handle, 1_000)
  end

  # A runner server whose control plane is a stream this process reads.
  defp connected do
    {sdk_side, _controller_side} = own_secondary()
    %RunnerServer.State{control_handle: sdk_side, control: Hold.new()}
  end

  # The controller's answer to `reference_id`, as the control stream delivers it.
  defp answered(reference_id, outcome, error, instance_id) do
    response = %RunnerCommands.SpawnBoundedResponse{
      outcome: outcome,
      error: error,
      instance_id: instance_id
    }

    bin = Codec.encode_request(reference_id, {:spawn_bounded_response, response})
    {:flz_data, "control", "control_command", bin, %{}}
  end

  defp warnings_naming(log, instance_id) do
    marker = "spawn_bounded(factory `worker`, id `#{instance_id}`) failed"
    length(String.split(log, marker)) - 1
  end

  describe "what a controller answer resolves to" do
    test "a spawn and a delivery are distinguishable outcomes carrying the id" do
      assert resolve("spawned") == {:ok, %{id: "w1", outcome: :spawned}}
      assert resolve("delivered") == {:ok, %{id: "w1", outcome: :delivered}}
    end

    test "a spawn without an id resolves the id the controller generated" do
      {handle, {:spawn, caller, spawn, _payload, :deliver}} = requested([])
      assert spawn.instance_id == ""

      Spawn.answer(caller, %{spawn | claimed?: true}, {"spawned", "worker-k7m2qzxw4pd3fn2a"}, "")

      assert Spawn.await(handle, 1_000) ==
               {:ok, %{id: "worker-k7m2qzxw4pd3fn2a", outcome: :spawned}}
    end

    test "the error policy and an unknown factory resolve errors naming the ids" do
      assert resolve("already_running") == {:error, {:already_running, "w1"}}
      assert resolve("factory_unknown") == {:error, {:factory_unknown, "worker"}}
    end

    test "a refusal carries the controller's reason" do
      assert resolve("refused", "quota_exceeded") == {:error, {:refused, "quota_exceeded"}}
    end

    test "an outcome this SDK does not know is a refusal naming the word" do
      assert resolve("adopted") == {:error, {:refused, "unknown outcome `adopted`"}}
    end
  end

  describe "the handle" do
    test "the call returns before the controller answers, and try_result leaves it pending" do
      {handle, {:spawn, caller, spawn, _payload, _policy}} = requested()
      assert Spawn.try_result(handle) == :pending

      Spawn.answer(caller, %{spawn | claimed?: true}, {"spawned", "w1"}, "")
      assert Spawn.try_result(handle) == {:ok, %{id: "w1", outcome: :spawned}}
    end

    test "a reply that never arrives times out rather than hanging" do
      {handle, _cast} = requested()
      assert Spawn.await(handle, 50) == {:error, :timeout}
    end

    test "await waits no longer than the deadline the request was made with" do
      {handle, _cast} = requested()
      passed = %{handle | deadline: System.monotonic_time(:millisecond)}

      {micros, result} = :timer.tc(fn -> Spawn.await(passed, 10_000) end)

      assert result == {:error, :timeout}
      assert micros < 5_000_000, "await waited #{div(micros, 1_000)} ms past its deadline"
    end
  end

  describe "the context a tasklet is handed" do
    test "with no runner the request resolves unavailable at once and sends nothing" do
      ctx = Ctx.new(Spawner, nil, 0)

      assert Spawn.try_result(Ctx.spawn_bounded(ctx, "worker", "payload", id: "w1")) ==
               {:error, :unavailable}

      assert Spawn.try_result(Ctx.spawn_bounded(ctx, "worker", "payload")) ==
               {:error, :unavailable}

      refute_received {:"$gen_cast", _}
    end

    test "a replayed invocation issues no request, and answers the given id or none" do
      ctx = Ctx.new(Spawner, nil, 0, runner: self(), replay: true)

      named = Ctx.spawn_bounded(ctx, "worker", "payload", id: "w1")
      assert Spawn.await(named, 1_000) == {:ok, %{id: "w1", outcome: :skipped_on_replay}}

      unnamed = Ctx.spawn_bounded(ctx, "worker", "payload")
      assert Spawn.await(unnamed, 1_000) == {:ok, %{id: "", outcome: :skipped_on_replay}}

      refute_received {:"$gen_cast", _}
    end

    test "a live invocation casts the request with its policy, deliver when only an id is given" do
      ctx = Ctx.new(Spawner, nil, 0, runner: self())
      caller = self()

      _handle = Ctx.spawn_bounded(ctx, "worker", "payload", id: "w1", if_running: :error)

      assert_received {:"$gen_cast",
                       {:spawn, ^caller,
                        %Spawn{
                          instance_id: "w1",
                          factory_id: "worker",
                          reference_id: "spawn-bounded-w1-" <> _
                        }, "payload", :error}}

      _handle = Ctx.spawn_bounded(ctx, "worker", "payload", id: "w2")
      assert_received {:"$gen_cast", {:spawn, ^caller, %Spawn{instance_id: "w2"}, _, :deliver}}
    end

    test "if_running without an id raises naming both, and sends nothing" do
      ctx = Ctx.new(Spawner, nil, 0, runner: self())

      error =
        assert_raise ArgumentError, fn ->
          Ctx.spawn_bounded(ctx, "worker", "payload", if_running: :error)
        end

      assert error.message =~ "`if_running`"
      assert error.message =~ "`id`"
      refute_received {:"$gen_cast", _}
    end

    test "a malformed option raises at the call, and sends nothing" do
      ctx = Ctx.new(Spawner, nil, 0, runner: self())

      for opts <- [[instance: "w1"], [id: ""], [id: :w1], [id: "w1", if_running: :replace]] do
        assert_raise ArgumentError, fn -> Ctx.spawn_bounded(ctx, "worker", "payload", opts) end
      end

      refute_received {:"$gen_cast", _}
    end
  end

  describe "the runner server's waiter" do
    test "with no control plane the request resolves unavailable at once and keeps no waiter" do
      {handle, cast} = requested(id: "w-down")

      log =
        AuthTestHelper.capture_log_at(:warning, fn ->
          assert {:noreply, %RunnerServer.State{waiters: waiters}} =
                   RunnerServer.handle_cast(cast, %RunnerServer.State{})

          assert waiters == %{}
        end)

      assert Spawn.try_result(handle) == {:error, :unavailable}
      assert warnings_naming(log, "w-down") == 1
    end

    test "a written request waits under its reference id until the controller's answer settles it" do
      {handle, {:spawn, _caller, spawn, _payload, _policy} = cast} =
        requested(id: "w1", if_running: :error)

      id = spawn.reference_id

      assert {:noreply, s} = RunnerServer.handle_cast(cast, connected())
      assert %{^id => _waiter} = s.waiters

      assert_receive {:flz_data, _stream, "control_command", bin, _meta}

      assert {:ok, %{reference_id: ^id, command: {:spawn_bounded_request, request}}} =
               Codec.decode_command(bin)

      assert %{instance_id: "w1", factory_id: "worker", payload: "payload", if_running: "error"} =
               request

      assert {:noreply, s} = RunnerServer.handle_info(answered(id, "delivered", "", "w1"), s)
      assert s.waiters == %{}
      assert Spawn.await(handle, 1_000) == {:ok, %{id: "w1", outcome: :delivered}}

      # The deadline and a late claim find nothing left to settle.
      assert {:noreply, ^s} = RunnerServer.handle_info({:spawn_deadline, id}, s)
      assert {:noreply, ^s} = RunnerServer.handle_cast({:claim_spawn, id}, s)
    end

    test "a request without an id is written with an empty id and resolves the one the controller answered" do
      {handle, {:spawn, _caller, spawn, _payload, _policy} = cast} = requested([])
      id = spawn.reference_id

      assert {:noreply, s} = RunnerServer.handle_cast(cast, connected())
      assert_receive {:flz_data, _stream, "control_command", bin, _meta}

      assert {:ok, %{reference_id: ^id, command: {:spawn_bounded_request, request}}} =
               Codec.decode_command(bin)

      assert %{instance_id: "", factory_id: "worker", if_running: "deliver"} = request

      generated = "worker-k7m2qzxw4pd3fn2a"
      assert {:noreply, s} = RunnerServer.handle_info(answered(id, "spawned", "", generated), s)
      assert s.waiters == %{}
      assert Spawn.await(handle, 1_000) == {:ok, %{id: generated, outcome: :spawned}}
    end

    test "an unread failure of a request without an id is logged once naming an empty id" do
      {handle, cast} = requested([])

      log =
        AuthTestHelper.capture_log_at(:warning, fn ->
          RunnerServer.handle_cast(cast, %RunnerServer.State{})
        end)

      assert Spawn.try_result(handle) == {:error, :unavailable}
      assert warnings_naming(log, "") == 1
    end

    test "an unanswered request settles timeout at its deadline, leaves the table, and is logged once" do
      {handle, {:spawn, caller, spawn, payload, policy}} = requested(id: "w-unanswered")
      id = spawn.reference_id
      due = %{spawn | deadline: System.monotonic_time(:millisecond)}

      assert {:noreply, s} =
               RunnerServer.handle_cast({:spawn, caller, due, payload, policy}, connected())

      assert_receive {:spawn_deadline, ^id}, 2_000

      log =
        AuthTestHelper.capture_log_at(:warning, fn ->
          assert {:noreply, settled} = RunnerServer.handle_info({:spawn_deadline, id}, s)
          assert settled.waiters == %{}
        end)

      assert Spawn.try_result(handle) == {:error, :timeout}
      assert warnings_naming(log, "w-unanswered") == 1
    end

    test "a failure whose handle was read before the answer is not logged" do
      {handle, {:spawn, _caller, spawn, _payload, _policy} = cast} = requested(id: "w-claimed")
      id = spawn.reference_id
      assert {:noreply, s} = RunnerServer.handle_cast(cast, connected())

      assert Spawn.try_result(handle) == :pending
      assert_receive {:"$gen_cast", {:claim_spawn, ^id} = claim}
      assert {:noreply, s} = RunnerServer.handle_cast(claim, s)

      log =
        AuthTestHelper.capture_log_at(:warning, fn ->
          RunnerServer.handle_info(answered(id, "refused", "quota_exceeded", "w-claimed"), s)
        end)

      assert Spawn.await(handle, 1_000) == {:error, {:refused, "quota_exceeded"}}
      assert warnings_naming(log, "w-claimed") == 0
    end
  end

  describe "inside a running tasklet" do
    test "a refused spawn leaves the invocation intact: the emit ships once and the handle resolves the refusal" do
      %{op: op, counters: counters} = start_operator(SpawnerOp)
      Inbox.input(op, "in", "w-refused", %{})

      assert_receive {:"$gen_cast", {:spawn, caller, %Spawn{} = spawn, "payload", :deliver}},
                     2_000

      Spawn.answer(caller, %{spawn | claimed?: true}, {"refused", "w-refused"}, "quota_exceeded")

      assert [{"op-1", payload, _meta}] = frames(1)
      assert payload == inspect({:error, {:refused, "quota_exceeded"}})
      refute_received {:"$gen_cast", {:spawn, _, _, _, _}}
      assert Counters.dispatches(counters) == 1
    end

    test "a failure nobody reads is logged exactly once by the runner server" do
      %{op: op} = start_operator(SpawnerOp)
      Inbox.input(op, "in", "unread:w-unread", %{})

      assert [{"op-1", "sent", _meta}] = frames(1)

      assert_received {:"$gen_cast",
                       {:spawn, _caller, %Spawn{} = spawn, _payload, _policy} = cast}

      refute_received {:"$gen_cast", {:claim_spawn, _}}

      assert {:noreply, s} = RunnerServer.handle_cast(cast, connected())

      log =
        AuthTestHelper.capture_log_at(:warning, fn ->
          RunnerServer.handle_info(
            answered(spawn.reference_id, "factory_unknown", "", "w-unread"),
            s
          )
        end)

      assert warnings_naming(log, "w-unread") == 1
    end
  end
end

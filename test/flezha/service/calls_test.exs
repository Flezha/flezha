defmodule FlezhaTest.Service.CallsTest do
  @moduledoc """
  The delivery of relays' calls: the forward worker a caller's requests to
  one target share, the per-payload deadline and idle stop it gains for them,
  and `RelayManager.spawn_or_deliver/8`'s call options over
  `FakeStartRegistry`, whose `insert_new` is the real registry claim.
  Authorization is off here; the judgment is the last describe's.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller's delivery.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Plugin.Provider.RelayForward.ForwardWorker
  alias Flezha.Process.CallDelivery
  alias Flezha.Service.{HubManager, RelayManager}
  alias Flezha.State.{Outbox, TestHelpers, WriterRegistry}
  alias FlezhaTest.Helper.FakeStartRegistry
  alias FlezhaTest.Helper.FakeStartRegistry.Instance

  @ns "calls-delivery-test"
  @caller %{source: :sdk, principal: "runner@" <> @ns, namespace: @ns}

  setup :set_mimic_global_until_exit

  defp now, do: System.system_time(:millisecond)

  defp call(id, overrides \\ %{}),
    do:
      Map.merge(
        %{
          caller: "gateway",
          call: id,
          deadline_at: now() + 5_000,
          created: false,
          factory: "agent"
        },
        overrides
      )

  describe "a forward worker delivering calls" do
    defp worker(opts) do
      base = %{
        plugin_id: "call",
        source_namespace: @ns,
        source_relay_id: "gateway",
        target_relay_id: "call",
        queue_max: :infinity,
        retry_backoff_ms: 10,
        retry_max_backoff_ms: 20,
        call_timeout_ms: 1_000,
        confirm: false,
        owner: self()
      }

      start_supervised!({ForwardWorker, Map.merge(base, opts)})
    end

    test "retries a call the callee cannot take yet and delivers it before its deadline" do
      test_pid = self()
      attempts = :counters.new(1, [])

      pid =
        worker(%{
          deliver: fn message, _md ->
            :counters.add(attempts, 1, 1)

            if :counters.get(attempts, 1) < 3,
              do: {:error, :not_ready},
              else: send(test_pid, {:delivered, message}) && :ok
          end,
          expires_at: fn _message, md -> md.deadline_at end
        })

      :ok = ForwardWorker.absorb(pid, {"turn", %{deadline_at: now() + 5_000}})
      assert_receive {:delivered, "turn"}, 2_000
      assert :counters.get(attempts, 1) == 3
    end

    test "drops a call at its deadline instead of retrying it" do
      test_pid = self()

      pid =
        worker(%{
          deliver: fn _message, _md -> {:error, :not_ready} end,
          expires_at: fn _message, md -> md.deadline_at end,
          on_expire: fn message, _md -> send(test_pid, {:expired, message}) end
        })

      :ok = ForwardWorker.absorb(pid, {"late", %{deadline_at: now() + 100}})
      assert_receive {:expired, "late"}, 2_000
      assert %{buffer_len: 0} = ForwardWorker.stats(pid)
    end

    test "tells its owner it is idle once its queue has been empty that long" do
      pid = worker(%{deliver: fn _m, _md -> :ok end, idle_ms: 50})
      :ok = ForwardWorker.absorb(pid, {"one", %{}})
      assert_receive {:worker_idle, ^pid}, 1_000
    end
  end

  describe "the router" do
    test "delivers two requests from one caller to one target in order, then stops the idle worker" do
      on_exit(TestHelpers.override_env(:call_worker_idle_ms, 100))
      test_pid = self()

      stub(RelayManager, :spawn_or_deliver, fn @ns,
                                               "anna-42",
                                               "agent",
                                               payload,
                                               :deliver,
                                               _ctx,
                                               id,
                                               opts ->
        send(test_pid, {:reached, payload, id, opts})
        {:delivered, "anna-42"}
      end)

      stub(RelayManager, :get_relay_pid, fn @ns, "gateway" -> {:ok, test_pid} end)

      request = fn id, text ->
        %{
          id: id,
          target: "factory:agent",
          key: "anna-42",
          create: false,
          channel: "turn",
          request: text,
          response: "ask.response",
          deadline_at: now() + 5_000,
          deadline_ms: 5_000,
          callee: nil,
          created: false
        }
      end

      CallDelivery.request(@ns, "gateway", request.("c1", "turn 1"))
      CallDelivery.request(@ns, "gateway", request.("c2", "turn 2"))

      assert_receive {:reached, "turn 1", "c1", opts}
      assert_receive {:reached, "turn 2", "c2", _opts}
      assert opts[:if_absent] == :not_found
      assert opts[:runs] == [:steps, :flow]
      assert opts[:metadata] == %{"channel" => "turn"}
      assert %{caller: "gateway", call: "c1", factory: "agent"} = opts[:call]
      assert_receive {:"$gen_cast", {:call_accepted, "c1", "anna-42", false}}
      assert_receive {:"$gen_cast", {:call_accepted, "c2", "anna-42", false}}

      router = Process.whereis(CallDelivery.name())
      assert eventually(fn -> :sys.get_state(router).workers == %{} end)
    end

    # Each case answers from its own callee, so each has its own worker.
    defp answer_to_gateway(id, metadata \\ %{}),
      do:
        CallDelivery.answer(
          @ns,
          "agent-" <> id,
          call(id),
          %{result: {:ok, "hi"}, target: "agent-" <> id, created: false},
          metadata
        )

    # The answer's delivery, in flight until the test replies to it, and the
    # worker holding it.
    defp answer_in_flight(id) do
      test_pid = self()
      stub(RelayManager, :get_relay_pid, fn @ns, "gateway" -> {:ok, test_pid} end)
      answer_to_gateway(id)
      assert_receive {:"$gen_call", from, {:call_answer, ^id, _outcome}}
      router = Process.whereis(CallDelivery.name())

      {:ok, worker} =
        Map.fetch(:sys.get_state(router).workers, {:answer, @ns, "agent-" <> id, "gateway"})

      {router, worker, from}
    end

    test "an idle report racing a delivery in flight keeps the worker, so nothing goes twice" do
      {router, worker, from} = answer_in_flight("c1")

      send(router, {:worker_idle, worker})
      assert %{{:answer, @ns, "agent-c1", "gateway"} => ^worker} = :sys.get_state(router).workers

      GenServer.reply(from, :ok)
      assert eventually(fn -> ForwardWorker.stats(worker).buffer_len == 0 end)
      refute_received {:"$gen_call", _, {:call_answer, _, _}}
    end

    test "a router that crashes takes its workers down with it" do
      {router, worker, from} = answer_in_flight("c2")
      ref = Process.monitor(worker)

      log =
        capture_log(fn ->
          Process.exit(router, :kill)
          assert_receive {:DOWN, ^ref, :process, ^worker, _reason}, 1_000
          assert eventually(fn -> Process.whereis(CallDelivery.name()) not in [nil, router] end)
        end)

      assert log =~
               "Child Flezha.Service.Calls.Router of Supervisor Flezha.Service.Calls terminated"

      GenServer.reply(from, :ok)
    end

    # Each `answer_dropped` reaches the test as `{:answer_dropped, metadata}`,
    # and each confirmation to the answer's Cell as the writer's own cast.
    defp observe_answer(cell) do
      test_pid = self()
      handler = "calls-test-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:flezha, :call, :answer_dropped],
        fn _event, _m, metadata, _ -> send(test_pid, {:answer_dropped, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      stub(WriterRegistry, :lookup, fn
        ^cell -> {:ok, test_pid}
        other -> Mimic.call_original(WriterRegistry, :lookup, [other])
      end)
    end

    test "an answer to a caller no longer declared is dropped at once, its hold answered" do
      cell = {"agent", "account", "k"}
      observe_answer(cell)

      stub(RelayManager, :get_relay_pid, fn @ns, "gateway" ->
        {:error, {:relay_not_found, @ns, "gateway"}}
      end)

      stub(HubManager, :declared_relays, fn @ns, _unreadable ->
        [%{id: "agent-c3", factory_id: nil}]
      end)

      answer_to_gateway("c3", %{"flz.ack" => Outbox.token(cell, {:cursor, 4})})
      assert_receive {:"$gen_cast", {:outbox_confirmed, {:cursor, 4}}}, 1_000

      assert_receive {:answer_dropped,
                      %{reason: "caller_gone", caller: "gateway", relay_id: "agent-c3"}}
    end

    test "an answer to an absent caller whose hub cannot be read is retried, not dropped" do
      test_pid = self()
      cell = {"agent", "account", "k"}
      observe_answer(cell)
      tries = :counters.new(1, [])

      # The namespace has no live hub here: its declared set cannot be read.
      stub(RelayManager, :get_relay_pid, fn @ns, "gateway" ->
        :counters.add(tries, 1, 1)

        if :counters.get(tries, 1) == 1,
          do: {:error, {:relay_not_found, @ns, "gateway"}},
          else: {:ok, test_pid}
      end)

      answer_to_gateway("c4", %{"flz.ack" => Outbox.token(cell, {:cursor, 5})})
      assert_receive {:"$gen_call", from, {:call_answer, "c4", _outcome}}, 2_000
      assert :counters.get(tries, 1) == 2
      GenServer.reply(from, :ok)
      assert_receive {:"$gen_cast", {:outbox_confirmed, {:cursor, 5}}}, 1_000
      refute_received {:answer_dropped, _metadata}
    end
  end

  defp eventually(fun, tries \\ 40) do
    fun.() or (tries > 0 and (Process.sleep(25) || eventually(fun, tries - 1)))
  end

  describe "spawn_or_deliver for a call" do
    setup do
      %{registry: FakeStartRegistry.install(runners: ["runner-1"], run: :steps)}
    end

    defp call_to(id, opts),
      do:
        RelayManager.spawn_or_deliver(
          @ns,
          id,
          "agent",
          "turn",
          :deliver,
          @caller,
          Keyword.get(opts, :identity, "c1"),
          Keyword.merge([runs: [:steps, :flow], metadata: %{}, call: call("c1")], opts)
        )

    test "creates the instance with the call as its seed, then delivers the next call", %{
      registry: registry
    } do
      assert {:spawned, "anna-42"} = call_to("anna-42", [])
      assert [builder] = FakeStartRegistry.started()
      assert {"turn", seed_md} = Keyword.fetch!(builder.opts, :seed_input)
      assert seed_md["flz.msg_id"] == "c1"

      assert %{created: true, call: "c1", caller: "gateway"} =
               Keyword.fetch!(builder.opts, :seed_call)

      assert {:delivered, "anna-42"} = call_to("anna-42", identity: "c2", call: call("c2"))
      {:ok, pid} = FakeStartRegistry.pid_of(registry, "anna-42")
      assert [{"turn", %{call: "c2", created: false}}] = Instance.calls(pid)
    end

    test "two concurrent calls for one absent id start one instance and deliver the other", %{
      registry: registry
    } do
      results =
        for id <- ["c1", "c2"] do
          Task.async(fn -> call_to("bob-7", identity: id, call: call(id)) end)
        end
        |> Enum.map(&Task.await/1)

      assert Enum.sort(Enum.map(results, &elem(&1, 0))) == [:delivered, :spawned]
      # Both may try the start; the registry's claim lets one through.
      assert [%{id: "bob-7"}] = FakeStartRegistry.rows(registry)
      {:ok, pid} = FakeStartRegistry.pid_of(registry, "bob-7")
      assert [_other] = Instance.calls(pid)
    end

    test "an absent id with creation off is relay.not_found and starts nothing" do
      assert {{:refused, %Flezha.Error{code: "relay.not_found"}}, "carol-1"} =
               call_to("carol-1", if_absent: :not_found)

      assert FakeStartRegistry.started() == []
    end

    test "a creation past the namespace's max_relays is quota.exceeded" do
      stub(Flezha.AuthZ.Quotas, :check_allocation, fn _ns, key, _n ->
        {:error, {:quota_exceeded, key, %{limit: 0}}}
      end)

      assert {{:refused, %Flezha.Error{code: "quota.exceeded"}}, _id} = call_to("dave-1", [])
    end

    test "a call without a key starts a steps instance per call under a generated id" do
      assert {:spawned, a} = call_to("", identity: "c1", call: call("c1"))
      assert {:spawned, b} = call_to("", identity: "c2", call: call("c2"))
      assert a != b and a =~ ~r/\Aagent-/

      assert {:spawned, ^a} = call_to("", identity: "c1", call: call("c1")),
             "a repeat finds its own"
    end
  end

  describe "spawn_or_deliver for a call to a flow factory" do
    setup do
      %{registry: FakeStartRegistry.install(runners: ["runner-1"], run: :flow)}
    end

    test "a key starts a flow instance; no key is refused as not steps" do
      opts = [runs: [:steps, :flow], metadata: %{}, call: call("c1")]

      assert {:spawned, "user-1"} =
               RelayManager.spawn_or_deliver(
                 @ns,
                 "user-1",
                 "agent",
                 "t",
                 :deliver,
                 @caller,
                 "c1",
                 opts
               )

      assert [%{run: :flow}] = FakeStartRegistry.started()

      assert {{:refused, %Flezha.Error{code: "relay.factory_not_steps"}}, _} =
               RelayManager.spawn_or_deliver(@ns, "", "agent", "t", :deliver, @caller, "c2", [
                 {:runs, [:steps]} | Keyword.delete(opts, :runs)
               ])
    end
  end
end

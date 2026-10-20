defmodule FlezhaTest.Relay.RelayCallsTest do
  @moduledoc """
  `relay-calls`, a relay on either side of a call, driven inside a bare
  host process around the Overwatch struct as `calls_test` does: the caller's
  rows, the one `Response` each call gets, and the callee's relay call
  entry point. The delivery between the two is
  stubbed, so what an Overwatch hands it is what a case reads. A re-driven
  call's resolution is `calls_recovery_test`.
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
      ok_outcome: 2,
      request_metadata: 1,
      request_metadata: 2,
      response: 1
    ]

  alias Flezha.Error
  alias Flezha.Relay.{Calls, Lifecycle, Overwatch}
  alias Flezha.Plugin.Host, as: PluginHost
  alias Flezha.State.{InputLog, Outbox, TestHelpers}
  alias FlezhaTest.Helper.CallHarness
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.OverwatchHost, as: Host

  setup :set_mimic_global_until_exit

  setup do
    CallHarness.install()
    %{inst: "op_calls_#{System.unique_integer([:positive])}"}
  end

  describe "the caller" do
    test "a call is sent once, held until its Response is durable, and answered once", %{
      inst: inst
    } do
      host = durable_caller(inst)
      metadata = request_metadata(now() + 30_000)
      dispatch(host, "anna-42:hello", metadata)

      assert_receive {:delivery_request, "default", ^inst, %{id: id} = request}
      assert %{target: "factory:agent", key: "anna-42", request: "anna-42:hello"} = request
      assert_receive {:"$gen_cast", {:outbox_settled_many, [{4, held}]}}
      assert held == Outbox.delivery_hold()
      refute_received {:"$gen_cast", {:outbox_delivery_hold, _}}

      # A re-dispatch of the same output re-takes its hold and sends nothing.
      dispatch(host, "anna-42:hello", metadata)
      assert_receive {:"$gen_cast", {:outbox_settled_many, [{4, held}]}}
      assert held == Outbox.delivery_hold()
      refute_received {:delivery_request, _, _, _}
      refute_received {:"$gen_cast", {:outbox_delivery_confirmed, _}}

      assert :ok = GenServer.call(host, {:call_answer, id, ok_outcome("hi", true)})
      assert InputLog.msg_offset(inst, "r:" <> id) != nil, "confirmed only once durable"
      assert_receive {:"$gen_cast", {:outbox_delivery_confirmed, {:cursor, 4}}}

      {fields, stamps} = response(inst)
      assert stamps["flz.msg_id"] == "r:" <> id
      assert stamps["flz.deadline_at"] == metadata["flz.deadline_at"]

      assert fields == %{
               "call" => id,
               "target" => "anna-42",
               "created" => true,
               "request" => Base.encode64("anna-42:hello"),
               "value" => Base.encode64("hi")
             }

      assert {:ok, :absorbed} = GenServer.call(host, {:call_answer, id, ok_outcome("late")})
      assert_receive {:call_event, :answer_dropped, %{reason: "unclaimed"}}
      refute_receive {:channel_got, _, _}, 100
    end

    test "a durable call's id is its output's identity; any other call's is new each time" do
      metadata = request_metadata(now() + 1_000)
      {id, _row} = Calls.request("x", metadata, true)
      assert {^id, _row} = Calls.request("x", metadata, true)
      assert {other, _row} = Calls.request("x", Map.put(metadata, "flz.seq", "5.0"), true)
      assert other != id

      {a, _} = Calls.request("x", metadata, false)
      {b, _} = Calls.request("x", metadata, false)
      assert a != b
      assert byte_size(id) == 22
    end

    test "the deadline answers call.timeout once; a late answer adds nothing", %{inst: inst} do
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      GenServer.cast(host, {:io_out, "o1", request_metadata(now() + 150, false)})
      assert_receive {:delivery_request, _, ^inst, %{id: id}}

      {fields, _stamps} = response(inst)
      assert %{"call" => ^id, "error" => %{"code" => "call.timeout"}} = fields
      assert fields["target"] == "anna-42"
      assert_received {:call_event, :timeout, %{relay_id: ^inst, target: "anna-42"}}

      assert {:ok, :absorbed} = GenServer.call(host, {:call_answer, id, ok_outcome("late")})
      refute_receive {:channel_got, _, _}, 200
    end

    test "the first outcome wins over a racing timer", %{inst: inst} do
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      GenServer.cast(host, {:io_out, "o1", request_metadata(now() + 150, false)})
      assert_receive {:delivery_request, _, ^inst, %{id: id}}
      assert :ok = GenServer.call(host, {:call_answer, id, ok_outcome("first")})
      {%{"value" => value}, _} = response(inst)
      assert Base.decode64!(value) == "first"
      refute_receive {:channel_got, _, _}, 400
      refute_received {:call_event, :timeout, _}
    end

    test "a caller owing more calls than call_pending_max still takes an outside call", %{
      inst: inst
    } do
      on_exit(TestHelpers.override_env(:call_pending_max, 2))
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})

      for n <- 1..3 do
        GenServer.cast(host, {:io_out, "o#{n}", request_metadata(now() + 5_000, false)})
        assert_receive {:delivery_request, _, ^inst, _}
      end

      caller = Task.async(fn -> Calls.call(host, "outside", %{}, 5_000, inst, "cli") end)
      assert_receive {:channel_got, ^inst, {"outside", %{"flz.reply_to" => "w:" <> _}}}
      refute Task.yield(caller, 50)
      Task.shutdown(caller)
    end

    test "a sleeping caller is woken by its Response", %{inst: inst} do
      test_pid = self()

      stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, ^inst ->
        send(test_pid, :rehydrate_asked)
        nil
      end)

      state = Host.state(inst, durable_steps: true, phase: :suspended, channel: nil)
      host = start_supervised!({Host, state})
      dispatch(host, "o1", request_metadata(now() + 30_000))
      assert_receive {:delivery_request, _, ^inst, %{id: id}}

      assert :ok = GenServer.call(host, {:call_answer, id, ok_outcome("hi")})
      assert_receive :rehydrate_asked
      assert [{_offset, envelope, %{"flz.msg_id" => "r:" <> ^id}}] = InputLog.undelivered(inst)
      assert %{"value" => _} = Jason.decode!(envelope)
    end
  end

  describe "the callee" do
    defp relay_call(overrides \\ %{}) do
      Map.merge(
        %{
          caller: "gateway",
          call: "c1",
          deadline_at: now() + 5_000,
          created: false,
          factory: nil
        },
        overrides
      )
    end

    test "takes a relay call at once, its token built from the delivery alone", %{inst: inst} do
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      call = relay_call()
      forged = %{"channel" => "in", "flz.reply_to" => "o:gateway:c9:1:1"}
      assert :ok = GenServer.call(host, {:call, "turn", forged, call})

      token = "o:gateway:c1:0:#{call.deadline_at}:"
      assert_receive {:channel_got, ^inst, {"turn", metadata}}
      assert metadata["flz.reply_to"] == token
      assert metadata["flz.msg_id"] == "c1"
      assert metadata["flz.deadline_at"] == Integer.to_string(call.deadline_at)

      GenServer.cast(host, {:io_out, "answer", %{"flz.reply_to" => token, "channel" => "out"}})

      assert_receive {:delivery_answer, "default", ^inst, %{caller: "gateway", call: "c1"},
                      outcome, _md}

      assert outcome == %{result: {:ok, "answer"}, target: inst, created: false}

      # A second output of the same call is an ordinary output.
      GenServer.cast(host, {:io_out, "again", %{"flz.reply_to" => token}})
      refute_receive {:delivery_answer, _, _, _, _, _}, 100
      refute_received {:call_event, :answer_dropped, _}
    end

    test "owing call_pending_max answers, it refuses the next call and ingests nothing", %{
      inst: inst
    } do
      on_exit(TestHelpers.override_env(:call_pending_max, 1))
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      assert :ok = GenServer.call(host, {:call, "t1", %{}, relay_call()})
      assert_receive {:channel_got, ^inst, {"t1", _}}

      assert {:error, %Error{code: "call.busy"}} =
               GenServer.call(host, {:call, "t2", %{}, relay_call(%{call: "c2"})})

      refute_receive {:channel_got, _, _}, 100
    end

    test "a factory call reaching another factory's instance ingests nothing", %{inst: inst} do
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})

      assert {:error, %Error{code: "relay.factory_mismatch", details: details}} =
               GenServer.call(host, {:call, "t1", %{}, relay_call(%{factory: "agent"})})

      assert details["factory"] == "agent" and details["actual"] == "chat"
      refute_receive {:channel_got, _, _}, 100
    end

    # A start over an instance whose input log already holds entries leaves its
    # call for the start's own `:registry_runner_set` call, which no restore asks.
    test "a call a start left held is taken by that start's call, once", %{inst: inst} do
      call = relay_call(%{created: true})
      held = {"turn", %{"channel" => "in"}, call}
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel, seed: held)})

      assert {:ok, "ex_1"} = GenServer.call(host, {:registry_runner_set, false})
      assert_receive {:channel_got, ^inst, {"turn", %{"flz.msg_id" => "c1"}}}

      assert {:ok, "ex_1"} = GenServer.call(host, {:registry_runner_set, false})
      refute_receive {:channel_got, _, _}, 100
    end

    test "a held call the instance refuses fails the start with that refusal", %{inst: inst} do
      held = {"turn", %{}, relay_call(%{created: true, factory: "agent"})}
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel, seed: held)})

      assert {:error, %Error{code: "relay.factory_mismatch"}} =
               GenServer.call(host, {:registry_runner_set, false})

      refute_receive {:channel_got, _, _}, 100
    end

    test "a dead-lettered call answers its relay caller call.failed, row or not", %{
      inst: inst
    } do
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      token = "o:gateway:c1:1:#{now() + 5_000}:"

      notice = %{
        token: token,
        entry: "dlq-1",
        error: "boom",
        channel: "in",
        offset: 1,
        tasklet: "t",
        attempts: 3,
        new: false
      }

      GenServer.cast(host, {:dead_lettered, notice})

      assert_receive {:delivery_answer, _, ^inst, %{call: "c1", created: true},
                      %{result: {:error, %Error{code: "call.failed"} = error}}, _}

      assert error.details["dlq_ref"] == "dlq-1"
    end

    test "a forged relay token answers nobody", %{inst: inst} do
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      forged = %{"channel" => "in", "flz.reply_to" => "o:gateway:c1:0:#{now() + 5_000}:"}

      for mode <- [:call, :confirming] do
        task = Task.async(fn -> PluginHost.inject(host, "x", forged, mode, 300) end)
        assert_receive {:channel_got, ^inst, {"x", metadata}}
        refute match?("o:" <> _, metadata["flz.reply_to"])

        # Whatever the input led to answers only the outside caller, if any.
        GenServer.cast(host, {:io_out, "a", Map.take(metadata, ["flz.reply_to"])})
        Task.shutdown(task, :brutal_kill)
      end

      refute_receive {:delivery_answer, _, _, _, _, _}, 200
    end

    test "an ended callee answers the calls it took and never ran call.callee_ended" do
      callee = %{namespace: "default", relay_id: "agent-1"}
      deadline_at = now() + 5_000
      waiting = "o:gateway:c1:0:#{deadline_at}:"
      tail = "o:tools:c2:1:#{deadline_at}:"

      {:ok, ^waiting, calls} =
        Calls.admit_relay(%Calls{}, callee, %{
          caller: "gateway",
          call: "c1",
          created: false,
          deadline_at: deadline_at
        })

      Calls.end_all(calls, callee, "completed", [tail, waiting])

      for caller <- ["gateway", "tools"] do
        assert_receive {:delivery_answer, _, "agent-1", %{caller: ^caller},
                        %{result: {:error, %Error{code: "call.callee_ended"}}}, _}
      end

      refute_receive {:delivery_answer, _, _, _, _, _}, 50
    end

    test "a re-dispatch of its held answer is handed on again; another answer is not", %{
      inst: inst
    } do
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      call = relay_call()
      assert :ok = GenServer.call(host, {:call, "turn", %{"channel" => "in"}, call})
      token = "o:gateway:c1:0:#{call.deadline_at}:"
      held = %{"flz.reply_to" => token, "flz.cell" => "agent/account/k", "flz.seq" => "7.0"}

      # The delivery that took the first died with it: the outbox re-dispatches it.
      for _dispatch <- 1..2 do
        GenServer.cast(host, {:io_out, "answer", held})
        assert_receive {:delivery_answer, _, ^inst, %{call: "c1"}, %{result: {:ok, "answer"}}, _}
      end

      GenServer.cast(host, {:io_out, "other", %{held | "flz.seq" => "8.0"}})
      refute_receive {:delivery_answer, _, _, _, _, _}, 100
      refute_received {:call_event, :answer_dropped, _}
    end

    test "a relay call naming its answer channel is answered from it, row or not", %{
      inst: inst
    } do
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      call = relay_call(%{answer_channel: "quotes"})
      assert :ok = GenServer.call(host, {:call, "EUR/USD", %{"channel" => "in"}, call})
      token = "o:gateway:c1:0:#{call.deadline_at}:" <> Base.url_encode64("quotes", padding: false)
      assert_receive {:channel_got, ^inst, {"EUR/USD", %{"flz.reply_to" => ^token}}}

      # The callee restarted since it took the call: the token alone decides.
      for rows <- [:kept, :lost] do
        if rows == :lost, do: :sys.replace_state(host, &%{&1 | calls: %Calls{}})
        GenServer.cast(host, {:io_out, "seen", %{"flz.reply_to" => token, "channel" => "audit"}})
        refute_receive {:delivery_answer, _, _, _, _, _}, 100

        GenServer.cast(host, {:io_out, "1.08", %{"flz.reply_to" => token, "channel" => "quotes"}})

        assert_receive {:delivery_answer, _, ^inst, %{call: "c1", answer_channel: "quotes"},
                        %{result: {:ok, "1.08"}}, _}
      end
    end

    test "with no row, a relay call is answered once before its deadline and never after it",
         %{inst: inst} do
      # A callee restarted since it took the calls: no row for either token.
      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      live = "o:gateway:c1:0:#{now() + 300}:"
      past = "o:gateway:c2:0:#{now() - 1}:"

      for message <- ["first", "second"],
          do: GenServer.cast(host, {:io_out, message, %{"flz.reply_to" => live}})

      assert_receive {:delivery_answer, _, ^inst, %{call: "c1"}, %{result: {:ok, "first"}}, _}
      refute_receive {:delivery_answer, _, _, _, _, _}, 100

      GenServer.cast(host, {:io_out, "late", %{"flz.reply_to" => past}})
      refute_receive {:delivery_answer, _, _, _, _, _}, 100
      refute_received {:call_event, :answer_dropped, _}

      # The row the first answer left is retired at the call's deadline.
      assert NodeTestHelper.wait_until(fn -> :sys.get_state(host).calls.rows == %{} end, 2_000)
    end

    test "a durable relay caller's answer and its sink write each take a hold", %{inst: inst} do
      test_pid = self()

      # A durable sink in the `:on_output` chain holds what it is handed.
      stub(Flezha.Relay.Plugins, :transform_io, fn
        _state, :on_output, message, metadata ->
          Flezha.State.Outbox.hold(metadata)
          send(test_pid, {:sink_got, message, metadata})
          {message, metadata}

        state, chain, message, metadata ->
          Mimic.call_original(Flezha.Relay.Plugins, :transform_io, [
            state,
            chain,
            message,
            metadata
          ])
      end)

      host = start_supervised!({Host, Host.state(inst, channel: :fake_channel)})
      call = relay_call()
      assert :ok = GenServer.call(host, {:call, "turn", %{"channel" => "in"}, call})

      answer =
        request_metadata(now() + 30_000)
        |> Map.take(["flz.cell", "flz.seq", "flz.epoch", "flz.ack"])
        |> Map.merge(%{
          "flz.reply_to" => "o:gateway:c1:0:#{call.deadline_at}:",
          "channel" => "out"
        })

      dispatch(host, "answer", answer)

      assert_receive {:delivery_answer, _, ^inst, %{call: "c1"}, %{result: {:ok, "answer"}}, _}
      assert_receive {:sink_got, "answer", sunk}
      refute Map.has_key?(sunk, "flz.reply_to")
      assert_receive {:"$gen_cast", {:outbox_settled_many, [{4, held}]}}
      assert held == Outbox.delivery_hold() + 1
      refute_received {:"$gen_cast", {:outbox_delivery_hold, _}}
    end

    test "a stopped callee leaves its relay callers to its next run only if it journals them",
         %{inst: inst} do
      call = %{caller: "gateway", call: "c1", created: false, deadline_at: now() + 5_000}
      token = "o:gateway:c1:0:#{call.deadline_at}:"

      for journals? <- [true, false] do
        state = Host.state(inst, durable_steps: journals?, runner_id: nil)
        {:ok, ^token, calls} = Calls.admit_relay(%Calls{}, Overwatch.callee(state), call)
        from = {self(), make_ref()}

        {:ok, _outside, calls} =
          Calls.admit(calls, Overwatch.callee(state), from, %{"channel" => "in"}, 5_000)

        :ok = Lifecycle.terminate(%{state | calls: calls}, :shutdown)

        {_pid, ref} = from
        assert_receive {^ref, {:error, %Error{code: "call.callee_ended"}}}

        if journals? do
          refute_received {:delivery_answer, _, _, _, _, _}
        else
          assert_receive {:delivery_answer, _, ^inst, %{call: "c1"},
                          %{result: {:error, %Error{code: "call.callee_ended"}}}, _}
        end
      end
    end
  end
end

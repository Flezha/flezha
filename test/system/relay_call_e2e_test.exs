defmodule FlezhaTest.System.RelayCallE2ETest do
  @moduledoc """
  A relay calling another, end to end on the runtime `FLEZHA_E2E_RUNTIME`
  selects, through the `relay_call` fixtures:

    * `call_orders` calls `call_pricing` by name (unbounded to unbounded),
      naming `call_pricing`'s sink `quotes` as its answer channel;
    * `call_gateway` calls `call_agent` by key through its factory (flow
      to a per-session durable steps relay): the first call creates the
      session, the second reaches it, a failed turn is answered `call.failed`
      while the session goes on, a `resume:` turn with creation off is refused
      `relay.not_found`, and a session asleep is woken by its call;
    * `call_tool_session` calls `call_pricing` from a turn and reads the
      outcome as its next one (a steps caller);
    * `call_orders_steps` is `call_orders` as a steps session, whose asking
      tasklet's channel ends only in the call: no runtime journals a handoff
      for it;

  a call waiting on a session that ends `cancelled` and keeps nothing,
  answered with that status; and the crash drills of a durable pair, each
  asserting exactly one outcome per call and one committed turn: the runner
  killed while a turn runs, and the gateway's Overwatch and Cell writers killed
  while a call waits.

  Every outcome is one sink line, `<request>=ok:<answer>@<target>/<created>`
  or `<request>=error:<code>@<target>/<created>`.

      FLEZHA_E2E_RUNTIME=node mix test --include system test/system/relay_call_e2e_test.exs
  """

  use FlezhaTest.SystemCase, capability: :relay_call

  import FlezhaTest.Helper.StepsResumeHarness,
    only: [
      await_status: 4,
      boot_runner: 4,
      collect_loop: 1,
      collected_with_meta: 1,
      forward_sink: 2,
      suspended?: 2
    ]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [
      await_outbox_drained: 1,
      cleanup: 3,
      crash: 2,
      force_fresh_rebind: 2,
      kill_overwatch: 2,
      os_pid: 1,
      sigkill: 1,
      unique: 1,
      unique_id_path: 0
    ]

  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{Listener, RelayManager}
  alias Flezha.Service.RelayManager.Teardown
  alias Flezha.Service.Steps.Journal
  alias Flezha.State.{InputLog, Paths, TaskJournal}
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.RelayTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 300_000
  @moduletag transport: :uds

  setup ctx do
    namespace = unique("relay_call")
    id_path = unique_id_path()
    # The fixtures call the instance `pricing` by name, and a class keeps the
    # namespace it first ran in while the controller lives: each test's
    # releases it once its namespace's tree is gone.
    on_exit(fn -> :ets.match_delete(:flezha_state_namespaces, {{:_, "pricing"}, :_}) end)
    on_exit(fn -> cleanup(namespace, nil, id_path) end)
    collector = spawn_link(fn -> collect_loop([]) end)
    {:ok, namespace: namespace, id_path: id_path, collector: collector, ctx: ctx}
  end

  defp boot(%{namespace: namespace, id_path: id_path, ctx: ctx}, env \\ []),
    do: boot_runner(namespace, ctx, id_path, release?: false, extra_env: env)

  defp start(namespace, factory, kind, id, collector \\ nil) do
    plugins = if collector, do: [forward_sink("out", collector)], else: []

    assert {:ok, _} =
             RelayManager.start_relay(namespace, %Builder{
               id: id,
               factory_id: factory,
               run: kind,
               opts: [bindings: RelayFixture.bindings(namespace, id, plugins)]
             })

    id
  end

  defp send_in(namespace, id, text, channel \\ "in"),
    do: :ok = RelayTestHelper.cast_io(id, namespace, text, %{"channel" => channel})

  # The lines the sink has, once it has `count` of them. A durable output
  # re-driven after a crash reaches the sink again under its identity, which a
  # sink de-duplicates by; so does this.
  defp lines(collector, count) do
    assert NodeTestHelper.wait_until(fn -> length(collected(collector)) >= count end, 60_000),
           "the sink holds #{inspect(collected(collector))}, not #{count} lines"

    collected(collector)
  end

  defp collected(collector) do
    collector
    |> collected_with_meta()
    |> Enum.uniq_by(fn {line, metadata} ->
      if metadata["flz.seq"],
        do: {metadata["flz.cell"], metadata["flz.seq"]},
        else: {line, make_ref()}
    end)
    |> Enum.map(&elem(&1, 0))
  end

  defp session(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"

  test "a flow relay calls another by name, naming its answer channel", ctx do
    boot(ctx)
    start(ctx.namespace, "call_pricing", :flow, "pricing")
    orders = start(ctx.namespace, "call_orders", :flow, unique("ordering"), ctx.collector)
    test = self()

    # A calls listener on the callee sees each call with its caller and source.
    calls =
      spawn(fn ->
        Listener.listen(
          ctx.namespace,
          "pricing",
          [
            direction: :calls,
            window_ms: 60_000,
            before: fn ->
              send(test, :watching)
              :ok
            end
          ],
          fn calls ->
            send(test, {:calls, calls})
            :continue
          end
        )
      end)

    assert_receive :watching, 30_000

    before = fn ->
      send_in(ctx.namespace, orders, "o1")
      send_in(ctx.namespace, orders, "o2")
    end

    # The callee's input carries the relay token the request's stamps built,
    # its last field the answer channel the call end named.
    assert {:ok, :done} =
             Listener.listen(
               ctx.namespace,
               "pricing",
               [direction: :input, window_ms: 60_000, before: before],
               fn
                 {:input, _payload, metadata} ->
                   send(test, {:token, metadata["flz.reply_to"]})
                   :done

                 _event ->
                   :continue
               end
             )

    assert_received {:token, "o:" <> _ = token}
    assert String.ends_with?(token, ":" <> Base.url_encode64("quotes", padding: false))

    assert lines(ctx.collector, 2) == [
             "o1=ok:price:o1@pricing/false",
             "o2=ok:price:o2@pricing/false"
           ]

    for request <- ["o1", "o2"] do
      assert_receive {:calls, {:call, ^request, %{"flz.call_id" => id} = called}}, 30_000

      assert %{"flz.call_source" => "relay", "flz.call_answer_channel" => "quotes"} = called
      assert called["flz.call_caller"] == orders

      assert_receive {:calls, {:answered, "price:" <> ^request, answered}}, 30_000
      assert %{"flz.call_id" => ^id, "channel" => "quotes"} = answered
    end

    Process.exit(calls, :kill)
  end

  test "a gateway creates a session, reaches it again, and outlives its failed turn", ctx do
    boot(ctx)
    gateway = start(ctx.namespace, "call_gateway", :flow, unique("gateway"), ctx.collector)
    anna = session("anna")

    send_in(ctx.namespace, gateway, "#{anna}:hello")
    assert lines(ctx.collector, 1) == ["#{anna}:hello=ok:answer:#{anna}:hello@#{anna}/true"]

    for turn <- ["again", "boom", "after"], do: send_in(ctx.namespace, gateway, "#{anna}:#{turn}")

    assert lines(ctx.collector, 4) == [
             "#{anna}:hello=ok:answer:#{anna}:hello@#{anna}/true",
             "#{anna}:again=ok:answer:#{anna}:again@#{anna}/false",
             "#{anna}:boom=error:call.failed@#{anna}/false",
             "#{anna}:after=ok:answer:#{anna}:after@#{anna}/false"
           ]

    # Creation off: a running session is reached, an absent one is refused.
    bob = session("bob")
    send_in(ctx.namespace, gateway, "resume:#{anna}:more")
    send_in(ctx.namespace, gateway, "resume:#{bob}:hi")
    tail = ctx.collector |> lines(6) |> Enum.drop(4) |> Enum.sort()

    assert tail ==
             Enum.sort([
               "#{anna}:more=ok:answer:#{anna}:more@#{anna}/false",
               "#{bob}:hi=error:relay.not_found@#{bob}/false"
             ])

    refute RelayManager.exist?(ctx.namespace, bob)
    assert settled_lines(ctx.collector, [gateway, anna]) == 6
  end

  test "a steps session calls pricing and reads the outcome as its next turn", ctx do
    boot(ctx)
    start(ctx.namespace, "call_pricing", :flow, "pricing")
    tool = start(ctx.namespace, "call_tool_session", :steps, unique("tool"), ctx.collector)

    send_in(ctx.namespace, tool, "t1", "turn")
    send_in(ctx.namespace, tool, "t2", "turn")

    assert lines(ctx.collector, 2) |> Enum.sort() == [
             "t1=ok:price:t1@pricing/false",
             "t2=ok:price:t2@pricing/false"
           ]
  end

  test "a steps tasklet whose channel ends only in a call journals no handoff for it", ctx do
    boot(ctx)
    start(ctx.namespace, "call_pricing", :flow, "pricing")

    orders =
      start(ctx.namespace, "call_orders_steps", :steps, unique("ordering_steps"), ctx.collector)

    send_in(ctx.namespace, orders, "o1")
    send_in(ctx.namespace, orders, "o2")

    assert lines(ctx.collector, 2) |> Enum.sort() == [
             "o1=ok:price:o1@pricing/false",
             "o2=ok:price:o2@pricing/false"
           ]

    # Two orders and their two answers, each an iteration of its own.
    assert NodeTestHelper.wait_until(fn -> committed_turns(orders) == 4 end, 10_000)
    {:ok, %{entries: entries}} = Journal.report(orders)

    handoffs = for %{handoff_items: items, handoff_edges: edges} <- entries, do: {items, edges}
    assert length(handoffs) >= 4
    assert Enum.uniq(handoffs) == [{0, []}]
  end

  test "a call to an asleep session wakes it and is answered", ctx do
    boot(ctx, [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}])
    gateway = start(ctx.namespace, "call_gateway", :flow, unique("gateway"), ctx.collector)
    anna = session("anna")

    send_in(ctx.namespace, gateway, "#{anna}:hello")
    assert lines(ctx.collector, 1) == ["#{anna}:hello=ok:answer:#{anna}:hello@#{anna}/true"]
    assert NodeTestHelper.wait_until(fn -> suspended?(ctx.namespace, anna) end, 30_000)

    send_in(ctx.namespace, gateway, "#{anna}:again")

    assert lines(ctx.collector, 2) |> List.last() ==
             "#{anna}:again=ok:answer:#{anna}:again@#{anna}/false"
  end

  # A session whose durable state outlives its instance, as after a controller
  # restart whose reconcile has not started it: the next call starts it, and
  # its request is journaled and answered, not dropped as a restart. A restart
  # of the instance that call started takes the request no second time.
  test "a call starting a session restored from its durable state is journaled and answered",
       ctx do
    boot(ctx)
    gateway = start(ctx.namespace, "call_gateway", :flow, unique("gateway"), ctx.collector)
    anna = session("anna")

    send_in(ctx.namespace, gateway, "#{anna}:hello")
    assert lines(ctx.collector, 1) == ["#{anna}:hello=ok:answer:#{anna}:hello@#{anna}/true"]

    assert :ok = Teardown.hub_stop_children(ctx.namespace, [anna], 5_000)
    refute RelayManager.exist?(ctx.namespace, anna)
    assert InputLog.last_offset(anna) == 0

    send_in(ctx.namespace, gateway, "#{anna}:again")

    assert lines(ctx.collector, 2) |> List.last() ==
             "#{anna}:again=ok:answer:#{anna}:again@#{anna}/true"

    assert InputLog.last_offset(anna) == 1

    assert restarted_log_end(ctx.namespace, anna) == 1
    assert settled_lines(ctx.collector, [gateway, anna]) == 2
    assert committed_turns(anna) == 2
  end

  # The same over a start carrying a message (`flz relay start --text`): it has
  # no identity of its own, so only the start's own call may take it.
  test "a start with a message over a session's durable state takes it once", ctx do
    boot(ctx)
    anna = session("anna")

    start_with = fn text ->
      assert {:ok, _} =
               RelayManager.start_relay(ctx.namespace, %Builder{
                 id: anna,
                 factory_id: "call_agent",
                 run: :steps,
                 opts: [seed_input: {text, %{"channel" => "turn"}}]
               })
    end

    start_with.("hello")
    assert NodeTestHelper.wait_until(fn -> committed_turns(anna) == 1 end, 30_000)
    assert :ok = Teardown.hub_stop_children(ctx.namespace, [anna], 5_000)

    start_with.("again")
    assert NodeTestHelper.wait_until(fn -> committed_turns(anna) == 2 end, 30_000)
    assert InputLog.last_offset(anna) == 1

    assert restarted_log_end(ctx.namespace, anna) == 1
  end

  # The same over a session asleep when its instance went: the start finds it
  # suspended, and the call it carries wakes it.
  test "a call starting an asleep session restored from its durable state is answered", ctx do
    boot(ctx, [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}])
    gateway = start(ctx.namespace, "call_gateway", :flow, unique("gateway"), ctx.collector)
    anna = session("anna")

    send_in(ctx.namespace, gateway, "#{anna}:hello")
    assert lines(ctx.collector, 1) == ["#{anna}:hello=ok:answer:#{anna}:hello@#{anna}/true"]
    assert NodeTestHelper.wait_until(fn -> suspended?(ctx.namespace, anna) end, 30_000)
    assert :ok = Teardown.hub_stop_children(ctx.namespace, [anna], 5_000)

    send_in(ctx.namespace, gateway, "#{anna}:again")

    assert lines(ctx.collector, 2) |> List.last() ==
             "#{anna}:again=ok:answer:#{anna}:again@#{anna}/true"

    assert InputLog.last_offset(anna) == 1
  end

  # A session stopped while a call waits behind its running turn ends
  # `cancelled` and keeps nothing: the waiting call is answered with that
  # status, which the teardown carries in memory, not `stopped`.
  test "a call waiting on a session that ends cancelled and keeps nothing is answered cancelled",
       ctx do
    boot(ctx)
    gateway = start(ctx.namespace, "call_gateway", :flow, unique("gateway"), ctx.collector)
    anna = session("anna")
    test = self()

    send_in(ctx.namespace, gateway, "#{anna}:hello")
    assert lines(ctx.collector, 1) == ["#{anna}:hello=ok:answer:#{anna}:hello@#{anna}/true"]

    calls =
      spawn(fn ->
        Listener.listen(
          ctx.namespace,
          anna,
          [
            direction: :calls,
            window_ms: 60_000,
            before: fn ->
              send(test, :watching)
              :ok
            end
          ],
          fn calls ->
            send(test, {:calls, calls})
            :continue
          end
        )
      end)

    assert_receive :watching, 30_000
    queued = "#{anna}:queued"
    send_in(ctx.namespace, gateway, "#{anna}:slow")
    send_in(ctx.namespace, gateway, queued)

    assert_receive {:calls, {:call, ^queued, %{"flz.call_id" => call_id}}}, 30_000
    assert {:ok, :cooperative} = RelayManager.request_stop(ctx.namespace, anna)

    assert_receive {:calls, {:refused, refusal, %{"flz.call_id" => ^call_id}}}, 30_000

    assert %Flezha.Error{code: "call.callee_ended", details: %{"status" => "cancelled"}} = refusal

    assert "#{queued}=error:call.callee_ended@#{anna}/false" in lines(ctx.collector, 3)

    assert %{state_kept: false} = await_status(ctx.namespace, anna, "cancelled", 30_000)

    assert NodeTestHelper.wait_until(fn -> not File.exists?(Paths.class_dir(anna)) end, 15_000),
           "the cancelled session's state directory was kept"

    Process.exit(calls, :kill)
  end

  describe "a durable pair's crash drills" do
    test "the runner killed while a turn runs: one outcome and one committed turn", ctx do
      {port, runner} = boot(ctx)
      gateway = start(ctx.namespace, "call_gateway", :flow, unique("gateway"), ctx.collector)
      anna = session("anna")

      send_in(ctx.namespace, gateway, "#{anna}:slow")
      assert NodeTestHelper.wait_until(fn -> journaled?(anna, 0) end, 30_000)

      sigkill(os_pid(port))
      boot(ctx)

      assert NodeTestHelper.wait_until(
               fn -> runner not in FleetManager.active_runners(ctx.namespace) end,
               15_000
             )

      assert lines(ctx.collector, 1) == ["#{anna}:slow=ok:answer:#{anna}:slow@#{anna}/true"]
      assert settled_lines(ctx.collector, [gateway, anna]) == 1
      assert committed_turns(anna) == 1
    end

    test "the gateway's Overwatch and writers killed while its call waits", ctx do
      boot(ctx)
      gateway = start(ctx.namespace, "call_gateway", :flow, unique("gateway"), ctx.collector)
      anna = session("anna")

      send_in(ctx.namespace, gateway, "#{anna}:hello")
      assert lines(ctx.collector, 1) != []

      send_in(ctx.namespace, gateway, "#{anna}:slow")
      assert NodeTestHelper.wait_until(fn -> journaled?(anna, 1) end, 30_000)

      for writer <- Flezha.State.Supervisor.live_writers(gateway), do: crash(writer, gateway)
      kill_overwatch(ctx.namespace, gateway)

      assert lines(ctx.collector, 2) |> List.last() ==
               "#{anna}:slow=ok:answer:#{anna}:slow@#{anna}/false"

      assert settled_lines(ctx.collector, [gateway, anna]) == 2
      assert committed_turns(anna) == 2
    end
  end

  # The session's last journaled offset once a replacement of its killed
  # Overwatch, started from the same start, has bound and its log has answered
  # every append it asked for.
  defp restarted_log_end(namespace, session) do
    force_fresh_rebind(namespace, session)
    {:ok, replacement} = RelayManager.get_relay_pid(namespace, session)

    assert NodeTestHelper.wait_until(
             fn -> :gen_server.reqids_size(:sys.get_state(replacement).journal_reqids) == 0 end,
             5_000
           )

    InputLog.last_offset(session)
  end

  # The session's input log holds its `offset`th request.
  defp journaled?(session, offset),
    do: match?(last when is_integer(last) and last >= offset, InputLog.last_offset(session))

  # Every committed iteration of a session is one turn.
  defp committed_turns(session) do
    {:ok, entries} = TaskJournal.read(session)
    Enum.count(entries, &(&1.type == :iter_committed))
  end

  # The line count once no outbox of the pair owes anything more: a second
  # outcome for any call would have reached the sink by then.
  defp settled_lines(collector, relays) do
    for id <- relays, do: assert(await_outbox_drained(id), "#{id} still owes output")
    length(collected(collector))
  end
end

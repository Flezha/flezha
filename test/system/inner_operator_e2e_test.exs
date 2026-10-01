defmodule FlezhaTest.System.InnerOperatorE2ETest do
  @moduledoc """
  An operator inside an operator, end to end against a live runner of the
  selected SDK runtime over UDS.

  `gateway` is a flow class whose keyed edge ends in the steps class
  `session`: every key runs its own stepped session inside the gateway, which
  counts its turns in its own state and answers `key#turns:text` on the
  gateway's `replies` sink. A session idle past `FLEZHA_FIXTURE_SLEEP_AFTER_MS`
  sleeps on its own and wakes on its key's next message with its count intact,
  while the gateway and the other keys go on. The session's second stage fails
  on `fail` and holds `hold` until the release env, the gateway's reply holds
  `stall` the same way, and `loop_gateway` embeds the chained `countdown`, which
  answers only its converged `label:0`.

  Tagged `:system` (boots real subprocesses); run with:

      mix test test/system/inner_operator_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :inner_operator

  import FlezhaTest.Helper.TierE2EHarness, only: [cleanup: 3, unique: 1, unique_id_path: 0]
  import FlezhaTest.Helper.StepsResumeHarness

  import ExUnit.CaptureIO, only: [capture_io: 1]

  alias Flezha.Operator.{Builder, PartitionGroup, Scopes}
  alias Flezha.Partition
  alias Flezha.SDK.KeyGroup
  alias Flezha.Service.PluginManager
  alias Flezha.State.{InputLog, TaskJournal}
  alias Flezha.Service.Steps
  alias Flezha.Service.Cli.Journal
  alias Flezha.State.ScopePaths
  alias Flezha.Service.Cli.Info
  alias Flezha.Service.OperatorManager
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 180_000

  describe "a session inside a gateway (uds)" do
    @describetag transport: :uds

    test "sleeps on its own and wakes with its state while other keys keep flowing", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("inner_sleep")
      id = unique("gateway")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "2000"}]
      )

      start_stream(namespace, id, "session_gateway",
        plugins: [forward_sink("replies", collector)]
      )

      send_input(namespace, id, "alice:hi")
      send_input(namespace, id, "bob:yo")
      assert await_replies(collector, ["alice#1:hi", "bob#1:yo"])

      # Bob keeps talking while Alice's session goes idle.
      keep_talking = Task.async(fn -> talk(namespace, id, 1, 12) end)

      assert NodeTestHelper.wait_until(fn -> sleeping(namespace, id) == 1 end, 20_000),
             "alice's session never slept: #{inspect(counts(namespace, id))}"

      refute suspended?(namespace, id), "the gateway slept on a session's account"

      info = capture_io(fn -> Info.operator_info(namespace, %{opts: []}) end)
      assert info =~ ~r/- session: \d+ awake, 1 sleeping/, info
      bob_turns = Task.await(keep_talking, 30_000)
      assert "bob##{bob_turns + 1}:b#{bob_turns}" in collected(collector)

      send_input(namespace, id, "alice:back")
      assert await_replies(collector, ["alice#2:back"]), inspect(collected(collector))

      assert NodeTestHelper.wait_until(fn -> awake(namespace, id) == 2 end, 20_000),
             "alice's session never woke: #{inspect(counts(namespace, id))}"

      # The scope's journal and input log read as an instance's, by its id.
      alice = ScopePaths.scope_id(id, "session", "alice")
      journal = capture_io(fn -> Journal.state_journal(alice, %{opts: []}) end)
      assert journal =~ "task-journal for instance #{alice}", journal
      inbox = capture_io(fn -> Journal.state_inbox(alice, %{opts: []}) end)

      assert inbox =~ ~r/durable input log for #{Regex.escape(alice)} .*\n\n  offset=0 consumed/s,
             inbox

      assert :ok = Steps.rewind_instance(namespace, alice, 0)
      assert capture_io(fn -> Journal.state_journal(alice, %{opts: []}) end) =~ "rewound"
    end

    test "a chained loop converges before the next outer tasklet sees its output", ctx do
      %{namespace: namespace, id: id, collector: collector} = gateway(ctx, "loop_gateway")

      send_input(namespace, id, "a:3")
      assert await_replies(collector, ["a:0"])
      send_input(namespace, id, "b:2")
      assert await_replies(collector, ["a:0", "b:0"])

      # A pass's intermediate value would have reached the sink before its
      # loop's converged one.
      assert collected(collector) == ["a:0", "b:0"]
    end

    test "a crash between two inner stages resumes at the second, the outer runs nothing again",
         ctx do
      %{namespace: namespace, id: id, collector: collector, runner: runner, id_path: id_path} =
        gateway(ctx, "session_gateway")

      alice = ScopePaths.scope_id(id, "session", "alice")
      send_input(namespace, id, "alice:hi")
      assert await_replies(collector, ["alice#1:hi"])

      send_input(namespace, id, "alice:hold")

      assert NodeTestHelper.wait_until(fn -> first_stage_only?(alice, 1) end, 30_000),
             "alice's second turn never committed its first stage alone"

      replace_runner(namespace, ctx, runner, id_path)

      # A re-run of the counting stage would answer `alice#3:hold`.
      assert await_replies(collector, ["alice#1:hi", "alice#2:hold"]),
             inspect(collected(collector))

      send_input(namespace, id, "alice:after")
      assert await_replies(collector, ["alice#3:after"])
      assert collected(collector) == ["alice#1:hi", "alice#2:hold", "alice#3:after"]
      assert length(InputLog.read_range(alice, 0, 10)) == 3, "the gateway delivered a turn twice"
    end

    test "an answer the gateway had not passed on when its runner died reaches it once", ctx do
      %{namespace: namespace, id: id, collector: collector, runner: runner, id_path: id_path} =
        gateway(ctx, "session_gateway")

      alice = ScopePaths.scope_id(id, "session", "alice")
      send_input(namespace, id, "alice:stall")

      # The session answered and committed its turn; the gateway's reply holds
      # the answer when the runner dies.
      assert NodeTestHelper.wait_until(fn -> turn_committed?(alice, 0) end, 30_000),
             "alice's first turn never committed"

      replace_runner(namespace, ctx, runner, id_path)
      assert await_replies(collector, ["alice#1:stall"]), inspect(collected(collector))

      send_input(namespace, id, "alice:after")
      assert await_replies(collector, ["alice#1:stall", "alice#2:after"])
      assert collected(collector) == ["alice#1:stall", "alice#2:after"]
    end

    test "an inner undo_all failure compensates the scope's tasklets alone", ctx do
      %{namespace: namespace, id: id, collector: collector} = gateway(ctx, "session_gateway")
      test_pid = self()
      handler = "inner-undo-#{id}"

      :telemetry.attach(
        handler,
        [:flezha, :state, :task_commit],
        fn _event, _measurements, metadata, _ ->
          if metadata.compensating, do: send(test_pid, {:compensated, metadata.operator_id})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      carol = ScopePaths.scope_id(id, "session", "carol")
      send_input(namespace, id, "carol:hi")
      send_input(namespace, id, "bob:yo")
      assert await_replies(collector, ["carol#1:hi", "bob#1:yo"])

      send_input(namespace, id, "carol:fail")
      assert_receive {:compensated, ^carol}, 30_000

      # The failed session ended; its key starts a new one, and the gateway and
      # the other keys never noticed.
      send_input(namespace, id, "carol:again")
      send_input(namespace, id, "bob:still")

      assert await_replies(collector, ["carol#1:again", "bob#2:still"]),
             inspect(collected(collector))

      assert registered?(namespace, id)
      assert Enum.uniq([carol | compensations()]) == [carol]
    end
  end

  describe "a partitioned gateway (uds)" do
    @describetag transport: :uds

    # Under two members SOL-USD (key group 60) is p0's, BTC-USD (129) and
    # ETH-USD (213) p1's; under three ETH-USD moves to p2.
    test "a rescale moves every session with its key's range", ctx do
      namespace = unique("inner_rescale")
      group = unique("pgw")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, group, id_path) end)
      boot_runner(namespace, ctx, id_path, release?: true)

      assert {:ok, %{members: [p0, p1]}} =
               PartitionGroup.start(
                 namespace,
                 %Builder{id: group, factory_id: "session_gateway", init_values: %{}, opts: []},
                 2
               )

      collector = collect_group(namespace, group)

      for key <- ~w(SOL-USD BTC-USD ETH-USD),
          do: send_input(namespace, owner(group, 2, key), "#{key}:a")

      assert await_replies(collector, ~w(SOL-USD#1:a BTC-USD#1:a ETH-USD#1:a))

      assert ScopePaths.scopes_of(p1) |> Enum.sort() == [
               {"session", "BTC-USD"},
               {"session", "ETH-USD"}
             ]

      assert {:ok, %{members: [^p0, ^p1, p2]}} = PartitionGroup.rescale(namespace, group, 3)
      assert ScopePaths.scopes_of(p1) == [{"session", "BTC-USD"}]
      assert ScopePaths.scopes_of(p2) == [{"session", "ETH-USD"}]

      for key <- ~w(SOL-USD BTC-USD ETH-USD),
          do: send_input(namespace, owner(group, 3, key), "#{key}:b")

      assert await_replies(collector, ~w(SOL-USD#2:b BTC-USD#2:b ETH-USD#2:b)),
             inspect(collected(collector))
    end
  end

  # A flow class embedding a scope, started on a runner of its own, its
  # `replies` sink collected.
  defp gateway(ctx, factory_id) do
    collector = spawn_link(fn -> collect_loop([]) end)
    namespace = unique("inner_#{factory_id}")
    id = unique(factory_id)
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, id, id_path) end)
    runner = boot_runner(namespace, ctx, id_path, release?: false)
    start_stream(namespace, id, factory_id, plugins: [forward_sink("replies", collector)])
    %{namespace: namespace, id: id, collector: collector, runner: runner, id_path: id_path}
  end

  # Every scope id a compensating commit named since the last look.
  defp compensations do
    receive do
      {:compensated, undone} -> [undone | compensations()]
    after
      0 -> []
    end
  end

  defp turn_committed?(scope_id, iter) do
    {:ok, entries} = TaskJournal.read(scope_id)
    Enum.any?(entries, &match?(%{type: :iter_committed, iter: ^iter}, &1))
  end

  # The pass of `iter` committed its first stage and nothing after it.
  defp first_stage_only?(scope_id, iter) do
    {:ok, entries} = TaskJournal.read(scope_id)
    stages = for %{type: :task_committed, iter: ^iter, task_idx: idx} <- entries, do: idx
    stages == [0] and not Enum.any?(entries, &match?(%{type: :iter_committed, iter: ^iter}, &1))
  end

  # Every member's `replies`, into one collector; a rescale re-mints the plugin
  # onto the new members.
  defp collect_group(namespace, group) do
    collector = spawn_link(fn -> collect_loop([]) end)
    sink = %{forward_sink("replies", collector) | namespace: namespace, operator_id: group}
    assert {:ok, _} = PluginManager.enable_plugin("replies", sink)
    collector
  end

  defp owner(group, members, key),
    do:
      Partition.member_id(
        group,
        Partition.owner(Partition.ranges(members), KeyGroup.group_of(key))
      )

  # Bob's turns, one every 500 ms; answers how many were sent.
  defp talk(_namespace, _id, n, last) when n > last, do: last

  defp talk(namespace, id, n, last) do
    send_input(namespace, id, "bob:b#{n}")
    Process.sleep(500)
    talk(namespace, id, n + 1, last)
  end

  defp await_replies(collector, expected),
    do: NodeTestHelper.wait_until(fn -> expected -- collected(collector) == [] end, 30_000)

  defp counts(namespace, id) do
    {:ok, state} = OperatorManager.get_overwatch_state(namespace, id)
    Scopes.counts(state)
  end

  defp sleeping(namespace, id), do: get_in(counts(namespace, id), ["session", :sleeping])
  defp awake(namespace, id), do: get_in(counts(namespace, id), ["session", :awake])
end

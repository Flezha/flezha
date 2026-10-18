defmodule FlezhaTest.System.StepsFailedTurnE2ETest do
  @moduledoc """
  A failed turn of a durable `per_input` keep-progress session, end to end on
  a live runner of the selected runtime: the turn's message is parked in the
  dead-letter queue, the turn's committed tasklets are undone, and the session
  answers its next message as the next iteration. A replay of the parked turn
  is a new turn, a rewind over it undoes nothing twice, and an all-or-nothing
  session still ends on a failed turn.

  Tagged `:system` (boots real subprocesses); run with:

      mix test test/system/steps_failed_turn_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :steps

  import ExUnit.CaptureIO

  import FlezhaTest.Helper.TierE2EHarness, only: [cleanup: 3, unique: 1, unique_id_path: 0]
  import FlezhaTest.Helper.StepsResumeHarness

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Service.Cli
  alias Flezha.State.Completion
  alias Flezha.State.TaskJournal
  alias FlezhaTest.Helper.DlqFixtures
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  @moduletag :steps_failed_turn
  @moduletag timeout: 180_000
  @moduletag transport: :uds

  setup do
    DlqFixtures.isolated_dir(replay_grace_ms: 500)
    :ok
  end

  test "a failed turn is dead-lettered, undone once, and every surface shows it", ctx do
    session = start_session(ctx, "failing_turn_loop_steps")
    %{namespace: namespace, relay_id: relay_id} = session

    Telemetry.capture([:flezha, :steps, :turn_dead_lettered],
      filter: &match?(%{relay_id: ^relay_id}, &1),
      as: fn _event, _measurements, metadata -> {:dead_lettered, metadata} end
    )

    for turn <- ~w(hello boom again), do: send_turn(namespace, relay_id, turn)
    answered!(session, "re:again")

    assert collected(session.collector) == ["re:hello", "re:again"]
    assert registered?(namespace, relay_id)

    assert_iterations(relay_id, [{:iter_committed, 0}, {:dead_lettered, 1}, {:iter_committed, 2}])

    assert [entry] = DlqStore.list(relay_id)
    assert %{"tasklet_id" => "_turn", "channel" => "in", "origin_offset" => 1} = entry
    ref = entry_ref(entry)
    assert {:ok, _entry, "boom"} = DlqStore.get(ref)

    # `note`'s undo ran once, and journaled the record its hook wrote.
    assert [%{payload: %{cells_ran: [%{seq: seq}]}}] = compensated(relay_id, 0, 1)
    assert is_integer(seq)

    info = capture_io(fn -> Cli.call("Relay.info", Jason.encode!([namespace]), []) end)
    assert info =~ "Failed turns: 1 (last: iteration 1)"

    journal = capture_io(fn -> Cli.call("Steps.journal", Jason.encode!([relay_id]), []) end)
    assert journal =~ "dead_lettered task_idx=0 iter=1"
    assert journal =~ "parked as #{ref}"

    assert_received {:dead_lettered, %{iter: 1, consumed_offset: 1, tasklet: "answer"}}
    refute_received {:dead_lettered, _second}
  end

  test "a replayed turn is answered as the next iteration and its entry goes", ctx do
    session = start_session(ctx, "failing_turn_loop_steps")
    %{namespace: namespace, relay_id: relay_id} = session

    for turn <- ~w(boom-until-healed heal), do: send_turn(namespace, relay_id, turn)
    answered!(session, "re:heal")
    assert [entry] = DlqStore.list(relay_id)

    assert capture_io(fn -> Cli.call("Dlq.replay", Jason.encode!([entry_ref(entry)]), []) end) =~
             "replay succeeded"

    answered!(session, "re:boom-until-healed")

    assert_iterations(relay_id, [{:dead_lettered, 0}, {:iter_committed, 1}, {:iter_committed, 2}])

    assert NodeTestHelper.wait_until(fn -> DlqStore.list(relay_id) == [] end, 10_000),
           "the replayed entry was not removed"
  end

  test "a rewind over a failed turn undoes nothing twice and leaves its entry", ctx do
    session = start_session(ctx, "failing_turn_loop_steps")
    %{namespace: namespace, relay_id: relay_id} = session

    for turn <- ~w(hello boom), do: send_turn(namespace, relay_id, turn)

    assert NodeTestHelper.wait_until(fn -> compensated(relay_id, 0, 1) != [] end, 60_000),
           "the failed turn was never undone"

    rewound =
      capture_io(fn ->
        Cli.call("Steps.rewind", Jason.encode!([namespace, relay_id, 0]), [])
      end)

    assert rewound =~ "rewound to iteration 0"

    send_turn(namespace, relay_id, "again")
    answered!(session, "re:again")

    assert NodeTestHelper.wait_until(
             fn -> TaskJournal.committed_iterations(relay_id) == [0, 1] end,
             30_000
           )

    assert length(compensated(relay_id, 0, 1)) == 1
    assert [%{"origin_offset" => 1}] = DlqStore.list(relay_id)
  end

  test "an all-or-nothing session still ends on a failed turn and parks nothing", ctx do
    session = start_session(ctx, "failing_turn_undo_all_steps")
    %{namespace: namespace, relay_id: relay_id} = session

    for turn <- ~w(hello boom), do: send_turn(namespace, relay_id, turn)

    await_status(relay_id, "error", 60_000)
    assert DlqStore.list(relay_id) == []
  end

  describe "a crash" do
    test "after the park and before the record re-runs the turn, and parks it once", ctx do
      session = start_session(ctx, "failing_turn_loop_steps")
      %{namespace: namespace, relay_id: relay_id} = session

      stall_at(:turn_parked, relay_id)
      send_turn(namespace, relay_id, "boom-once")
      assert_receive {:stalled, :turn_parked}, 60_000
      assert [%{"origin_offset" => 0}] = DlqStore.list(relay_id)

      # The replacement is released, so the turn succeeds on its re-run and
      # the entry of the turn that never reached its record is gone.
      session = restart_runner(session, ctx)
      answered!(session, "re:boom-once")
      assert DlqStore.list(relay_id) == []

      stall_at(:turn_parked, relay_id)
      send_turn(namespace, relay_id, "boom")
      assert_receive {:stalled, :turn_parked}, 60_000

      session = restart_runner(session, ctx)

      assert NodeTestHelper.wait_until(
               fn -> {:dead_lettered, 1} in iteration_records(relay_id) end,
               60_000
             ),
             "the re-run of the failed turn was not dead-lettered"

      assert [%{"origin_offset" => 1}] = DlqStore.list(relay_id)
      send_turn(namespace, relay_id, "again")
      answered!(session, "re:again")
    end

    test "after the record and before the undo is journaled undoes the turn once, on the rebind",
         ctx do
      session = start_session(ctx, "failing_turn_loop_steps")
      %{namespace: namespace, relay_id: relay_id} = session

      send_turn(namespace, relay_id, "hello")
      answered!(session, "re:hello")
      stall_at(:dead_lettered, relay_id)
      send_turn(namespace, relay_id, "boom")
      assert_receive {:stalled, :dead_lettered}, 60_000
      assert iteration_records(relay_id) == [{:iter_committed, 0}, {:dead_lettered, 1}]
      assert compensated(relay_id, 0, 1) == []

      session = restart_runner(session, ctx)

      assert NodeTestHelper.wait_until(fn -> compensated(relay_id, 0, 1) != [] end, 60_000),
             "the rebind did not run the undo the failed turn owed"

      send_turn(namespace, relay_id, "again")
      answered!(session, "re:again")

      assert length(compensated(relay_id, 0, 1)) == 1
      assert collected(session.collector) == ["re:hello", "re:again"]

      assert_iterations(relay_id, [
        {:iter_committed, 0},
        {:dead_lettered, 1},
        {:iter_committed, 2}
      ])

      assert [%{"origin_offset" => 1}] = DlqStore.list(relay_id)
    end
  end

  # `Commits` blocks at `step` of this relay's next dead-lettered commit,
  # and says so; the runner is then killed under it.
  defp stall_at(step, relay_id) do
    test_pid = self()

    Application.put_env(:flezha, :commit_stall_fn, fn
      ^step, ^relay_id ->
        Application.delete_env(:flezha, :commit_stall_fn)
        send(test_pid, {:stalled, step})
        Process.sleep(:infinity)

      _step, _relay_id ->
        :ok
    end)

    on_exit(fn -> Application.delete_env(:flezha, :commit_stall_fn) end)
  end

  # The dead session goes, and its stalled commit worker with it.
  defp restart_runner(session, ctx) do
    {port, runner_id} =
      replace_runner(session.namespace, ctx, {session.port, session.runner_id}, session.id_path)

    %{session | port: port, runner_id: runner_id}
  end

  defp start_session(ctx, factory_id) do
    collector = spawn_link(fn -> collect_loop([]) end)
    namespace = unique("failed_turn")
    relay_id = unique(factory_id)
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    {port, runner_id} = boot_runner(namespace, ctx, id_path, release?: false)

    start_steps(namespace, relay_id, factory_id,
      first_message: nil,
      plugins: [forward_sink("answer_sink", collector)]
    )

    %{
      namespace: namespace,
      relay_id: relay_id,
      collector: collector,
      port: port,
      runner_id: runner_id,
      id_path: id_path
    }
  end

  defp answered!(%{collector: collector, relay_id: relay_id}, answer) do
    assert NodeTestHelper.wait_until(fn -> answer in collected(collector) end, 60_000),
           "the session never answered #{answer}: " <>
             inspect(collected(collector)) <>
             ", completion " <>
             inspect(Completion.read(relay_id))
  end

  defp iteration_records(relay_id) do
    {:ok, entries} = TaskJournal.read(relay_id)

    for %{type: type, iter: iter} <- entries,
        type in [:iter_committed, :dead_lettered],
        do: {type, iter}
  end

  # An answer leaves at its tasklet's commit, before the runner sends the
  # iteration's own, so the iteration's record may land just after it.
  defp assert_iterations(relay_id, expected) do
    NodeTestHelper.wait_until(fn -> iteration_records(relay_id) == expected end, 10_000)
    assert iteration_records(relay_id) == expected
  end

  defp compensated(relay_id, task_idx, iter) do
    {:ok, entries} = TaskJournal.read(relay_id)
    for %{type: :compensated, task_idx: ^task_idx, iter: ^iter} = entry <- entries, do: entry
  end

  defp entry_ref(entry), do: "#{entry["relay_class"]}/#{entry["tasklet_id"]}/#{entry["id"]}"
end

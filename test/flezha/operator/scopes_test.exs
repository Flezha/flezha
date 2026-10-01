defmodule Flezha.Operator.ScopesTest do
  @moduledoc """
  A stepped scope counts against its namespace's operator quota from the
  moment its outer instance knows it until it is forgotten or the instance
  stops, and a new scope past the quota dead-letters its message. A scope that
  ends leaves its key's next scope only the messages its run never took. A
  replayed outer input appends nothing new, a message that landed while its
  scope parked binds it again, and a finished `once` scope and a sleeping
  scope's wake outlive the instance's process.
  """
  # async: false: a shared on-disk root.
  use FlezhaTest.Case,
    fixtures: [:tmp_app_data, {:auth_stores, prefix: "flz_scopes_quota", claim: true}],
    async: false

  alias Flezha.AuthZ.{NamespaceStore, QuotaCounters}
  import ExUnit.CaptureLog

  alias Flezha.Operator.{Builder, ClassRegistry, Migration, Overwatch, Scopes}
  alias Flezha.Runner.ControlChannel.Requests
  alias Flezha.State.{InputLog, Paths, TaskJournal}

  setup do
    {:ok, _} = NamespaceStore.create("team-sessions", quotas: %{max_operators: 2})

    id = "gateway_#{System.unique_integer([:positive])}"

    state = %Overwatch{
      id: id,
      namespace: "team-sessions",
      builder: %Builder{id: id, factory_id: "session_gateway", init_values: %{}, opts: []},
      settings: %{scopes: %{"session" => "session"}},
      scopes: %{}
    }

    %{state: state}
  end

  defp operators do
    _ = :sys.get_state(QuotaCounters)
    QuotaCounters.get("team-sessions", :operators)
  end

  # fails when: a scope is not counted, counted twice, or never released.
  test "a scope is counted while its instance knows it", %{state: state} do
    state = Scopes.parked(state, {"session", "alice"}, nil)
    state = Scopes.parked(state, {"session", "alice"}, nil)
    state = Scopes.parked(state, {"session", "bob"}, nil)
    assert operators() == 2

    state = Scopes.completed(state, {"session", "bob"}, nil)
    assert operators() == 1

    assert [%{run: run}] = Enum.filter(Migration.shutdown_steps(), &(&1.name == :scope_quota))
    assert :ok = run.({state, :normal}, 1_000)
    assert operators() == 0
  end

  # fails when: a scope past the quota is bound anyway.
  test "a new scope past the quota dead-letters its message", %{state: state} do
    :ok = Flezha.AuthZ.Quotas.record_allocation("team-sessions", :max_operators, 2)
    _ = operators()

    state = Scopes.input(state, {"session", "carol"}, turn("carol:hi"), {self(), "ref-1"})

    assert_receive {:sdk_reply, "ref-1", {:scope_input_response, %{error: nil, refused: true}}, _}
    assert Scopes.counts(state) == %{"session" => %{awake: 0, sleeping: 0, in_pass: 0}}
    assert operators() == 2
  end

  # fails when: a message for a scope of an instance this node does not hold
  # is left unanswered, so the runner's hold on its input never releases.
  test "a scope input for an instance with no Overwatch here is refused to its runner" do
    input = %RunnerCommands.ScopeInput{
      operator_id: "nobody_#{System.unique_integer([:positive])}",
      scope: %RunnerCommands.Scope{inner: "session", key: "k"},
      channel: "turns",
      payload: "k:hi"
    }

    assert :ok = Requests.scope_input(%{namespace: "team-sessions"}, input, "ref-9")

    assert_receive {:sdk_reply, "ref-9", {:scope_input_response, %{error: %{code: code}}},
                    "ScopeInputResponse"}

    assert is_binary(code) and code != ""
  end

  describe "a scope's end" do
    # fails when: a message the ended run never took is lost, or the one it
    # took runs again in the key's next scope.
    test "re-enters all but the message its last pass took", %{state: state} do
      assert reentered(state, nil) == ~w(a b c)
      assert reentered(state, 1) == ~w(a c)
    end

    # fails when: a finished `once` scope takes a message again once its
    # instance's process restarted.
    test "a finished once scope stays finished across a rebind", %{state: state} do
      scope = {"session", "once-#{System.unique_integer([:positive])}"}
      :ok = TaskJournal.append_iter_committed(Scopes.id(state, scope), 0, nil, [])
      _ = Scopes.completed(%{state | scopes: %{each: %{"session" => :once}}}, scope, nil)

      state = Scopes.rebound(state)
      _ = Scopes.input(state, scope, turn("again"), {self(), "ref-2"})

      assert_receive {:sdk_reply, "ref-2", {:scope_input_response, %{refused: true}}, _}
    end
  end

  # fails when: a sleeping scope forgets its wake deadline with its
  # instance's process, and sleeps until its key's next message.
  test "a sleeping scope's wake outlives its instance's process", %{state: state} do
    scope = {"session", "wake-#{System.unique_integer([:positive])}"}
    :ok = TaskJournal.append_iter_committed(Scopes.id(state, scope), 0, nil, [])
    deadline = System.system_time(:millisecond) + 50

    # Parked in a process that is gone by the deadline, with its timer.
    Task.async(fn -> Scopes.parked(state, scope, deadline) end) |> Task.await()
    _ = Scopes.rebound(state)

    assert_receive {:scope_wake, ^scope}, 2_000
  end

  # fails when: a torn record of a scope crashes its instance's rebind
  # instead of being logged and passed over.
  test "a scope whose record is torn goes on without it", %{state: state} do
    scope = {"session", "torn-#{System.unique_integer([:positive])}"}
    journal = Paths.journal_id(Scopes.id(state, scope))
    :ok = TaskJournal.append_iter_committed(Scopes.id(state, scope), 0, nil, [])
    File.write!(Paths.scope_fact_path(journal), <<131, 116, 0>>)

    on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))
    log = capture_log(fn -> send(self(), {:rebound, Scopes.rebound(state)}) end)

    assert log =~ "is unreadable"
    assert_received {:rebound, state}
    assert %{"session" => %{sleeping: 1}} = Scopes.counts(state)
  end

  # fails when: a scope's record stays behind when a rescale, an adoption or
  # a remote restore moves its journal.
  test "a scope's record moves with its journal", %{state: state} do
    scope = {"session", "moved-#{System.unique_integer([:positive])}"}
    journal = Paths.journal_id(Scopes.id(state, scope))
    :ok = TaskJournal.append_iter_committed(Scopes.id(state, scope), 0, nil, [])
    declared = %{ClassRegistry.decode_declarations("") | scopes: %{"session" => "session"}}
    :ok = ClassRegistry.put_declarations(state.id, declared)

    assert {:cell, journal} in Paths.durable_refs(state.id)
    assert Paths.scope_fact_path(journal) in Paths.durable_files({:cell, journal})
  end

  # fails when: a message journaled while its scope was parking waits for
  # the key's next message instead of binding the scope again.
  test "a message that landed while its scope parked binds it again", %{state: state} do
    state = %{state | runner_id: "runner-#{System.unique_integer([:positive])}"}
    scope = {"session", "park-#{System.unique_integer([:positive])}"}
    {:ok, _offset} = InputLog.append(Scopes.id(state, scope), "late", %{"channel" => "turns"})

    state = Scopes.parked(state, scope, nil)

    assert_receive {:scope_bound, ^scope, {:error, :no_control_unit}}, 5_000
    assert %{"session" => %{awake: 1, sleeping: 0}} = Scopes.counts(state)
  end

  # fails when: a replay of the outer input that sent a message appends it to
  # the scope's log a second time, so the scope runs its turn twice.
  test "a replayed outer input appends nothing new", %{state: state} do
    scope = {"session", "replay-#{System.unique_integer([:positive])}"}
    input = %RunnerCommands.ScopeInput{channel: "turns", payload: "hi", msg_id: "gw@7:0"}

    state = Scopes.input(state, scope, Requests.scope_message(input), {self(), "ref-3"})
    state = await_journal(state)
    state = Scopes.input(state, scope, Requests.scope_message(input), {self(), "ref-4"})
    _ = await_journal(state)

    assert_receive {:sdk_reply, "ref-4", {:scope_input_response, %{error: nil}}, _}
    assert [{0, "hi", _}] = InputLog.read_range(Scopes.id(state, scope), 0, 10)
  end

  defp turn(text), do: {text, %{"channel" => "turns"}}

  # The input log's answer to a scope's append, handled as its Overwatch does.
  defp await_journal(state) do
    receive do
      msg ->
        case Scopes.journal_reply(state, msg) do
          {:ok, state} -> state
          :ignore -> await_journal(state)
        end
    after
      5_000 -> flunk("the scope's input log never answered")
    end
  end

  # The messages the key's next scope is owed after a scope that took `a`,
  # `b` and `c` into its log ends, its last pass having taken `took`.
  defp reentered(state, took) do
    scope = {"session", "k#{System.unique_integer([:positive])}"}
    id = Scopes.id(state, scope)
    for text <- ~w(a b c), do: {:ok, _offset} = InputLog.append(id, text, %{"channel" => "turns"})

    state = %{state | scopes: %{each: %{"session" => :per_input}}}
    Scopes.completed(state, scope, took)
    for {_offset, text, _metadata} <- InputLog.undelivered(id), do: text
  end
end

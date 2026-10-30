defmodule Flezha.Convergence.LoopTest do
  @moduledoc """
  The Loop and what it reports: misbound streaks, the user health map,
  deterministic runner placement, durable-evidence intent mismatch, the tier
  progress cursors and the transition telemetry.
  """

  # async: false: writes Application env; Mimic stubs; a shared on-disk root.

  use FlezhaTest.Case, async: false
  use Mimic

  import FlezhaTest.Helper.MimicGlobal

  alias FlezhaTest.Helper.ConvergenceFixtures

  alias Flezha.Convergence.{Diff, Intent}
  alias Flezha.Convergence.Loop.Progress
  alias Flezha.State.Completion
  alias FlezhaTest.Helper.Telemetry
  alias FlezhaTest.Helper.WireEncoders

  describe "Loop.apply_misbound_streak/2 (B5)" do
    alias Flezha.Convergence.Loop

    test "a relay misbound for one tick is not yet migrated; two ticks releases it" do
      delta = %Diff{misbound_relays: [{{:ns, "r"}, "e_dead", :probe}]}

      {streaks, tick1} = Loop.apply_misbound_streak(%{}, delta)
      assert tick1.misbound_relays == []

      {_streaks, tick2} = Loop.apply_misbound_streak(streaks, delta)
      assert tick2.misbound_relays == [{{:ns, "r"}, "e_dead", :probe}]
    end

    test "a declared disconnect is not streak-gated: the fleet observed the close" do
      delta = %Diff{misbound_relays: [{{:ns, "r"}, "e_dead", :runner_disconnected}]}

      {_streaks, tick1} = Loop.apply_misbound_streak(%{}, delta)
      assert tick1.misbound_relays == [{{:ns, "r"}, "e_dead", :runner_disconnected}]
    end

    test "a relay seen bound again resets its streak" do
      delta = %Diff{misbound_relays: [{{:ns, "r"}, "e_dead", :probe}]}
      {streaks, _} = Loop.apply_misbound_streak(%{}, delta)

      # Bound again this tick → drops out → streak resets.
      {streaks, _} = Loop.apply_misbound_streak(streaks, %Diff{misbound_relays: []})
      {_streaks, tick} = Loop.apply_misbound_streak(streaks, delta)
      assert tick.misbound_relays == []
    end
  end

  describe "a runner connecting is news for the relays it can take" do
    alias Flezha.Config.Backoff
    alias Flezha.Convergence.{Facts, Heal, Loop, Probe, RecoveryGate}
    alias Flezha.Runner.FleetManager
    alias Flezha.Service.HubManager

    @dead "e_dead"
    @fresh "e_fresh"
    @misbound_key {:runner_disconnected, "op"}

    setup :set_mimic_global_until_exit

    setup do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "op", factory_id: "f", plugins: []}]
      }

      stub(Intent, :declared, fn -> intent end)
      stub(RecoveryGate, :ready_namespaces, fn _ -> MapSet.new([:ns]) end)
      stub(HubManager, :durable_registry_pending?, fn -> false end)
      stub(Probe, :snapshot, fn -> reality() end)
      stub(FleetManager, :factories_for_runner, fn _ns, _runner -> MapSet.new(["f"]) end)
      stub(Heal, :execute, fn _op -> :error end)

      start_supervised!({Loop, tick_ms: 3_600_000})
      :ok
    end

    # One declared relay bound to a runner the active set no longer holds,
    # so the tick judges it `failed`. `@fresh` is in the set, so no hub is
    # reclaimed under the tick.
    defp reality(observed \\ %{}) do
      ConvergenceFixtures.snapshot(
        %{
          {:ns, "op"} => Map.merge(%{pid: self(), bound_runner: @dead, factory_id: "f"}, observed)
        },
        %{ns: MapSet.new([@fresh])}
      )
    end

    defp arm_misbound_rung() do
      Facts.declare({:runner_disconnected, {:ns, @dead}, %{}})
      :ok = Loop.tick_now()
      assert attempt(@misbound_key) == 1, "the misbound rebind did not walk a rung"
    end

    defp connect(runner_id) do
      send(Loop, {:declare, :runner_connected, {:ns, runner_id}, %{}})
      _ = :sys.get_state(Loop)
      :ok
    end

    defp attempt(key), do: Backoff.attempt(:sys.get_state(Loop).backoff, key)

    # Every heal from here on reports its primitive and answers `answer`. With
    # `:unmet` a later tick neither advances nor resets the ladder, so what a
    # test reads no longer depends on when the Loop's own tick runs.
    defp observe_heals(answer) do
      test_pid = self()
      stub(Heal, :execute, fn op -> send(test_pid, {:healed, op.primitive}) && answer end)
    end

    # fails when: the release walks only the parked relays and resets only
    # the two key shapes it listed. A relay misbound to a departed runner
    # is in neither set, so it waited out its own rung — up to five minutes —
    # with a healthy runner already there.
    test "a relay whose runner died is released the moment a capable one connects" do
      arm_misbound_rung()
      observe_heals(:unmet)

      connect(@fresh)

      assert_receive {:healed, :rebind_relay}, 1_000

      assert attempt(@misbound_key) == 0,
             "the rebind stayed on its rung with a capable runner present"
    end

    # fails when: the guard goes missing and any runner wakes every waiting
    # relay in its namespace, whatever it can actually serve.
    test "a runner advertising nothing the relay needs wakes nobody" do
      stub(FleetManager, :factories_for_runner, fn _ns, _runner -> MapSet.new(["other"]) end)
      arm_misbound_rung()

      connect(@fresh)

      assert attempt(@misbound_key) == 1
    end

    # fails when: a runner that has advertised nothing YET — an SDK may send
    # its factories on a later frame — is read as one that serves nothing, and
    # the relay it was going to take waits out its rung anyway.
    test "a runner that has advertised nothing yet still releases" do
      stub(FleetManager, :factories_for_runner, fn _ns, _runner -> MapSet.new() end)
      arm_misbound_rung()
      observe_heals(:unmet)

      connect(@fresh)

      assert_receive {:healed, :rebind_relay}, 1_000
      assert attempt(@misbound_key) == 0
    end

    # fails when: an attempt that never had a runner to bind to is charged a
    # rung. The ladder spaces out repairs that could have worked; charging it
    # for one that could not is what turns a needless wake into a longer wait.
    test "a rebind with no capable runner neither advances nor resets the ladder" do
      stub(Heal, :execute, fn _op -> :unmet end)
      watch_ticks()
      Facts.declare({:runner_disconnected, {:ns, @dead}, %{}})

      :ok = Loop.tick_now()
      assert attempt(@misbound_key) == 0, "an unmet precondition advanced the ladder"

      seed_rung(@misbound_key, 2)
      :ok = Loop.tick_now()
      assert attempt(@misbound_key) == 2, "an unmet precondition reset the ladder"

      # The ladder is also left alone by a tick that RAISED on the outcome and
      # was rescued, so the tick has to say it completed for the two assertions
      # above to mean what they claim.
      assert_receive {:tick_state, :ticking}
      refute_received {:tick_state, :error}
    end

    # fails when: every tick logs each declared relay no runner serves.
    test "declared relays no runner serves are reported once per namespace and reason" do
      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:info))

      # Stubbed inside the capture: the Loop may tick on its own at any time.
      blocked =
        ExUnit.CaptureLog.capture_log([level: :info], fn ->
          unplaced(["op"])
          starts(%{"op" => {:unmet, {:factory_unknown, "f"}}})
          for _ <- 1..3, do: :ok = Loop.tick_now()
        end)

      assert [_once] = Regex.scan(~r/cannot be started/, blocked)

      assert blocked =~
               ~s|1 declared relays in ns run nowhere and cannot be started: | <>
                 ~s|{:factory_unknown, "f"}|

      unplaced([])

      cleared = ExUnit.CaptureLog.capture_log([level: :info], fn -> :ok = Loop.tick_now() end)
      assert cleared =~ ~s|declared relays in ns no longer blocked: {:factory_unknown, "f"}|
    end

    # fails when: a reason is tracked per namespace rather than per relay, so a
    # change of reason, or one that clears and comes back while other relays
    # stay unplaced, goes unreported, or the count is the namespace's.
    test "a reason that changes, clears and returns is reported each time, counted per reason" do
      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:info))
      stub(Intent, :declared, fn -> declared(["op", "op2"]) end)
      unknown = {:unmet, {:factory_unknown, "f"}}
      no_members = {:unmet, {:no_group_members, ["f"]}}

      both =
        ExUnit.CaptureLog.capture_log([level: :info], fn ->
          unplaced(["op", "op2"])
          starts(%{"op" => unknown, "op2" => unknown})
          :ok = Loop.tick_now()
        end)

      assert both =~
               ~s|2 declared relays in ns run nowhere and cannot be started: {:factory_unknown|

      changed =
        ExUnit.CaptureLog.capture_log([level: :info], fn ->
          starts(%{"op" => no_members, "op2" => :ok})
          :ok = Loop.tick_now()
        end)

      assert changed =~ ~s|no longer blocked: {:factory_unknown, "f"}|
      assert changed =~ ~s|1 declared relays in ns run nowhere and cannot be started: {:no_group|

      back =
        ExUnit.CaptureLog.capture_log([level: :info], fn ->
          starts(%{"op" => unknown, "op2" => :ok})
          :ok = Loop.tick_now()
        end)

      assert back =~ ~s|cannot be started: {:factory_unknown, "f"}|
      assert back =~ ~s|no longer blocked: {:no_group_members, ["f"]}|
    end

    defp unplaced(ids) do
      stub(Intent, :detect_unplaced, fn _intent, since, _now, _watched ->
        {Enum.map(ids, &{:ns, &1}), since}
      end)
    end

    # Each relay's start answers as `answers` says; every other heal is unmet.
    defp starts(answers) do
      stub(Heal, :execute, fn
        %{primitive: :start_relay, target: id} -> Map.fetch!(answers, id)
        _op -> :unmet
      end)
    end

    defp watch_ticks() do
      Telemetry.capture([:flezha, :convergence, :tick],
        as: fn _e, _m, meta -> {:tick_state, meta.state} end
      )
    end

    # fails when: the parked shape stops being released. The published scenario
    # — "a runner arriving mid-cooldown is used at once" — is now carried by
    # the verdict records rather than by the parked facts, so it needs a test
    # that goes through the judge.
    test "a parked relay is released by the same path" do
      stub(Probe, :snapshot, fn ->
        reality(%{phase: :halted, halt_reason: {:ping_failed, :timeout}, bound_runner: @fresh})
      end)

      :ok = Loop.tick_now()
      assert attempt({:halted, "op"}) == 1, "the halted rebind did not walk a rung"
      observe_heals(:unmet)

      connect(@fresh)

      assert_receive {:healed, :rebind_relay}, 1_000
      assert attempt({:halted, "op"}) == 0
    end

    # fails when: a runner connecting releases only the relays waiting for a
    # binding. A suspended relay sent a message while nothing could run it
    # keeps the message journaled, and only the next message or its timer
    # would wake it, with a capable runner already there.
    test "a suspended relay of a factory the runner serves is asked to wake if work is owed" do
      sleeping_relay()

      connect(@fresh)

      assert_receive {:"$gen_cast", :rehydrate_if_pending}, 1_000
    end

    test "a suspended relay of a factory the runner does not serve stays asleep" do
      stub(FleetManager, :factories_for_runner, fn _ns, _runner -> MapSet.new(["other"]) end)
      sleeping_relay()

      connect(@fresh)

      refute_receive {:"$gen_cast", :rehydrate_if_pending}, 200
    end

    test "a runner whose factories are not known yet wakes no sleeper" do
      stub(FleetManager, :factories_for_runner, fn _ns, _runner -> MapSet.new() end)
      sleeping_relay()

      connect(@fresh)

      refute_receive {:"$gen_cast", :rehydrate_if_pending}, 200
    end

    defp sleeping_relay() do
      test_pid = self()
      stub(Probe, :snapshot, fn -> reality(%{phase: :suspended, bound_runner: nil}) end)
      stub(HubManager, :get_pid, fn :ns, "op" -> {:ok, test_pid} end)
      :ok = Loop.tick_now()
      assert :sys.get_state(Loop).verdicts[{:relay, {:ns, "op"}}].state == :suspended
    end

    # A rung already walked, eligible again: what a rebind that finds no runner
    # must leave exactly as it is.
    defp seed_rung(key, attempt) do
      elapsed = System.monotonic_time(:millisecond) - 1_000

      :sys.replace_state(Loop, fn state ->
        entries = Map.put(state.backoff.entries, key, {attempt, elapsed})
        %{state | backoff: %{state.backoff | entries: entries}}
      end)
    end
  end

  describe "a namespace hub without quorum" do
    alias Flezha.Convergence.{Facts, Heal, Loop, Probe, RecoveryGate}
    alias Flezha.Convergence.Snapshot
    alias Flezha.Runner.FleetManager
    alias Flezha.Service.HubManager

    setup :set_mimic_global_until_exit

    setup do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "op", factory_id: "f", plugins: []}]
      }

      stub(Intent, :declared, fn -> intent end)
      stub(RecoveryGate, :ready_namespaces, fn _ -> MapSet.new([:ns]) end)
      stub(HubManager, :durable_registry_pending?, fn -> false end)
      stub(HubManager, :get_running_namespaces, fn -> [:ns] end)
      stub(Probe, :snapshot, fn -> reality() end)
      stub(FleetManager, :factories_for_runner, fn _ns, _runner -> MapSet.new(["f"]) end)
      observe_heals(:error)

      start_supervised!({Loop, tick_ms: 3_600_000})
      :ok
    end

    # fails when: the loop restarts or rebinds the relays of a hub that stopped
    # them for lack of quorum, which the other side of the split now runs.
    test "is left alone, and healed again on the tick after quorum returns" do
      stub(HubManager, :quorum, fn _hub_id -> {:error, :no_quorum} end)
      Facts.declare({:runner_disconnected, {:ns, @dead}, %{}})

      :ok = Loop.tick_now()

      refute_received {:healed, _primitive}
      assert attempt(@misbound_key) == 0, "a skipped hub was charged a heal attempt"
      assert Snapshot.current().no_quorum == [:ns]

      stub(HubManager, :quorum, fn _hub_id -> :ok end)
      :ok = Loop.tick_now()

      assert_receive {:healed, :rebind_relay}, 1_000
      assert Snapshot.current().no_quorum == []
    end
  end

  describe "Planner.waiting_keys/1 — what a runner connecting releases" do
    alias Flezha.Config.Backoff
    alias Flezha.Convergence.{Facts, Planner}

    # One relay in every waiting field at once, under every reason the
    # kinds table stamps on those fields plus the probe's reasonless one: what
    # the Planner can key a binding op on for a single relay.
    defp every_waiting_shape(ref) do
      reasons = [:probe | Facts.reasons_for(Diff.waiting_fields())]

      %Diff{
        unbound_relays: [ref],
        misbound_relays: Enum.map(reasons, &{ref, "e_dead", &1}),
        halted_relays: [{ref, %{reason: :timeout}, "e_dead"}]
      }
    end

    # fails when: the release set stops being derived from the plan. Both sides
    # read the declared kinds, so a reason added to the table joins the planned
    # ops and the released keys together; a hand-written list of key shapes
    # would move only one of them, which is how `{:runner_disconnected, id}`
    # escaped and waited out a rung nothing would clear.
    test "every key the binding rules can plan for one relay is released" do
      planned =
        {:ns, "op-1"}
        |> every_waiting_shape()
        |> Planner.plan(Backoff.new(:heal))
        |> Enum.filter(&(&1.target == "op-1"))
        |> Enum.map(&Planner.key/1)
        |> Enum.uniq()

      assert planned != [], "the binding rules planned nothing to release"
      assert Enum.sort(planned) == Enum.sort(Planner.waiting_keys({:ns, "op-1"}))
    end

    # fails when: the shape this change was reported for stops being released —
    # a rebind carrying the declared disconnect.
    test "the reasoned rebind is in the released set" do
      released = Planner.waiting_keys({:ns, "op-1"})

      assert {:runner_disconnected, "op-1"} in released
    end

    # fails when: a namespace-wide release becomes a namespace-wide tick. The
    # per-tick cap is the only thing bounding it — the heal row carries no
    # jitter — so 500 released keys must still yield one capped plan.
    test "releasing a whole namespace still plans at most one tick's cap" do
      delta = %Diff{unbound_relays: for(n <- 1..500, do: {:ns, "op-#{n}"})}

      assert length(Planner.plan(delta, Backoff.new(:heal))) == Planner.max_ops()
    end
  end

  describe "Intent.intent_mismatch/2 — durable evidence vs live intent" do
    alias Flezha.Convergence.{Intent, Loop}
    alias Flezha.Service.HubManager

    setup do
      %{tmp: root} = FlezhaTest.Case.install([:tmp_dir, :env])
      Application.put_env(:flezha, :state_root, root)

      # Other suites leak hubs into the global registry; pin the durable-
      # registry evidence to empty so only the on-disk dirs drive these tests.
      stub(HubManager, :durable_relay_ids, fn -> MapSet.new() end)

      {:ok, root: root}
    end

    defp seed_cell(root, relay, tasklet, key) do
      dir = Path.join([root, "_system", relay, tasklet])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "#{key}.wal"), <<1, 2, 3>>)
    end

    defp intent_for(ids) do
      %Intent{
        relays:
          Enum.map(ids, fn id ->
            %{namespace: :ns, relay_id: id, factory_id: nil, plugins: []}
          end)
      }
    end

    test "fresh install with no durable evidence → empty mismatch" do
      assert {[], _cache} = Intent.intent_mismatch(intent_for([]), nil)
    end

    test "wiped registry with on-disk evidence → mismatch names the relays", %{root: root} do
      seed_cell(root, "btc_feed", "op", "k1")
      seed_cell(root, "eth_feed", "op", "k1")

      assert {["btc_feed", "eth_feed"], cache} = Intent.intent_mismatch(intent_for([]), nil)

      # Live intent covering one relay clears exactly that entry.
      assert {["eth_feed"], _} = Intent.intent_mismatch(intent_for(["btc_feed"]), cache)
    end

    test "a finished steps relay retained for inspection is not evidence", %{root: root} do
      seed_cell(root, "btc_feed", "op", "k1")
      seed_cell(root, "done_steps", "op", "k1")

      :ok =
        Completion.write("done_steps", %{
          namespace: "ns",
          status: "ok",
          error: "",
          result: "",
          finished_at: 0
        })

      assert {["btc_feed"], _} = Intent.intent_mismatch(intent_for([]), nil)
    end

    test "a finished relay whose delete a write in flight held back is not evidence", %{
      root: root
    } do
      seed_cell(root, "btc_feed", "op", "k1")
      seed_cell(root, "held_steps", "op", "k1")
      File.write!(Path.join([root, "_system", "held_steps", ".deleting"]), "")

      assert {["btc_feed"], _} = Intent.intent_mismatch(intent_for([]), nil)
    end

    test "a leftover class costs one file check, and a completed one is still dropped", %{
      root: root
    } do
      for i <- 1..20, do: seed_cell(root, "leftover_#{i}", "op", "k1")
      seed_cell(root, "done_steps", "op", "k1")

      :ok =
        Completion.write("done_steps", %{
          namespace: "ns",
          status: "ok",
          error: "",
          result: "",
          finished_at: 0
        })

      # Settled directories, so the evidence cache is trusted on the second pass.
      past = System.os_time(:second) - 10
      Enum.each([root, Path.join(root, "_system")], &File.touch!(&1, past))
      {_, cache} = Intent.intent_mismatch(intent_for([]), nil)

      {{mismatch, _cache}, calls} =
        Flezha.State.TestHelpers.file_calls(fn ->
          Intent.intent_mismatch(intent_for([]), cache)
        end)

      assert mismatch == Enum.sort(for i <- 1..20, do: "leftover_#{i}")

      completion_checks =
        for {_function, [path | _]} <- calls,
            String.ends_with?(to_string(path), "completion.record"),
            do: path

      assert length(completion_checks) == 21
      assert Enum.count(calls, &match?({:list_dir, _}, &1)) == 1, inspect(calls)
      refute Enum.any?(calls, &match?({:get_cwd, _}, &1))
    end

    test "durable-registry rows without live intent also count as evidence" do
      stub(HubManager, :durable_relay_ids, fn -> MapSet.new(["ghost_relay"]) end)

      assert {["ghost_relay"], _} = Intent.intent_mismatch(intent_for([]), nil)
    end

    test "flz state delete clears the entry", %{root: root} do
      seed_cell(root, "btc_feed", "op", "k1")
      assert {["btc_feed"], cache} = Intent.intent_mismatch(intent_for([]), nil)

      # The CLI `flz state delete btc_feed/op/k1` path.
      {:ok, _} = Flezha.State.Supervisor.delete_cell({"btc_feed", "op", "k1"})

      assert {[], _} = Intent.intent_mismatch(intent_for([]), cache)
    end

    # A stop takes its relay out of the intent before its state is gone.
    test "evidence is a mismatch only once it outlives the unmet-intent grace", %{root: root} do
      seed_cell(root, "btc_feed", "op", "k1")
      grace = Flezha.Config.Durations.get(:convergence_unmet_intent_grace_ms)

      assert {{[], since}, cache} = Intent.detect_mismatch(intent_for([]), {[], %{}}, nil, 0)
      assert since == %{"btc_feed" => 0}

      assert {{[], ^since}, cache} =
               Intent.detect_mismatch(intent_for([]), {[], since}, cache, grace)

      assert {{["btc_feed"], ^since}, cache} =
               Intent.detect_mismatch(intent_for([]), {[], since}, cache, grace + 1)

      # Intent again clears the clock with the entry.
      assert {{[], %{}}, _} =
               Intent.detect_mismatch(
                 intent_for(["btc_feed"]),
                 {["btc_feed"], since},
                 cache,
                 grace + 2
               )
    end
  end

  describe "the net: a declared relay that runs nowhere is started after the grace" do
    alias Flezha.Config.Backoff
    alias Flezha.Convergence.{Heal, Planner}
    alias Flezha.Service.{HubManager, RelayManager}

    import ExUnit.CaptureLog

    defp declared(ids) do
      %Intent{
        relays: Enum.map(ids, &%{namespace: :ns, relay_id: &1, factory_id: "f", plugins: []})
      }
    end

    test "a declared relay no member holds is named once it outlives the unmet-intent grace" do
      stub(HubManager, :placed_relay_ids, fn :ns, :unreadable -> MapSet.new(["btc_feed"]) end)
      stub(HubManager, :leads?, fn :ns -> true end)
      grace = Flezha.Config.Durations.get(:convergence_unmet_intent_grace_ms)
      intent = declared(["btc_feed", "eth_feed"])

      assert {[], since} = Intent.detect_unplaced(intent, %{}, 0, :all)
      assert since == %{{:ns, "eth_feed"} => 0}
      assert {[], ^since} = Intent.detect_unplaced(intent, since, grace, MapSet.new())

      assert {[{:ns, "eth_feed"}], ^since} =
               Intent.detect_unplaced(intent, since, grace + 1, MapSet.new())

      # Only the hub's first member is answered the relay; the others keep its clock.
      stub(HubManager, :leads?, fn :ns -> false end)
      assert {[], ^since} = Intent.detect_unplaced(intent, since, grace + 1, MapSet.new())

      # Placed again clears its clock.
      stub(HubManager, :placed_relay_ids, fn :ns, :unreadable -> MapSet.new(["eth_feed"]) end)

      assert {[], %{{:ns, "btc_feed"} => _}} =
               Intent.detect_unplaced(intent, since, grace + 2, MapSet.new([:ns]))
    end

    test "a namespace no fact names and no relay waits in is not read" do
      reject(&HubManager.placed_relay_ids/2)

      assert {[], %{}} =
               Intent.detect_unplaced(declared(["eth_feed"]), %{}, 0, MapSet.new([:other]))
    end

    test "a hub that cannot be read names none of its relays" do
      stub(HubManager, :placed_relay_ids, fn :ns, :unreadable -> :unreadable end)

      assert {[], %{}} = Intent.detect_unplaced(declared(["eth_feed"]), %{}, 0, :all)
    end

    test "a row bound to a hub member counts as placed, connected or held in a grace" do
      pid = self()
      hub_id = Flezha.Identificator.hub("ns")
      stub(ProcessHub, :is_alive?, fn ^hub_id -> true end)
      stub(ProcessHub.Hub, :get, fn ^hub_id -> %{storage: %{misc: :misc}} end)

      stub(ProcessHub.Service.Cluster, :nodes, fn :misc, [:include_local] ->
        [node(), :held@gone]
      end)

      stub(ProcessHub, :process_list, fn ^hub_id, :global ->
        [
          {"on_here", [{node(), pid}]},
          {"on_held", [{:held@gone, pid}]},
          {"on_departed", [{:departed@gone, pid}]},
          {"unbound", []},
          {{:plugin, "p"}, [{node(), pid}]}
        ]
      end)

      assert HubManager.placed_relay_ids("ns", :unreadable) == MapSet.new(["on_here", "on_held"])
    end

    test "the Diff keeps only relays still declared, and the Planner starts each" do
      reality = %{unplaced: [{:ns, "eth_feed"}, {:ns, "stopped_feed"}]}

      assert %Diff{unplaced_relays: [{:ns, "eth_feed"}]} =
               delta = Diff.compute(declared(["eth_feed"]), reality)

      assert [%{primitive: :start_relay, namespace: :ns, target: "eth_feed"}] =
               Planner.plan(delta, Backoff.new(:heal))

      assert %Diff{unplaced_relays: []} = Diff.compute(declared([]), reality)
    end

    test "start_relay starts the declared relay through the start authority" do
      relay = %{id: "eth_feed", factory_id: "f"}
      stub(HubManager, :declared_relay, fn "ns", "eth_feed" -> {:ok, relay} end)

      expect(RelayManager, :start_relay, fn "ns", ^relay, [announce: false, record?: false] ->
        {:ok, %{relay_id: "eth_feed"}}
      end)

      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))

      log =
        capture_log([level: :warning], fn ->
          assert Heal.execute(%{
                   primitive: :start_relay,
                   namespace: :ns,
                   target: "eth_feed",
                   args: %{}
                 }) == :ok
        end)

      assert log =~ "started declared relay eth_feed in ns: it had run nowhere"
    end

    test "start_relay no runner can serve is unmet, named for the namespace, and not logged" do
      relay = %{id: "eth_feed", factory_id: "gone"}
      stub(HubManager, :declared_relay, fn "ns", "eth_feed" -> {:ok, relay} end)

      stub(RelayManager, :start_relay, fn "ns", ^relay, _opts ->
        {:error, {:factory_unknown, "ns", "gone"}}
      end)

      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))

      log =
        capture_log([level: :warning], fn ->
          assert Heal.execute(%{
                   primitive: :start_relay,
                   namespace: :ns,
                   target: "eth_feed",
                   args: %{}
                 }) == {:unmet, {:factory_unknown, "gone"}}
        end)

      refute log =~ "eth_feed"
    end

    test "start_relay never starts a relay a stop took out of the declared set" do
      stub(HubManager, :declared_relay, fn "ns", "stopped_feed" -> :error end)
      reject(&RelayManager.start_relay/3)

      assert Heal.execute(%{
               primitive: :start_relay,
               namespace: :ns,
               target: "stopped_feed",
               args: %{}
             }) == :unmet
    end
  end

  describe "Progress.progress_markers/0 — the tier cursors reach the classifier" do
    alias Flezha.Convergence.{Loop, StallClassifier}
    alias Flezha.Relay.ClassRegistry
    alias Flezha.State.{InputLog, Supervisor, WalWriter}

    setup do
      FlezhaTest.Case.install([:tmp_app_data, :state_tree])
      ClassRegistry.ensure_table()

      case start_supervised(Flezha.State.Watchdog) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end

      :ok
    end

    defp effect(outputs, offset) do
      WireEncoders.encode_effect_body(%{
        changes: [<<1>>],
        outputs: outputs,
        consumed_offset: offset,
        dlq: nil
      })
    end

    defp effect_hdr(), do: <<1, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0>>

    test "a tier-on Cell's markers carry its OWN consumed offset and the log tail" do
      rc = "tiered-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "k1"}
      ClassRegistry.put_overwatch(rc, self(), nil)

      {:ok, 0} = InputLog.append(rc, "i0", %{})
      {:ok, 1} = InputLog.append(rc, "i1", %{})

      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, effect_hdr(), effect([], 0))
      :ok = WalWriter.fsync(pid)

      marker = Enum.find(Progress.progress_markers(), &(&1.relay_id == rc))

      assert marker.cell == {"op", "k1"}
      assert marker.input_log_tail == 1
      assert marker.consumed_offset == 0, "the Cell's own offset, not the class-wide min"

      # The record was dispatched (its consumed offset landed), so the flush cursor
      # advanced past it — but it bore no outputs, so out_pending_seq did NOT move.
      # That asymmetry is precisely what keeps `outbox_flat?` from firing on a
      # Cell that persists state and emits nothing.
      assert marker.out_pending_seq == -1
      assert marker.outbox_flushed_seq == 0

      :ok = WalWriter.close(pid)
    end

    test "a tier-off Cell gets no cursor fields and no input log is created" do
      rc = "plain-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "k1"}

      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, <<1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0>>, "change")
      :ok = WalWriter.fsync(pid)

      marker = Enum.find(Progress.progress_markers(), &(&1.relay_id == rc))

      refute Map.has_key?(marker, :input_log_tail)
      refute Map.has_key?(marker, :consumed_offset)

      assert Supervisor.lookup({rc, :input_log}) == :error,
             "polling markers must never conjure an input log for a tier-off relay"

      :ok = WalWriter.close(pid)
    end

    test "a Cell that stops consuming while the log grows classifies STUCK, naming it" do
      rc = "stalling-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "poisoned"}
      ClassRegistry.put_overwatch(rc, self(), nil)

      {:ok, 0} = InputLog.append(rc, "i0", %{})
      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, effect_hdr(), effect([], 0))
      :ok = WalWriter.fsync(pid)

      baseline = Enum.filter(Progress.progress_markers(), &(&1.relay_id == rc))

      # The source keeps feeding, but this Cell never consumes another offset —
      # a poison payload mid-retry, or a tasklet wedged on it.
      {:ok, 1} = InputLog.append(rc, "i1", %{})
      {:ok, 2} = InputLog.append(rc, "i2", %{})

      current = Enum.filter(Progress.progress_markers(), &(&1.relay_id == rc))

      assert %{^rc => {:stuck, :watermark_frozen, %{cell: {"op", "poisoned"}}}} =
               StallClassifier.classify_producing(current, baseline, %{
                 now: System.monotonic_time(:millisecond),
                 grace_ms: 60_000
               })

      :ok = WalWriter.close(pid)
    end
  end

  describe "Progress.rotate_progress_history/4 — grace-window cursor baselines" do
    alias Flezha.Convergence.Loop

    test "baseline is nil until a snapshot has aged one grace window, then rotates" do
      {baseline, h} = Progress.rotate_progress_history([], [:a], 0, 100)
      assert baseline == nil

      {baseline, h} = Progress.rotate_progress_history(h, [:b], 50, 100)
      assert baseline == nil

      {baseline, h} = Progress.rotate_progress_history(h, [:c], 100, 100)
      assert baseline == [:a]

      # Older-than-baseline entries are pruned; the newest aged one wins.
      {baseline, h} = Progress.rotate_progress_history(h, [:d], 160, 100)
      assert baseline == [:b]
      refute Enum.any?(h, fn {_at, snap} -> snap == [:a] end)
    end
  end

  describe "Loop stuck / intent-mismatch transition telemetry" do
    alias Flezha.Convergence.Loop

    setup do
      ref = make_ref()

      Telemetry.capture(
        [[:flezha, :convergence, :relay_stuck], [:flezha, :convergence, :intent_mismatch]],
        as: fn event, measurements, metadata -> {ref, event, measurements, metadata} end
      )

      {:ok, ref: ref}
    end

    test "freshly-stuck relays emit relay_stuck once, not on steady state", %{ref: ref} do
      stuck = %{"btc" => %{detail: :wal_frozen, cell: "op/k1"}}

      Progress.note_stuck_transitions(%{}, stuck)

      assert_receive {^ref, [:flezha, :convergence, :relay_stuck], %{count: 1},
                      %{relay_id: "btc", detail: :wal_frozen, cell: "op/k1"}}

      Progress.note_stuck_transitions(stuck, stuck)
      refute_received {^ref, [:flezha, :convergence, :relay_stuck], _, _}
    end

    test "intent-mismatch transitions emit with the named relays", %{ref: ref} do
      Intent.note_intent_mismatch_transition([], ["btc", "eth"])

      assert_receive {^ref, [:flezha, :convergence, :intent_mismatch], %{count: 2},
                      %{relays: ["btc", "eth"]}}

      # Unchanged and cleared transitions do not re-emit.
      Intent.note_intent_mismatch_transition(["btc", "eth"], ["btc", "eth"])
      Intent.note_intent_mismatch_transition(["btc", "eth"], [])
      refute_received {^ref, [:flezha, :convergence, :intent_mismatch], _, _}
    end
  end

  describe "Loop.Summary unmet-intent clock" do
    alias Flezha.Convergence.{Snapshot, Verdict}
    alias Flezha.Convergence.Loop.{State, Summary}

    # A relay another controller hosts is `:unknown` here: it must not hold the
    # clock, or a local relay losing its runner skips its `recovering` grace.
    test "an unknown relay holds no clock, so a local relay's lost runner reads recovering" do
      foreign = {{:relay, {:ns, "foreign"}}, Verdict.new({:relay, {:ns, "foreign"}}, :unknown)}
      local = {:relay, {:ns, "local"}}
      long_ago = System.monotonic_time(:millisecond) - 600_000

      healthy = %State{
        verdicts: Map.new([foreign, {local, Verdict.new(local, :healthy)}]),
        unmet_intent_since: long_ago
      }

      state = Summary.publish(%Intent{}, %{recovery: %{}}, healthy)
      assert state.unmet_intent_since == nil

      lost = %{state | verdicts: Map.new([foreign, {local, Verdict.new(local, :failed)}])}
      Summary.publish(%Intent{}, %{recovery: %{}}, lost)
      assert Snapshot.relay_health(:ns, "local") == :recovering
    end
  end
end

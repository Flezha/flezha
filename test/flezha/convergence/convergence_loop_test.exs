defmodule Flezha.Convergence.LoopTest do
  @moduledoc """
  The Loop and what it reports: misbound streaks, the operator health map,
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

  describe "Loop.apply_misbound_streak/2 (B5)" do
    alias Flezha.Convergence.Loop

    test "a operative misbound for one tick is not yet migrated; two ticks releases it" do
      delta = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead", :probe}]}

      {streaks, tick1} = Loop.apply_misbound_streak(%{}, delta)
      assert tick1.misbound_operatives == []

      {_streaks, tick2} = Loop.apply_misbound_streak(streaks, delta)
      assert tick2.misbound_operatives == [{{:ns, "r"}, "e_dead", :probe}]
    end

    test "a declared disconnect is not streak-gated: the fleet observed the close" do
      delta = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead", :runner_disconnected}]}

      {_streaks, tick1} = Loop.apply_misbound_streak(%{}, delta)
      assert tick1.misbound_operatives == [{{:ns, "r"}, "e_dead", :runner_disconnected}]
    end

    test "a operative seen bound again resets its streak" do
      delta = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead", :probe}]}
      {streaks, _} = Loop.apply_misbound_streak(%{}, delta)

      # Bound again this tick → drops out → streak resets.
      {streaks, _} = Loop.apply_misbound_streak(streaks, %Diff{misbound_operatives: []})
      {_streaks, tick} = Loop.apply_misbound_streak(streaks, delta)
      assert tick.misbound_operatives == []
    end
  end

  describe "a runner connecting is news for the operatives it can take" do
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
        operatives: [%{namespace: :ns, operative_id: "op", factory_id: "f", plugins: []}]
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

    # One declared operative bound to a runner the active set no longer holds,
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
      send(Loop, {:runner_connected_event, {:ns, runner_id}})
      _ = :sys.get_state(Loop)
      :ok
    end

    defp attempt(key), do: Backoff.attempt(:sys.get_state(Loop).backoff, key)

    # fails when: the release walks only the parked operatives and resets only
    # the two key shapes it listed. An operative misbound to a departed runner
    # is in neither set, so it waited out its own rung — up to five minutes —
    # with a healthy runner already there.
    test "an operative whose runner died is released the moment a capable one connects" do
      arm_misbound_rung()

      connect(@fresh)

      assert attempt(@misbound_key) == 0,
             "the rebind stayed on its rung with a capable runner present"
    end

    # fails when: the guard goes missing and any runner wakes every waiting
    # operative in its namespace, whatever it can actually serve.
    test "a runner advertising nothing the operative needs wakes nobody" do
      stub(FleetManager, :factories_for_runner, fn _ns, _runner -> MapSet.new(["other"]) end)
      arm_misbound_rung()

      connect(@fresh)

      assert attempt(@misbound_key) == 1
    end

    # fails when: a runner that has advertised nothing YET — an SDK may send
    # its factories on a later frame — is read as one that serves nothing, and
    # the operative it was going to take waits out its rung anyway.
    test "a runner that has advertised nothing yet still releases" do
      stub(FleetManager, :factories_for_runner, fn _ns, _runner -> MapSet.new() end)
      arm_misbound_rung()

      connect(@fresh)

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

    defp watch_ticks() do
      parent = self()
      handler = {__MODULE__, :ticks, make_ref()}

      :telemetry.attach(
        handler,
        [:flezha, :convergence, :tick],
        fn _e, _m, meta, _ -> send(parent, {:tick_state, meta.state}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
    end

    # fails when: the parked shape stops being released. The published scenario
    # — "a runner arriving mid-cooldown is used at once" — is now carried by
    # the verdict records rather than by the parked facts, so it needs a test
    # that goes through the judge.
    test "a parked operative is released by the same path" do
      stub(Probe, :snapshot, fn ->
        reality(%{parked?: true, park_reason: {:ping_failed, :timeout}, bound_runner: @fresh})
      end)

      :ok = Loop.tick_now()
      assert attempt({:parked, "op"}) == 1, "the parked rebind did not walk a rung"

      connect(@fresh)

      assert attempt({:parked, "op"}) == 0
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

  describe "Planner.waiting_keys/1 — what a runner connecting releases" do
    alias Flezha.Config.Backoff
    alias Flezha.Convergence.{Facts, Planner}

    # One operative in every waiting field at once, under every reason the
    # kinds table stamps on those fields plus the probe's reasonless one: what
    # the Planner can key a binding op on for a single operative.
    defp every_waiting_shape(ref) do
      reasons = [:probe | Facts.reasons_for(Diff.waiting_fields())]

      %Diff{
        unbound_operatives: [ref],
        misbound_operatives: Enum.map(reasons, &{ref, "e_dead", &1}),
        parked_operatives: [{ref, %{reason: :timeout}, "e_dead"}],
        replace_operatives: [{ref, "e_dead"}]
      }
    end

    # fails when: the release set stops being derived from the plan. Both sides
    # read the declared kinds, so a reason added to the table joins the planned
    # ops and the released keys together; a hand-written list of key shapes
    # would move only one of them, which is how `{:runner_disconnected, id}`
    # escaped and waited out a rung nothing would clear.
    test "every key the binding rules can plan for one operative is released" do
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

    # fails when: the two shapes this change was reported for stop being
    # released — a rebind carrying the declared disconnect, and the re-place a
    # factory only another node serves is planned on.
    test "the reasoned rebind and the re-place are both in the released set" do
      released = Planner.waiting_keys({:ns, "op-1"})

      assert {:runner_disconnected, "op-1"} in released
      assert {:replace_operative, "op-1"} in released
    end

    # fails when: a namespace-wide release becomes a namespace-wide tick. The
    # per-tick cap is the only thing bounding it — the heal row carries no
    # jitter — so 500 released keys must still yield one capped plan.
    test "releasing a whole namespace still plans at most one tick's cap" do
      delta = %Diff{unbound_operatives: for(n <- 1..500, do: {:ns, "op-#{n}"})}

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
      stub(HubManager, :durable_operative_ids, fn -> MapSet.new() end)

      {:ok, root: root}
    end

    defp seed_entity(root, operative, procedure, key) do
      dir = Path.join([root, "_system", operative, procedure])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "#{key}.wal"), <<1, 2, 3>>)
    end

    defp intent_for(ids) do
      %Intent{
        operatives:
          Enum.map(ids, fn id ->
            %{namespace: :ns, operative_id: id, factory_id: nil, plugins: []}
          end)
      }
    end

    test "fresh install with no durable evidence → empty mismatch" do
      assert {[], _cache} = Intent.intent_mismatch(intent_for([]), nil)
    end

    test "wiped registry with on-disk evidence → mismatch names the operatives", %{root: root} do
      seed_entity(root, "btc_kraken", "op", "k1")
      seed_entity(root, "eth_kraken", "op", "k1")

      assert {["btc_kraken", "eth_kraken"], cache} = Intent.intent_mismatch(intent_for([]), nil)

      # Live intent covering one operative clears exactly that entry.
      assert {["eth_kraken"], _} = Intent.intent_mismatch(intent_for(["btc_kraken"]), cache)
    end

    test "a finished workflow retained for inspection is not evidence", %{root: root} do
      seed_entity(root, "btc_kraken", "op", "k1")
      seed_entity(root, "done_workflow", "op", "k1")

      :ok =
        Completion.write("done_workflow", %{
          namespace: "ns",
          status: "ok",
          error: "",
          result: "",
          finished_at: 0
        })

      assert {["btc_kraken"], _} = Intent.intent_mismatch(intent_for([]), nil)
    end

    test "durable-registry rows without live intent also count as evidence" do
      stub(HubManager, :durable_operative_ids, fn -> MapSet.new(["ghost_operative"]) end)

      assert {["ghost_operative"], _} = Intent.intent_mismatch(intent_for([]), nil)
    end

    test "flz state delete clears the entry", %{root: root} do
      seed_entity(root, "btc_kraken", "op", "k1")
      assert {["btc_kraken"], cache} = Intent.intent_mismatch(intent_for([]), nil)

      # The CLI `flz state delete btc_kraken/op/k1` path.
      {:ok, _} = Flezha.State.Supervisor.delete_entity({"btc_kraken", "op", "k1"})

      assert {[], _} = Intent.intent_mismatch(intent_for([]), cache)
    end
  end

  describe "Progress.progress_markers/0 — the tier cursors reach the classifier" do
    alias Flezha.Convergence.{Loop, StallClassifier}
    alias Flezha.Operative.ClassRegistry
    alias Flezha.State.{InputLog, Supervisor, WalWriter, Wire}

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
      Wire.encode_effect_body(%{
        changes: [<<1>>],
        outputs: outputs,
        consumed_offset: offset,
        dlq: nil
      })
    end

    defp effect_hdr(), do: <<1, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0>>

    test "a tier-on Entity's markers carry its OWN consumed offset and the log tail" do
      rc = "tiered-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "k1"}
      ClassRegistry.put_overwatch(rc, self(), nil)

      {:ok, 0} = InputLog.append(rc, "i0", %{})
      {:ok, 1} = InputLog.append(rc, "i1", %{})

      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, effect_hdr(), effect([], 0))
      :ok = WalWriter.fsync(pid)

      marker = Enum.find(Progress.progress_markers(), &(&1.operative_id == rc))

      assert marker.entity == {"op", "k1"}
      assert marker.input_log_tail == 1
      assert marker.consumed_offset == 0, "the Entity's own offset, not the class-wide min"

      # The record was dispatched (its consumed offset landed), so the flush cursor
      # advanced past it — but it bore no outputs, so out_pending_seq did NOT move.
      # That asymmetry is precisely what keeps `outbox_flat?` from firing on a
      # Entity that persists state and emits nothing.
      assert marker.out_pending_seq == -1
      assert marker.outbox_flushed_seq == 0

      :ok = WalWriter.close(pid)
    end

    test "a tier-off Entity gets no cursor fields and no input log is created" do
      rc = "plain-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "k1"}

      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, <<1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0>>, "change")
      :ok = WalWriter.fsync(pid)

      marker = Enum.find(Progress.progress_markers(), &(&1.operative_id == rc))

      refute Map.has_key?(marker, :input_log_tail)
      refute Map.has_key?(marker, :consumed_offset)

      assert Supervisor.lookup({rc, :input_log}) == :error,
             "polling markers must never conjure an input log for a tier-off operative"

      :ok = WalWriter.close(pid)
    end

    test "an Entity that stops consuming while the log grows classifies STUCK, naming it" do
      rc = "stalling-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "poisoned"}
      ClassRegistry.put_overwatch(rc, self(), nil)

      {:ok, 0} = InputLog.append(rc, "i0", %{})
      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, effect_hdr(), effect([], 0))
      :ok = WalWriter.fsync(pid)

      baseline = Enum.filter(Progress.progress_markers(), &(&1.operative_id == rc))

      # The source keeps feeding, but this Entity never consumes another offset —
      # a poison payload mid-retry, or a procedure wedged on it.
      {:ok, 1} = InputLog.append(rc, "i1", %{})
      {:ok, 2} = InputLog.append(rc, "i2", %{})

      current = Enum.filter(Progress.progress_markers(), &(&1.operative_id == rc))

      assert %{^rc => {:stuck, :watermark_frozen, %{entity: {"op", "poisoned"}}}} =
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
      parent = self()
      ref = make_ref()
      handler_id = {__MODULE__, ref}

      :telemetry.attach_many(
        handler_id,
        [
          [:flezha, :convergence, :operative_stuck],
          [:flezha, :convergence, :intent_mismatch]
        ],
        fn event, measurements, metadata, _ ->
          send(parent, {ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
      {:ok, ref: ref}
    end

    test "freshly-stuck operatives emit operative_stuck once, not on steady state", %{ref: ref} do
      stuck = %{"btc" => %{detail: :wal_frozen, entity: "op/k1"}}

      Progress.note_stuck_transitions(%{}, stuck)

      assert_receive {^ref, [:flezha, :convergence, :operative_stuck], %{count: 1},
                      %{operative_id: "btc", detail: :wal_frozen, entity: "op/k1"}}

      Progress.note_stuck_transitions(stuck, stuck)
      refute_received {^ref, [:flezha, :convergence, :operative_stuck], _, _}
    end

    test "intent-mismatch transitions emit with the named operatives", %{ref: ref} do
      Intent.note_intent_mismatch_transition([], ["btc", "eth"])

      assert_receive {^ref, [:flezha, :convergence, :intent_mismatch], %{count: 2},
                      %{operatives: ["btc", "eth"]}}

      # Unchanged and cleared transitions do not re-emit.
      Intent.note_intent_mismatch_transition(["btc", "eth"], ["btc", "eth"])
      Intent.note_intent_mismatch_transition(["btc", "eth"], [])
      refute_received {^ref, [:flezha, :convergence, :intent_mismatch], _, _}
    end
  end
end

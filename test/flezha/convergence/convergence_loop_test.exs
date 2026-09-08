defmodule Flezha.Convergence.LoopTest do
  @moduledoc """
  The Loop and what it reports: misbound streaks, the operator health map,
  deterministic runner placement, durable-evidence intent mismatch, the tier
  progress cursors and the transition telemetry.
  """

  # async: false: writes Application env; Mimic stubs; a shared on-disk root.

  use FlezhaTest.Case, async: false
  use Mimic

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

  describe "Loop.waiting_keys/1 — what a runner connecting releases" do
    alias Flezha.Convergence.Loop
    alias Flezha.Convergence.Planner

    # fails when: the release names a ladder the planner no longer cools on. An
    # operative held with no binding is judged `unbound` and planned on the
    # rebind key, not the park key, so releasing only the park ladder would
    # leave it waiting out its own rung after its runner came back.
    test "both cooldowns an operative waiting for a runner can be planned on" do
      parked_op = %{primitive: :rebind_operative, target: "op-1", args: %{reason: :parked}}
      unbound_op = %{primitive: :rebind_operative, target: "op-1", args: %{}}

      keys = Loop.waiting_keys("op-1")

      assert Planner.key(parked_op) in keys
      assert Planner.key(unbound_op) in keys
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
      ClassRegistry.put_overwatch(rc, self())

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
      ClassRegistry.put_overwatch(rc, self())

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

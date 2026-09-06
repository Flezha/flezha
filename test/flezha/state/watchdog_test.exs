defmodule Flezha.State.WatchdogTest do
  @moduledoc """
  Defect B: the per-Entity output-staleness sweep and the
  compaction-handshake timeout. A live Entity whose persisted seq
  freezes is detected and resynced; a genuinely idle Entity is not
  flagged; a `CompactEntity` whose snapshot never returns is timed out
  and recovered, and stays retriable.
  """

  # async: false: writes Application env; a shared on-disk root; the primary Logger.

  use FlezhaTest.Case, fixtures: [:tmp_app_data, :state_tree, :env], async: false

  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]

  import Flezha.State.TestHelpers, only: [uniq: 0]
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 3]

  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.DurabilityTier
  alias Flezha.State.DurabilityTier.Shipper
  alias Flezha.State.Paths
  alias Flezha.State.{Supervisor, TestHelpers, WalWriter, Watchdog}
  alias FlezhaTest.Helper.DurabilityBackendStub

  @stall_ms 20
  @compaction_ms 20

  setup %{base: base} do
    # The reconciler drives sweeps in production; these tests drive them
    # explicitly via sweep_now/0 (the Watchdog holds no self-timer).
    case start_supervised(Watchdog) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    # The durability sweep enumerates every registry cspec on this node, so the
    # cspec root has to be isolated with the state root — otherwise a case picks
    # up whatever hubs the developer's own `app_data/` happens to hold.
    on_exit(TestHelpers.override_env(:process_hub_registry_root, Path.join(base, "process_hub")))

    on_exit(TestHelpers.override_env(:default_output_stall_timeout_ms, @stall_ms))
    on_exit(TestHelpers.override_env(:compaction_timeout_ms, @compaction_ms))

    parent = self()

    on_exit(
      TestHelpers.override_env(:state_resync_fun, fn entity_id, opts ->
        send(parent, {:resync_called, entity_id, opts})
        {:ok, make_ref()}
      end)
    )

    out_id = {__MODULE__, :output_stall, System.unique_integer([:positive])}
    comp_id = {__MODULE__, :compaction_timeout, System.unique_integer([:positive])}
    trig_id = {__MODULE__, :compaction_trigger, System.unique_integer([:positive])}
    give_id = {__MODULE__, :compaction_gave_up, System.unique_integer([:positive])}

    :telemetry.attach(
      out_id,
      [:flezha, :state, :output_stall],
      fn _e, meas, meta, _ -> send(parent, {:output_stall, meas, meta}) end,
      nil
    )

    :telemetry.attach(
      comp_id,
      [:flezha, :state, :compaction_timeout],
      fn _e, meas, meta, _ -> send(parent, {:compaction_timeout, meas, meta}) end,
      nil
    )

    :telemetry.attach(
      trig_id,
      [:flezha, :state, :compaction_trigger],
      fn _e, meas, meta, _ -> send(parent, {:compaction_trigger, meas, meta}) end,
      nil
    )

    :telemetry.attach(
      give_id,
      [:flezha, :state, :compaction_gave_up],
      fn _e, meas, meta, _ -> send(parent, {:compaction_gave_up, meas, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(out_id)
      :telemetry.detach(comp_id)
      :telemetry.detach(trig_id)
      :telemetry.detach(give_id)
    end)

    %{header: TestHelpers.stub_header()}
  end

  defp live_writer(suffix, header) do
    pid = TestHelpers.sample_entity_id(suffix <> uniq())
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, "z")
    :ok = WalWriter.append(w, 1, header, "a")
    :ok = WalWriter.append(w, 2, header, "b")
    {pid, w}
  end

  test "a live entity whose seq froze is flagged, and the sensor heals nothing",
       %{header: header} do
    {_entity_id, writer} = live_writer("stall_", header)

    # The advance window lapses, then Changes keep arriving (duplicates)
    # without advancing the seq — the stall signature.
    TestHelpers.age_writer(writer, @stall_ms * 2)
    :duplicate = WalWriter.append(writer, 1, header, "a")

    :ok = Watchdog.sweep_now()

    assert_receive {:output_stall, %{stalled_ms: ms, last_seq: 2}, meta}, 1_000
    assert ms >= @stall_ms
    # `operative_instance` is the rebind target the convergence Loop consumes;
    # by the flezha convention it equals the on-disk `operative_class`.
    assert meta.operative_instance == "TestOperative"
    assert meta.operative_class == "TestOperative"

    # The Watchdog emits and stops there. Its former direct resync is the
    # second actor the convergence pipeline replaced with the output-stall
    # ladder; a resync from here would race that ladder under no backoff.
    refute_received {:resync_called, _entity_id, _opts}
  end

  test "every completed sweep stamps the sensor freshness mark" do
    ConvergenceSnapshot.observe_sensor(:watchdog, -1, 0)

    :ok = Watchdog.sweep_now()

    assert %{watchdog: {at, period}} = ConvergenceSnapshot.sensors()
    assert period > 0
    assert :erlang.monotonic_time(:millisecond) - at < 1_000
  end

  test "a genuinely idle entity is not flagged", %{header: header} do
    {{_, _, key} = entity_id, writer} = live_writer("idle_", header)

    # No Changes arrive after the window lapses — last_change_seen_at goes
    # stale alongside last_advance_at, so this is idle, not stalled.
    TestHelpers.age_writer(writer, @stall_ms * 2)

    :ok = Watchdog.sweep_now()

    # Scoped to THIS test's entity key. The Watchdog is a singleton and every
    # test in this file uses the operative class "TestOperative", so an unscoped
    # refute — or one scoped only to the operative — matches a sibling test's
    # message and fails whenever the suite interleaves.
    refute_received {:output_stall, _, %{key: ^key}}
    refute_received {:resync_called, ^entity_id, _}
  end

  test "a stalled compaction times out and stays retriable, without resyncing a live entity below give-up" do
    {_, _, key} = entity_id = TestHelpers.sample_entity_id("comp_" <> uniq())

    Watchdog.compaction_started(entity_id, "exec-comp")
    age_compaction(entity_id, @compaction_ms * 2)

    :ok = Watchdog.sweep_now()

    # Scoped to this test's entity KEY, not its operative: the Watchdog is a
    # singleton and every test here shares the class "TestOperative", so a sweep
    # also sees compactions a sibling test left pending.
    assert_receive {:compaction_timeout, %{waited_ms: ms}, %{key: ^key, attempt: 1}}, 1_000
    assert ms >= @compaction_ms
    # Below the give-up threshold a still-live, still-correct Entity is NOT
    # force-resynced — the backoff-gated trigger sweep is the retry.
    refute_received {:resync_called, ^entity_id, _}

    # Entry was cleared on timeout — a second sweep does not re-fire.
    :ok = Watchdog.sweep_now()
    refute_received {:compaction_timeout, _, %{key: ^key}}
  end

  test "repeated compaction failures back off and give up loudly, declaring the give-up" do
    become_loop()
    {_, _, key} = entity_id = TestHelpers.sample_entity_id("giveup_" <> uniq())

    # Drive four timeouts (compaction_started bypasses the backoff-gated trigger
    # sweep, so this reaches the give-up threshold without waiting out cooldowns).
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        for _ <- 1..4 do
          Watchdog.compaction_started(entity_id, "exec-giveup")
          age_compaction(entity_id, @compaction_ms * 2)
          :ok = Watchdog.sweep_now()
        end

        assert_receive {:compaction_gave_up, %{attempts: 4}, %{key: ^key}}, 1_000
      end)

    # "Loudly" is half the contract: the operator gets a named alarm.
    assert log =~ "compaction GAVE UP after 4 failed attempts"
    assert log =~ "key=#{key}"

    # The fourth crossing declares the give-up, with the runner the WalWriter
    # status held, to the loop — which plans the resync. The Watchdog itself
    # resyncs nothing.
    assert_receive {:declare, :compaction_gave_up, ^entity_id, %{runner_id: "exec-giveup"}}, 1_000
    refute_received {:resync_called, ^entity_id, _}
  end

  test "a completed compaction is declared as the counter-fact of a give-up" do
    become_loop()
    entity_id = TestHelpers.sample_entity_id("compdone_" <> uniq())

    Watchdog.compaction_started(entity_id, "exec-done")
    Watchdog.compaction_completed(entity_id)
    :ok = Watchdog.sweep_now()

    assert_receive {:declare, :compaction_completed, ^entity_id, %{}}, 1_000
  end

  test "a compaction that completes before the bound never times out" do
    {_, _, key} = entity_id = TestHelpers.sample_entity_id("compok_" <> uniq())

    Watchdog.compaction_started(entity_id, "exec-ok")
    Watchdog.compaction_completed(entity_id)

    :ok = Watchdog.sweep_now()
    # Keyed on this entity: the sweep is global, so a sibling test's in-flight
    # compaction must not be able to answer for this one.
    refute_received {:compaction_timeout, _, %{key: ^key}}
  end

  # An owned writer (runner_id on append) is the trigger sweeps' subject;
  # writers without an owning runner are skipped (no session to ask).
  # Returns the entity KEY: the Watchdog is a singleton whose sweep walks every
  # registered entity, so every assertion here must be scoped to its own — any
  # other test's WAL (a proc-journal, say) triggers the same telemetry.
  defp owned_writer(suffix, header, body) do
    {_rc, _op, key} = pid = TestHelpers.sample_entity_id(suffix <> uniq())
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, body, "exec-trig")
    # A triggered entity sits in the shared Watchdog's in-flight map; clear
    # it so it cannot expire as compaction_timeout noise in a later test.
    on_exit(fn -> Watchdog.compaction_completed(pid) end)
    {key, w}
  end

  test "a WAL past compact_after_bytes triggers a size compaction once", %{header: header} do
    Application.put_env(:flezha, :default_compact_after_bytes, 16)
    {key, _w} = owned_writer("size_", header, "0123456789abcdef")

    :ok = Watchdog.sweep_now()

    assert_receive {:compaction_trigger, %{wal_size_bytes: bytes}, %{key: ^key} = meta}, 1_000
    assert bytes >= 16
    assert meta.trigger == :size
    assert meta.runner == "exec-trig"

    # In-flight guard: the next sweep does not re-enqueue.
    :ok = Watchdog.sweep_now()
    refute_received {:compaction_trigger, _, %{key: ^key}}
  end

  test "a small-but-old WAL triggers an age compaction", %{header: header} do
    Application.put_env(:flezha, :compact_after_age_ms, 30)
    {key, w} = owned_writer("age_", header, "x")

    TestHelpers.age_writer(w, 60)
    :ok = Watchdog.sweep_now()

    assert_receive {:compaction_trigger, _meas, %{key: ^key} = meta}, 1_000
    assert meta.trigger == :age
  end

  test "age trigger is disabled at 0", %{header: header} do
    Application.put_env(:flezha, :compact_after_age_ms, 0)
    {key, w} = owned_writer("age0_", header, "x")

    TestHelpers.age_writer(w, 60)
    :ok = Watchdog.sweep_now()

    refute_received {:compaction_trigger, _, %{key: ^key}}
  end

  test "size and age never double-enqueue across consecutive ticks", %{header: header} do
    Application.put_env(:flezha, :default_compact_after_bytes, 16)
    Application.put_env(:flezha, :compact_after_age_ms, 30)
    {key, w} = owned_writer("both_", header, "0123456789abcdef")

    TestHelpers.age_writer(w, 60)
    :ok = Watchdog.sweep_now()

    # Both conditions hold, but the shared in-flight guard yields ONE enqueue.
    assert_receive {:compaction_trigger, _meas, %{key: ^key, trigger: :size}}, 1_000
    refute_received {:compaction_trigger, _, %{key: ^key}}

    :ok = Watchdog.sweep_now()
    refute_received {:compaction_trigger, _, %{key: ^key}}
  end

  test "a workflow class is not flagged for output stall or catastrophic dataflow", %{
    header: header
  } do
    class = "WorkflowWatch" <> uniq()
    ClassRegistry.put(class, :workflow)
    on_exit(fn -> ClassRegistry.delete(class) end)

    parent = self()
    cat_id = {__MODULE__, :catastrophic, System.unique_integer([:positive])}

    :telemetry.attach(
      cat_id,
      [:flezha, :state, :catastrophic_dataflow_stop],
      fn _e, meas, meta, _ -> send(parent, {:catastrophic, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(cat_id) end)

    pid = {class, "proc", "seg" <> uniq()}
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, "z")
    :ok = WalWriter.append(w, 1, header, "a")
    :ok = WalWriter.append(w, 2, header, "b")

    # The same stall signature that flags a stream class: window lapses,
    # then a duplicate Change keeps it "not idle" with a frozen seq.
    TestHelpers.age_writer(w, @stall_ms * 2)
    :duplicate = WalWriter.append(w, 1, header, "a")

    :ok = Watchdog.sweep_now()

    # Scoped to THIS test's class and entity. The Watchdog is a singleton whose
    # sweep walks every registered entity, so an unscoped refute also matches a
    # sibling test's resync (a compaction timeout on its own WAL, say).
    refute_received {:output_stall, _, %{operative_class: ^class}}
    refute_received {:resync_called, ^pid, _}
    refute_received {:catastrophic, _, %{operative_class: ^class}}
  end

  test "a checkpoint-ack class's quiet interval is not a dead data-flow", %{header: header} do
    # The dataflow-stop detector reads "produced output before, silent since"
    # as a dead feed. For a checkpoint-ack class that silence is the steady
    # state: it makes nothing durable until the interval closes.
    on_exit(TestHelpers.override_env(:catastrophic_dataflow_timeout_ms, @stall_ms))

    per_input = "PerInputWatch" <> uniq()
    checkpoint = "CheckpointWatch" <> uniq()
    ClassRegistry.put_checkpoint_ack(checkpoint, true)

    on_exit(fn ->
      ClassRegistry.delete(per_input)
      ClassRegistry.delete(checkpoint)
    end)

    parent = self()
    cat_id = {__MODULE__, :cp_catastrophic, System.unique_integer([:positive])}

    :telemetry.attach(
      cat_id,
      [:flezha, :state, :catastrophic_dataflow_stop],
      fn _e, _meas, meta, _ -> send(parent, {:catastrophic, meta.operative_class}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(cat_id) end)

    for class <- [per_input, checkpoint] do
      pid = {class, "proc", "seg" <> uniq()}
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, w} = Supervisor.lookup(pid)
      :ok = WalWriter.append(w, 0, header, "z")
      :ok = WalWriter.append(w, 1, header, "a")
      # Silent past the timeout, with output behind it.
      TestHelpers.age_writer(w, @stall_ms * 2)
    end

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        :ok = Watchdog.sweep_now()

        # The control proves the detector is armed and this signature trips it...
        assert_receive {:catastrophic, ^per_input}, 1_000
        # ...and that the checkpoint-ack class is exempt, not merely unobserved.
        refute_received {:catastrophic, ^checkpoint}
      end)

    # The positive control alarms by name; the exempt class never appears.
    assert log =~ ~s(data-flow stop operative="#{per_input}")
    refute log =~ checkpoint
  end

  test "a remote-durability RPO breach alarms once and clears when it catches up" do
    rpo_id = {__MODULE__, :rpo_exceeded, System.unique_integer([:positive])}
    parent = self()

    :telemetry.attach(
      rpo_id,
      [:flezha, :state, :rpo_exceeded],
      fn _e, meas, meta, _ -> send(parent, {:rpo_exceeded, meas, meta}) end,
      nil
    )

    DurabilityBackendStub.install()

    Application.put_env(
      :flezha,
      :durability_tier,
      Application.get_env(:flezha, :durability_tier) |> Keyword.put(:rpo_target_ms, 1)
    )

    on_exit(fn ->
      :telemetry.detach(rpo_id)
      DurabilityBackendStub.uninstall()
    end)

    {:ok, _pid} = start_supervised(Flezha.State.DurabilityTier.Shipper)

    ref = shippable_ref("rpo")
    DurabilityBackendStub.set_down(true)
    assert {:error, :backend_down} = Shipper.ship_now(ref)

    # A second reference, breached through the same production path and never
    # cleared. It stands in for the entry another module leaves in the ledger,
    # which is what an unpinned read of it picks up.
    foreign = shippable_ref("rpo_foreign")
    assert {:error, :backend_down} = Shipper.ship_now(foreign)

    # Pinned to THIS reference, for the reason the `assert_receive` below is:
    # `breaches/0` folds node-global shipper state, so an unpinned read takes
    # whatever another module left in the ledger -- which is what `foreign` is.
    breached? = fn -> Enum.any?(DurabilityTier.breaches(), &(&1.ref == ref)) end

    # The breach is a millisecond past the failed ship.
    assert wait_until(breached?, 1_000, 1)

    assert ExUnit.CaptureLog.capture_log(fn -> :ok = Watchdog.sweep_now() end) =~
             "RPO exceeded"

    # Pinned to THIS reference: the RPO sweep is node-global, so an unpinned
    # receive takes whatever reference another module left pending.
    reported = inspect(ref)
    assert_receive {:rpo_exceeded, %{lag_ms: lag}, %{ref: ^reported}}, 1_000
    assert lag >= 0

    # Latched: a sustained outage alarms once per breach, not once per sweep.
    :ok = Watchdog.sweep_now()
    refute_received {:rpo_exceeded, _, %{ref: ^reported}}

    # And it re-arms once the reference catches up and falls behind again.
    DurabilityBackendStub.set_down(false)
    assert :ok = Shipper.ship_now(ref)
    :ok = Watchdog.sweep_now()
    refute breached?.()

    # ...while the ledger is not empty, which is what makes the pin above an
    # assertion about this reference rather than about the node.
    assert Enum.any?(DurabilityTier.breaches(), &(&1.ref == foreign))
  end

  # A reference with a WAL on disk, ready to be shipped.
  defp shippable_ref(prefix) do
    entity_id = {"#{prefix}_#{uniq()}", "proc", "k1"}
    path = Paths.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, TestHelpers.build_wal_record(0, "change"))
    {:entity, entity_id}
  end

  # A compaction handed out `ms` ago, on the shared Watchdog's own ledger.
  defp age_compaction(entity_id, ms) do
    :sys.replace_state(Watchdog, fn state ->
      update_in(state.compactions[entity_id], fn {started_at, runner_id} ->
        {started_at - ms, runner_id}
      end)
    end)
  end
end

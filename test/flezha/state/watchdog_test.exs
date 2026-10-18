defmodule Flezha.State.WatchdogTest do
  @moduledoc """
  Defect B: the per-Cell output-staleness sweep and the
  compaction-handshake timeout. A live Cell whose persisted seq
  freezes is detected and resynced; a genuinely idle Cell is not
  flagged; a `CompactCell` whose snapshot never returns is timed out
  and recovered, and stays retriable.
  """

  # async: false: writes Application env; a shared on-disk root; the primary Logger.

  use FlezhaTest.Case, fixtures: [:tmp_app_data, :state_tree, :env], async: false

  use Mimic

  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]
  import FlezhaTest.Helper.MimicGlobal

  import Flezha.State.TestHelpers, only: [uniq: 0]
  import FlezhaTest.Helper.NodeTestHelper, only: [override_log_level: 1, wait_until: 3]

  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Relay.ClassRegistry
  alias Flezha.State.DurabilityTier
  alias Flezha.State.DurabilityTier.Shipper
  alias Flezha.State.Paths
  alias Flezha.State.{Recovery, Supervisor, TestHelpers, WalWriter, Watchdog}
  alias Flezha.State.WriterRegistry
  alias FlezhaTest.Helper.DurabilityBackendStub
  alias FlezhaTest.Helper.Telemetry

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
      TestHelpers.override_env(:state_resync_fun, fn cell_id, opts ->
        send(parent, {:resync_called, cell_id, opts})
        {:ok, make_ref()}
      end)
    )

    for event <- [:output_stall, :compaction_timeout, :compaction_trigger, :compaction_gave_up] do
      forward_state_event(event)
    end

    %{header: TestHelpers.stub_header()}
  end

  # Forward `[:flezha, :state, <event>]` to the running test as
  # `{event, measurements, metadata}`, detached when the test ends. Setup runs in
  # the test's own process, so one helper serves a setup watcher and a per-test
  # one alike.
  defp forward_state_event(event) do
    Telemetry.capture([:flezha, :state, event], as: fn _e, meas, meta -> {event, meas, meta} end)
  end

  defp live_writer(suffix, header) do
    pid = TestHelpers.sample_cell_id(suffix <> uniq())
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, "z")
    :ok = WalWriter.append(w, 1, header, "a")
    :ok = WalWriter.append(w, 2, header, "b")
    {pid, w}
  end

  test "a writer that refuses its status read does not take the Watchdog down",
       %{header: header} do
    {cell_id, writer} = live_writer("refused_", header)

    # The Registry clears an entry from the exit it is linked by, and a suspended
    # partition handles none, so the sweep provably still finds this writer after
    # it is gone. `WalWriter.stall_status/1` then answers its declared refusal:
    # a value where a status map belongs, and no exit for anyone to catch.
    partition = Process.whereis(Module.concat(WriterRegistry.name(), "PIDPartition0"))
    :sys.suspend(partition)
    on_exit(fn -> :sys.resume(partition) end)

    ref = Process.monitor(writer)
    :ok = WalWriter.close(writer)
    assert_receive {:DOWN, ^ref, :process, ^writer, _}, 1_000
    assert {:ok, ^writer} = WriterRegistry.lookup(cell_id)

    watchdog = Process.whereis(Watchdog)
    :ok = Watchdog.sweep_now()

    assert Process.whereis(Watchdog) == watchdog,
           "the sweep killed the node's Watchdog on one refused status read"
  end

  test "a live cell whose seq froze is flagged, and the sensor heals nothing",
       %{header: header} do
    {_cell_id, writer} = live_writer("stall_", header)

    # The advance window lapses, then Changes keep arriving (duplicates)
    # without advancing the seq — the stall signature.
    TestHelpers.age_writer(writer, @stall_ms * 2)
    :duplicate = WalWriter.append(writer, 1, header, "a")

    :ok = Watchdog.sweep_now()

    assert_receive {:output_stall, %{stalled_ms: ms, last_seq: 2}, meta}, 1_000
    assert ms >= @stall_ms
    # `relay_instance` is the rebind target the convergence Loop consumes;
    # by the flezha convention it equals the on-disk `relay_class`.
    assert meta.relay_instance == "TestRelay"
    assert meta.relay_class == "TestRelay"

    # The Watchdog emits and stops there. Its former direct resync is the
    # second actor the convergence pipeline replaced with the output-stall
    # ladder; a resync from here would race that ladder under no backoff.
    refute_received {:resync_called, _cell_id, _opts}
  end

  test "the liveness read walks no directory and answers every live writer", %{header: header} do
    # Class directories relays left behind cost the tick's liveness read nothing.
    for i <- 1..200,
        do: File.mkdir_p!(Path.join([Paths.state_root(), "default", "leftover_#{i}"]))

    {cell_id, _writer} = live_writer("live_", header)

    # A writer whose files are not on disk is a live Cell all the same.
    {bare_id, _writer} = live_writer("bare_", header)
    Enum.each(Paths.cell_files(bare_id), &File.rm/1)

    {statuses, calls} = TestHelpers.file_calls(&Watchdog.cell_statuses/0)

    assert calls == [], "the liveness read touched the disk: #{length(calls)} file calls"
    ids = Enum.map(statuses, &elem(&1, 0))
    assert cell_id in ids
    assert bare_id in ids
  end

  test "every completed sweep stamps the sensor freshness mark" do
    ConvergenceSnapshot.observe_sensor(:watchdog, -1, 0)

    :ok = Watchdog.sweep_now()

    assert %{watchdog: {at, period}} = ConvergenceSnapshot.sensors()
    assert period > 0
    assert :erlang.monotonic_time(:millisecond) - at < 1_000
  end

  test "a genuinely idle cell is not flagged", %{header: header} do
    {{_, _, key} = cell_id, writer} = live_writer("idle_", header)

    # No Changes arrive after the window lapses — last_change_seen_at goes
    # stale alongside last_advance_at, so this is idle, not stalled.
    TestHelpers.age_writer(writer, @stall_ms * 2)

    :ok = Watchdog.sweep_now()

    # Scoped to THIS test's cell key. The Watchdog is a singleton and every
    # test in this file uses the relay class "TestRelay", so an unscoped
    # refute — or one scoped only to the relay — matches a sibling test's
    # message and fails whenever the suite interleaves.
    refute_received {:output_stall, _, %{key: ^key}}
    refute_received {:resync_called, ^cell_id, _}
  end

  test "a stalled compaction times out and stays retriable, without resyncing a live cell below give-up" do
    {_, _, key} = cell_id = TestHelpers.sample_cell_id("comp_" <> uniq())

    Watchdog.compaction_started(cell_id, "exec-comp")
    age_compaction(cell_id, @compaction_ms * 2)

    :ok = Watchdog.sweep_now()

    # Scoped to this test's cell KEY, not its relay: the Watchdog is a
    # singleton and every test here shares the class "TestRelay", so a sweep
    # also sees compactions a sibling test left pending.
    assert_receive {:compaction_timeout, %{waited_ms: ms}, %{key: ^key, attempt: 1}}, 1_000
    assert ms >= @compaction_ms
    # Below the give-up threshold a still-live, still-correct Cell is NOT
    # force-resynced — the backoff-gated trigger sweep is the retry.
    refute_received {:resync_called, ^cell_id, _}

    # Entry was cleared on timeout — a second sweep does not re-fire.
    :ok = Watchdog.sweep_now()
    refute_received {:compaction_timeout, _, %{key: ^key}}
  end

  test "repeated compaction failures back off and give up loudly, declaring the give-up" do
    become_loop()
    {_, _, key} = cell_id = TestHelpers.sample_cell_id("giveup_" <> uniq())

    # Drive four timeouts (compaction_started bypasses the backoff-gated trigger
    # sweep, so this reaches the give-up threshold without waiting out cooldowns).
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        for _ <- 1..4 do
          Watchdog.compaction_started(cell_id, "exec-giveup")
          age_compaction(cell_id, @compaction_ms * 2)
          :ok = Watchdog.sweep_now()
        end

        assert_receive {:compaction_gave_up, %{attempts: 4}, %{key: ^key}}, 1_000
      end)

    # "Loudly" is half the contract: the user gets a named alarm.
    assert log =~ "compaction GAVE UP after 4 failed attempts"
    assert log =~ "key=#{key}"

    # The fourth crossing declares the give-up, with the runner the WalWriter
    # status held, to the loop — which plans the resync. The Watchdog itself
    # resyncs nothing.
    assert_receive {:declare, :compaction_gave_up, ^cell_id, %{runner_id: "exec-giveup"}}, 1_000
    refute_received {:resync_called, ^cell_id, _}
  end

  test "a completed compaction is declared as the counter-fact of a give-up" do
    become_loop()
    cell_id = TestHelpers.sample_cell_id("compdone_" <> uniq())

    Watchdog.compaction_started(cell_id, "exec-done")
    Watchdog.compaction_completed(cell_id)
    :ok = Watchdog.sweep_now()

    assert_receive {:declare, :compaction_completed, ^cell_id, %{}}, 1_000
  end

  test "a compaction that completes before the bound never times out" do
    {_, _, key} = cell_id = TestHelpers.sample_cell_id("compok_" <> uniq())

    Watchdog.compaction_started(cell_id, "exec-ok")
    Watchdog.compaction_completed(cell_id)

    :ok = Watchdog.sweep_now()
    # Keyed on this cell: the sweep is global, so a sibling test's in-flight
    # compaction must not be able to answer for this one.
    refute_received {:compaction_timeout, _, %{key: ^key}}
  end

  # An owned writer (runner_id on append) is the trigger sweeps' subject;
  # writers without an owning runner are skipped (no session to ask).
  # Returns the cell KEY: the Watchdog is a singleton whose sweep walks every
  # registered cell, so every assertion here must be scoped to its own — any
  # other test's WAL (a task-journal, say) triggers the same telemetry.
  defp owned_writer(suffix, header, body) do
    {_rc, _op, key} = pid = TestHelpers.sample_cell_id(suffix <> uniq())
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, body, "exec-trig")
    # A triggered cell sits in the shared Watchdog's in-flight map; clear
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

  # Fails as: an idle Cell's compaction keeps going to the runner session that
  # last wrote it, which reconnected since, and gives up for good.
  test "a compaction goes to the runner the relay is bound to now", %{header: header} do
    set_mimic_global_until_exit()
    Application.put_env(:flezha, :default_compact_after_bytes, 16)

    stub(Recovery, :bound_runners, fn -> {:ok, %{"TestRelay" => "exec-live"}} end)

    {key, _w} = owned_writer("bound_", header, "0123456789abcdef")
    :ok = Watchdog.sweep_now()

    assert_receive {:compaction_trigger, _meas, %{key: ^key} = meta}, 1_000
    assert meta.runner == "exec-live"
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

  test "an awake silent steps is flagged like a stream; a sleeping instance is not", %{
    header: header
  } do
    # The sweep is off at its default, so the rule is only observable with the
    # window armed.
    on_exit(TestHelpers.override_env(:input_silence_timeout_ms, @stall_ms))

    steps = "StepsWatch" <> uniq()
    asleep = "SleepingWatch" <> uniq()
    ClassRegistry.put(steps, :steps)
    :ok = ClassRegistry.put_parked(asleep, %{wake_deadline_ms: nil})

    on_exit(fn ->
      ClassRegistry.put_parked(asleep, nil)
      ClassRegistry.delete(steps)
      ClassRegistry.delete(asleep)
    end)

    forward_state_event(:input_silence)

    for class <- [steps, asleep] do
      pid = {class, "task", "seg" <> uniq()}
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, w} = Supervisor.lookup(pid)
      :ok = WalWriter.append(w, 0, header, "z")
      :ok = WalWriter.append(w, 1, header, "a")
      # Silent past the timeout, with output behind it.
      TestHelpers.age_writer(w, @stall_ms * 2)
    end

    on_exit(override_log_level(:warning))

    ExUnit.CaptureLog.capture_log(fn ->
      :ok = Watchdog.sweep_now()

      assert_receive {:input_silence, _, %{relay_class: ^steps}}, 1_000
      refute_received {:input_silence, _, %{relay_class: ^asleep}}
    end)
  end

  test "a checkpoint-ack class's quiet interval does not trip the input-silence sweep", %{
    header: header
  } do
    # The sweep reads "produced output before, silent since". For a
    # checkpoint-ack class that silence is the steady state: it makes nothing
    # durable until the interval closes.
    on_exit(TestHelpers.override_env(:input_silence_timeout_ms, @stall_ms))

    per_input = "PerInputWatch" <> uniq()
    checkpoint = "CheckpointWatch" <> uniq()
    ClassRegistry.put_ack_checkpoint(checkpoint, true)

    on_exit(fn ->
      ClassRegistry.delete(per_input)
      ClassRegistry.delete(checkpoint)
    end)

    forward_state_event(:input_silence)

    for class <- [per_input, checkpoint] do
      pid = {class, "task", "seg" <> uniq()}
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, w} = Supervisor.lookup(pid)
      :ok = WalWriter.append(w, 0, header, "z")
      :ok = WalWriter.append(w, 1, header, "a")
      # Silent past the timeout, with output behind it.
      TestHelpers.age_writer(w, @stall_ms * 2)
    end

    # The sweep reports at warning and the suite's primary level is error, which
    # filters the event before any capture handler sees it.
    on_exit(override_log_level(:warning))

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        :ok = Watchdog.sweep_now()

        # The control proves the detector is armed and this signature trips it...
        assert_receive {:input_silence, _, %{relay_class: ^per_input}}, 1_000
        # ...and that the checkpoint-ack class is exempt, not merely unobserved.
        refute_received {:input_silence, _, %{relay_class: ^checkpoint}}
      end)

    # The positive control alarms by name; the exempt class never appears.
    assert log =~ ~s(input silent for relay="#{per_input}")
    # The line states what was seen, and claims nothing about a dead feed.
    assert log =~ "having produced output earlier"
    refute log =~ "data-flow stop"
    refute log =~ checkpoint
  end

  test "the sweep does nothing at its default window", %{header: header} do
    class = "SilentDefault" <> uniq()
    on_exit(fn -> ClassRegistry.delete(class) end)

    forward_state_event(:input_silence)

    pid = {class, "task", "seg" <> uniq()}
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, "z")
    :ok = WalWriter.append(w, 1, header, "a")
    # An hour of silence with output behind it: the old window's firing shape,
    # ten times over.
    TestHelpers.age_writer(w, 3_600_000)

    # Lowered so the refute is a real one: at the suite's primary level the line
    # could not appear whatever the sweep did.
    on_exit(override_log_level(:warning))

    log = ExUnit.CaptureLog.capture_log(fn -> :ok = Watchdog.sweep_now() end)

    refute_received {:input_silence, _, %{relay_class: ^class}}
    refute log =~ "input silent"
  end

  test "the latch reports once per episode and clears when input resumes", %{header: header} do
    on_exit(TestHelpers.override_env(:input_silence_timeout_ms, @stall_ms))

    class = "SilenceLatch" <> uniq()
    on_exit(fn -> ClassRegistry.delete(class) end)

    forward_state_event(:input_silence)

    pid = {class, "task", "seg" <> uniq()}
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, "z")
    :ok = WalWriter.append(w, 1, header, "a")
    TestHelpers.age_writer(w, @stall_ms * 2)

    :ok = Watchdog.sweep_now()
    assert_receive {:input_silence, _, %{relay_class: ^class}}, 1_000

    # Still silent: the latch holds, so the episode reports once.
    :ok = Watchdog.sweep_now()
    refute_received {:input_silence, _, %{relay_class: ^class}}

    # Input resumes, and a later silence is reported afresh.
    :ok = WalWriter.append(w, 2, header, "b")
    :ok = Watchdog.sweep_now()

    TestHelpers.age_writer(w, @stall_ms * 2)
    :ok = Watchdog.sweep_now()
    assert_receive {:input_silence, _, %{relay_class: ^class}}, 1_000
  end

  test "a relay is silent only when none of its cells that produced output hears a Change", %{
    header: header
  } do
    # A config cell that produced once and has nothing new to hear sits beside
    # cells fed every second: the relay is not silent, and flagging it would
    # latch the relay and hide the real outage that follows.
    on_exit(TestHelpers.override_env(:input_silence_timeout_ms, @stall_ms))

    class = "QuietCell" <> uniq()
    on_exit(fn -> ClassRegistry.delete(class) end)
    forward_state_event(:input_silence)

    [quiet, live] =
      for task <- ["config", "ohlc"] do
        pid = {class, task, "seg" <> uniq()}
        {:ok, _} = Supervisor.find_or_start(pid)
        {:ok, w} = Supervisor.lookup(pid)
        :ok = WalWriter.append(w, 0, header, "z")
        :ok = WalWriter.append(w, 1, header, "a")
        w
      end

    TestHelpers.age_writer(quiet, @stall_ms * 2)
    :ok = Watchdog.sweep_now()
    refute_received {:input_silence, _, %{relay_class: ^class}}

    TestHelpers.age_writer(live, @stall_ms * 2)
    on_exit(override_log_level(:warning))

    ExUnit.CaptureLog.capture_log(fn ->
      :ok = Watchdog.sweep_now()
      assert_receive {:input_silence, _, %{relay_class: ^class}}, 1_000
    end)
  end

  test "a remote-durability RPO breach alarms once and clears when it catches up" do
    forward_state_event(:rpo_exceeded)

    DurabilityBackendStub.install()

    Application.put_env(
      :flezha,
      :durability_tier,
      Application.get_env(:flezha, :durability_tier) |> Keyword.put(:rpo_target_ms, 1)
    )

    on_exit(&DurabilityBackendStub.uninstall/0)

    {:ok, _pid} = start_supervised(Flezha.State.DurabilityTier.Shipper)

    ref = shippable_ref("rpo")
    DurabilityBackendStub.set_down(true)

    # A second reference, breached through the same production path and never
    # cleared. It stands in for the entry another module leaves in the ledger,
    # which is what an unpinned read of it picks up.
    foreign = shippable_ref("rpo_foreign")

    # Pinned to one reference, for the reason the `assert_receive` below is:
    # `breaches/0` folds node-global shipper state, so an unpinned read takes
    # whatever another module left in the ledger -- which is what `foreign` is.
    breached? = fn r -> Enum.any?(DurabilityTier.breaches(), &(&1.ref == r)) end

    # Both breaches are a millisecond past their failed ship, and both must be
    # breached before the captured sweep: one it has not seen yet is flagged by
    # the next sweep, outside the capture. The capture also spans the window,
    # where the Watchdog's own timer may sweep first.
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, :backend_down} = Shipper.ship_now(ref)
        assert {:error, :backend_down} = Shipper.ship_now(foreign)
        assert wait_until(fn -> breached?.(ref) and breached?.(foreign) end, 1_000, 1)
        :ok = Watchdog.sweep_now()
      end)

    for r <- [ref, foreign], do: assert(log =~ "RPO exceeded for #{inspect(r)}")

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
    refute breached?.(ref)

    # ...while the ledger is not empty, which is what makes the pin above an
    # assertion about this reference rather than about the node.
    assert breached?.(foreign)
  end

  # A reference with a WAL on disk, ready to be shipped.
  defp shippable_ref(prefix) do
    cell_id = {"#{prefix}_#{uniq()}", "task", "k1"}
    path = Paths.wal_path(cell_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, TestHelpers.build_wal_record(0, "change"))
    {:cell, cell_id}
  end

  # A compaction handed out `ms` ago, on the shared Watchdog's own ledger.
  defp age_compaction(cell_id, ms) do
    :sys.replace_state(Watchdog, fn state ->
      update_in(state.compactions[cell_id], fn {started_at, runner_id} ->
        {started_at - ms, runner_id}
      end)
    end)
  end
end

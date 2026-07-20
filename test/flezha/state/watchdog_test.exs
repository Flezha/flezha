defmodule Flezha.State.WatchdogTest do
  @moduledoc """
  Defect B: the per-Segment output-staleness sweep and the
  compaction-handshake timeout. A live Segment whose persisted seq
  freezes is detected and resynced; a genuinely idle Segment is not
  flagged; a `CompactSegment` whose snapshot never returns is timed out
  and recovered, and stays retriable.
  """

  use ExUnit.Case, async: false

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{Supervisor, TestHelpers, Watchdog, WalWriter}

  @stall_ms 50
  @compaction_ms 50

  setup do
    base = TestHelpers.tmp_app_data_dir()

    for spec <- [
          {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
          Supervisor,
          Flezha.State.FsyncBatcher,
          # The reconciler drives sweeps in production; these tests drive them
          # explicitly via sweep_now/0 (the Watchdog holds no self-timer).
          Watchdog
        ] do
      case start_supervised(spec) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    Application.put_env(:flezha, :default_output_stall_timeout_ms, @stall_ms)
    Application.put_env(:flezha, :compaction_timeout_ms, @compaction_ms)

    parent = self()

    Application.put_env(:flezha, :state_resync_fun, fn segment_id, opts ->
      send(parent, {:resync_called, segment_id, opts})
      {:ok, make_ref()}
    end)

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

    prev_bytes = Application.get_env(:flezha, :default_compact_after_bytes)
    prev_age = Application.get_env(:flezha, :compact_after_age_ms)

    on_exit(fn ->
      :telemetry.detach(out_id)
      :telemetry.detach(comp_id)
      :telemetry.detach(trig_id)
      :telemetry.detach(give_id)
      Application.delete_env(:flezha, :state_resync_fun)
      Application.delete_env(:flezha, :default_output_stall_timeout_ms)
      Application.delete_env(:flezha, :compaction_timeout_ms)
      Application.put_env(:flezha, :default_compact_after_bytes, prev_bytes)
      Application.put_env(:flezha, :compact_after_age_ms, prev_age)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{header: TestHelpers.stub_header()}
  end

  defp live_writer(suffix, header) do
    pid = TestHelpers.sample_segment_id(suffix <> uniq())
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, "z")
    :ok = WalWriter.append(w, 1, header, "a")
    :ok = WalWriter.append(w, 2, header, "b")
    {pid, w}
  end

  test "a live segment whose seq froze is flagged and resynced", %{header: header} do
    {segment_id, writer} = live_writer("stall_", header)

    # Let the advance window lapse, then keep Changes arriving (duplicates)
    # without advancing the seq — the stall signature.
    Process.sleep(@stall_ms * 2)
    :duplicate = WalWriter.append(writer, 1, header, "a")

    :ok = Watchdog.sweep_now()

    assert_receive {:output_stall, %{stalled_ms: ms, last_seq: 2}, meta}, 1_000
    assert ms >= @stall_ms
    # `operative_instance` is the rebind target the convergence Loop consumes;
    # by the flezha convention it equals the on-disk `operative_class`.
    assert meta.operative_instance == "TestOperative"
    assert meta.operative_class == "TestOperative"
    assert_receive {:resync_called, ^segment_id, opts}, 1_000
    assert opts[:reason] == :output_stall
  end

  test "a genuinely idle segment is not flagged", %{header: header} do
    {{_, _, key} = segment_id, _writer} = live_writer("idle_", header)

    # No Changes arrive after the window lapses — last_change_seen_at goes
    # stale alongside last_advance_at, so this is idle, not stalled.
    Process.sleep(@stall_ms * 2)

    :ok = Watchdog.sweep_now()

    # Scoped to THIS test's segment key. The Watchdog is a singleton and every
    # test in this file uses the operative class "TestOperative", so an unscoped
    # refute — or one scoped only to the operative — matches a sibling test's
    # message and fails whenever the suite interleaves.
    refute_receive {:output_stall, _, %{key: ^key}}, 200
    refute_receive {:resync_called, ^segment_id, _}, 200
  end

  test "a stalled compaction times out and stays retriable, without resyncing a live segment below give-up" do
    {_, _, key} = segment_id = TestHelpers.sample_segment_id("comp_" <> uniq())

    Watchdog.compaction_started(segment_id, "exec-comp")
    Process.sleep(@compaction_ms * 2)

    :ok = Watchdog.sweep_now()

    # Scoped to this test's segment KEY, not its operative: the Watchdog is a
    # singleton and every test here shares the class "TestOperative", so a sweep
    # also sees compactions a sibling test left pending.
    assert_receive {:compaction_timeout, %{waited_ms: ms}, %{key: ^key, attempt: 1}}, 1_000
    assert ms >= @compaction_ms
    # Below the give-up threshold a still-live, still-correct Segment is NOT
    # force-resynced — the backoff-gated trigger sweep is the retry.
    refute_receive {:resync_called, ^segment_id, _}, 200

    # Entry was cleared on timeout — a second sweep does not re-fire.
    :ok = Watchdog.sweep_now()
    refute_receive {:compaction_timeout, _, %{key: ^key}}, 200
  end

  test "repeated compaction failures back off and give up loudly with a real resync" do
    {_, _, key} = segment_id = TestHelpers.sample_segment_id("giveup_" <> uniq())

    # Drive four timeouts (compaction_started bypasses the backoff-gated trigger
    # sweep, so this reaches the give-up threshold without waiting out cooldowns).
    for _ <- 1..4 do
      Watchdog.compaction_started(segment_id, "exec-giveup")
      Process.sleep(@compaction_ms * 2)
      :ok = Watchdog.sweep_now()
    end

    # The fourth crossing alarms loudly and dispatches a real resync carrying the
    # runner_id the WalWriter status held — the fix for the old silent no-op.
    assert_receive {:compaction_gave_up, %{attempts: 4}, %{key: ^key}}, 1_000
    assert_receive {:resync_called, ^segment_id, opts}, 1_000
    assert opts[:reason] == :compaction_gave_up
    assert opts[:runner_id] == "exec-giveup"
  end

  test "a compaction that completes before the bound never times out" do
    segment_id = TestHelpers.sample_segment_id("compok_" <> uniq())

    Watchdog.compaction_started(segment_id, "exec-ok")
    Watchdog.compaction_completed(segment_id)
    Process.sleep(@compaction_ms * 2)

    :ok = Watchdog.sweep_now()
    refute_receive {:compaction_timeout, _, _}, 200
  end

  # An owned writer (runner_id on append) is the trigger sweeps' subject;
  # writers without an owning runner are skipped (no session to ask).
  # Returns the segment KEY: the Watchdog is a singleton whose sweep walks every
  # registered segment, so every assertion here must be scoped to its own — any
  # other test's WAL (an op-journal, say) triggers the same telemetry.
  defp owned_writer(suffix, header, body) do
    {_rc, _op, key} = pid = TestHelpers.sample_segment_id(suffix <> uniq())
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, body, "exec-trig")
    # A triggered segment sits in the shared Watchdog's in-flight map; clear
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
    refute_receive {:compaction_trigger, _, %{key: ^key}}, 200
  end

  test "a small-but-old WAL triggers an age compaction", %{header: header} do
    Application.put_env(:flezha, :compact_after_age_ms, 30)
    {key, _w} = owned_writer("age_", header, "x")

    Process.sleep(60)
    :ok = Watchdog.sweep_now()

    assert_receive {:compaction_trigger, _meas, %{key: ^key} = meta}, 1_000
    assert meta.trigger == :age
  end

  test "age trigger is disabled at 0", %{header: header} do
    Application.put_env(:flezha, :compact_after_age_ms, 0)
    {key, _w} = owned_writer("age0_", header, "x")

    Process.sleep(60)
    :ok = Watchdog.sweep_now()

    refute_receive {:compaction_trigger, _, %{key: ^key}}, 200
  end

  test "size and age never double-enqueue across consecutive ticks", %{header: header} do
    Application.put_env(:flezha, :default_compact_after_bytes, 16)
    Application.put_env(:flezha, :compact_after_age_ms, 30)
    {key, _w} = owned_writer("both_", header, "0123456789abcdef")

    Process.sleep(60)
    :ok = Watchdog.sweep_now()

    # Both conditions hold, but the shared in-flight guard yields ONE enqueue.
    assert_receive {:compaction_trigger, _meas, %{key: ^key, trigger: :size}}, 1_000
    refute_receive {:compaction_trigger, _, %{key: ^key}}, 200

    :ok = Watchdog.sweep_now()
    refute_receive {:compaction_trigger, _, %{key: ^key}}, 200
  end

  test "a bounded class is not flagged for output stall or catastrophic dataflow", %{
    header: header
  } do
    class = "BoundedWatch" <> uniq()
    ClassRegistry.put(class, :bounded)
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

    pid = {class, "op", "seg" <> uniq()}
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 0, header, "z")
    :ok = WalWriter.append(w, 1, header, "a")
    :ok = WalWriter.append(w, 2, header, "b")

    # The same stall signature that flags an unbounded class: window lapses,
    # then a duplicate Change keeps it "not idle" with a frozen seq.
    Process.sleep(@stall_ms * 2)
    :duplicate = WalWriter.append(w, 1, header, "a")

    :ok = Watchdog.sweep_now()

    # Scoped to THIS test's class and segment. The Watchdog is a singleton whose
    # sweep walks every registered segment, so an unscoped refute also matches a
    # sibling test's resync (a compaction timeout on its own WAL, say).
    refute_receive {:output_stall, _, %{operative_class: ^class}}, 200
    refute_receive {:resync_called, ^pid, _}, 200
    refute_receive {:catastrophic, _, %{operative_class: ^class}}, 200
  end

  test "a checkpoint-ack class's quiet interval is not a dead data-flow", %{header: header} do
    # The dataflow-stop detector reads "produced output before, silent since"
    # as a dead feed. For a checkpoint-ack class that silence is the steady
    # state: it makes nothing durable until the interval closes.
    prev = Application.get_env(:flezha, :catastrophic_dataflow_timeout_ms)
    Application.put_env(:flezha, :catastrophic_dataflow_timeout_ms, @stall_ms)

    per_input = "PerInputWatch" <> uniq()
    checkpoint = "CheckpointWatch" <> uniq()
    ClassRegistry.put_checkpoint_ack(checkpoint, true)

    on_exit(fn ->
      Application.put_env(:flezha, :catastrophic_dataflow_timeout_ms, prev)
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
      pid = {class, "op", "seg" <> uniq()}
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, w} = Supervisor.lookup(pid)
      :ok = WalWriter.append(w, 0, header, "z")
      :ok = WalWriter.append(w, 1, header, "a")
    end

    # Both go silent past the timeout, with output behind them.
    Process.sleep(@stall_ms * 2)
    :ok = Watchdog.sweep_now()

    # The control proves the detector is armed and this signature trips it...
    assert_receive {:catastrophic, ^per_input}, 1_000
    # ...and that the checkpoint-ack class is exempt, not merely unobserved.
    refute_receive {:catastrophic, ^checkpoint}, 200
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end

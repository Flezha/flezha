defmodule FlezhaTest.System.CompactionE2ETest do
  @moduledoc """
  End-to-end proof that snapshot compaction actually runs — the producing half
  that `entity-compaction-is-a-stub` makes real.

  Boots a real Rust runner, registers a changeful `store_input` operative,
  drives inputs to grow its per-Entity WAL, then arms a low
  `compact_after_age_ms` and forces a Watchdog sweep. The sweep ships
  `CompactEntity`; the SDK must answer with an `entity_state_snapshot` frame,
  and the controller must write a snapshot FILE and truncate the WAL behind it.

  The gap this closes was measured on a live run BEFORE the fix (real Rust
  runner, `compact_after_age_ms = 2000`, 40 s window): **9 `compaction_trigger`,
  8 `compaction_timeout`, 0 `compaction_completed`, 0 snapshot files, WAL flat at
  48 728 bytes across nine attempts** — the SDK stub answered `snapshot_seq: 0`
  and shipped no frame. This test asserts the opposite on every axis: a snapshot
  file appears, the WAL strictly shrinks, and `compaction_completed` fires. It
  FAILS against a stub SDK, which is the point (no controller-only test could,
  because they stub `:state_resync_fun` / `send_control` and never run an SDK).

  Tagged `:system` — boots `cargo run` and a real subprocess. Run via:

      mix test test/system/compaction_e2e_test.exs
      mix test --include system test/system/compaction_e2e_test.exs
  """

  use FlezhaTest.SystemCase, capability: :state_persistence

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.TestHelpers
  alias Flezha.State.Watchdog
  alias FlezhaTest.Helper.CompactionAssertions, as: CA
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "compaction_e2e"
  @factory "store_input"
  @procedure "store_input_procedure"

  # One scenario, one runner.
  setup do
    prev_age = Application.get_env(:flezha, :compact_after_age_ms)
    prev_timeout = Application.get_env(:flezha, :compaction_timeout_ms)

    on_exit(fn ->
      Application.put_env(:flezha, :compact_after_age_ms, prev_age)
      Application.put_env(:flezha, :compaction_timeout_ms, prev_timeout)

      try do
        OperativeManager.stop_all_operatives(@namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  test "a triggered compaction produces a durable snapshot file and shrinks the WAL" do
    {port, ce_id} = NodeTestHelper.start_runner_port(@namespace)

    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "compaction_e2e_#{System.unique_integer([:positive])}"
    entity_id = {operative_id, @procedure, ""}

    builder = %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{},
      # No periodic capture — the ONLY path to a snapshot file is the compaction
      # this test triggers, so an appearing file cannot come from anywhere else.
      opts: [snapshot_interval: 0]
    }

    assert {:ok, info} = OperativeManager.start_operative(@namespace, builder)
    assert info.runner_id == ce_id

    # Grow the WAL with changeful inputs (each PushText is one Change record).
    for i <- 1..50 do
      OperativeManager.send_io(operative_id, @namespace, "line-#{i}-#{operative_id}", %{
        "input_type" => "2"
      })
    end

    wal_before = CA.wait_for_wal_bytes(entity_id, 5_000)
    assert wal_before > 0, "precondition: the Entity's WAL must hold records before compaction"
    assert CA.snapshot_files(entity_id) == [], "precondition: no snapshot file exists yet"

    parent = self()
    done_id = {__MODULE__, :compaction_completed, System.unique_integer([:positive])}

    :telemetry.attach(
      done_id,
      [:flezha, :state, :compaction_completed],
      fn _e, _meas, meta, _ -> send(parent, {:compaction_completed, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(done_id) end)

    # Arm the age trigger low and force a sweep with the Entity's oldest
    # un-snapshotted record already past it.
    Application.put_env(:flezha, :compact_after_age_ms, 1)
    Application.put_env(:flezha, :compaction_timeout_ms, 30_000)
    {:ok, writer} = Flezha.State.Supervisor.lookup(entity_id)
    TestHelpers.age_writer(writer, 50)
    :ok = Watchdog.sweep_now()

    assert_receive {:compaction_completed, %{operative: ^operative_id, procedure: @procedure}},
                   10_000,
                   "the SDK never produced a snapshot (a stub answers snapshot_seq: 0 and ships no frame)"

    # The snapshot file appears and the WAL is truncated behind it.
    assert CA.wait_for_snapshot_file(entity_id, 10_000),
           "no snapshot file was written under the Entity's state dir"

    wal_after = CA.wait_for_wal_shrink(entity_id, wal_before, 10_000)

    assert wal_after < wal_before,
           "WAL did not shrink after compaction (before=#{wal_before}, after=#{wal_after})"
  end
end

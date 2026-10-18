defmodule FlezhaTest.System.CompactionE2ETest do
  @moduledoc """
  End-to-end proof that snapshot compaction actually runs — the producing half
  that `cell-compaction-is-a-stub` makes real.

  Boots a real Rust runner, registers a changeful `store_input` relay,
  drives inputs to grow its per-Cell WAL, then arms a low
  `compact_after_age_ms` and forces a Watchdog sweep. The sweep ships
  `CompactCell`; the SDK must answer with a `cell_state_snapshot` frame,
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

  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.TestHelpers
  alias Flezha.State.Watchdog
  alias FlezhaTest.Helper.CompactionAssertions, as: CA
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  @namespace "compaction_e2e"
  @factory "store_input"
  @tasklet "store_input_tasklet"

  # One scenario, one runner.
  setup do
    prev_age = Application.get_env(:flezha, :compact_after_age_ms)
    prev_timeout = Application.get_env(:flezha, :compaction_timeout_ms)

    on_exit(fn ->
      Application.put_env(:flezha, :compact_after_age_ms, prev_age)
      Application.put_env(:flezha, :compaction_timeout_ms, prev_timeout)

      try do
        RelayManager.stop_all_relays(@namespace)
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

    relay_id = "compaction_e2e_#{System.unique_integer([:positive])}"
    cell_id = {relay_id, @tasklet, ""}

    builder = %Builder{
      id: relay_id,
      factory_id: @factory,
      # The controller takes no periodic snapshot: the ONLY path to a snapshot
      # file is the compaction this test triggers.
      init_values: %{}
    }

    assert {:ok, info} = RelayManager.start_relay(@namespace, builder)
    assert info.runner_id == ce_id

    # Grow the WAL with changeful inputs (each PushText is one Change record).
    for i <- 1..50 do
      RelayManager.send_io(relay_id, @namespace, "line-#{i}-#{relay_id}", %{
        "input_type" => "2"
      })
    end

    wal_before = CA.wait_for_wal_bytes(cell_id, 5_000)
    assert wal_before > 0, "precondition: the Cell's WAL must hold records before compaction"
    assert CA.snapshot_files(cell_id) == [], "precondition: no snapshot file exists yet"

    Telemetry.capture([:flezha, :state, :compaction_completed],
      as: fn _e, _meas, meta -> {:compaction_completed, meta} end
    )

    # Arm the age trigger low and force a sweep with the Cell's oldest
    # un-snapshotted record already past it.
    Application.put_env(:flezha, :compact_after_age_ms, 1)
    Application.put_env(:flezha, :compaction_timeout_ms, 30_000)
    {:ok, writer} = Flezha.State.Supervisor.lookup(cell_id)
    TestHelpers.age_writer(writer, 50)
    :ok = Watchdog.sweep_now()

    assert_receive {:compaction_completed, %{relay: ^relay_id, tasklet: @tasklet}},
                   10_000,
                   "the SDK never produced a snapshot (a stub answers snapshot_seq: 0 and ships no frame)"

    # The snapshot file appears and the WAL is truncated behind it.
    assert CA.wait_for_snapshot_file(cell_id, 10_000),
           "no snapshot file was written under the Cell's state dir"

    wal_after = CA.wait_for_wal_shrink(cell_id, wal_before, 10_000)

    assert wal_after < wal_before,
           "WAL did not shrink after compaction (before=#{wal_before}, after=#{wal_after})"
  end
end

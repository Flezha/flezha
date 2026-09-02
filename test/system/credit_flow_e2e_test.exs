defmodule FlezhaTest.System.CreditFlowE2ETest do
  @moduledoc """
  End-to-end proof for `credit-grant-flow-control` against live runners:
  a deliberately tiny credit window is overdriven by a burst of
  state-changing inputs, so the SDK exhausts its window and parks between
  the controller's fsync-minted grants — and every Change still lands
  (park, never drop): zero sheds, zero overruns, zero gaps, with the
  park visible in the SDK's `credit_parked` liveness self-report.

  Runs the same scenario on both the Rust and the Java runner
  (identical sender-window semantics). Tagged `:system` (boots real
  subprocesses); run with
  `mix test --include system test/system/credit_flow_e2e_test.exs`.
  The Java cell needs the committed fat jar — refresh with
  `scripts/build-dist.sh` after any SDK change.
  """

  use FlezhaTest.SystemCase, capability: :state_persistence

  import FlezhaTest.Helper.CreditTestHelper
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, ensure_port_dead: 1]

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Metrics
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "credit_flow_e2e"
  @factory "store_input"
  @procedure "store_input_procedure"

  # A ~300 KiB burst against a 64 KiB window with a 1 s fsync tick: the
  # window exhausts within the first second and refills at ~64 KiB/s, so
  # the SDK must park repeatedly while the whole burst still lands.
  @window_bytes 65_536
  @input_count 300
  @input_bytes 1_024
  @slow_fsync_ms 1_000

  @credit_fields [:credit_active, :credit_ledger_level, :credit_overrun_total, :hb_credit_parked]

  # One scenario, one runner, under a deliberately slowed fsync path.
  setup do
    prev_fsync = Application.get_env(:flezha, :agent_fsync_interval_ms)

    # Deliberately slowed fsync path: the batcher reads the interval at
    # init, so restart it under its supervisor to pick the value up.
    Application.put_env(:flezha, :agent_fsync_interval_ms, @slow_fsync_ms)
    restart_fsync_batcher()

    on_exit(fn ->
      Application.delete_env(:flezha, :credit_window_bytes)

      if prev_fsync,
        do: Application.put_env(:flezha, :agent_fsync_interval_ms, prev_fsync),
        else: Application.delete_env(:flezha, :agent_fsync_interval_ms)

      restart_fsync_batcher()

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

  @tag :credit_flow_e2e
  test "an overdriven entity parks on credit, resumes on grants, zero sheds and zero drops" do
    run_credit_scenario()
  end

  defp run_credit_scenario do
    operative_id = "credit_e2e_#{System.unique_integer([:positive])}"

    # The stream window resolves per-Operative from the first Change's class.
    Application.put_env(:flezha, :credit_window_bytes, %{operative_id => @window_bytes})

    {port, ce_id} =
      NodeTestHelper.start_runner_port(@namespace,
        transport: :uds,
        # Fast heartbeat so the credit_parked self-report lands within the
        # assertion window instead of the default 10s cadence.
        env: [{~c"FLEZHA_DATA_HEARTBEAT_MS", ~c"300"}]
      )

    on_exit(fn -> ensure_port_dead(port) end)

    builder = %Builder{id: operative_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, info} = OperativeManager.start_operative(@namespace, builder)
    assert info.runner_id == ce_id

    # Prime with ONE input so the first state frame runs the credit
    # handshake, and burst only once the session is credit-speaking —
    # otherwise the burst races the handshake and flows un-metered.
    OperativeManager.send_io(operative_id, @namespace, "0:prime", %{"input_type" => "2"})

    # The priming Change proves state frames flow at all (separating a
    # broken persistence path from a broken handshake in the failure mode).
    assert wait_for_entity(operative_id, @procedure, 30_000)

    credit_row = wait_until(fn -> active_credit_row(ce_id) end, 30_000)
    assert credit_row, "credit handshake never completed: #{credit_rows(ce_id)}"

    # Overdrive: ~300 KiB of state changes against a 64 KiB window on a 1 s
    # fsync tick. The SDK can only progress as fast as fsync mints credit,
    # so it parks repeatedly.
    payload = String.duplicate("x", @input_bytes)

    for i <- 1..@input_count do
      OperativeManager.send_io(operative_id, @namespace, "#{i}:#{payload}", %{
        "input_type" => "2"
      })
    end

    # Zero drops: every Change lands in seq order despite the parking
    # (seq 0 was the priming input).
    entity_id = wait_for_entity(operative_id, @procedure, 30_000)
    writer = wait_for_writer(entity_id, 10_000)
    await_seq(writer, @input_count, 60_000)

    # Zero sheds / zero overruns: the sender honoured its window end-to-end.
    metrics = Metrics.for_entity(entity_id)
    assert Metrics.get(metrics, :wal_shed_total) == 0

    # The verdict is read from the channel the handshake settled on, by name:
    # its counters array is one per channel process, and a socket close after
    # the burst (a legitimate re-attach) clears `credit_active` on it, so a
    # fresh search for a credit-speaking row would find none and blame the
    # handshake. `credit_overrun_total` is monotonic for the channel's life.
    row = counters_row(ce_id, credit_row.name)
    assert row, "#{inspect(credit_row.name)} left the liveness registry: #{credit_rows(ce_id)}"

    assert row.credit_overrun_total == 0,
           "credit overrun on the burst's channel: #{credit_rows(ce_id)}"

    # Park-then-resume is visible: the SDK's credit_parked self-report
    # (data_heartbeat metadata, 300ms cadence here) advanced while the
    # window was exhausted.
    assert wait_until(fn -> parked_counter(ce_id) > 0 end, 15_000),
           "SDK never reported a credit park despite the overdriven window: #{credit_rows(ce_id)}"
  end

  # The runner's channel rows, so a red names its cause: the flag gone (a
  # close), a ledger overrun, or a park never reported.
  defp credit_rows(runner_id) do
    Enum.map_join(channel_rows(runner_id), "; ", fn row ->
      "#{inspect(row.name)} " <>
        Enum.map_join(@credit_fields, " ", &"#{&1}=#{Map.fetch!(row, &1)}")
    end)
  end
end

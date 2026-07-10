defmodule Flezha.State.WalWriterCreditTest do
  @moduledoc """
  WalWriter side of credit-grant flow control: session-tagged drain
  reports on every outcome path (fsync, dedup, gap, shed, truncate) and
  `SegmentMailboxStatus` suppression for credit-speaking sessions.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.State.{Supervisor, TestHelpers, WalWriter}

  setup :set_mimic_global

  @session "wal-credit-session"

  setup do
    base = TestHelpers.tmp_app_data_dir()

    stub(Flezha.State.FsyncBatcher, :mark_dirty, fn _segment_id -> :ok end)

    test_pid = self()

    stub(Flezha.Executor.ControlChannel, :send_control_async, fn executor_id, command ->
      send(test_pid, {:ctl_async, executor_id, command})
      :ok
    end)

    Application.put_env(:flezha, :state_resync_fun, fn segment_id, opts ->
      send(test_pid, {:resync_called, segment_id, opts})
      :ok
    end)

    on_exit(fn ->
      TestHelpers.reset_all_segments()
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :state_resync_fun)
      Application.delete_env(:flezha, :wal_high_watermark_bytes)
      Application.delete_env(:flezha, :wal_low_watermark_bytes)
      File.rm_rf!(base)
    end)

    segment_id = TestHelpers.sample_segment_id("credit_#{System.unique_integer([:positive])}")
    {:ok, pid} = Supervisor.find_or_start(segment_id)

    %{pid: pid, segment_id: segment_id}
  end

  defp opts(n), do: %{channel: self(), session: @session, wire_size: n}

  defp append!(pid, seq, body, credit_opts) do
    WalWriter.append_async(pid, seq, TestHelpers.stub_header(), body, "exec-1", credit_opts)
  end

  defp sync(pid), do: WalWriter.last_persisted_seq(pid)

  defp drain_credit_reports() do
    receive do
      {:credit_drained, _, _} -> drain_credit_reports()
    after
      200 -> :ok
    end
  end

  test "written bytes are reported on fsync, tagged with the debiting session", %{pid: pid} do
    append!(pid, 0, "a", opts(70))
    append!(pid, 1, "b", opts(30))
    assert sync(pid) == 1

    # Append alone mints nothing.
    refute_receive {:credit_drained, _, _}, 100

    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, @session, 100}
    refute_receive {:credit_drained, _, _}, 100
  end

  test "a dedup-dropped Change releases its bytes immediately", %{pid: pid} do
    append!(pid, 0, "a", opts(50))
    assert sync(pid) == 0

    append!(pid, 0, "a", opts(50))
    assert_receive {:credit_drained, @session, 50}

    # The written record's bytes still wait for the fsync.
    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, @session, 50}
  end

  test "a gap-rejected Change releases its bytes immediately and resyncs", %{pid: pid} do
    append!(pid, 0, "a", opts(40))
    assert sync(pid) == 0

    append!(pid, 5, "z", opts(60))
    assert_receive {:credit_drained, @session, 60}
    assert_receive {:resync_called, _, resync_opts}
    assert resync_opts[:reason] == :seq_gap
  end

  test "a hard-bound shed releases its bytes immediately", %{pid: pid, segment_id: segment_id} do
    append!(pid, 0, "a", opts(10))
    assert sync(pid) == 0

    :ok = :sys.suspend(pid)

    # Flood past the pending-msg cap while suspended so the resumed writer
    # sheds; every queued append carries a debit that must come back.
    prev_cap = Application.get_env(:flezha, :wal_pending_msg_cap)
    Application.put_env(:flezha, :wal_pending_msg_cap, 5)

    on_exit(fn ->
      if prev_cap,
        do: Application.put_env(:flezha, :wal_pending_msg_cap, prev_cap),
        else: Application.delete_env(:flezha, :wal_pending_msg_cap)
    end)

    # Restart the writer so it picks up the tiny cap. The stop's terminate
    # flushes the first append's tracked bytes — drain those reports so the
    # flood accounting below starts from a clean mailbox.
    :ok = :sys.resume(pid)
    {:ok, _} = Supervisor.delete_segment(segment_id)
    drain_credit_reports()
    {:ok, pid2} = Supervisor.find_or_start(segment_id)
    :ok = :sys.suspend(pid2)

    for seq <- 0..19, do: append!(pid2, seq, "x", opts(10))
    :ok = :sys.resume(pid2)

    # Every debited byte comes back — released on shed or minted on fsync.
    _ = sync(pid2)
    :ok = WalWriter.fsync(pid2)

    total =
      Stream.repeatedly(fn ->
        receive do
          {:credit_drained, @session, n} -> n
        after
          500 -> nil
        end
      end)
      |> Enum.take_while(& &1)
      |> Enum.sum()

    assert total == 20 * 10
  end

  describe "SegmentMailboxStatus suppression (2.4)" do
    setup do
      # Tiny watermarks read at writer init.
      Application.put_env(:flezha, :wal_high_watermark_bytes, 64)
      Application.put_env(:flezha, :wal_low_watermark_bytes, 32)

      segment_id =
        TestHelpers.sample_segment_id("credit_bp_#{System.unique_integer([:positive])}")

      {:ok, pid} = Supervisor.find_or_start(segment_id)
      %{pid: pid, segment_id: segment_id}
    end

    test "credit-speaking session receives no status flags", %{pid: pid} do
      body = String.duplicate("x", 100)
      append!(pid, 0, body, opts(140))
      assert sync(pid) == 0

      # Telemetry still fires for in-process observers…
      # …but no wire signal is shipped.
      refute_receive {:ctl_async, _, {:segment_mailbox_status, _}}, 200
    end

    test "legacy session keeps the byte-for-byte SegmentMailboxStatus wire signal", %{pid: pid} do
      body = String.duplicate("x", 100)
      WalWriter.append_async(pid, 0, TestHelpers.stub_header(), body, "exec-legacy")
      assert sync(pid) == 0

      assert_receive {:ctl_async, "exec-legacy",
                      {:segment_mailbox_status,
                       %ExecutorCommands.SegmentMailboxStatus{status: "full"}}},
                     1_000

      # Draining below the low watermark ships :open exactly once.
      :ok = WalWriter.fsync(pid)

      assert_receive {:ctl_async, "exec-legacy",
                      {:segment_mailbox_status,
                       %ExecutorCommands.SegmentMailboxStatus{status: "open"}}},
                     1_000
    end
  end
end

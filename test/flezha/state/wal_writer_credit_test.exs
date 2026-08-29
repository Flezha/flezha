defmodule Flezha.State.WalWriterCreditTest do
  @moduledoc """
  WalWriter side of credit-grant flow control: session-tagged drain
  reports on every outcome path (fsync, dedup, gap, shed, truncate) and
  `EntityMailboxStatus` suppression for credit-speaking sessions.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.State.{Supervisor, TestHelpers, WalWriter}

  setup :set_mimic_global_until_exit

  @session "wal-credit-session"

  setup do
    base = TestHelpers.tmp_app_data_dir()

    stub(Flezha.State.FsyncBatcher, :mark_dirty, fn _entity_id -> :ok end)

    test_pid = self()

    stub(Flezha.Runner.ControlChannel, :send_control_async, fn runner_id, command ->
      send(test_pid, {:ctl_async, runner_id, command})
      :ok
    end)

    Application.put_env(:flezha, :state_resync_fun, fn entity_id, opts ->
      send(test_pid, {:resync_called, entity_id, opts})
      :ok
    end)

    on_exit(fn ->
      TestHelpers.reset_all_entities()
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :state_resync_fun)
      Application.delete_env(:flezha, :wal_high_watermark_bytes)
      Application.delete_env(:flezha, :wal_low_watermark_bytes)
      File.rm_rf!(base)
    end)

    entity_id = TestHelpers.sample_entity_id("credit_#{System.unique_integer([:positive])}")
    {:ok, pid} = Supervisor.find_or_start(entity_id)

    %{pid: pid, entity_id: entity_id}
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
    refute_received {:credit_drained, _, _}

    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, @session, 100}
    refute_received {:credit_drained, _, _}
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

  test "a hard-bound shed releases its bytes immediately", %{pid: pid, entity_id: entity_id} do
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
    {:ok, _} = Supervisor.delete_entity(entity_id)
    drain_credit_reports()
    {:ok, pid2} = Supervisor.find_or_start(entity_id)
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

  describe "EntityMailboxStatus suppression (2.4)" do
    setup do
      # Tiny watermarks read at writer init.
      Application.put_env(:flezha, :wal_high_watermark_bytes, 64)
      Application.put_env(:flezha, :wal_low_watermark_bytes, 32)

      entity_id =
        TestHelpers.sample_entity_id("credit_bp_#{System.unique_integer([:positive])}")

      {:ok, pid} = Supervisor.find_or_start(entity_id)
      %{pid: pid, entity_id: entity_id}
    end

    test "credit-speaking session receives no status flags", %{pid: pid} do
      body = String.duplicate("x", 100)
      append!(pid, 0, body, opts(140))
      assert sync(pid) == 0

      # Telemetry still fires for in-process observers…
      # …but no wire signal is shipped.
      refute_received {:ctl_async, _, {:entity_mailbox_status, _}}
    end

    test "legacy session keeps the byte-for-byte EntityMailboxStatus wire signal", %{pid: pid} do
      body = String.duplicate("x", 100)
      WalWriter.append_async(pid, 0, TestHelpers.stub_header(), body, "exec-legacy")
      assert sync(pid) == 0

      assert_receive {:ctl_async, "exec-legacy",
                      {:entity_mailbox_status,
                       %RunnerCommands.EntityMailboxStatus{status: "full"}}},
                     1_000

      # Draining below the low watermark ships :open exactly once.
      :ok = WalWriter.fsync(pid)

      assert_receive {:ctl_async, "exec-legacy",
                      {:entity_mailbox_status,
                       %RunnerCommands.EntityMailboxStatus{status: "open"}}},
                     1_000
    end
  end
end

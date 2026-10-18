defmodule Flezha.State.WalWriterCreditTest do
  @moduledoc """
  WalWriter side of credit-grant flow control: session-tagged drain
  reports on every outcome path (fsync, dedup, gap, shed, truncate) and
  `CellMailboxStatus` suppression for credit-speaking sessions.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0, collect: 2]
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.State.{Supervisor, TestHelpers, WalWriter}

  setup :set_mimic_global_until_exit

  @session "wal-credit-session"

  setup do
    stub(Flezha.State.FsyncBatcher, :mark_dirty, fn _cell_id -> :ok end)

    test_pid = self()

    stub(Flezha.Runner.ControlChannel, :send_control_async, fn runner_id, command ->
      send(test_pid, {:ctl_async, runner_id, command})
      :ok
    end)

    on_exit(&TestHelpers.reset_all_cells/0)

    cell_id = TestHelpers.sample_cell_id("credit_#{System.unique_integer([:positive])}")
    {:ok, pid} = Supervisor.find_or_start(cell_id)

    %{pid: pid, cell_id: cell_id}
  end

  defp opts(n), do: %{channel: self(), session: @session, wire_size: n}

  defp append!(pid, seq, body, credit_opts) do
    WalWriter.append_async(pid, seq, TestHelpers.stub_header(), body, "exec-1", credit_opts)
  end

  defp sync(pid), do: WalWriter.last_persisted_seq(pid)

  defp drain_credit_reports() do
    _ =
      collect after: 200 do
        {:credit_drained, _, _, _} -> :ok
      end

    :ok
  end

  test "written bytes are reported on fsync, tagged with the debiting session and the Cell",
       %{pid: pid, cell_id: cell_id} do
    append!(pid, 0, "a", opts(70))
    append!(pid, 1, "b", opts(30))
    assert sync(pid) == 1

    # Append alone mints nothing.
    refute_received {:credit_drained, _, _, _}

    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, @session, 100, ^cell_id}
    refute_received {:credit_drained, _, _, _}
  end

  # A processed-prefix marker is confirmed against these reports, so a stream
  # that speaks no credit is told what drained too.
  test "a stream with no credit session is still told what drained",
       %{pid: pid, cell_id: cell_id} do
    append!(pid, 0, "a", %{channel: self(), session: nil, wire_size: 40})
    assert sync(pid) == 0
    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, nil, 40, ^cell_id}
  end

  # A drain confirms the stream's processed-prefix markers, so bytes that may
  # be on no disk are released: the window is freed, no input is acknowledged.
  describe "bytes a writer lets go unsynced are released, never drained" do
    test "a clean stop syncs its pending bytes and drains them", %{pid: pid, cell_id: cell_id} do
      append!(pid, 0, "a", opts(70))
      assert sync(pid) == 0

      {:ok, _} = Supervisor.delete_cell(cell_id)

      assert_receive {:credit_drained, @session, 70, ^cell_id}
      refute_received {:credit_released, _, _, _}
    end

    # The handle is closed inside the writer, so terminate's final datasync fails.
    test "a stop whose final datasync fails", %{pid: pid, cell_id: cell_id} do
      append!(pid, 0, "a", opts(70))
      assert sync(pid) == 0

      :sys.replace_state(pid, fn state ->
        :ok = :file.close(state.file_handle)
        state
      end)

      {:ok, _} = Supervisor.delete_cell(cell_id)

      assert_receive {:credit_released, @session, 70, ^cell_id}
      refute_received {:credit_drained, _, _, _}
    end

    test "a realign that truncates them", %{pid: pid, cell_id: cell_id} do
      append!(pid, 0, "a", opts(70))
      assert sync(pid) == 0

      # A restarted runner's diverged seq discards record 0's lineage.
      WalWriter.append_async(pid, 5, TestHelpers.stub_header(), "b", "exec-2", opts(30))
      assert sync(pid) == 5

      assert_received {:credit_released, @session, 70, ^cell_id}
      refute_received {:credit_drained, _, _, _}
    end

    test "a rewind that cuts them", %{pid: pid, cell_id: cell_id} do
      append!(pid, 0, "a", opts(70))
      :ok = WalWriter.fsync(pid)
      assert_receive {:credit_drained, @session, 70, ^cell_id}

      append!(pid, 1, "b", opts(30))
      :ok = WalWriter.rewind_to(pid, 0, 1)

      assert_received {:credit_released, @session, 30, ^cell_id}
      refute_received {:credit_drained, _, _, _}
    end
  end

  test "a dedup-dropped Change releases its bytes immediately", %{pid: pid} do
    append!(pid, 0, "a", opts(50))
    assert sync(pid) == 0

    append!(pid, 0, "a", opts(50))
    assert_receive {:credit_drained, @session, 50, _cell}

    # The written record's bytes still wait for the fsync.
    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, @session, 50, _cell}
  end

  test "a gap-rejected Change releases its bytes immediately and declares the gap", %{pid: pid} do
    become_loop()
    append!(pid, 0, "a", opts(40))
    assert sync(pid) == 0

    append!(pid, 5, "z", opts(60))
    assert_receive {:credit_drained, @session, 60, _cell}
    assert_receive {:declare, :seq_gap, _, %{trigger: :seq_gap}}
  end

  test "a hard-bound shed releases its bytes immediately", %{pid: pid, cell_id: cell_id} do
    append!(pid, 0, "a", opts(10))
    assert sync(pid) == 0

    :ok = :sys.suspend(pid)

    # Flood past the pending-msg cap while suspended so the resumed writer
    # sheds; every queued append carries a debit that must come back.
    Application.put_env(:flezha, :wal_pending_msg_cap, 5)

    # Restart the writer so it picks up the tiny cap. The stop's terminate
    # flushes the first append's tracked bytes — drain those reports so the
    # flood accounting below starts from a clean mailbox.
    :ok = :sys.resume(pid)
    {:ok, _} = Supervisor.delete_cell(cell_id)
    drain_credit_reports()
    {:ok, pid2} = Supervisor.find_or_start(cell_id)
    :ok = :sys.suspend(pid2)

    for seq <- 0..19, do: append!(pid2, seq, "x", opts(10))
    :ok = :sys.resume(pid2)

    # Every debited byte comes back — released on shed or minted on fsync.
    _ = sync(pid2)
    :ok = WalWriter.fsync(pid2)

    total =
      Stream.repeatedly(fn ->
        receive do
          {:credit_drained, @session, n, _cell} -> n
        after
          500 -> nil
        end
      end)
      |> Enum.take_while(& &1)
      |> Enum.sum()

    assert total == 20 * 10
  end

  test "a credit-speaking shed flood is one anomaly event", %{pid: pid} do
    handler = :telemetry_test.attach_event_handlers(self(), [[:flezha, :state, :credit_anomaly]])
    on_exit(fn -> :telemetry.detach(handler) end)

    state = %{:sys.get_state(pid) | credit_active?: true, shedding?: false}
    state = Enum.reduce(1..3, state, &WalWriter.Admission.shed(&2, &1, "exec-1"))

    assert state.shedding?
    assert_received {[:flezha, :state, :credit_anomaly], ^handler, _, %{kind: :shed}}
    refute_received {[:flezha, :state, :credit_anomaly], ^handler, _, _}
  end

  describe "CellMailboxStatus suppression (2.4)" do
    setup do
      # Tiny watermarks read at writer init.
      Application.put_env(:flezha, :wal_high_watermark_bytes, 64)
      Application.put_env(:flezha, :wal_low_watermark_bytes, 32)

      cell_id =
        TestHelpers.sample_cell_id("credit_bp_#{System.unique_integer([:positive])}")

      {:ok, pid} = Supervisor.find_or_start(cell_id)
      %{pid: pid, cell_id: cell_id}
    end

    test "credit-speaking session receives no status flags", %{pid: pid} do
      body = String.duplicate("x", 100)
      append!(pid, 0, body, opts(140))
      assert sync(pid) == 0

      # Telemetry still fires for in-process observers…
      # …but no wire signal is shipped.
      refute_received {:ctl_async, _, {:cell_mailbox_status, _}}
    end

    test "legacy session keeps the byte-for-byte CellMailboxStatus wire signal", %{pid: pid} do
      body = String.duplicate("x", 100)
      WalWriter.append_async(pid, 0, TestHelpers.stub_header(), body, "exec-legacy")
      assert sync(pid) == 0

      assert_receive {:ctl_async, "exec-legacy",
                      {:cell_mailbox_status, %RunnerCommands.CellMailboxStatus{status: "full"}}},
                     1_000

      # Draining below the low watermark ships :open exactly once.
      :ok = WalWriter.fsync(pid)

      assert_receive {:ctl_async, "exec-legacy",
                      {:cell_mailbox_status, %RunnerCommands.CellMailboxStatus{status: "open"}}},
                     1_000
    end
  end
end

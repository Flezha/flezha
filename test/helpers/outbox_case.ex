defmodule FlezhaTest.OutboxCase do
  @moduledoc """
  The case template the two effectively-once outbox modules share:

      use FlezhaTest.OutboxCase, async: false

  It stands up a per-test state tree with its own tmp root and cell id, tears
  the writer down before the root is removed (a late persist would otherwise
  race the `rm_rf`), and carries the record-shaping and dispatch-observing
  helpers both modules need.

  The test process stands in for the Overwatch throughout: the plugin chain run
  is what settles a dispatched output, so a test expecting the cursor to move
  reports it the way the real dispatcher does.
  """

  use ExUnit.CaseTemplate

  import ExUnit.Assertions
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Operator.ClassRegistry
  alias Flezha.SDK.RecordFrame
  alias Flezha.SDK.Wire
  alias Flezha.State.{InputLog, Outbox, Supervisor, TestHelpers, WalWriter}

  using do
    quote do
      use Mimic

      import ExUnit.CaptureLog
      import FlezhaTest.Helper.MimicGlobal
      import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
      import FlezhaTest.OutboxCase

      import Flezha.State.TestHelpers,
        only: [
          assert_batch: 1,
          in_one_window: 1,
          metadata_of: 1,
          next_output: 0,
          next_output: 1,
          refute_dispatched: 0,
          refute_dispatched: 1,
          run_batch: 1,
          run_batch: 2
        ]

      alias Flezha.Dlq.Store, as: DlqStore
      alias Flezha.Operator.ClassRegistry
      alias Flezha.SDK.{RecordFrame, Wire}
      alias Flezha.Service.CliManager

      alias Flezha.State.{
        FsyncBatcher,
        InputLog,
        Outbox,
        Paths,
        Supervisor,
        TestHelpers,
        WalWriter
      }
    end
  end

  setup do
    %{base: base} = FlezhaTest.Case.install([:tmp_app_data, :state_tree])
    ClassRegistry.ensure_table()

    rc = "wo_" <> Integer.to_string(System.unique_integer([:positive]))
    cell_id = {rc, "task", "k1"}

    # Registered after the fixture table's own, so it runs BEFORE it: the writer
    # has to stop before `:tmp_app_data` removes the root under it.
    on_exit(fn ->
      try do
        case Supervisor.lookup(cell_id) do
          {:ok, pid} -> WalWriter.close(pid)
          :error -> :ok
        end
      catch
        # A test that closed its own writer may still see the registry entry
        # until the sweep lands; stopping it twice is not a failure.
        :exit, _ -> :ok
      end
    end)

    %{rc: rc, cell_id: cell_id, base: base}
  end

  # Header byte 9 is the format id: 2 marks an effect record.
  def effect_header(),
    do: RecordFrame.encode_header(1, 1, 1, RecordFrame.format_id(:effect_record), <<>>)

  def plain_header(),
    do: RecordFrame.encode_header(1, 1, 1, RecordFrame.format_id(:postcard), <<>>)

  def effect_body(outputs, offset, dlq \\ nil) do
    Wire.encode_effect_body(%{
      changes: [<<1>>],
      outputs: outputs,
      consumed_offset: offset,
      dlq: dlq
    })
  end

  # Point the global DLQ store at a per-test tmp dir.
  def redirect_dlq_dir(base) do
    original = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.put(original, :dir, Path.join(base, "dlq")))
    on_exit(fn -> Application.put_env(:flezha, :dlq, original) end)
  end

  # This test process stands in for the Overwatch: the plugin chain run is what
  # settles a dispatched output, so a test expecting the cursor to move reports
  # it the same way the real dispatcher does.
  def settle(metadata), do: :ok = Outbox.settled(metadata)

  def output(key, payload, metadata \\ []) do
    %{channel: "out", key: key, metadata: metadata, payload: payload}
  end

  # The writer's own call order for the two facts KI-41 relates: the DLQ append
  # that materializes a park, and the input-log watermark advance. Tracing
  # observes without displacing either collaborator.
  def trace_park_and_watermark(pid) do
    patterns = [{DlqStore, :append, 1}, {InputLog, :record_consumed, 3}]
    Enum.each(patterns, &:erlang.trace_pattern(&1, true, [:local]))
    :erlang.trace(pid, true, [:call])
    on_exit(fn -> Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local])) end)
  end

  # Drains only the trace messages, so the dispatched-output mailbox is intact.
  def traced_order(acc \\ []) do
    receive do
      {:trace, _pid, :call, {DlqStore, :append, [%{origin_offset: offset}]}} ->
        traced_order([{:park, offset} | acc])

      {:trace, _pid, :call, {InputLog, :record_consumed, [_class, _cell, offset]}} ->
        traced_order([{:consumed, offset} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  def start_writer(cell_id) do
    {:ok, pid} = Supervisor.find_or_start(cell_id)
    pid
  end

  # Close and wait for the registry sweep, so the next find_or_start starts
  # a fresh writer instead of racing the dead entry.
  def close_writer(pid, cell_id) do
    :ok = WalWriter.close(pid)

    assert wait_until(fn -> Supervisor.lookup(cell_id) == :error end, 2_000),
           "writer registry entry never cleared"
  end

  @doc """
  Arm this process as the operator's Overwatch and start its writer — the two
  steps every dispatch test needs before the first append, because a dispatch
  with no resolvable Overwatch schedules a retry instead of a delivery.
  """
  def armed_writer(rc, cell_id) do
    ClassRegistry.put_overwatch(rc, self(), nil)
    start_writer(cell_id)
  end

  @doc """
  Append one effect record and make it durable — the smallest thing that puts an
  output into the outbox window, since dispatch is fsync-gated.
  """
  def append_effect(pid, seq, outputs, offset, dlq \\ nil) do
    :ok = WalWriter.append(pid, seq, effect_header(), effect_body(outputs, offset, dlq))
    :ok = WalWriter.fsync(pid)
    pid
  end

  @doc """
  Append one effect record WITHOUT making it durable — an output that is queued
  but not yet dispatchable, which is how a test puts two records in one window.
  """
  def stage_effect(pid, seq, outputs, offset, dlq \\ nil) do
    :ok = WalWriter.append(pid, seq, effect_header(), effect_body(outputs, offset, dlq))
    pid
  end

  @doc """
  Reopen a closed writer with its flush cursor reset to stale — the crash window
  between a batch reaching the plugins and the cursor write landing. What comes
  back owes the whole record again.
  """
  def reopen_with_stale_cursor(cell_id) do
    dedup = Outbox.read(cell_id)
    :ok = Outbox.write(cell_id, %{dedup | outbox_flushed_seq: -1})

    pid = start_writer(cell_id)
    _ = WalWriter.last_persisted_seq(pid)
    pid
  end
end

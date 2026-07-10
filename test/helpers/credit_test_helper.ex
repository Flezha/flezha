defmodule FlezhaTest.Helper.CreditTestHelper do
  @moduledoc """
  Shared scaffolding for the credit-grant flow-control suites
  (unit, e2e, soak): wire-frame building, ledger counter reads,
  segment/writer discovery, and fsync-cadence control.
  """

  import ExUnit.Assertions

  alias Flezha.State.{TestHelpers, WalWriter, Wire}
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.NodeTestHelper

  @doc "Build one `segment_state_change` wire frame (`Flezha.State.Wire` layout)."
  def state_change_frame(class, key, seq, body, operation \\ "op1") do
    sid = Wire.encode_segment_id(class, operation, key)
    header = TestHelpers.stub_header()

    <<byte_size(sid)::16-big, sid::binary, seq::64-big, header::binary, byte_size(body)::32-big,
      body::binary, :erlang.crc32(body)::32-big>>
  end

  @doc "LivenessCounters row for `executor_id`'s channel `name`, or nil."
  def counters_row(executor_id, name) do
    LivenessCounters.snapshot_all().channels
    |> Enum.find(fn c -> c.executor_id == executor_id and c.name == name end)
  end

  @doc "First credit-active channel row for `executor_id`, or nil."
  def active_credit_row(executor_id) do
    LivenessCounters.snapshot_all().channels
    |> Enum.find(fn c -> c.executor_id == executor_id and c.credit_active == 1 end)
  end

  @doc "Max `hb_credit_parked` self-report across `executor_id`'s channels."
  def parked_counter(executor_id) do
    LivenessCounters.snapshot_all().channels
    |> Enum.filter(fn c -> c.executor_id == executor_id end)
    |> Enum.map(& &1.hb_credit_parked)
    |> Enum.max(fn -> 0 end)
  end

  @doc "Wait for `operative_id`'s WalWriter registration under `operation`; returns the segment_id."
  def wait_for_segment(operative_id, operation, timeout_ms) do
    registry = Flezha.State.Supervisor.registry_name()

    result =
      NodeTestHelper.wait_until(
        fn ->
          Registry.select(registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
          |> Enum.find(fn
            {^operative_id, ^operation, _pk} -> true
            _ -> false
          end)
        end,
        timeout_ms
      )

    assert result, "no WalWriter registered for #{operative_id} — is the feed flowing?"
    result
  end

  @doc "Wait for `segment_id`'s WalWriter pid."
  def wait_for_writer(segment_id, timeout_ms) do
    assert NodeTestHelper.wait_until(
             fn -> match?({:ok, _}, Flezha.State.Supervisor.lookup(segment_id)) end,
             timeout_ms
           )

    {:ok, pid} = Flezha.State.Supervisor.lookup(segment_id)
    pid
  end

  @doc "Wait for `writer`'s persisted seq to reach exactly `seq`."
  def await_seq(writer, seq, timeout_ms \\ 5_000) do
    assert NodeTestHelper.wait_until(
             fn -> WalWriter.last_persisted_seq(writer) == seq end,
             timeout_ms
           ),
           "expected persisted seq #{seq}, got #{WalWriter.last_persisted_seq(writer)}"
  end

  @doc """
  Restart the (permanent) FsyncBatcher so it re-reads
  `:agent_fsync_interval_ms` from the current env.
  """
  def restart_fsync_batcher() do
    case GenServer.whereis(Flezha.State.FsyncBatcher) do
      nil ->
        :ok

      pid ->
        GenServer.stop(pid)

        NodeTestHelper.wait_until(
          fn ->
            case GenServer.whereis(Flezha.State.FsyncBatcher) do
              new_pid when is_pid(new_pid) -> new_pid != pid
              _ -> false
            end
          end,
          5_000
        )

        :ok
    end
  end
end

defmodule FlezhaTest.Helper.CreditTestHelper do
  @moduledoc """
  Shared scaffolding for the credit-grant flow-control suites
  (unit, e2e, soak): wire-frame building, ledger counter reads,
  entity/writer discovery, and fsync-cadence control.
  """

  import ExUnit.Assertions

  alias Flezha.State.{TestHelpers, WalWriter, Wire}
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.NodeTestHelper

  @doc "Build one `entity_state_change` wire frame (`Flezha.State.Wire` layout)."
  def state_change_frame(class, key, seq, body, procedure \\ "op1") do
    sid = Wire.encode_entity_id(class, procedure, key)
    header = TestHelpers.stub_header()

    Wire.encode_frame(sid, seq, header, body)
  end

  @doc "Every LivenessCounters channel row registered for `runner_id`."
  def channel_rows(runner_id) do
    Enum.filter(LivenessCounters.snapshot_all().channels, &(&1.runner_id == runner_id))
  end

  @doc "LivenessCounters row for `runner_id`'s channel `name`, or nil."
  def counters_row(runner_id, name),
    do: Enum.find(channel_rows(runner_id), &(&1.name == name))

  @doc """
  One counter off `runner_id`'s channel `name`, or `nil` when the row has not
  appeared yet.

  The row is published asynchronously, so a poll closure that writes
  `counters_row(...).field` raises out of `wait_until/2` the one time the row is
  late instead of reading as "not yet" and retrying — which is a flake on a
  loaded box, not a failure.
  """
  def counter(runner_id, name, field) do
    case counters_row(runner_id, name) do
      nil -> nil
      row -> Map.fetch!(row, field)
    end
  end

  @doc "First credit-active channel row for `runner_id`, or nil."
  def active_credit_row(runner_id),
    do: Enum.find(channel_rows(runner_id), &(&1.credit_active == 1))

  @doc "Max `hb_credit_parked` self-report across `runner_id`'s channels."
  def parked_counter(runner_id) do
    channel_rows(runner_id) |> Enum.map(& &1.hb_credit_parked) |> Enum.max(fn -> 0 end)
  end

  @doc "Wait for `operative_id`'s WalWriter registration under `procedure`; returns the entity_id."
  def wait_for_entity(operative_id, procedure, timeout_ms) do
    registry = Flezha.State.Supervisor.registry_name()

    result =
      NodeTestHelper.wait_until(
        fn ->
          Registry.select(registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
          |> Enum.find(fn
            {^operative_id, ^procedure, _pk} -> true
            _ -> false
          end)
        end,
        timeout_ms
      )

    assert result, "no WalWriter registered for #{operative_id} — is the feed flowing?"
    result
  end

  @doc "Wait for `entity_id`'s WalWriter pid."
  def wait_for_writer(entity_id, timeout_ms) do
    assert NodeTestHelper.wait_until(
             fn -> match?({:ok, _}, Flezha.State.Supervisor.lookup(entity_id)) end,
             timeout_ms
           )

    {:ok, pid} = Flezha.State.Supervisor.lookup(entity_id)
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

        NodeTestHelper.wait_until(fn -> replaced?(pid) end, 5_000)

        :ok
    end
  end

  defp replaced?(pid) do
    case GenServer.whereis(Flezha.State.FsyncBatcher) do
      new_pid when is_pid(new_pid) -> new_pid != pid
      _ -> false
    end
  end
end

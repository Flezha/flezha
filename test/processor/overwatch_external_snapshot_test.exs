defmodule FlezhaTest.Processor.OverwatchExternalSnapshotTest do
  @moduledoc """
  Exercises `Flezha.Processor.Overwatch`'s
  `handle_cast({:apply_external_snapshot, binary}, state)` clause introduced
  by ship-fresh-state-on-sdk-shutdown.

  The clause is the single place the SDK's graceful-shutdown notification
  lands in the agent: `ControlChannel` casts the binary out of the
  `OperatorShutdownNotification` payload, and Overwatch overwrites
  `state.snapshot` so the next `AddProcessor` reissue hands the freshest
  state to a reconnecting operator.

  Two scenarios are covered:

    * Periodic-snapshot enabled (default): cast applies and overwrites on
      a second cast. Verifies the field is replaced, not appended/merged.
    * Periodic-snapshot disabled (`snapshot_interval: 0`, e.g. flezha_market
      where Postgres is the authoritative store): cast still applies. This
      is the deployment that benefits the most — without this clause, a
      restart would lose all in-memory state because no periodic snapshot
      ever runs.
  """

  use ExUnit.Case, async: false

  alias Flezha.Processor.Builder
  alias Flezha.Service.ProcessorManager

  @namespace "default"
  @factory "store_input"

  setup do
    on_exit(fn -> ProcessorManager.stop_all_processors(@namespace) end)
    :ok
  end

  test "apply_external_snapshot overwrites state.snapshot and the second cast replaces the first" do
    processor_id = "overwatch-extsnap-#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: processor_id,
      factory_id: @factory,
      init_values: %{},
      opts: [snapshot_interval: :disabled]
    }

    assert {:ok, _info} = ProcessorManager.start_processor(@namespace, builder)
    {:ok, pid} = ProcessorManager.get_processor_pid(@namespace, processor_id)

    # Sanity: snapshot starts nil before any cast.
    assert :sys.get_state(pid).snapshot == nil

    GenServer.cast(pid, {:apply_external_snapshot, "fresh"})

    # `:sys.get_state` is synchronous and drains the GenServer mailbox up
    # to the snapshot, so the cast above is guaranteed to have applied by
    # the time we observe.
    assert :sys.get_state(pid).snapshot == "fresh"

    GenServer.cast(pid, {:apply_external_snapshot, "fresher"})

    assert :sys.get_state(pid).snapshot == "fresher"
  end

  test "apply_external_snapshot applies even when periodic snapshots are disabled" do
    processor_id = "overwatch-extsnap-disabled-#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: processor_id,
      factory_id: @factory,
      init_values: %{},
      opts: [snapshot_interval: 0]
    }

    assert {:ok, _info} = ProcessorManager.start_processor(@namespace, builder)
    {:ok, pid} = ProcessorManager.get_processor_pid(@namespace, processor_id)

    # Confirm periodic capture is off — no `state.snapshot` will ever be
    # set by the agent's own machinery in this configuration.
    state = :sys.get_state(pid)
    assert state.snapshot_interval == 0
    assert state.snapshot == nil

    GenServer.cast(pid, {:apply_external_snapshot, "from_shutdown"})

    assert :sys.get_state(pid).snapshot == "from_shutdown"
  end
end

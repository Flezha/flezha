defmodule FlezhaTest.Runner.ControlUnitPartitionTest do
  @moduledoc """
  Partition identity on the `AddOperative` wire.

  The SDK's mis-route guard is armed by these four fields and nothing else, so
  a member whose bind loses them starts accepting keys it does not own — which
  is why the migrate-time bind is pinned here too. The unpartitioned frame must
  stay byte-identical: proto3 omits zero-valued scalars, and that is the whole
  compatibility story.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Lifecycle
  alias Flezha.Partition
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.Identificator
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-unit-partition-test"

  setup :set_mimic_global_until_exit

  setup do
    test_pid = self()
    runner_id = "exec-part-#{System.unique_integer([:positive])}"

    ControlChannel
    |> stub(:send_control, fn ^runner_id, command ->
      send(test_pid, {:sent, command})
      {:ok, make_ref()}
    end)

    {:ok, pid} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000) end)

    %{runner_id: runner_id}
  end

  defp add_operative(runner_id, opts) do
    operative = %Builder{id: "ohlc#p1", factory_id: "candles", kind: :stream, opts: opts}

    catch_exit(
      GenServer.call(
        Identificator.control_unit(runner_id),
        {:start_operative, operative},
        50
      )
    )

    assert_receive {:sent, {:add_operative, add}}, 500
    add
  end

  test "a member's bind carries its index, count and owned range", %{runner_id: runner_id} do
    [_p0, range, _p2] = Partition.ranges(3)
    add = add_operative(runner_id, Partition.member_opts("ohlc", 1, 3, range))

    assert add.partition_index == 1
    assert add.partition_count == 3
    assert {add.key_group_from, add.key_group_to} == range
  end

  test "an unpartitioned bind encodes byte-identically to a partition-free build", %{
    runner_id: runner_id
  } do
    add = add_operative(runner_id, [])

    assert add.partition_index == 0
    assert add.partition_count == 0
    assert add.key_group_from == 0
    assert add.key_group_to == 0

    # The four fields are all zero, so proto3 emits none of them: the frame is
    # exactly the one a pre-partition controller produced.
    {:ok, encoded, _size} = RunnerCommands.AddOperative.encode(add)

    {:ok, reference, _ref_size} =
      RunnerCommands.AddOperative.encode(%RunnerCommands.AddOperative{
        operative_id: add.operative_id,
        factory_id: add.factory_id,
        init_values: add.init_values,
        kind: add.kind,
        resume_entity_count: add.resume_entity_count,
        proc_journal: add.proc_journal
      })

    assert IO.iodata_to_binary(encoded) == IO.iodata_to_binary(reference)
  end

  test "a one-member group is unpartitioned on the wire", %{runner_id: runner_id} do
    add = add_operative(runner_id, Partition.member_opts("ohlc", 0, 1, {0, 255}))

    assert add.partition_count == 0
    assert add.key_group_to == 0
  end

  test "the migrate-time bind re-arms the guard" do
    [_p0, range, _p2] = Partition.ranges(3)

    state = %Flezha.Operative.Overwatch{
      id: "ohlc#p1",
      factory_id: "candles",
      namespace: @namespace,
      builder: %Builder{
        id: "ohlc#p1",
        factory_id: "candles",
        kind: :workflow,
        opts: [{:plugins, []} | Partition.member_opts("ohlc", 1, 3, range)]
      }
    }

    migrated = Lifecycle.migrate_operative(state)

    # The kind travels with the migrate: a workflow instance re-added as a
    # stream class is refused by the runner and never heals.
    assert migrated.kind == :workflow

    assert Partition.identity(migrated.opts) == %{
             group: "ohlc",
             index: 1,
             count: 3,
             range: range
           }

    # Only the partition keys survive — a migrate still ships no plugins.
    assert Keyword.get(migrated.opts, :plugins) == nil
  end
end

defmodule Flezha.Relay.HandoverCutTest do
  @moduledoc """
  A moved state leaves this node only once the records its runner shipped up
  to the cut are durable here, because the destination pulls its floor from
  this node's copy next.
  """
  use FlezhaTest.OutboxCase, async: false

  alias Flezha.Relay.Handover
  alias Flezha.State.Wire

  # What `serialize_moved` ships: the values, and each Cell's next seq.
  defp moved_state({rc, sh, pk}, next_seq) do
    identity = rc |> Wire.encode_cell_id(sh, pk) |> :binary.bin_to_list()
    Jason.encode!(%{sh => 3, "$cells" => [%{identity: identity, timers: [], next_seq: next_seq}]})
  end

  # Fails as: the handover leaves while the record that closed the checkpoint
  # interval at the cut is still in flight; the destination pulls a floor below
  # the inputs the state holds, and its re-forward counts them twice.
  test "a moved state waits for its cut's records, within the grace", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [], 1)
    moved = moved_state(cell_id, 2)

    refute Handover.durable_cut?(moved, 50), "the cut's record at seq 1 is not here yet"

    waiter = Task.async(fn -> Handover.durable_cut?(moved, 5_000) end)
    append_effect(pid, 1, [], 3)
    assert Task.await(waiter), "the record landing inside the grace releases the state"

    assert Handover.durable_cut?(moved_state(cell_id, 0), 0), "a Cell that shipped nothing"
  end
end

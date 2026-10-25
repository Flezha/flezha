defmodule Flezha.Service.Cli.StateRotateTest do
  @moduledoc """
  `flz state rotate <relay/tasklet/cell>`: the verb awaits the runner's answer,
  and each verdict reaches the caller as its own line.
  """
  # async: false: Mimic global mode; Mimic stubs.
  use ExUnit.Case, async: false

  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.Cli
  alias Flezha.State.{Commands, Paths, Recovery}
  alias FlezhaTest.Helper.CliCapture

  setup :set_mimic_global_until_exit

  # A persisted Cell whose runner answers the awaited compaction with `verdict`.
  defp rotate(verdict) do
    rc = "rot_#{System.unique_integer([:positive])}"
    cell_id = {rc, "candle", "BTCUSD@1m"}
    stub(Paths, :all_cells, fn -> [cell_id] end)
    stub(Flezha.Service.RelayManager, :namespace_of_class, fn ^rc -> "default" end)
    stub(Recovery, :resolve_runner, fn ^cell_id -> {:ok, "runner-1"} end)

    stub(Commands, :dispatch_compact_cell, fn "runner-1", ^cell_id, [await: true] ->
      {:ok, verdict}
    end)

    path = "#{rc}/candle/BTCUSD@1m"

    {_outcome, out, err} =
      CliCapture.run(fn -> Cli.call("Steps.rotate", Jason.encode!([path]), []) end)

    {path, out <> err}
  end

  # Fails as: a rotate the runner refused, never answered or failed prints
  # "compaction queued", and the operator waits for a snapshot that never comes.
  test "each verdict of an awaited compaction prints as its own line" do
    {path, out} = rotate(:queued)
    assert out == "compaction queued for #{path}\n"

    {path, out} = rotate(:deferred)
    assert out == "compaction refused for #{path}: busy mid-retry; the next sweep asks again\n"

    {path, out} = rotate(:timed_out)
    assert out == "compaction timed out for #{path}: the runner did not answer\n"

    {path, out} = rotate({:failed, :no_snapshot})
    assert out == "compaction failed for #{path}: :no_snapshot\n"

    {path, out} = rotate({:failed, Flezha.Error.new("state.cell_not_live", cell_id: "ab")})
    assert out =~ ~r/^compaction failed for #{path}: .+ \(state\.cell_not_live\)\n$/
  end
end

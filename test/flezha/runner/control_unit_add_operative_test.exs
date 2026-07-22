defmodule FlezhaTest.Runner.ControlUnitAddOperativeTest do
  @moduledoc """
  The resuming bind's two instance-scoped fields on `AddOperative`, both stamped
  onto the operative's opts by `Overwatch.with_resume_state/3`.

  `op_journal` rides the bind rather than a per-Segment `RestoreInitState`
  because it is instance-scoped: a durable bounded class with no per-Segment
  state ships no restore at all, so a journal delivered there would never arrive
  and the driver would resume at iteration 0 on every wake.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Operative.Builder
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit

  @namespace "ctrl-unit-add-operative-test"

  setup :set_mimic_global

  setup do
    test_pid = self()
    runner_id = "exec-add-#{System.unique_integer([:positive])}"

    ControlChannel
    |> stub(:send_control, fn ^runner_id, command ->
      send(test_pid, {:sent, command})
      {:ok, make_ref()}
    end)

    {:ok, pid} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000) end)

    %{runner_id: runner_id}
  end

  defp start_operative(runner_id, opts) do
    operative = %Builder{id: "op-1", factory_id: "f", kind: :bounded, opts: opts}
    # The reply only lands on the AddOperative response, which never arrives here.
    catch_exit(
      GenServer.call(
        Flezha.Service.Identificator.control_unit(runner_id),
        {:start_operative, operative},
        50
      )
    )
  end

  test "a resuming bind carries the op-journal even with nothing to restore", %{
    runner_id: runner_id
  } do
    journal = %RunnerCommands.OpJournalState{iter: 7, consumed_offset: 3}
    start_operative(runner_id, resume_segment_count: 0, op_journal: journal)

    assert_receive {:sent, {:add_operative, add}}, 500
    assert add.resume_segment_count == 0
    assert add.op_journal == journal
  end

  test "a fresh bind carries no journal", %{runner_id: runner_id} do
    start_operative(runner_id, [])

    assert_receive {:sent, {:add_operative, add}}, 500
    assert add.resume_segment_count == 0
    assert add.op_journal == nil
  end
end

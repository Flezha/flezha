defmodule FlezhaTest.Relay.Steps.JournalTest do
  @moduledoc """
  `Flezha.Service.Steps.Journal.report/1` and `flz state journal` report the
  run's outcome as the engine recorded it, never a rule derived from the
  journal's shape.
  """
  # async: false: a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  alias Flezha.Service.Cli
  alias Flezha.Service.Steps.Journal
  alias Flezha.State.{Completion, Paths, TaskJournal}

  defp instance_id(), do: "journal_report_#{System.unique_integer([:positive])}"

  defp rendered(inst),
    do:
      ExUnit.CaptureIO.capture_io(fn -> Cli.call("Steps.journal", Jason.encode!([inst]), []) end)

  defp complete(inst, status, error) do
    Completion.write(inst, %{
      namespace: Paths.namespace_of_class(inst),
      run: "steps",
      status: status,
      error: error,
      result: "",
      finished_at: System.system_time(:second)
    })
  end

  # A `per_input` loop's journal: one committed iteration per input, the second
  # calling `complete`.
  defp two_iterations(inst) do
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])
    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = TaskJournal.append_task_committed(inst, 0, 1, [], [], "done")
    :ok = TaskJournal.append_iter_committed(inst, 1, 1, [], "done")
  end

  test "a per_input loop that calls complete in its second iteration reports ok" do
    inst = instance_id()
    two_iterations(inst)

    assert {:ok, %{outcome: nil, entries: entries}} = Journal.report(inst)
    assert length(entries) == 4
    assert rendered(inst) =~ "\n  outcome: running\n"

    :ok = complete(inst, "ok", "")

    assert {:ok, %{outcome: %{status: "ok", error: ""}}} = Journal.report(inst)
    text = rendered(inst)
    assert text =~ "\n  outcome: ok\n"
    refute text =~ "complete:"
  end

  test "a failed run prints its status with the error the engine recorded" do
    inst = instance_id()
    two_iterations(inst)
    :ok = complete(inst, "error", "max_duration exceeded")

    assert {:ok, %{outcome: %{status: "error", error: "max_duration exceeded"}}} =
             Journal.report(inst)

    assert rendered(inst) =~ "\n  outcome: error (max_duration exceeded)\n"
  end
end

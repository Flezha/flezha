defmodule Flezha.Service.Steps.Journal do
  @moduledoc """
  A steps instance's task journal as `Flezha.Service.Steps.journal/2` answers
  it: one row per entry in append order (seq, type, task index, iteration, the
  cells that ran with the effect records each wrote and the channels they wrote
  on, and the handoff), the leaf tasklets that define "done", and whether the
  in-flight iteration has committed every leaf.
  """

  alias Flezha.Relay.ClassRegistry
  alias Flezha.SDK.Wire
  alias Flezha.State.Outbox
  alias Flezha.State.TaskJournal

  @doc "The journal report for `instance_id`."
  @spec report(String.t()) :: {:ok, map()} | {:error, term()}
  def report(instance_id) do
    with {:ok, entries} <- TaskJournal.read(instance_id) do
      leaf_tasks = ClassRegistry.leaf_tasks(instance_id)

      {:ok,
       %{
         instance: instance_id,
         entries: Enum.map(entries, &journal_row/1),
         leaf_tasks: leaf_tasks,
         complete: TaskJournal.complete?(instance_id, leaf_tasks)
       }}
    end
  end

  defp journal_row(%{type: :task_committed, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.merge(journal_handoff(payload.handoff))
    |> Map.put(:cells_ran, journal_cells(payload.cells_ran))
    |> journal_completion(payload.completion)
    |> journal_trigger(payload.trigger, payload.cells_ran)
  end

  defp journal_row(%{type: :compensated, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:cells_ran, journal_cells(payload.cells_ran))
  end

  defp journal_row(%{type: :iter_committed, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.merge(journal_handoff(payload.handoff))
    |> Map.put(:consumed_offset, payload.consumed_offset)
    |> Map.put(:entry_channel, payload.entry_channel)
    |> journal_completion(payload.completion)
  end

  defp journal_row(%{type: :dead_lettered, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.merge(
      Map.take(payload, [:consumed_offset, :entry_channel, :tasklet, :attempts, :dlq_entry])
    )
  end

  # A `rewound` entry's `iter` IS the iteration it rewound to; spell that out
  # rather than leaving the reader to infer it from a bare `iter=` column.
  defp journal_row(%{type: :rewound} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:rewound_to, entry.iter)
  end

  defp journal_row(%{type: :input_released, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:released_offset, payload.consumed_offset)
  end

  defp journal_row(entry), do: base_journal_row(entry)

  defp base_journal_row(entry) do
    %{
      seq: entry.seq,
      type: Atom.to_string(entry.type),
      task_idx: entry.task_idx,
      iter: entry.iter,
      epoch: entry.epoch
    }
  end

  # A handoff is per-edge: each item carries the channel it was released on,
  # which is what routes it to the branch consuming that channel. The totals are
  # the edges summed, so one grouping answers both.
  defp journal_handoff(handoff) do
    edges =
      handoff
      |> Enum.group_by(fn {cid, _} -> cid end, fn {_, bytes} -> byte_size(bytes) end)
      |> Enum.sort()
      |> Enum.map(fn {cid, sizes} ->
        %{channel_id: cid, items: length(sizes), bytes: Enum.sum(sizes)}
      end)

    %{
      handoff_items: edges |> Enum.map(& &1.items) |> Enum.sum(),
      handoff_bytes: edges |> Enum.map(& &1.bytes) |> Enum.sum(),
      handoff_edges: edges
    }
  end

  # A completing entry names the result's size rather than printing an
  # author's bytes.
  defp journal_completion(row, nil), do: row
  defp journal_completion(row, result), do: Map.put(row, :completes_bytes, byte_size(result))

  # The root of a timer-fed pass names the firing that opened it, on the
  # tasklet whose Cell set the timer.
  defp journal_trigger(row, nil, _cells_ran), do: row

  defp journal_trigger(row, trigger, cells_ran) do
    tasklet =
      Enum.find_value(cells_ran, "?", fn %{cell_id: cell_id} ->
        case Wire.decode_cell_id(cell_id) do
          {:ok, {_class, tasklet, key}} when key == trigger.cell_key -> tasklet
          _ -> nil
        end
      end)

    Map.put(row, :opened_by, %{
      kind: trigger.kind,
      scheduled_at: trigger.scheduled_at,
      tasklet: tasklet,
      cell: trigger.cell_key
    })
  end

  defp journal_cells(cells) do
    Enum.map(cells, fn cell ->
      %{
        cell: Outbox.cell_label(cell.cell_id),
        first_seq: cell.first_seq,
        seq: cell.seq,
        channels: cell.channels
      }
    end)
  end
end

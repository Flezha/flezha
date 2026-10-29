defmodule Flezha.Service.Steps.Journal do
  @moduledoc """
  A steps instance's task journal as `Flezha.Service.Steps.journal/2` answers
  it: one row per entry in append order (seq, type, task index, iteration, the
  cells that ran with the effect records each wrote and the channels they wrote
  on, and the handoff), and the run's `outcome`: the status and error the
  engine completed it with (`Flezha.Service.Steps.outcome/2`), or `nil` while it
  runs.
  """

  alias Flezha.Cluster.Page
  alias Flezha.Service.RelayManager
  alias Flezha.Service.Steps
  alias Flezha.State.Wire
  alias Flezha.State.Outbox
  alias Flezha.State.TaskJournal

  @doc """
  The journal report for `instance_id` of `namespace` (its class's when `nil`);
  with a `Flezha.Cluster.Page`, its entries paged by seq under `rows`, only the
  page's rows built.
  """
  @spec report(String.t(), Page.t() | nil, String.t() | nil) :: {:ok, map()} | {:error, term()}
  def report(instance_id, page \\ nil, namespace \\ nil) do
    namespace = namespace || RelayManager.namespace_of_class(instance_id)

    with {:ok, entries} <- TaskJournal.read(instance_id) do
      {:ok,
       Map.merge(
         %{instance: instance_id, outcome: outcome(namespace, instance_id)},
         rows(entries, page)
       )}
    end
  end

  defp rows(entries, nil), do: %{entries: Enum.map(entries, &journal_row/1)}

  defp rows(entries, page) do
    %{rows: kept} = answer = Page.slice(entries, page, & &1.seq)
    %{answer | rows: Enum.map(kept, &journal_row/1)}
  end

  defp outcome(namespace, instance_id) do
    case Steps.outcome(namespace, instance_id) do
      {:ok, record} -> Map.take(record, [:status, :error])
      :error -> nil
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

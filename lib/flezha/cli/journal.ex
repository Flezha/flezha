defmodule Flezha.Service.Cli.Journal do
  @moduledoc """
  `flz state journal | inbox` — a workflow instance's task-journal entries, and
  the durable input log of an instance of either kind.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.SDK.Wire
  alias Flezha.Service.OperativeManager
  alias Flezha.State.InputLog
  alias Flezha.State.Outbox
  alias Flezha.State.TaskJournal

  @doc "The row both reads are judged by: the instance's class namespace."
  def instance_row([instance_id | _]),
    do: {:ok, %{namespace: OperativeManager.namespace_of_class(instance_id)}}

  @doc """
  `flz state journal <operative/instance>` — render a workflow instance's
  task-journal entries (`workflow-task-journal`) in append order: seq, type,
  task index, iteration, the cells that ran with the effect records each wrote
  and the channels they wrote on, and the handoff batch size.
  """
  def state_journal(instance_id, output_format, _call) do
    {:ok, entries} = TaskJournal.read(instance_id)

    rows = Enum.map(entries, &journal_row/1)
    leaf_tasks = ClassRegistry.leaf_tasks(instance_id)
    complete = TaskJournal.complete?(instance_id, leaf_tasks)

    case output_format do
      "json" ->
        IO.puts(
          Jason.encode!(%{
            instance: instance_id,
            entries: rows,
            leaf_tasks: leaf_tasks,
            complete: complete
          })
        )

      _ ->
        if rows == [] do
          IO.puts("no task-journal for instance #{instance_id}")
        else
          IO.puts("task-journal for instance #{instance_id}:\n")
          Enum.each(rows, &print_journal_row/1)
          print_journal_completion(leaf_tasks, complete)
        end
    end
  end

  # The leaf set is what "done" means for a tree-shaped chain: the operative is
  # complete when the in-flight iteration has committed every leaf. A class that
  # declared none (a stream, an older SDK) leaves it underivable.
  defp print_journal_completion(nil, _complete),
    do: IO.puts("\n  complete: unknown (class declares no leaf tasklets)")

  defp print_journal_completion(leaf_tasks, complete),
    do: IO.puts("\n  leaves: #{Enum.join(leaf_tasks, ",")}  complete: #{complete}")

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

  # A `rewound` entry's `iter` IS the iteration it rewound to; spell that out
  # rather than leaving the reader to infer it from a bare `iter=` column.
  defp journal_row(%{type: :rewound} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:rewound_to, entry.iter)
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

  defp print_journal_row(row) do
    IO.puts(
      "  seq=#{row.seq} #{row.type} task_idx=#{row.task_idx} iter=#{row.iter} epoch=#{row.epoch}"
    )

    Enum.each(Map.get(row, :cells_ran, []), &print_journal_cell/1)

    if Map.has_key?(row, :handoff_items) do
      IO.puts("    handoff: #{row.handoff_items} item(s), #{row.handoff_bytes} byte(s)")
    end

    Enum.each(Map.get(row, :handoff_edges, []), fn edge ->
      IO.puts(
        "      edge: channel_id=#{edge.channel_id} #{edge.items} item(s), #{edge.bytes} byte(s)"
      )
    end)

    case Map.fetch(row, :consumed_offset) do
      {:ok, nil} -> IO.puts("    consumed no journaled input")
      {:ok, offset} -> IO.puts("    consumed input-log offset: #{offset}")
      :error -> :ok
    end

    if Map.has_key?(row, :completes_bytes) do
      IO.puts("    completes the instance: result #{row.completes_bytes} byte(s)")
    end

    if timer = Map.get(row, :opened_by) do
      IO.puts(
        "    opened by: timer #{timer.kind} #{timer.scheduled_at} on #{timer.tasklet}/#{timer.cell}"
      )
    end

    if Map.has_key?(row, :rewound_to) do
      IO.puts("    rewound to iteration: #{row.rewound_to}")
    end

    IO.puts("")
  end

  defp print_journal_cell(%{cell: cell, seq: nil}), do: IO.puts("    ran: #{cell}")

  defp print_journal_cell(%{cell: cell, first_seq: first, seq: last, channels: channels}) do
    records = if first == last, do: "record=#{last}", else: "records=#{first}..#{last}"
    written = if channels == [], do: "", else: " channels=#{Enum.join(channels, ",")}"
    IO.puts("    ran: #{cell} #{records}#{written}")
  end

  @doc """
  `flz state inbox <instance>` — the instance's durable input log, for a stream
  on the effectively-once tier and a durable workflow alike
  (`durable-input-log`): every retained record with its offset, whether it is
  at or below the instance's floor (consumed) or above it (pending), its size
  and channel.
  """
  def state_inbox(instance_id, output_format, _call) do
    case retained_input(instance_id) do
      {:ok, floor, records} ->
        rows =
          Enum.map(records, fn {offset, message, metadata} ->
            %{
              offset: offset,
              consumed: offset <= floor,
              bytes: byte_size(message),
              channel: Map.get(metadata, "channel")
            }
          end)

        render_inbox(instance_id, floor, rows, output_format)

      {:error, refusal} ->
        IO.puts("the input log for #{instance_id} refused to serve: #{inspect(refusal)}")
    end
  end

  # The floor is `-1` while no reporter has consumed anything.
  defp retained_input(instance_id) do
    with {:ok, floor, last} when last >= 0 <- InputLog.tail(instance_id),
         records when is_list(records) <- InputLog.read_range(instance_id, 0, last) do
      {:ok, floor, records}
    else
      {:ok, floor, -1} -> {:ok, floor, []}
      {:error, _refusal} = refused -> refused
    end
  end

  defp render_inbox(instance_id, floor, rows, "json"),
    do: IO.puts(Jason.encode!(%{instance: instance_id, floor: floor, entries: rows}))

  defp render_inbox(instance_id, _floor, [], _text),
    do: IO.puts("no durable input log for #{instance_id}")

  defp render_inbox(instance_id, floor, rows, _text) do
    IO.puts("durable input log for #{instance_id} (#{consumed_through(floor)}):\n")

    Enum.each(rows, fn row ->
      IO.puts(
        "  offset=#{row.offset} #{if row.consumed, do: "consumed", else: "PENDING"} " <>
          "#{row.bytes} byte(s)" <>
          if(row.channel, do: " channel=#{row.channel}", else: "")
      )
    end)

    IO.puts("")
  end

  defp consumed_through(-1), do: "nothing consumed yet"
  defp consumed_through(floor), do: "consumed through #{floor}"
end

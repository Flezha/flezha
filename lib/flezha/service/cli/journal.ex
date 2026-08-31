defmodule Flezha.Service.Cli.Journal do
  @moduledoc """
  `flz state journal | inbox` — a workflow instance's proc-journal entries and
  its durable inbox.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.ProcInbox
  alias Flezha.State.ProcJournal
  alias Flezha.State.Wire

  @doc """
  `flz state journal <operative/instance>` — render a workflow instance's
  proc-journal entries (`workflow-proc-journal`) in append order: seq, type,
  proc index, iteration, the entities that ran, and the handoff batch size.
  """
  def state_journal(instance_id, output_format \\ "text", _opts \\ []) do
    {:ok, entries} = ProcJournal.read(instance_id)

    rows = Enum.map(entries, &journal_row/1)
    leaf_procs = ClassRegistry.leaf_procs(instance_id)
    complete = ProcJournal.complete?(instance_id, leaf_procs)

    case output_format do
      "json" ->
        IO.puts(
          Jason.encode!(%{
            instance: instance_id,
            entries: rows,
            leaf_procs: leaf_procs,
            complete: complete
          })
        )

      _ ->
        if rows == [] do
          IO.puts("no proc-journal for instance #{instance_id}")
        else
          IO.puts("proc-journal for instance #{instance_id}:\n")
          Enum.each(rows, &print_journal_row/1)
          print_journal_completion(leaf_procs, complete)
        end
    end
  end

  # The leaf set is what "done" means for a tree-shaped chain: the operative is
  # complete when the in-flight iteration has committed every leaf. A class that
  # declared none (run-sugar, stream, an older SDK) leaves it underivable.
  defp print_journal_completion(nil, _complete),
    do: IO.puts("\n  complete: unknown (class declares no leaf procedures)")

  defp print_journal_completion(leaf_procs, complete),
    do: IO.puts("\n  leaves: #{Enum.join(leaf_procs, ",")}  complete: #{complete}")

  defp journal_row(%{type: :proc_committed, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.merge(journal_handoff(payload.handoff))
    |> Map.merge(%{
      entities_ran: Enum.map(payload.entities_ran, &Wire.entity_label/1),
      outbox: journal_outbox(payload.outbox)
    })
  end

  defp journal_row(%{type: :compensated, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:outbox, journal_outbox(payload.outbox))
  end

  defp journal_row(%{type: :iter_committed, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.merge(journal_handoff(payload.handoff))
    |> Map.put(:consumed_offset, payload.consumed_offset)
  end

  defp journal_row(%{type: :parked, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:wake_deadline_ms, payload.wake_deadline_ms)
  end

  # `for_type` names which entry's outbox this marks delivered — a procedure
  # can have both a forward `proc_committed` outbox and a `compensated` undo one.
  defp journal_row(%{type: :outbox_flushed, payload: %{for_type: for_type}} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:flushes, to_string(for_type))
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
      proc_idx: entry.proc_idx,
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

  defp journal_outbox(outbox) do
    Enum.map(outbox, fn item ->
      %{
        entity: Wire.entity_label(item.entity_id),
        seq_in_proc: item.seq_in_proc,
        channel: item.channel,
        bytes: byte_size(item.payload)
      }
    end)
  end

  defp print_journal_row(row) do
    IO.puts(
      "  seq=#{row.seq} #{row.type} proc_idx=#{row.proc_idx} iter=#{row.iter} epoch=#{row.epoch}"
    )

    Enum.each(Map.get(row, :entities_ran, []), fn seg -> IO.puts("    ran: #{seg}") end)

    if Map.has_key?(row, :handoff_items) do
      IO.puts("    handoff: #{row.handoff_items} item(s), #{row.handoff_bytes} byte(s)")
    end

    Enum.each(Map.get(row, :handoff_edges, []), fn edge ->
      IO.puts(
        "      edge: channel_id=#{edge.channel_id} #{edge.items} item(s), #{edge.bytes} byte(s)"
      )
    end)

    Enum.each(Map.get(row, :outbox, []), fn item ->
      IO.puts(
        "    outbox: #{item.entity} seq_in_proc=#{item.seq_in_proc} " <>
          "channel=#{item.channel} #{item.bytes} byte(s)"
      )
    end)

    if flushes = Map.get(row, :flushes) do
      IO.puts("    flushes: #{flushes}")
    end

    if Map.has_key?(row, :consumed_offset) do
      IO.puts("    consumed inbox offset: #{row.consumed_offset}")
    end

    if Map.has_key?(row, :wake_deadline_ms) do
      IO.puts("    wakes at: #{row.wake_deadline_ms || "next input"}")
    end

    if Map.has_key?(row, :rewound_to) do
      IO.puts("    rewound to iteration: #{row.rewound_to}")
    end

    IO.puts("")
  end

  @doc """
  `flz state inbox <instance>` — the durable inbox of a workflow instance
  (`workflow-durable-suspension`): every appended input with its offset, marking
  which are still undelivered (above the consumed offset the last committed
  iteration recorded).
  """
  def state_inbox(instance_id, output_format \\ "text", _opts \\ []) do
    entries = ProcInbox.read(instance_id)
    consumed = ProcJournal.consumed_offset(instance_id)

    rows =
      Enum.map(entries, fn entry ->
        %{
          offset: entry.offset,
          consumed: entry.offset <= consumed,
          bytes: byte_size(entry.message),
          channel: Map.get(entry.metadata, "channel"),
          wake: Map.get(entry.metadata, "flz.wake")
        }
      end)

    case output_format do
      "json" ->
        IO.puts(Jason.encode!(%{instance: instance_id, consumed_offset: consumed, entries: rows}))

      _ ->
        if rows == [] do
          IO.puts("no durable inbox for instance #{instance_id}")
        else
          IO.puts("durable inbox for instance #{instance_id} (consumed through #{consumed}):\n")

          Enum.each(rows, fn row ->
            IO.puts(
              "  offset=#{row.offset} #{if row.consumed, do: "consumed", else: "PENDING"} " <>
                "#{row.bytes} byte(s)" <>
                if(row.channel, do: " channel=#{row.channel}", else: "") <>
                if(row.wake, do: " wake=#{row.wake}", else: "")
            )
          end)

          IO.puts("")
        end
    end
  end
end

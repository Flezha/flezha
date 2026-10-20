defmodule Flezha.Service.Cli.Journal do
  @moduledoc """
  `flz state journal | inbox` — the text forms of a steps instance's task
  journal and of an instance's durable input log.
  """

  @doc "`flz state journal`."
  def journal(%{entries: [], instance: instance}, _args, _opts),
    do: IO.puts("no task-journal for instance #{instance}")

  def journal(report, _args, _opts) do
    IO.puts("task-journal for instance #{report.instance}:\n")
    Enum.each(report.entries, &print_journal_row/1)
    print_journal_outcome(report.outcome)
  end

  defp print_journal_outcome(nil), do: IO.puts("\n  outcome: running")

  defp print_journal_outcome(%{status: status} = outcome) do
    case Map.get(outcome, :error) do
      blank when blank in [nil, ""] -> IO.puts("\n  outcome: #{status}")
      error -> IO.puts("\n  outcome: #{status} (#{error})")
    end
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

    if Map.has_key?(row, :released_offset) do
      IO.puts("    released unread input-log offset: #{row.released_offset}")
    end

    if Map.has_key?(row, :rewound_to) do
      IO.puts("    rewound to iteration: #{row.rewound_to}")
    end

    if Map.has_key?(row, :dlq_entry) do
      IO.puts(
        "    turn failed in #{row.tasklet} after #{row.attempts} attempt(s); " <>
          "parked as #{row.dlq_entry}"
      )
    end

    IO.puts("")
  end

  defp print_journal_cell(%{cell: cell, seq: nil}), do: IO.puts("    ran: #{cell}")

  defp print_journal_cell(%{cell: cell, first_seq: first, seq: last, channels: channels}) do
    records = if first == last, do: "record=#{last}", else: "records=#{first}..#{last}"
    written = if channels == [], do: "", else: " channels=#{Enum.join(channels, ",")}"
    IO.puts("    ran: #{cell} #{records}#{written}")
  end

  @doc "`flz state inbox`."
  def inbox(%{entries: [], instance: instance}, _args, _opts),
    do: IO.puts("no durable input log for #{instance}")

  def inbox(log, _args, _opts) do
    IO.puts("durable input log for #{log.instance} (#{consumed_through(log.floor)}):\n")

    Enum.each(log.entries, fn row ->
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

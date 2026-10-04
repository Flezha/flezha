defmodule Flezha.Service.Cli.State do
  @moduledoc """
  `flz state get | list | at | rotate | delete | outbox | retention` — the text
  forms of the Cell-scoped state reads and writes.
  """

  @doc "`flz state get`."
  def state_get(read, _args, _opts) do
    IO.puts("state get #{read.path}:")
    IO.puts("  change_seq: #{read.change_seq || "-"}")
    IO.puts("  state:      #{Jason.encode!(read.state)}")
  end

  @doc "`flz state list`."
  def cells(%{operator_class: operator_class, cells: cells}, _args, _opts) do
    IO.puts("Cells under operator #{operator_class}:\n")

    Enum.each(cells, fn row ->
      IO.puts("  #{row.tasklet_id}/#{row.cell_key}")
      IO.puts("    wal_seq:                 #{row.wal_seq}")
      IO.puts("    snapshot_seq:            #{row.snapshot_seq}")
      IO.puts("    wal_bytes:               #{row.wal_bytes}")
      IO.puts("    flush_latency_ms_p99:    #{row.flush_latency_ms_p99}")
      IO.puts("    fsync_durability_lag_ms: #{row.fsync_durability_lag_ms}")
      IO.puts("    dedup_drops_total:       #{row.dedup_drops_total}")
      IO.puts("")
    end)
  end

  @doc "`flz state at`."
  def time_travel(at, _args, _opts) do
    IO.puts("state at #{at.cell} target=#{at.target}:")
    IO.puts("  terminal_seq:    #{at.terminal_seq}")
    IO.puts("  snapshot bytes:  #{at.snapshot_size}")
    IO.puts("  change_log bytes:#{at.change_log_size}")
  end

  @doc "`flz state rotate`."
  def rotate(%{cell: cell}, _args, _opts), do: IO.puts("compaction queued for #{cell}")

  @doc "`flz state delete`."
  def delete(%{cell: cell}, _args, _opts), do: IO.puts("deleted persisted state for #{cell}")

  @doc "`flz state outbox`."
  def outbox(status, _args, _opts) do
    IO.puts("cell              #{status.cell}")
    IO.puts("last_persisted_seq #{status.last_persisted_seq}")
    IO.puts("outbox_flushed_seq #{status.outbox_flushed_seq}")
    IO.puts("epoch              #{status.epoch}")
    IO.puts("outbox_lag         #{status.pending || "unknown (writer not running)"}")
    IO.puts("held_for_stage     #{status.held || "unknown (writer not running)"}")
    IO.puts("input_watermark    #{status.input_watermark || "none"}")
  end

  @doc "`flz state retention`."
  def set_retention(%{operator_class: operator_class, hours: hours}, _args, _opts),
    do:
      IO.puts(
        "retention[#{operator_class}] = #{hours} hours (in-memory; persist via runtime.exs)"
      )
end

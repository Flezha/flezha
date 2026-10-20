defmodule Flezha.State.TaskJournal.Records do
  @moduledoc """
  The effect records a steps instance's stage entries name, and the Cell
  writers that hold them: the ranges an entry names, the wait for a record to
  be durable, the release of an entry's outputs once it is, and what the live
  writers still owe the sinks. Which entries are live is the journal's fold
  (`Flezha.State.TaskJournal.committed_records/1`).
  """

  alias Flezha.State.{Lineage, Outbox, Supervisor, TaskJournal, WalWriter, Wire}

  @doc """
  The records a stage entry's Cells wrote, `{cell_id, first_seq, seq}` for
  each Cell that wrote any.
  """
  @spec record_ranges([TaskJournal.committed_cell()]) :: [
          {binary(), non_neg_integer(), non_neg_integer()}
        ]
  def record_ranges(cells),
    do:
      for(
        %{cell_id: id, first_seq: first, seq: last} <- cells,
        is_integer(last),
        do: {id, first, last}
      )

  @doc """
  Ask the writer of the Cell `cell_id` names to answer once its record at
  `seq` is durable (`WalWriter.await_durable_async/4`, under `label` into
  `reqids`). A Cell whose writer cannot be reached is refused with the reason
  a barrier naming it fails with.
  """
  @spec await_record_async(binary(), non_neg_integer(), term(), term()) ::
          {:ok, term()} | {:error, String.t()}
  def await_record_async(cell_id, seq, reqids, label) do
    case record_writer(cell_id) do
      {:ok, writer} -> {:ok, WalWriter.await_durable_async(writer, seq, reqids, label)}
      {:error, _reason} -> {:error, record_not_durable(cell_id, seq)}
    end
  end

  @doc "Why a barrier naming the record at `seq` of `cell_id` fails."
  @spec record_not_durable(binary(), non_neg_integer()) ::
          {:record_not_durable, String.t(), non_neg_integer()}
  def record_not_durable(cell_id, seq),
    do: {:record_not_durable, Outbox.cell_label(cell_id), seq}

  @doc """
  The entry naming `records` (`record_ranges/1`) is durable: each Cell's
  writer releases their outputs.
  """
  @spec release_records([{binary(), non_neg_integer(), non_neg_integer()}]) :: :ok
  def release_records(records) do
    for {id, first, last} <- records,
        {:ok, writer} <- [record_writer(id)],
        do: WalWriter.commit_records(writer, first, last)

    :ok
  end

  @doc """
  The outbox entries the instance's live Cell writers still owe the sinks:
  dispatchable, or in the chain awaiting confirmation. An entry held for a
  stage marker is owed nothing, and a writer that is not running, or exits
  before it answers, holds its debt on disk for its next boot.
  """
  @spec owed_outputs(binary()) :: non_neg_integer()
  def owed_outputs(instance_id) do
    for writer <- Supervisor.live_writers(instance_id),
        %{pending: pending, awaiting: awaiting} <- [WalWriter.outbox_status(writer)],
        reduce: 0,
        do: (owed -> owed + pending + awaiting)
  end

  @doc """
  Whether any of `records` (`record_ranges/1`) sits above its Cell's persisted
  flush cursor, so the sinks may still be owed its output. The cursor lags what
  the sinks confirmed, never leads it, so an entry naming one is kept a little
  longer than needed and never too briefly.
  """
  @spec unflushed?([{binary(), non_neg_integer(), non_neg_integer()}]) :: boolean()
  def unflushed?(records) do
    Enum.any?(records, fn {id, _first, seq} ->
      case Wire.decode_cell_id(id) do
        {:ok, cell} -> seq > Lineage.progress(cell).outbox_flushed_seq
        {:error, _} -> false
      end
    end)
  end

  defp record_writer(cell_id) do
    with {:ok, cell} <- Wire.decode_cell_id(cell_id), do: Supervisor.find_or_start(cell)
  end
end

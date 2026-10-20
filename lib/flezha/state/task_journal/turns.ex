defmodule Flezha.State.TaskJournal.Turns do
  @moduledoc """
  A steps instance's failed turns in the dead-letter queue: parking a turn's
  message as the records step of its `dead_lettered` commit, announcing it once
  that record is durable, and dropping a park a crash left ahead of its record.
  The count of failed turns is the journal's fold
  (`Flezha.State.TaskJournal.failed_turns/1`).
  """

  require Logger

  alias Flezha.Dlq.{Entry, Store}
  alias Flezha.State.{InputLog, TaskJournal}

  # The one DLQ slot, and cell key, every failed turn of an instance parks under.
  @turn_slot "_turn"

  @doc """
  Park a failed turn's message in the dead-letter queue without waiting, as
  the "records" step of its dead-lettered iteration commit: the bytes at the
  turn's consumed offset are read from the instance's input log (retained,
  since the floor is still below it) and appended under the `_turn` slot,
  keyed by `_turn@<offset>` so a re-parked turn lands on the same entry. The
  append is issued under `label` into `reqids`; its answer, `{:ok, entry_ref}`
  once the entry is durable, comes back as a message for
  `:gen_server.check_response/3`. A turn whose record is gone parks without
  its bytes, and says so.
  """
  @spec park_turn_async(binary(), map(), :gen_server.request_id_collection(), term()) ::
          {:ok, :gen_server.request_id_collection()} | {:error, term()}
  def park_turn_async(instance_id, turn, reqids, label) when is_binary(instance_id) do
    fields = %{
      relay_class: instance_id,
      tasklet_id: @turn_slot,
      cell_key: @turn_slot,
      channel: turn.entry_channel,
      origin_offset: turn.consumed_offset,
      error: turn_error(turn),
      attempts: turn.attempts,
      payload: turn_payload(instance_id, turn.consumed_offset)
    }

    with {:ok, entry} <- Entry.new(fields),
         do: {:ok, :gen_server.send_request(Store, {:append, entry}, label, reqids)}
  end

  @doc """
  Announce a failed turn's dead letter (`Flezha.Dlq.Store.announce/3`): called
  once the turn's `dead_lettered` record is durable and before its input is
  released, since a caller's token rides the input-log record the turn consumed.
  """
  @spec announce_turn(binary(), map(), String.t()) :: :ok
  def announce_turn(instance_id, turn, entry_ref) do
    offset = turn.consumed_offset

    Store.announce(instance_id, {:ok, entry_ref}, %{
      token: offset && InputLog.reply_to(instance_id, offset),
      error: turn_error(turn),
      channel: turn.entry_channel,
      offset: offset,
      tasklet: turn.tasklet,
      attempts: turn.attempts
    })
  end

  defp turn_error(turn),
    do: "tasklet `#{turn.tasklet}` failed after #{turn.attempts} attempt(s): #{turn.error}"

  @doc """
  Remove the instance's `_turn` DLQ entries whose origin offset is above its
  floor: each is a turn parked before a crash that landed ahead of its
  `dead_lettered` record, and which the bind is about to run again. An entry
  at or below the floor belongs to a recorded dead letter and stays.
  """
  @spec drop_unrecorded_turns(binary()) :: :ok
  def drop_unrecorded_turns(instance_id) when is_binary(instance_id) do
    floor = TaskJournal.released_offset(instance_id)

    for %{"tasklet_id" => @turn_slot, "origin_offset" => offset, "id" => id} <-
          Store.list(instance_id),
        is_integer(offset) and (floor == nil or offset > floor),
        do: Store.resolve("#{instance_id}/#{@turn_slot}/#{id}")

    :ok
  end

  defp turn_payload(_instance_id, nil), do: nil

  defp turn_payload(instance_id, offset) do
    case InputLog.read_range(instance_id, offset, offset) do
      [{^offset, message, _metadata}] ->
        message

      _gone_or_refused ->
        Logger.warning(
          "[task_journal] #{instance_id}: the failed turn at input-log offset #{offset} " <>
            "is no longer in the log; it is parked without its bytes"
        )

        nil
    end
  end
end

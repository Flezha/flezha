defmodule Flezha.State.InputLog.MsgIndex do
  @moduledoc """
  An input log's index over its retained records: each `flz.msg_id` with the
  offset of the record carrying it, which absorbs a payload sent again, and the
  `flz.deadline_at` of each record carrying a live call.

  The log keeps a record carrying an id until its append time plus
  `call_retention_margin_ms` has passed, and one carrying a call until the
  call's deadline plus that margin, whatever the watermark allows (see
  `floor/2`). So a sender that never saw its answer can send again within the
  margin and be absorbed, and a recovered caller finds its `Response`. Ids and
  deadlines are learned from the append, or from the one walk of the file that
  builds the index, so a deadline costs no read of its own; the append times
  the id hold needs are read by the sweep, only over the records it releases,
  never by an append. A log whose senders never stamp an id walks nothing.
  """

  alias Flezha.Config.Durations
  alias Flezha.Relay.Calls.Inbound
  alias Flezha.State.RecordFrame

  @msg_id_key "flz.msg_id"
  @deadline_key "flz.deadline_at"

  defstruct ids: %{}, deadlines: %{}

  @type t :: %__MODULE__{
          ids: %{binary() => non_neg_integer()},
          deadlines: %{non_neg_integer() => integer()}
        }

  @typedoc "What one append indexes: its message id, with its call's deadline when it has one."
  @type key :: binary() | {binary(), integer()} | nil

  @doc "The key a record's metadata indexes under; `nil` for a record without an id."
  @spec key(map()) :: key()
  def key(metadata) do
    case {Map.get(metadata, @msg_id_key), deadline(metadata)} do
      {id, _deadline} when not is_binary(id) or id == "" -> nil
      {id, nil} -> id
      {id, deadline_at} -> {id, deadline_at}
    end
  end

  defp deadline(metadata) do
    with at when is_binary(at) <- Map.get(metadata, @deadline_key),
         {deadline_at, ""} <- Integer.parse(at),
         do: deadline_at,
         else: (_ -> nil)
  end

  @doc "Build the index by one walk of the log file."
  @spec build(:file.io_device(), Path.t()) :: t()
  def build(device, path) do
    {:ok, indexed} =
      RecordFrame.index_by(device, path, fn {_seq, _header, body} ->
        {_message, metadata} = :erlang.binary_to_term(body)
        key(metadata)
      end)

    Enum.reduce(indexed, %__MODULE__{}, fn {key, offset}, index -> note(index, key, offset) end)
  end

  @doc "Index the record at `offset` under `key`."
  @spec note(t(), key(), non_neg_integer()) :: t()
  def note(index, nil, _offset), do: index

  def note(index, {id, deadline_at}, offset),
    do: %{note(index, id, offset) | deadlines: Map.put(index.deadlines, offset, deadline_at)}

  def note(index, id, offset), do: %{index | ids: Map.put(index.ids, id, offset)}

  @doc "`{:duplicate, offset}` when a retained record carries `key`'s id, else `:none`."
  @spec retained(t(), key()) :: {:duplicate, non_neg_integer()} | :none
  def retained(_index, nil), do: :none
  def retained(index, {id, _deadline_at}), do: retained(index, id)

  def retained(index, id) do
    case Map.fetch(index.ids, id) do
      {:ok, offset} -> {:duplicate, offset}
      :error -> :none
    end
  end

  @doc "The index once the records at the offsets `gone?` names are truncated."
  @spec drop(t(), (non_neg_integer() -> boolean())) :: t()
  def drop(index, gone?) do
    %{
      index
      | ids: Map.reject(index.ids, fn {_id, offset} -> gone?.(offset) end),
        deadlines: Map.reject(index.deadlines, fn {offset, _} -> gone?.(offset) end)
    }
  end

  @doc """
  The lowest offset a live call or a recent message id still holds, or `nil`:
  no truncation may reach it. A deadline plus the margin that has passed holds
  nothing and is dropped. Only an id below `sweep_floor` can lower the floor,
  so only those are judged: `aged_out.(time, from)` answers the highest offset
  from `from` on appended before `time`, and is not asked when no id is below
  the floor.
  """
  @spec floor(
          t(),
          non_neg_integer(),
          (integer(), non_neg_integer() -> non_neg_integer() | nil | {:error, term()})
        ) :: {non_neg_integer() | nil, t()}
  def floor(index, sweep_floor, aged_out) do
    now = System.system_time(:millisecond)
    live = Map.filter(index.deadlines, fn {_offset, at} -> Inbound.retained_until(at) > now end)
    held = Map.keys(live) ++ recent_ids(index.ids, sweep_floor, aged_out, now)
    {Enum.min(held, fn -> nil end), %{index | deadlines: live}}
  end

  # A record appended at or before `now - margin` has had its margin.
  defp recent_ids(ids, sweep_floor, aged_out, now) do
    case for {_id, offset} <- ids, offset < sweep_floor, do: offset do
      [] ->
        []

      below ->
        case aged_out.(now - Durations.get(:call_retention_margin_ms) + 1, Enum.min(below)) do
          aged when is_integer(aged) -> Enum.filter(below, &(&1 > aged))
          # No record that old, or a walk the log refused: every id is held.
          _none_or_refused -> below
        end
    end
  end
end

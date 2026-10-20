defmodule Flezha.State.InputLog.MsgIndex do
  @moduledoc """
  An input log's index over its retained records: each `flz.msg_id` with the
  offset of the record carrying it, which absorbs a payload sent again, and the
  `flz.deadline_at` of each record carrying a live call.

  A call's request in its callee's log and its `Response` in its caller's carry
  a deadline, and the log keeps each until that deadline plus
  `call_retention_margin_ms` has passed, whatever the watermark allows (see
  `floor/1`), so a request sent again is absorbed by its message id and a
  recovered caller finds its `Response`. Both are learned from the append, or
  from the one walk of the file that builds the index, so a deadline costs no
  read of its own. A log whose senders never stamp an id walks nothing.
  """

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

  @doc "The index once everything below `floor` is truncated."
  @spec trim(t(), non_neg_integer()) :: t()
  def trim(index, floor) do
    %{
      index
      | ids: Map.reject(index.ids, fn {_id, offset} -> offset < floor end),
        deadlines: Map.reject(index.deadlines, fn {offset, _} -> offset < floor end)
    }
  end

  @doc """
  The lowest offset a live call still holds, or `nil`: no truncation may reach
  it. A deadline plus the margin that has passed holds nothing and is dropped.
  """
  @spec floor(t()) :: {non_neg_integer() | nil, t()}
  def floor(index) do
    now = System.system_time(:millisecond)
    live = Map.filter(index.deadlines, fn {_offset, at} -> Inbound.retained_until(at) > now end)
    lowest = if live == %{}, do: nil, else: live |> Map.keys() |> Enum.min()
    {lowest, %{index | deadlines: live}}
  end
end

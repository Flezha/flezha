defmodule Flezha.Process.Backlog do
  @moduledoc """
  A FIFO bounded by payload bytes and by entries, for a process that holds what
  its consumer has not taken yet: a listener's relay, a shared plugin's
  forwarder.

  It holds and it makes room; what a holder does with an entry that still does
  not fit is the holder's own rule, so it is not decided here. Room is made by
  dropping the oldest entries a holder names droppable.
  """

  @type t :: %__MODULE__{
          queue: :queue.queue({term(), non_neg_integer()}),
          bytes: non_neg_integer(),
          frames: non_neg_integer(),
          max_bytes: pos_integer(),
          max_frames: pos_integer()
        }

  @enforce_keys [:max_bytes, :max_frames]
  defstruct [:max_bytes, :max_frames, queue: :queue.new(), bytes: 0, frames: 0]

  @spec new(pos_integer(), pos_integer()) :: t()
  def new(max_bytes, max_frames), do: %__MODULE__{max_bytes: max_bytes, max_frames: max_frames}

  @doc "Whether one more entry of `size` bytes stays inside both bounds."
  @spec fits?(t(), non_neg_integer()) :: boolean()
  def fits?(%__MODULE__{} = b, size),
    do: b.frames + 1 <= b.max_frames and b.bytes + size <= b.max_bytes

  @doc "Append `entry`, counted at `size` bytes, whether or not it fits."
  @spec push(t(), term(), non_neg_integer()) :: t()
  def push(%__MODULE__{} = b, entry, size),
    do: %{
      b
      | queue: :queue.in({entry, size}, b.queue),
        bytes: b.bytes + size,
        frames: b.frames + 1
    }

  @doc """
  Drop the oldest entries `droppable?` accepts until `size` fits or none is
  left, answering the backlog and how many it dropped.
  """
  @spec make_room(t(), non_neg_integer(), (term() -> boolean())) :: {t(), non_neg_integer()}
  def make_room(%__MODULE__{} = b, size, droppable?), do: make_room(b, size, droppable?, 0)

  defp make_room(b, size, droppable?, dropped) do
    cond do
      fits?(b, size) -> {b, dropped}
      gone = drop_oldest(b, droppable?) -> make_room(gone, size, droppable?, dropped + 1)
      true -> {b, dropped}
    end
  end

  # The head is taken in O(1) when it is droppable; only a holder that keeps
  # some entries walks past them.
  defp drop_oldest(b, droppable?) do
    case :queue.peek(b.queue) do
      :empty ->
        nil

      {:value, {entry, gone}} ->
        if droppable?.(entry),
          do: shrink(b, :queue.drop(b.queue), gone),
          else: drop_past_kept(b, droppable?)
    end
  end

  defp drop_past_kept(b, droppable?) do
    case Enum.split_while(:queue.to_list(b.queue), fn {entry, _size} -> not droppable?.(entry) end) do
      {_all, []} -> nil
      {before, [{_entry, gone} | rest]} -> shrink(b, :queue.from_list(before ++ rest), gone)
    end
  end

  defp shrink(b, queue, gone),
    do: %{b | queue: queue, bytes: b.bytes - gone, frames: b.frames - 1}

  @doc "The oldest entry and the backlog without it, or `:empty`."
  @spec out(t()) :: {:empty, t()} | {term(), t()}
  def out(%__MODULE__{} = b) do
    case :queue.out(b.queue) do
      {:empty, _} ->
        {:empty, b}

      {{:value, {entry, size}}, rest} ->
        {entry, shrink(b, rest, size)}
    end
  end

  @doc "The same bounds, holding nothing."
  @spec clear(t()) :: t()
  def clear(%__MODULE__{} = b), do: %{b | queue: :queue.new(), bytes: 0, frames: 0}
end

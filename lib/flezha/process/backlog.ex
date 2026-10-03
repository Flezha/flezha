defmodule Flezha.Process.Backlog do
  @moduledoc """
  A FIFO bounded by payload bytes and by entries, for a process that holds what
  its consumer has not taken yet: a listener's relay, a shared plugin's
  forwarder, a sink's queue of outputs to send.

  It holds and it makes room. Room is made by dropping the oldest entries a
  holder names droppable. `admit/5` is the one reading of a sink's
  `queue_overflow` policy; what becomes of an entry it refuses or drops (a
  park, a reply, a count) is the holder's own rule, so it is not decided here.
  """

  @type t :: %__MODULE__{
          queue: :queue.queue({term(), non_neg_integer()}),
          bytes: non_neg_integer(),
          frames: non_neg_integer(),
          max_bytes: pos_integer() | :infinity,
          max_frames: pos_integer()
        }

  @type policy :: :drop_nothing | :drop_newest | :drop_oldest

  @enforce_keys [:max_bytes, :max_frames]
  defstruct [:max_bytes, :max_frames, queue: :queue.new(), bytes: 0, frames: 0]

  @doc "An empty backlog; `:infinity` bytes bounds it by entries alone."
  @spec new(pos_integer() | :infinity, pos_integer()) :: t()
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
  def make_room(%__MODULE__{} = b, size, droppable?) do
    {b, dropped} = evict(b, size, droppable?, [])
    {b, length(dropped)}
  end

  @doc """
  Admit `entry`, counted at `size` bytes, under a full queue's `policy`:

    * `{:queued, backlog, dropped}`: `entry` is in, and `dropped` are the
      entries `:drop_oldest` took out to make room, oldest first.
    * `{:refused, backlog, dropped}`: `entry` is not in. `:drop_newest` refuses
      it whenever it does not fit; `:drop_oldest` once dropping every entry
      `droppable?` accepts (`dropped`) did not make room.
    * `{:full, backlog}`: `:drop_nothing`, and it does not fit. The holder makes
      its caller wait, or keeps it past the bound with `push/3`.
  """
  @spec admit(t(), term(), non_neg_integer(), policy(), (term() -> boolean())) ::
          {:queued | :refused, t(), [term()]} | {:full, t()}
  def admit(%__MODULE__{} = b, entry, size, policy, droppable? \\ fn _held -> true end) do
    cond do
      fits?(b, size) -> {:queued, push(b, entry, size), []}
      policy == :drop_nothing -> {:full, b}
      policy == :drop_newest -> {:refused, b, []}
      policy == :drop_oldest -> admit_dropping(b, entry, size, droppable?)
    end
  end

  defp admit_dropping(b, entry, size, droppable?) do
    {b, dropped} = evict(b, size, droppable?, [])

    if fits?(b, size),
      do: {:queued, push(b, entry, size), dropped},
      else: {:refused, b, dropped}
  end

  defp evict(b, size, droppable?, dropped) do
    if fits?(b, size) do
      {b, Enum.reverse(dropped)}
    else
      case drop_oldest(b, droppable?) do
        {gone, b} -> evict(b, size, droppable?, [gone | dropped])
        nil -> {b, Enum.reverse(dropped)}
      end
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
          do: {entry, shrink(b, :queue.drop(b.queue), gone)},
          else: drop_past_kept(b, droppable?)
    end
  end

  defp drop_past_kept(b, droppable?) do
    case Enum.split_while(:queue.to_list(b.queue), fn {entry, _size} -> not droppable?.(entry) end) do
      {_all, []} ->
        nil

      {before, [{entry, gone} | rest]} ->
        {entry, shrink(b, :queue.from_list(before ++ rest), gone)}
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

  @doc "The oldest entry, left in place, or `:empty`."
  @spec peek(t()) :: {:value, term()} | :empty
  def peek(%__MODULE__{} = b) do
    case :queue.peek(b.queue) do
      {:value, {entry, _size}} -> {:value, entry}
      :empty -> :empty
    end
  end

  @doc "Every entry, oldest first."
  @spec to_list(t()) :: [term()]
  def to_list(%__MODULE__{} = b), do: for({entry, _size} <- :queue.to_list(b.queue), do: entry)

  @doc """
  `entries`, each `{entry, size}`, put back ahead of every entry held, in their
  order, whether or not they fit: what was taken out to send and must go again.
  """
  @spec requeue(t(), [{term(), non_neg_integer()}]) :: t()
  def requeue(%__MODULE__{} = b, entries) do
    %{
      b
      | queue: :queue.join(:queue.from_list(entries), b.queue),
        bytes: Enum.reduce(entries, b.bytes, fn {_entry, size}, n -> n + size end),
        frames: b.frames + length(entries)
    }
  end

  @doc "The same bounds, holding nothing."
  @spec clear(t()) :: t()
  def clear(%__MODULE__{} = b), do: %{b | queue: :queue.new(), bytes: 0, frames: 0}
end

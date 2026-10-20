defmodule Flezha.Process.Backlog do
  @moduledoc """
  A FIFO bounded by payload bytes and by entries, for a process that holds what
  its consumer has not taken yet: a listener's receiver, a shared plugin's
  forwarder, a sink's queue of outputs to send.

  It holds and it makes room. Room is made by dropping the oldest entries a
  holder names droppable. `admit/5` is the one reading of a sink's
  `queue_overflow` policy; what becomes of an entry it refuses or drops (a
  park, a reply, a count) is the holder's own rule, so it is not decided here.

  It is also a sink's admission, in two forms. The cast form is a soft bound:
  `dispatch/2` casts while the worker's full flag is down and waits in a call
  while it is up, and the worker answers each arrival with `offer/5`, which
  admits a cast already on its way past the bound, holds a waiter until
  `room/1` finds space, at most `block_ms`, and keeps the flag in step. The
  call form is a hard bound for a sink that buffers every enqueue in a call
  and never drops: `park_or_reply/2` holds the caller until
  `release_waiters/1` finds room, with no block timeout.
  """

  alias Flezha.Config.Durations
  alias Flezha.Process.Timer

  @type t :: %__MODULE__{
          queue: :queue.queue({term(), non_neg_integer()}),
          bytes: non_neg_integer(),
          frames: non_neg_integer(),
          max_bytes: pos_integer() | :infinity,
          max_frames: pos_integer(),
          policy: policy(),
          block_ms: pos_integer() | nil,
          flag: :atomics.atomics_ref() | nil,
          tag: term(),
          full: boolean(),
          waiters: :queue.queue({waiter(), term(), non_neg_integer()}),
          timers: Timer.timers()
        }

  @type policy :: :drop_nothing | :drop_newest | :drop_oldest

  @typedoc """
  Who an arrival came from: a cast already on its way, a dispatch that read
  the flag up and waits for room, or a synchronous caller.
  """
  @type waiter :: :cast | {:wait, GenServer.from()} | {:call, GenServer.from()}

  @enforce_keys [:max_bytes, :max_frames]
  defstruct [
    :max_bytes,
    :max_frames,
    :block_ms,
    :flag,
    :tag,
    queue: :queue.new(),
    bytes: 0,
    frames: 0,
    policy: :drop_nothing,
    full: false,
    waiters: :queue.new(),
    timers: %{}
  ]

  @doc "An empty backlog; `:infinity` bytes bounds it by entries alone."
  @spec new(pos_integer() | :infinity, pos_integer()) :: t()
  def new(max_bytes, max_frames), do: %__MODULE__{max_bytes: max_bytes, max_frames: max_frames}

  @doc """
  A sink's queue, admitting under `opts`: `:policy` (`:drop_nothing` by
  default), `:block_ms`, the longest a waiter waits for room (nil: until there
  is room), `:flag`, the worker's full flag (`flag/0`), which also carries
  `:block_ms` to `dispatch/2`, and `:tag`, which names this queue in its block
  timer's message, `{:block_timeout, tag, waiter}`.
  """
  @spec new(pos_integer() | :infinity, pos_integer(), keyword()) :: t()
  def new(max_bytes, max_frames, opts) do
    b = struct(new(max_bytes, max_frames), opts)
    if b.flag && b.block_ms, do: :atomics.put(b.flag, 2, b.block_ms)
    b
  end

  @doc """
  A worker's full flag: up while any queue sharing it has a waiter or an entry
  past its bound, and the block timeout of the queues sharing it (`new/3`).
  Published as the registry value of the worker `name` names, when it is
  registered, for `dispatch/2` to read.
  """
  @spec flag(GenServer.name() | nil) :: :atomics.atomics_ref()
  def flag(name \\ nil) do
    flag = :atomics.new(2, [])

    with {:via, Registry, {registry, key}} <- name,
         do: Registry.update_value(registry, key, fn _ -> flag end)

    flag
  end

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
  def admit(%__MODULE__{} = b, entry, size, policy, droppable? \\ &any/1) do
    cond do
      fits?(b, size) -> {:queued, push(b, entry, size), []}
      policy == :drop_nothing -> {:full, b}
      policy == :drop_newest -> {:refused, b, []}
      policy == :drop_oldest -> admit_dropping(b, entry, size, droppable?)
    end
  end

  defp any(_held), do: true

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

  @doc "The same bounds, holding nothing; a sink's queue then admits its waiters (`room/1`)."
  @spec clear(t()) :: t()
  def clear(%__MODULE__{} = b), do: room(%{b | queue: :queue.new(), bytes: 0, frames: 0})

  ## The cast form

  @doc """
  The client half: hand `message` to the worker `server` names. While its full
  flag is down `message` is cast; while it is up the dispatch waits in a call
  with the same `message` until the worker admits it or lets it go, or its
  queue's block timeout and `backlog_wait_headroom_ms` have passed. `:none`
  when no worker is registered.
  """
  @spec dispatch(GenServer.server(), term()) :: :ok | :none
  def dispatch({:via, Registry, {registry, key}}, message) do
    case Registry.lookup(registry, key) do
      # The flag is published once the worker's `init/1` runs; until then it is nil.
      [{pid, flag}] ->
        if flag != nil and :atomics.get(flag, 1) > 0,
          do: wait(pid, message, :atomics.get(flag, 2)),
          else: GenServer.cast(pid, message)

      [] ->
        :none
    end
  end

  def dispatch(pid, message) when is_pid(pid), do: GenServer.cast(pid, message)

  # The worker's block timer is the policy; this bound only stops a worker too
  # busy to reach it from holding the caller.
  defp wait(pid, message, 0), do: call(pid, message, :infinity)

  defp wait(pid, message, block_ms),
    do: call(pid, message, block_ms + Durations.get(:backlog_wait_headroom_ms))

  defp call(pid, message, timeout) do
    GenServer.call(pid, message, timeout)
  catch
    # The worker stopped with the message waiting, or never answered in time.
    :exit, _gone -> :ok
  end

  @doc """
  The server half, for one arrival: `entry`, of `size` bytes, from `waiter`.

    * `{:queued, backlog, dropped}`: `entry` is in, and a dispatch waiter is
      let go. `dropped` are the entries `:drop_oldest` took out, oldest first,
      each one `droppable?` accepts.
    * `{:refused, backlog, dropped}`: `entry` is not in (`:drop_newest`, or
      `:drop_oldest` with nothing droppable left). A dispatch waiter is let go
      and a synchronous caller answered `{:error, {:queue_full, :overflow}}`.
    * `{:waiting, backlog, []}`: `:drop_nothing` and no room. A cast, or any
      arrival at an empty queue, is admitted past the bound instead
      (`:queued`). Either raises the flag.

  What becomes of `dropped` and of a refused `entry` is the holder's.
  """
  @spec offer(t(), term(), non_neg_integer(), waiter(), (term() -> boolean())) ::
          {:queued | :waiting | :refused, t(), [term()]}
  def offer(%__MODULE__{} = b, entry, size, waiter, droppable? \\ &any/1) do
    case admit(b, entry, size, b.policy, droppable?) do
      {:queued, b, dropped} ->
        release(waiter)
        {:queued, b, dropped}

      {:refused, b, dropped} ->
        refuse(waiter)
        {:refused, b, dropped}

      # An entry larger than the bound comes into an empty queue, or it never would.
      {:full, b} when waiter == :cast or b.frames == 0 ->
        release(waiter)
        {:queued, b |> push(entry, size) |> mark(true), []}

      {:full, b} ->
        {:waiting, b |> hold(entry, size, waiter) |> mark(true), []}
    end
  end

  defp hold(b, entry, size, waiter) do
    b = %{b | waiters: :queue.in({waiter, entry, size}, b.waiters)}

    if b.block_ms,
      do: %{b | timers: Timer.arm(b.timers, waiter, b.block_ms, {:block_timeout, b.tag, waiter})},
      else: b
  end

  @doc """
  Entries were taken out: the waiters that fit now come in, oldest first (one
  larger than the bound once the queue is empty), and the flag comes down once
  none is left and there is room for one more.
  """
  @spec room(t()) :: t()
  def room(%__MODULE__{full: false} = b), do: b

  def room(b) do
    case :queue.peek(b.waiters) do
      {:value, {waiter, entry, size}} ->
        if fits?(b, size) or b.frames == 0 do
          release(waiter)
          rest = %{b | waiters: :queue.drop(b.waiters), timers: Timer.cancel(b.timers, waiter)}
          room(push(rest, entry, size))
        else
          b
        end

      :empty ->
        mark(b, not fits?(b, 0))
    end
  end

  @doc """
  `waiter`'s wait for room ran out (`{:block_timeout, tag, waiter}`). A
  synchronous caller is answered `{:error, {:queue_full, :timeout}}`; a
  dispatch waiter is let go and its entry is answered, overflowed.
  """
  @spec block_timeout(t(), waiter()) :: {t(), [term()]}
  def block_timeout(%__MODULE__{} = b, waiter) do
    b = %{b | timers: Timer.fired(b.timers, waiter)}

    case Enum.split_with(:queue.to_list(b.waiters), &(elem(&1, 0) == waiter)) do
      {[{{:wait, from}, entry, _size}], kept} ->
        GenServer.reply(from, :ok)
        {room(%{b | waiters: :queue.from_list(kept)}), [entry]}

      {[{{:call, from}, _entry, _size}], kept} ->
        GenServer.reply(from, {:error, {:queue_full, :timeout}})
        {room(%{b | waiters: :queue.from_list(kept)}), []}

      {[], _kept} ->
        {b, []}
    end
  end

  @doc "Every waiter let go, oldest first, with the entries they held; their timers cancelled."
  @spec release_waiting(t()) :: {[term()], t()}
  def release_waiting(%__MODULE__{} = b) do
    waiting = :queue.to_list(b.waiters)
    Enum.each(waiting, &release(elem(&1, 0)))
    timers = Enum.reduce(waiting, b.timers, &Timer.cancel(&2, elem(&1, 0)))
    {Enum.map(waiting, &elem(&1, 1)), room(%{b | waiters: :queue.new(), timers: timers})}
  end

  @doc "How many arrivals wait for room."
  @spec waiting(t()) :: non_neg_integer()
  def waiting(%__MODULE__{} = b), do: :queue.len(b.waiters)

  defp release({:wait, from}), do: GenServer.reply(from, :ok)
  defp release(_cast_or_call), do: :ok

  defp refuse({:call, from}), do: GenServer.reply(from, {:error, {:queue_full, :overflow}})
  defp refuse(waiter), do: release(waiter)

  # The flag counts the queues sharing it that are full, so it is up while any is.
  defp mark(%{full: up} = b, up), do: b
  defp mark(%{flag: nil} = b, up), do: %{b | full: up}

  defp mark(b, up) do
    :atomics.add(b.flag, 1, if(up, do: 1, else: -1))
    %{b | full: up}
  end

  ## The call form

  @doc "Has the worker's buffer (`buffer_len`) reached `queue_max`?"
  @spec saturated?(map()) :: boolean()
  def saturated?(state), do: state.buffer_len >= state.queue_max

  @doc """
  Answer an enqueueing caller whose entry is buffered, or hold it in `waiters`
  while the buffer is saturated: delay, never loss. The worker state carries
  `:buffer_len`, `:queue_max` and `:waiters`.
  """
  @spec park_or_reply(map(), GenServer.from()) :: {:reply, :ok, map()} | {:noreply, map()}
  def park_or_reply(state, from) do
    if saturated?(state),
      do: {:noreply, %{state | waiters: [from | state.waiters]}},
      else: {:reply, :ok, state}
  end

  @doc "Answer every held caller once the buffer is back under `queue_max`."
  @spec release_waiters(map()) :: map()
  def release_waiters(%{waiters: []} = state), do: state

  def release_waiters(state) do
    if saturated?(state) do
      state
    else
      Enum.each(state.waiters, &GenServer.reply(&1, :ok))
      %{state | waiters: []}
    end
  end
end

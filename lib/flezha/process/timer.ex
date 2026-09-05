defmodule Flezha.Process.Timer do
  @moduledoc """
  A process NAMES its timers and the rest is done once: arming, re-arming,
  cancelling, and the guard against a message from a timer that had already
  fired when it was cancelled.

      use Flezha.Process.Timer, keys: [:tick, :sweep]

      state = arm_timer(state, :tick, 5_000, :tick)
      state = cancel_timer(state, :tick)

  `use` declares the keys and injects the state-threading wrappers over a
  `timers` field the module holds (a plain map is enough; a struct declares
  `timers: %{}`). Arming a key that was not declared raises at the call, so a
  timer nobody wrote down cannot be armed.

  A declared key names a FAMILY as well as a timer: with `:reconnect`
  declared, `{:reconnect, node}` is a live key too. That is what lets a
  per-item timer — one per node, one per subscription — be declared once
  rather than kept in a hand-rolled map beside the state.

  The stale-fire guard is why the message is stored beside the reference:
  `Process.cancel_timer/1` answers `false` when the timer had already fired,
  and its message is then sitting in the mailbox with nothing left to make
  sense of it. Cancelling flushes exactly that message and nothing else.
  """

  @type key :: atom()
  @type timers :: %{key() => {reference(), term()}}

  @doc "An empty timer field."
  @spec new() :: timers()
  def new(), do: %{}

  @doc """
  Arm `key` to send `msg` to this process in `ms`. Re-arming a live key
  cancels the old timer first, so a key holds at most one timer.
  """
  @spec arm(timers(), key(), non_neg_integer(), term()) :: timers()
  def arm(timers, key, ms, msg) when is_map(timers) and is_integer(ms) and ms >= 0 do
    timers = cancel(timers, key)
    Map.put(timers, key, {Process.send_after(self(), msg, ms), msg})
  end

  @doc "Cancel `key` if armed, flushing its message when it had already fired."
  @spec cancel(timers(), key()) :: timers()
  def cancel(timers, key) when is_map(timers) do
    case Map.pop(timers, key) do
      {nil, timers} ->
        timers

      {{ref, msg}, timers} ->
        if Process.cancel_timer(ref) == false, do: flush(msg)
        timers
    end
  end

  @doc "Cancel every armed key."
  @spec cancel_all(timers()) :: timers()
  def cancel_all(timers) when is_map(timers) do
    Enum.reduce(Map.keys(timers), timers, &cancel(&2, &1))
  end

  @doc """
  Clear `key` because its message has just been received. Returns the timers
  without it — the handler that woke on the message is what retires the row,
  and re-arming is a separate, explicit `arm/4`.
  """
  @spec fired(timers(), key()) :: timers()
  def fired(timers, key) when is_map(timers), do: Map.delete(timers, key)

  @doc "True when `key` holds a live timer."
  @spec armed?(timers(), key()) :: boolean()
  def armed?(timers, key) when is_map(timers), do: Map.has_key?(timers, key)

  @doc "The keys currently armed."
  @spec armed(timers()) :: [key()]
  def armed(timers) when is_map(timers), do: Map.keys(timers)

  # A cancelled-too-late timer has already put its message in our mailbox.
  # Take that one message and leave everything else in place.
  defp flush(msg) do
    receive do
      ^msg -> :ok
    after
      0 -> :ok
    end
  end

  defmacro __using__(opts) do
    keys = Keyword.fetch!(opts, :keys)

    quote do
      @timer_keys unquote(keys)

      @doc false
      def __timer_keys__, do: @timer_keys

      defp arm_timer(state, key, ms, msg) do
        put_timers(state, Flezha.Process.Timer.arm(timers_of(state), declared!(key), ms, msg))
      end

      defp cancel_timer(state, key) do
        put_timers(state, Flezha.Process.Timer.cancel(timers_of(state), declared!(key)))
      end

      defp cancel_all_timers(state) do
        put_timers(state, Flezha.Process.Timer.cancel_all(timers_of(state)))
      end

      defp timer_fired(state, key) do
        put_timers(state, Flezha.Process.Timer.fired(timers_of(state), declared!(key)))
      end

      defp timer_armed?(state, key) do
        Flezha.Process.Timer.armed?(timers_of(state), declared!(key))
      end

      # A key is the declared atom, or a `{family, id}` whose family is
      # declared — a per-item timer is still a timer this module wrote down.
      defp declared!(key) when key in @timer_keys, do: key
      defp declared!({family, _id} = key) when family in @timer_keys, do: key

      # A struct that forgot the field would silently gain an undeclared key
      # and become invalid, so it is refused at the first arm instead.
      defp timers_of(state) when is_struct(state), do: Map.fetch!(state, :timers)
      defp timers_of(state), do: Map.get(state, :timers) || Flezha.Process.Timer.new()

      defp put_timers(state, timers), do: Map.put(state, :timers, timers)
    end
  end
end

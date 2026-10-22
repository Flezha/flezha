defmodule Flezha.Config.Backoff do
  @moduledoc """
  Every wait-before-retry in the controller, as one table of declared ladders,
  and the one arithmetic every ladder waits by.

  A row is a key and the schedule that key waits: rungs (a list, indexed by the
  attempt and held at the last) or a geometric `base`/`cap` (`base · 2^(n−1)`),
  whether the wait is jittered, how the attempt count resets, and the attempt at which the
  site gives up. `base` and `cap` may name a `Flezha.Config.Durations` row
  instead of a number, so a user's knob is the ladder's bound without the
  ladder holding a second copy of it.

  Three ways in, one arithmetic:

    * `%__MODULE__{}` — the per-`{primitive, target}` cooldown the convergence
      Loop and the Watchdog carry in their state: `filter/3` drops planned ops
      still cooling, `update/4` records an outcome.
    * `delay/3` — the millisecond value for an attempt, for a site that arms
      its own timer.
    * `wait/4` — `delay/3` and the sleep, for a site that retries in place.

  Every wait and every reset is emitted on `[:flezha, :backoff, :wait]` and
  `[:flezha, :backoff, :reset]` naming the ladder, the target and the attempt,
  so a ladder's behaviour is observable without reading its site.

  **One jitter rule.** `jitter/2` is uniform in `[d/2, 3d/2)` and the cap is
  applied after it, so no row's wait exceeds its cap.
  """

  alias Flezha.Config.Durations

  @wait_event [:flezha, :backoff, :wait]
  @reset_event [:flezha, :backoff, :reset]

  # The convergence loop's rungs. Dense early because the common fault is a
  # runner restart measured in seconds, and a coarse first step punishes that
  # as though it were permanent; the ceiling is unchanged, so a target that is
  # genuinely broken still costs one attempt every five minutes.
  @heal_rungs [
    2_000,
    5_000,
    10_000,
    15_000,
    20_000,
    30_000,
    45_000,
    60_000,
    90_000,
    120_000,
    180_000,
    300_000
  ]

  # The Watchdog's compaction handshake gives up by ATTEMPT COUNT, so its rungs
  # ARE the wall clock it waits before abandoning a compaction. Sharing the heal
  # row would move that wall clock every time the heal rungs are retuned, which
  # is a change to durability behaviour and not to healing.
  @compaction_rungs [5_000, 30_000, 60_000, 300_000]

  # {key, fields}. `rungs` or `base`/`cap`; `jitter` false unless the site
  # spread its attempts; `give_up` is what the site checked with a counter of
  # its own. `reset` is a claim the seam test holds the site to: `:on_success`
  # means an attempt count outlives one call and something announces the reset
  # — `new/1` (the struct clears it in `update/4`) or `reset/2` at the site;
  # `:per_call` means the count lives for one invocation and there is nothing
  # to reset.
  @declared [
    # ---- convergence ------------------------------------------------------
    {:heal, rungs: @heal_rungs, reset: :on_success},
    {:compaction, rungs: @compaction_rungs, reset: :on_success, give_up: 4},

    # ---- cluster and transport --------------------------------------------
    {:cluster_reconnect,
     base: 1_000, cap: :reconnect_max_backoff_ms, jitter: true, reset: :per_call},

    # ---- services ---------------------------------------------------------
    {:no_response, base: 150, cap: 5_000, reset: :per_call, give_up: 8},
    {:port_restart, base: 5_000, cap: 5_000, reset: :per_call},
    {:hub_retry, base: 5_000, cap: 5_000, reset: :per_call},
    {:schedule_spawn, base: 1_000, cap: 60_000, reset: :per_call},

    # ---- plugins and sinks -------------------------------------------------
    {:otlp_post, rungs: [250, 500, 1_000, 2_000, 4_000], reset: :per_call, give_up: 5},
    {:hook_webhook_post, rungs: [1_000, 5_000], reset: :per_call},
    {:http_push, base: 250, cap: 10_000, jitter: true, reset: :per_call},
    {:durability_ship, base: :durability_ship_retry_ms, cap: 30_000, reset: :on_success},
    {:postgres_flush, base: 250, cap: 5_000, reset: :on_success},
    {:file_flush, base: 250, cap: 5_000, reset: :on_success},
    {:forward_deliver, base: 250, cap: 5_000, reset: :on_success},
    {:ws_reconnect, base: 250, cap: 30_000, factor: 1.5, reset: :on_success},
    {:ws_sink_ack, base: 1_000, cap: 30_000, reset: :on_success, give_up: 6}
  ]

  @ladders Map.new(@declared, fn {key, fields} ->
             {key,
              %{
                rungs: fields[:rungs],
                base: fields[:base],
                cap: fields[:cap],
                factor: fields[:factor] || 2,
                jitter: Keyword.get(fields, :jitter, false),
                reset: Keyword.fetch!(fields, :reset),
                give_up: Keyword.get(fields, :give_up, :never)
              }}
           end)

  @type key :: {atom(), term()}
  @type ladder :: atom()
  @type row :: %{
          rungs: [pos_integer()] | nil,
          base: pos_integer() | atom() | nil,
          cap: pos_integer() | atom() | nil,
          factor: number(),
          jitter: boolean(),
          reset: :on_success | :per_call,
          give_up: pos_integer() | :never
        }
  @type t :: %__MODULE__{
          ladder: ladder(),
          entries: %{key() => {non_neg_integer(), integer()}}
        }

  defstruct ladder: :heal, entries: %{}

  @doc "The whole table, one row per ladder key."
  @spec ladders() :: %{ladder() => row()}
  def ladders(), do: @ladders

  @doc "The row `ladder` declares."
  @spec ladder(ladder()) :: row()
  def ladder(ladder), do: Map.fetch!(@ladders, ladder)

  @doc "A cooldown table over `ladder`'s row."
  @spec new(ladder()) :: t()
  def new(ladder) when is_map_key(@ladders, ladder), do: %__MODULE__{ladder: ladder}

  @doc """
  The wait before `attempt` (1-based) of `ladder`: a row this table declares,
  by its key, or a row of the same shape that a plugin package keeps beside its
  own options and names with `:name`, so it waits and announces as the table's
  rows do without being one of them.

  `opts` may carry `:base` and `:cap` — the caller's own knob for a row whose
  bound is a per-plugin setting — and `:rand`, the one determinism seam a test
  injects in place of `:rand.uniform/1`.
  """
  @spec delay(ladder() | map(), pos_integer(), keyword()) :: non_neg_integer()
  def delay(ladder, attempt \\ 1, opts \\ [])
  def delay(ladder, attempt, opts) when is_atom(ladder), do: delay(ladder(ladder), attempt, opts)

  def delay(%{} = row, attempt, opts) do
    resolved = %{row | base: bound(opts[:base] || row.base), cap: bound(opts[:cap] || row.cap)}

    plain = rung(resolved, attempt)
    if resolved[:jitter], do: min(jitter(plain, opts[:rand]), cap(resolved)), else: plain
  end

  # `rungs` indexed by the attempt and held at the last, or a geometric `base`
  # growing by `factor` (2 unless the row says), the exponent held at
  # `exponent_cap` when the row names one, and capped at `cap` always.
  defp rung(%{rungs: rungs}, attempt) when is_list(rungs),
    do: Enum.at(rungs, attempt - 1) || List.last(rungs)

  defp rung(row, attempt) do
    exponent = if row[:exponent_cap], do: min(attempt - 1, row.exponent_cap), else: attempt - 1
    factor = row[:factor] || 2
    min(round(row.base * Float.pow(factor / 1, exponent / 1)), cap(row))
  end

  defp cap(row), do: row[:cap] || List.last(row.rungs)

  @doc "Uniform in `[value/2, value · 3/2)`, the one jitter rule every ladder spreads by."
  @spec jitter(non_neg_integer(), (pos_integer() -> pos_integer()) | nil) :: non_neg_integer()
  def jitter(value, rand \\ nil) do
    rand = rand || (&:rand.uniform/1)
    div(value, 2) + rand.(max(value, 1))
  end

  @doc """
  The wait before `attempt` against `target`, announced — for a site that arms
  a timer rather than sleeping. Answers the delay so it reads as one call.
  """
  @spec arm(ladder() | map(), pos_integer(), term(), keyword()) :: non_neg_integer()
  def arm(ladder, attempt, target, opts \\ []) do
    delay = delay(ladder, attempt, opts)

    :telemetry.execute(@wait_event, %{delay_ms: delay, attempt: attempt}, %{
      ladder: name(ladder),
      target: target
    })

    delay
  end

  defp name(%{name: name}), do: name
  defp name(ladder) when is_atom(ladder), do: ladder

  @doc """
  `arm/4` and the sleep — the one spelling of "wait before trying again" for a
  site that retries in place. The site holds no timer, no arithmetic and no
  literal either way.
  """
  @spec wait(ladder() | map(), pos_integer(), term(), keyword()) :: :ok
  def wait(ladder, attempt, target, opts \\ []) do
    ladder |> arm(attempt, target, opts) |> Process.sleep()
  end

  @doc """
  Wait before trying `target` again and answer `true`, or answer `false` once
  the row gives up after `failures` or the monotonic `deadline` has passed. The
  wait never runs past the deadline.
  """
  @spec again?(ladder(), non_neg_integer(), term(), integer()) :: boolean()
  def again?(ladder, failures, target, deadline) do
    left = deadline - monotonic_ms()
    retry? = left > 0 and not give_up?(ladder, failures)
    if retry?, do: wait(ladder, failures + 1, target, cap: left)
    retry?
  end

  @doc """
  Whether `failures` — the attempts already SPENT, not the 1-based attempt
  number `delay/3` takes — has reached the row's give-up bound.
  """
  @spec give_up?(ladder(), non_neg_integer()) :: boolean()
  def give_up?(ladder, failures) do
    case ladder(ladder).give_up do
      :never -> false
      bound -> failures >= bound
    end
  end

  @doc """
  Announce that `target` succeeded on `ladder`, and answer the attempt count a
  reset leaves behind — so a site that keeps its own counter zeroes it here and
  the reset is visible on telemetry rather than only in its state.
  """
  @spec reset(ladder(), term()) :: 0
  def reset(ladder, target) do
    :telemetry.execute(@reset_event, %{attempt: 0}, %{ladder: ladder, target: target})
    0
  end

  @doc """
  Keep only the ops whose `{primitive, target}` key is past its cooldown
  deadline. `key_fn` extracts the backoff key from an op. Ops come first
  so this composes in a pipe.
  """
  @spec filter([op], t(), (op -> key())) :: [op] when op: term()
  def filter(ops, %__MODULE__{} = b, key_fn, now \\ monotonic_ms()) when is_function(key_fn, 1) do
    Enum.filter(ops, fn op -> eligible?(b, key_fn.(op), now) end)
  end

  @spec eligible?(t(), key(), integer()) :: boolean()
  def eligible?(%__MODULE__{entries: entries}, key, now \\ monotonic_ms()) do
    case Map.get(entries, key) do
      nil -> true
      {_attempt, next_eligible} -> now >= next_eligible
    end
  end

  @doc """
  Consecutive failures recorded for `key` (0 when none, or after an `:ok`
  reset). Lets a caller read a give-up threshold off the existing attempt
  count rather than tracking a second counter.
  """
  @spec attempt(t(), key()) :: non_neg_integer()
  def attempt(%__MODULE__{entries: entries}, key) do
    case Map.get(entries, key) do
      nil -> 0
      {attempt, _next} -> attempt
    end
  end

  @doc """
  Record the result of executing the op keyed by `key`. `:ok` resets the
  entry; `:error` advances the attempt count and arms the row's next cooldown.
  """
  @spec update(t(), key(), :ok | :error, integer()) :: t()
  def update(backoff, key, result, now \\ monotonic_ms())

  def update(%__MODULE__{entries: entries, ladder: ladder} = b, key, :ok, _now) do
    if Map.has_key?(entries, key), do: reset(ladder, key)
    %{b | entries: Map.delete(entries, key)}
  end

  def update(%__MODULE__{entries: entries, ladder: ladder} = b, key, :error, now) do
    {attempt, _next} = Map.get(entries, key, {0, 0})
    delay = arm(ladder, attempt + 1, key)
    %{b | entries: Map.put(entries, key, {attempt + 1, now + delay})}
  end

  # A bound is a number, the name of the duration row a user tunes, or
  # absent on a rung row.
  defp bound(nil), do: nil
  defp bound(name) when is_atom(name), do: Durations.get(name)
  defp bound(ms) when is_integer(ms), do: ms

  defp monotonic_ms(), do: System.monotonic_time(:millisecond)
end

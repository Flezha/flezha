defmodule Flezha.Schedule.Cron do
  @moduledoc """
  Five-field cron expressions (`minute hour day-of-month month day-of-week`)
  evaluated in an IANA time zone.

  Numeric fields only: `*`, single values, ranges `a-b`, steps `*/n` and
  `a-b/n`, and comma lists. Day-of-week is `0-7` with both `0` and `7`
  meaning Sunday. Vixie day rule: when both day-of-month and day-of-week
  are restricted, a day matches if EITHER matches.

  DST semantics: a slot whose local wall time does not exist
  (spring-forward gap) is skipped; an ambiguous wall time (fall-back fold)
  fires once, at the first occurrence.
  """

  @enforce_keys [
    :expr,
    :minutes,
    :hours,
    :days,
    :months,
    :weekdays,
    :day_restricted,
    :weekday_restricted
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          expr: String.t(),
          minutes: MapSet.t(non_neg_integer()),
          hours: MapSet.t(non_neg_integer()),
          days: MapSet.t(pos_integer()),
          months: MapSet.t(pos_integer()),
          weekdays: MapSet.t(non_neg_integer()),
          day_restricted: boolean(),
          weekday_restricted: boolean()
        }

  @tz_db Tz.TimeZoneDatabase

  # Caps a fruitless day scan; > 4 years so `0 0 29 2 *` still finds a leap day.
  @max_scan_days 1500

  @fields [
    {:minutes, "minute", 0, 59},
    {:hours, "hour", 0, 23},
    {:days, "day-of-month", 1, 31},
    {:months, "month", 1, 12},
    {:weekdays, "day-of-week", 0, 7}
  ]

  @spec parse(String.t()) :: {:ok, t()} | {:error, String.t()}
  def parse(expr) when is_binary(expr) do
    case String.split(expr) do
      [_, _, dom, _, dow] = raw ->
        with {:ok, sets} <- parse_fields(raw) do
          {:ok,
           struct!(
             __MODULE__,
             Map.merge(sets, %{
               expr: expr,
               weekdays: normalize_weekdays(sets.weekdays),
               day_restricted: dom != "*",
               weekday_restricted: dow != "*"
             })
           )}
        end

      parts ->
        {:error,
         "cron expression must have 5 fields (minute hour day month weekday), got #{length(parts)}"}
    end
  end

  @doc "True when `tz` resolves in the bundled IANA database."
  @spec valid_timezone?(String.t()) :: boolean()
  def valid_timezone?(tz) when is_binary(tz) do
    match?({:ok, _}, DateTime.now(tz, @tz_db))
  end

  @doc """
  The first slot whose UTC instant is strictly after `after_utc`, as a UTC
  `DateTime`. `:none` when no slot exists within the scan horizon (an
  expression that never matches, e.g. `0 0 30 2 *`).
  """
  @spec next_fire(t(), String.t(), DateTime.t()) :: {:ok, DateTime.t()} | :none
  def next_fire(%__MODULE__{} = cron, tz, %DateTime{} = after_utc) do
    case DateTime.shift_zone(after_utc, tz, @tz_db) do
      {:ok, local} ->
        from = %{DateTime.to_naive(local) | second: 0, microsecond: {0, 0}}
        scan_days(cron, tz, after_utc, NaiveDateTime.to_date(from), from, @max_scan_days)

      {:error, _} ->
        :none
    end
  end

  @doc """
  All slots with `after_utc < slot <= until_utc`, oldest first, capped at
  `limit`. Returns `{slots, truncated?}`.
  """
  @spec slots_between(t(), String.t(), DateTime.t(), DateTime.t(), pos_integer()) ::
          {[DateTime.t()], boolean()}
  def slots_between(%__MODULE__{} = cron, tz, after_utc, until_utc, limit) do
    collect_slots(cron, tz, after_utc, until_utc, limit, [])
  end

  ## ----- private -----

  defp collect_slots(_cron, _tz, _after_utc, _until_utc, 0, acc), do: {Enum.reverse(acc), true}

  defp collect_slots(cron, tz, after_utc, until_utc, n, acc) do
    with {:ok, slot} <- next_fire(cron, tz, after_utc),
         true <- DateTime.compare(slot, until_utc) != :gt do
      collect_slots(cron, tz, slot, until_utc, n - 1, [slot | acc])
    else
      _ -> {Enum.reverse(acc), false}
    end
  end

  defp scan_days(_cron, _tz, _after_utc, _date, _from, 0), do: :none

  defp scan_days(cron, tz, after_utc, date, from, days_left) do
    with true <- date_matches?(cron, date),
         {:ok, slot} <- scan_minutes(cron, tz, after_utc, date, from) do
      {:ok, slot}
    else
      _ -> scan_days(cron, tz, after_utc, Date.add(date, 1), nil, days_left - 1)
    end
  end

  # `from` (the scan origin's local minute) prunes candidates on the first
  # day only; correctness rests on the `slot > after_utc` UTC comparison.
  defp scan_minutes(cron, tz, after_utc, date, from) do
    minutes = Enum.sort(cron.minutes)

    Enum.find_value(Enum.sort(cron.hours), :none, fn hour ->
      Enum.find_value(minutes, fn minute ->
        naive = NaiveDateTime.new!(date, Time.new!(hour, minute, 0))

        with true <- from == nil or NaiveDateTime.compare(naive, from) != :lt,
             {:ok, utc} <- local_to_utc(naive, tz),
             :gt <- DateTime.compare(utc, after_utc) do
          {:ok, utc}
        else
          _ -> nil
        end
      end)
    end)
  end

  defp local_to_utc(naive, tz) do
    case DateTime.from_naive(naive, tz, @tz_db) do
      # Fall-back fold: the slot fires once, at the first occurrence.
      {:ambiguous, first, _second} -> {:ok, DateTime.shift_zone!(first, "Etc/UTC", @tz_db)}
      {:ok, dt} -> {:ok, DateTime.shift_zone!(dt, "Etc/UTC", @tz_db)}
      # Spring-forward gap: the wall time does not exist — skip the slot.
      {:gap, _, _} -> :gap
      {:error, _} -> :gap
    end
  end

  defp date_matches?(cron, date) do
    day_ok =
      case {cron.day_restricted, cron.weekday_restricted} do
        {false, false} -> true
        {true, false} -> MapSet.member?(cron.days, date.day)
        {false, true} -> weekday_member?(cron, date)
        {true, true} -> MapSet.member?(cron.days, date.day) or weekday_member?(cron, date)
      end

    MapSet.member?(cron.months, date.month) and day_ok
  end

  defp weekday_member?(cron, date),
    do: MapSet.member?(cron.weekdays, rem(Date.day_of_week(date), 7))

  defp normalize_weekdays(set),
    do: MapSet.new(set, fn day -> rem(day, 7) end)

  defp parse_fields(raw) do
    @fields
    |> Enum.zip(raw)
    |> Enum.reduce_while({:ok, %{}}, fn {{key, label, min, max}, field}, {:ok, acc} ->
      case parse_field(field, min, max) do
        {:ok, set} -> {:cont, {:ok, Map.put(acc, key, set)}}
        {:error, msg} -> {:halt, {:error, "#{label} field #{msg}"}}
      end
    end)
  end

  defp parse_field(field, min, max) do
    field
    |> String.split(",")
    |> Enum.reduce_while({:ok, MapSet.new()}, fn element, {:ok, acc} ->
      case parse_element(element, min, max) do
        {:ok, values} -> {:cont, {:ok, MapSet.union(acc, MapSet.new(values))}}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp parse_element(element, min, max) do
    {range_part, step, stepped} =
      case String.split(element, "/") do
        [range] -> {range, {:ok, 1}, false}
        [range, step] -> {range, parse_int(step), true}
        _ -> {element, :error, true}
      end

    with {:ok, step} when step >= 1 <- step,
         # A step is only meaningful on `*` or a range (`*/n`, `a-b/n`);
         # a stepped single value (`30/10`, even `30/1`) is rejected,
         # never silently collapsed to the bare value.
         true <- not stepped or range_part == "*" or String.contains?(range_part, "-"),
         {:ok, {lo, hi}} <- parse_range(range_part, min, max) do
      {:ok, Enum.take_every(lo..hi, step)}
    else
      _ -> {:error, "has invalid element #{inspect(element)} (allowed #{min}-#{max})"}
    end
  end

  defp parse_range("*", min, max), do: {:ok, {min, max}}

  defp parse_range(range, min, max) do
    case String.split(range, "-") do
      [single] -> with {:ok, v} <- parse_int(single), do: {:ok, {v, v}}
      [lo, hi] -> with {:ok, l} <- parse_int(lo), {:ok, h} <- parse_int(hi), do: {:ok, {l, h}}
      _ -> :error
    end
    |> case do
      {:ok, {lo, hi}} when lo >= min and hi <= max and lo <= hi -> {:ok, {lo, hi}}
      _ -> :error
    end
  end

  defp parse_int(str) do
    case Integer.parse(str) do
      {value, ""} -> {:ok, value}
      _ -> :error
    end
  end
end

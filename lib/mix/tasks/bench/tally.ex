defmodule Mix.Tasks.Bench.Tally do
  @moduledoc """
  Bounded accounting shared by the bench tasks, whatever the run length: seqs
  are contiguous from 0, so one bit per seq dedups and finds loss, and
  latencies land in a fixed-resolution histogram. A soak of a hundred million
  records costs the collector the same memory as a run of fifteen thousand.

  The first `prime_count` seqs are warm-up: counted as seen, never measured.
  """

  alias Flezha.State.Metrics
  alias Mix.Tasks.Bench.Report

  @hist_step_ns 10_000
  @hist_buckets 6_000_000

  @type t :: map()

  @spec new(non_neg_integer(), non_neg_integer()) :: t()
  def new(expected, prime_count \\ 0) do
    %{
      expected: expected,
      prime_count: prime_count,
      bits: :atomics.new(div(expected, 64) + 1, signed: false),
      hist: :counters.new(@hist_buckets, []),
      seen: 0,
      received: 0,
      duplicates: 0,
      foreign: 0,
      max_ns: 0
    }
  end

  @doc "Fold one `{seq, latency_ns}` observation in (reduce-friendly argument order)."
  @spec record({integer(), non_neg_integer()}, t()) :: t()
  def record({seq, _lat}, %{expected: expected} = tally) when seq < 0 or seq >= expected,
    do: %{tally | foreign: tally.foreign + 1}

  def record({seq, lat}, tally) do
    cond do
      not set_bit(tally.bits, seq) ->
        %{tally | duplicates: tally.duplicates + 1}

      seq < tally.prime_count ->
        %{tally | seen: tally.seen + 1}

      true ->
        :counters.add(tally.hist, min(div(lat, @hist_step_ns), @hist_buckets - 1) + 1, 1)

        %{
          tally
          | seen: tally.seen + 1,
            received: tally.received + 1,
            max_ns: max(tally.max_ns, lat)
        }
    end
  end

  # `true` when the bit was clear. One writer, but the CAS keeps it honest.
  defp set_bit(bits, seq) do
    word = div(seq, 64) + 1
    mask = Bitwise.bsl(1, rem(seq, 64))
    old = :atomics.get(bits, word)

    cond do
      Bitwise.band(old, mask) != 0 -> false
      :atomics.compare_exchange(bits, word, old, Bitwise.bor(old, mask)) == :ok -> true
      true -> set_bit(bits, seq)
    end
  end

  @doc """
  The `{seq, send_ns}` stamp a bench input carries as `<seq>,<send_ns>[,<pad>]`,
  or `:error`. A key prefix up to the first `:` or `=` is skipped, so a keyed
  input (`k3:`) and a `key=value` output (`k3=`) parse like a bare one.
  """
  @spec parse_stamp(binary()) :: {integer(), integer()} | :error
  def parse_stamp(body) when is_binary(body) do
    with {seq, "," <> rest} <- digits(strip_key(body)),
         {ts, tail} when tail == "" or binary_part(tail, 0, 1) == "," <- signed(rest) do
      {seq, ts}
    else
      _ -> :error
    end
  end

  # Read by hand: the bench's receive loop parses every output, and a stamp
  # read there is time the next output spends queued. Monotonic time can be
  # negative, the seq never is.
  defp signed("-" <> rest), do: with({n, tail} <- digits(rest), do: {-n, tail})
  defp signed(rest), do: digits(rest)

  defp digits(<<d, _::binary>> = bin) when d in ?0..?9, do: digits(bin, 0)
  defp digits(_), do: :error

  defp digits(<<d, rest::binary>>, acc) when d in ?0..?9, do: digits(rest, acc * 10 + d - ?0)
  defp digits(rest, acc), do: {acc, rest}

  # Only the head is searched: a key is short, and a padded body can be long.
  defp strip_key(body), do: strip_key(body, body, 0)

  defp strip_key(_body, <<c, rest::binary>>, _i) when c in [?:, ?=], do: rest
  defp strip_key(body, <<_, rest::binary>>, i) when i < 63, do: strip_key(body, rest, i + 1)
  defp strip_key(body, _head, _i), do: body

  @doc """
  How many sends are due by now on a `rate`/s schedule started at `t0_ns`,
  given `issued` already sent. Never negative.
  """
  @spec due(integer(), pos_integer(), non_neg_integer()) :: non_neg_integer()
  def due(t0_ns, rate, issued),
    do: max(trunc((System.monotonic_time(:nanosecond) - t0_ns) / 1.0e9 * rate) - issued, 0)

  @doc "Seqs never recorded, up to `limit` of them."
  @spec missing(t(), pos_integer()) :: [non_neg_integer()]
  def missing(%{expected: expected, bits: bits}, limit \\ 10) do
    0..(expected - 1)//1
    |> Stream.reject(fn seq ->
      Bitwise.band(:atomics.get(bits, div(seq, 64) + 1), Bitwise.bsl(1, rem(seq, 64))) != 0
    end)
    |> Enum.take(limit)
  end

  @doc """
  Quantiles in one pass over the histogram, each answered with its bucket's
  midpoint (exact to 0.01 ms); the scan stops at the highest target.
  """
  @spec percentiles(t(), [float()]) :: %{float() => non_neg_integer()}
  def percentiles(%{received: 0}, qs), do: Map.new(qs, &{&1, 0})

  def percentiles(tally, qs) do
    targets =
      qs
      |> Enum.map(&{&1, nearest_rank(tally.received, &1)})
      |> Enum.sort_by(&elem(&1, 1))

    # A bucket's midpoint can sit above the largest sample in it.
    tally.hist
    |> scan(1, 0, targets, %{})
    |> Map.new(fn {q, ns} -> {q, min(ns, tally.max_ns)} end)
  end

  defp scan(_hist, _bucket, _cum, [], acc), do: acc

  defp scan(_hist, bucket, _cum, targets, acc) when bucket > @hist_buckets,
    do: Enum.reduce(targets, acc, fn {q, _}, a -> Map.put(a, q, bucket_ns(@hist_buckets)) end)

  defp scan(hist, bucket, cum, targets, acc) do
    cum = cum + :counters.get(hist, bucket)
    {done, pending} = Enum.split_while(targets, fn {_q, target} -> cum >= target end)
    acc = Enum.reduce(done, acc, fn {q, _}, a -> Map.put(a, q, bucket_ns(bucket)) end)
    scan(hist, bucket + 1, cum, pending, acc)
  end

  # The smallest rank covering a share `q` of `n`; rounding first keeps a
  # product like 100 * 0.95 from reading as 95.000000001.
  defp nearest_rank(n, q), do: max(ceil(Float.round(n * q, 6)), 1)

  defp bucket_ns(bucket), do: round((bucket - 0.5) * @hist_step_ns)

  @doc "`p50 / p95 / p99 / p99.9 / max` in ms, one line."
  @spec latency_line(t()) :: String.t()
  def latency_line(tally), do: tally |> latency_ms() |> render_latency()

  @doc """
  The percentiles and max as millisecond numbers, with the count they cover:
  what a report ships, and what `render_latency/1` prints.
  """
  @spec latency_ms(t()) :: %{atom() => number()}
  def latency_ms(tally) do
    pct = percentiles(tally, [0.50, 0.95, 0.99, 0.999])

    %{
      p50: ms(pct[0.50]),
      p95: ms(pct[0.95]),
      p99: ms(pct[0.99]),
      p99_9: ms(pct[0.999]),
      max: ms(tally.max_ns),
      n: tally.received
    }
  end

  @doc "`latency_ms/1`'s map as one line."
  @spec render_latency(map()) :: String.t()
  def render_latency(l) do
    "p50 #{text(l.p50)} / p95 #{text(l.p95)} / p99 #{text(l.p99)} / " <>
      "p99.9 #{text(l.p99_9)} / max #{text(l.max)} ms (n=#{l.n})"
  end

  # Two decimals: the histogram resolves to 0.01 ms and a percentile that reads
  # `0.05` says something `0.1` does not.
  defp ms(ns), do: Float.round(ns / 1_000_000, 2)
  defp text(ms), do: Report.decimal(ms, 2)

  @doc "The namespace's state directory on disk: bytes, `.wal` and `.snap.*` counts."
  @spec state_dir_summary(String.t()) :: String.t()
  def state_dir_summary(namespace) do
    {bytes, wals, snaps} = state_dir_stats(namespace)
    "#{div(bytes, 1_048_576)} MiB, #{wals} wal, #{snaps} snap"
  end

  @spec state_dir_stats(String.t()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  def state_dir_stats(namespace) do
    ["storage", "app", "state", namespace, "**"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.reduce({0, 0, 0}, fn path, {bytes, wals, snaps} ->
      case File.stat(path) do
        {:ok, %{type: :regular, size: size}} ->
          name = Path.basename(path)

          {bytes + size, wals + if(String.ends_with?(name, ".wal"), do: 1, else: 0),
           snaps + if(String.contains?(name, ".snap."), do: 1, else: 0)}

        _ ->
          {bytes, wals, snaps}
      end
    end)
  end

  @doc """
  Changes, bytes and fsyncs persisted by the Cells of `relay_ids`, read from
  the state layer's own per-Cell counters; given an earlier reading, the growth
  since it. A Cell id leads with its relay class, a flow relay's id.
  """
  @spec persisted(MapSet.t(String.t()), map() | nil) :: %{atom() => non_neg_integer()}
  def persisted(relay_ids, since \\ nil) do
    now =
      for {{class, _tasklet, _key}, counters} <- Metrics.snapshot_all(),
          MapSet.member?(relay_ids, class),
          reduce: %{changes: 0, bytes: 0, fsyncs: 0} do
        acc ->
          %{
            changes: acc.changes + counters.persisted_changes_total,
            bytes: acc.bytes + counters.persisted_bytes_total,
            fsyncs: acc.fsyncs + counters.fsync_total
          }
      end

    if since, do: Map.new(now, fn {k, v} -> {k, v - since[k]} end), else: now
  end

  @doc """
  The slowest cell's WAL fsync p99: a disk that stalls shows here first,
  before it shows as a broker timeout or a backed-up input log.
  """
  @spec fsync_p99_ms() :: number()
  def fsync_p99_ms do
    Metrics.snapshot_all()
    |> Enum.map(fn {_cell, counters} -> Map.get(counters, :flush_latency_ms_p99, 0) end)
    |> Enum.max(fn -> 0 end)
  end
end

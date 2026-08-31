defmodule Mix.Tasks.Bench.Tally do
  @moduledoc """
  Bounded accounting shared by the bench tasks, whatever the run length: seqs
  are contiguous from 0, so one bit per seq dedups and finds loss, and
  latencies land in a fixed-resolution histogram. A soak of a hundred million
  records costs the collector the same memory as a run of fifteen thousand.

  The first `prime_count` seqs are warm-up: counted as seen, never measured.
  """

  alias Flezha.State.Metrics

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
      qs |> Enum.map(&{&1, max(round(tally.received * &1), 1)}) |> Enum.sort_by(&elem(&1, 1))

    scan(tally.hist, 1, 0, targets, %{})
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

  defp bucket_ns(bucket), do: round((bucket - 0.5) * @hist_step_ns)

  @doc "`p50 / p95 / p99 / p99.9 / max` in ms, one line."
  @spec latency_line(t()) :: String.t()
  def latency_line(tally) do
    pct = percentiles(tally, [0.50, 0.95, 0.99, 0.999])

    "p50 #{ms(pct[0.50])} / p95 #{ms(pct[0.95])} / p99 #{ms(pct[0.99])} / " <>
      "p99.9 #{ms(pct[0.999])} / max #{ms(tally.max_ns)} ms (n=#{tally.received})"
  end

  defp ms(ns), do: Float.round(ns / 1_000_000, 2)

  @doc "The namespace's state directory on disk: bytes, `.wal` and `.snap.*` counts."
  @spec state_dir_summary(String.t()) :: String.t()
  def state_dir_summary(namespace) do
    {bytes, wals, snaps} = state_dir_stats(namespace)
    "#{div(bytes, 1_048_576)} MiB, #{wals} wal, #{snaps} snap"
  end

  @spec state_dir_stats(String.t()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  def state_dir_stats(namespace) do
    ["app_data", "state", namespace, "**"]
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
  The slowest entity's WAL fsync p99: a disk that stalls shows here first,
  before it shows as a broker timeout or a backed-up input log.
  """
  @spec fsync_p99_ms() :: number()
  def fsync_p99_ms do
    Metrics.snapshot_all()
    |> Enum.map(fn {_entity, counters} -> Map.get(counters, :flush_latency_ms_p99, 0) end)
    |> Enum.max(fn -> 0 end)
  end
end

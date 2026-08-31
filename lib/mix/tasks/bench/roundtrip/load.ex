defmodule Mix.Tasks.Bench.Roundtrip.Load do
  @moduledoc """
  The Kafka side of the round trip: the client, the topics, the generator that
  produces into the input topic and the collector that drains the output one.

  Owns the run's tally and its status line, and the WAL telemetry counters the
  status line reads — those three share one `:persistent_term` key built from
  `__MODULE__`, so they cannot be separated. Takes `cfg` as a plain map and
  names no other part of the task.
  """

  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Native
  alias Mix.Tasks.Bench.Tally

  def start_kafka_client!(cfg) do
    # Unnamed, and never routed through the plugin's WorkerRegistry: that
    # registry hands out refcounted shared clients, and stopping one from here
    # would take the producer plugins down with it.
    {:ok, pid} =
      KafkaEx.start_link_worker(:no_name, uris: [{cfg.broker_host, cfg.broker_port}])

    pid
  end

  def create_topics!(client, cfg) do
    # Explicit creation, not `auto_create_topic`: the plugin's auto-create is
    # error-driven (the first produce must fail with :unknown_topic_or_partition
    # before creation is attempted), which would land inside the
    # time-to-first-record-out window. replication_factor 1 — single broker.
    specs =
      for topic <- [cfg.in_topic, cfg.out_topic] do
        [topic: topic, num_partitions: cfg.pipelines, replication_factor: 1]
      end

    case KafkaEx.API.create_topics(client, specs, 15_000) do
      {:ok, _} -> :ok
      {:error, reason} -> raise "topic creation failed: #{inspect(reason)}"
    end

    # Creation is asynchronous on the broker; wait until every partition has a
    # queryable offset before anyone produces.
    Enum.each([cfg.in_topic, cfg.out_topic], fn topic ->
      Enum.each(0..(cfg.pipelines - 1), &wait_for_partition!(client, topic, &1))
    end)

    IO.puts("Topics ready: #{cfg.in_topic}, #{cfg.out_topic} (#{cfg.pipelines} partitions each)")
  end

  defp wait_for_partition!(client, topic, partition, tries \\ 600) do
    case KafkaEx.API.latest_offset(client, topic, partition) do
      {:ok, _} ->
        :ok

      {:error, _reason} when tries > 0 ->
        Process.sleep(25)
        wait_for_partition!(client, topic, partition, tries - 1)

      {:error, reason} ->
        raise "#{topic}/#{partition} never became available: #{inspect(reason)}"
    end
  end

  def delete_topics(client, cfg) do
    KafkaEx.API.delete_topics(client, [cfg.in_topic, cfg.out_topic], 15_000)
  rescue
    _ -> :ok
  end

  # ------------------------------------------------------------ generator --

  def prime(client, cfg) do
    # seq 0..pipelines-1, one per partition, so every partition has a record
    # waiting the moment the engine starts.
    Enum.each(0..(cfg.pipelines - 1), fn p ->
      produce_batch(client, cfg, p, [p])
    end)
  end

  def generate(_client, cfg) do
    first = cfg.prime_count

    0..(cfg.pipelines - 1)
    |> Enum.map(fn p ->
      Task.async(fn ->
        # One client per partition: a kafka_ex client answers one request at a
        # time, so a shared one would cap the generator at one broker RTT per
        # batch across all partitions and the engine would idle behind it.
        client = start_kafka_client!(cfg)
        lo = first + p * cfg.messages_per_pipeline

        # Chunked lazily: a soak's seqs never exist as one list.
        lo..(lo + cfg.messages_per_pipeline - 1)
        |> Stream.chunk_every(cfg.batch)
        |> Enum.each(fn chunk ->
          produce_batch(client, cfg, p, chunk)
          pace(cfg, length(chunk))
        end)
      end)
    end)
    |> Task.await_many(:infinity)
  end

  # A broker that stalls for a few seconds (its own heartbeat timed out in the
  # 30-min soak) must not end a soak: retry like a real source would, for up
  # to ~30 s, and count the retries so the summary shows them.
  @produce_retries 30

  defp produce_batch(client, cfg, partition, seqs, attempt \\ 1) do
    messages =
      Enum.map(seqs, fn seq ->
        %{value: payload(cfg, partition, seq, System.monotonic_time(:nanosecond))}
      end)

    case KafkaEx.API.produce(client, cfg.in_topic, partition, messages, required_acks: 1) do
      {:ok, _} ->
        :ok

      {:error, reason} when attempt < @produce_retries ->
        :counters.add(produce_retry_counter(), 1, 1)

        IO.puts(
          "\n  produce to #{cfg.in_topic}/#{partition} failed (#{inspect(reason)}), retry #{attempt}"
        )

        Process.sleep(1_000)
        produce_batch(client, cfg, partition, seqs, attempt + 1)

      {:error, reason} ->
        raise "produce to #{cfg.in_topic}/#{partition} failed after #{attempt} attempts: #{inspect(reason)}"
    end
  end

  def produce_retry_counter do
    case :persistent_term.get({__MODULE__, :produce_retries}, nil) do
      nil ->
        ref = :counters.new(1, [])
        :persistent_term.put({__MODULE__, :produce_retries}, ref)
        ref

      ref ->
        ref
    end
  end

  # `<seq>,<send_ts_ns>,<pad>`, keyed workload prefixed with `k<partition>:`.
  # Padding is alphanumeric so it can never contain the `:` / `=` / `,`
  # separators the parser keys off.
  defp payload(cfg, partition, seq, ts) do
    head =
      case cfg.workload do
        w when w == :keyed ->
          "k#{partition}_#{rem(seq, cfg.keys)}:#{seq},#{ts},"

        :identity ->
          "#{seq},#{ts},"
      end

    pad = max(cfg.payload_bytes - byte_size(head), 1)
    head <> binary_part(cfg.pad_pool, 0, pad)
  end

  defp pace(%{rate: 0}, _n), do: :ok

  defp pace(cfg, n) do
    Process.sleep(max(div(n * 1_000, cfg.rate), 1))
  end

  # ------------------------------------------------------------ collector --

  def start_collector(cfg) do
    parent = self()

    pids =
      Enum.map(0..(cfg.pipelines - 1), fn p ->
        spawn_link(fn -> collect_partition(parent, cfg, p) end)
      end)

    %{pids: pids}
  end

  defp collect_partition(parent, cfg, partition) do
    {:ok, client} =
      KafkaEx.start_link_worker(:no_name, uris: [{cfg.broker_host, cfg.broker_port}])

    fetch_loop(parent, client, cfg, partition, 0)
  end

  @committed_fetch_opts [
    max_wait_time: 50,
    min_bytes: 1,
    max_bytes: 5_000_000,
    api_version: 4,
    isolation_level: 1
  ]

  defp fetch_loop(parent, client, cfg, partition, offset) do
    opts = @committed_fetch_opts

    case KafkaEx.API.fetch(client, cfg.out_topic, partition, offset, opts) do
      {:ok, %{records: []}} ->
        fetch_loop(parent, client, cfg, partition, offset)

      {:ok, %{records: records}} ->
        now = System.monotonic_time(:nanosecond)
        decoded = Enum.map(records, &decode(&1.value, now))
        kept = for {:ok, entry} <- decoded, do: entry
        send(parent, {:records, kept, length(decoded) - length(kept)})
        fetch_loop(parent, client, cfg, partition, List.last(records).offset + 1)

      {:error, _reason} ->
        Process.sleep(50)
        fetch_loop(parent, client, cfg, partition, offset)
    end
  end

  # Tolerates the keyed workload's `<key>=` output prefix and the `<key>:`
  # input prefix; both sit ahead of the seq in the first comma-field.
  # Returns `:skip` for anything that is not a bench payload. Under Flink's
  # exactly-once sink the topic also carries transaction control markers, and
  # kafka_ex surfaces those as ordinary records — the control flag lives in the
  # record *batch* attributes, which it does not propagate. Skips are counted
  # and reported so a genuinely malformed payload cannot hide in here.
  defp decode(value, now) do
    with [head, ts | _] <- :binary.split(value, ",", [:global]),
         stripped = head |> :binary.split(["=", ":"], [:global]) |> List.last(),
         {seq, ""} <- Integer.parse(stripped),
         {sent, ""} <- Integer.parse(ts) do
      {:ok, {seq, now - sent}}
    else
      _ -> :skip
    end
  end

  def await_first_record(_collector, cfg) do
    receive do
      {:records, [{_seq, _lat} | _] = batch, skipped} ->
        send(self(), {:records, batch, skipped})
        System.monotonic_time(:nanosecond)

      {:EXIT, pid, reason} when reason not in [:normal, :killed] ->
        raise "bench worker #{inspect(pid)} crashed during bring-up: #{inspect(reason)}"
    after
      cfg.stall_timeout * 4 ->
        raise "no record reached #{cfg.out_topic} within #{cfg.stall_timeout * 4}ms — engine never came up?"
    end
  end

  # Answers the instant the last expected record was collected (or the stall
  # gave up), so the caller can split the window into generation and drain.
  def await_completion(collector, cfg) do
    tally = drain(new_tally(cfg), cfg, System.monotonic_time(:millisecond))
    Enum.each(collector.pids, &Process.exit(&1, :kill))
    {tally, tally.skipped, System.monotonic_time(:nanosecond)}
  end

  defp new_tally(cfg) do
    Map.merge(Tally.new(cfg.total + cfg.prime_count, cfg.prime_count), %{
      skipped: 0,
      status_at: System.monotonic_time(:millisecond),
      status_seen: 0
    })
  end

  defp drain(tally, cfg, last_progress) do
    if tally.seen >= tally.expected do
      tally
    else
      receive do
        {:records, batch, batch_skipped} ->
          tally = Enum.reduce(batch, tally, &Tally.record/2)
          tally = %{tally | skipped: tally.skipped + batch_skipped}
          progress(tally)
          drain(maybe_status(tally, cfg), cfg, System.monotonic_time(:millisecond))

        {:EXIT, pid, reason} when reason not in [:normal, :killed] ->
          raise "bench worker #{inspect(pid)} crashed mid-run: #{inspect(reason)}"
      after
        1_000 ->
          if System.monotonic_time(:millisecond) - last_progress > cfg.stall_timeout do
            IO.puts(
              "\nSTALLED: #{tally.seen}/#{tally.expected} records after #{cfg.stall_timeout}ms with no progress"
            )

            tally
          else
            drain(maybe_status(tally, cfg), cfg, last_progress)
          end
      end
    end
  end

  defp progress(%{seen: n, expected: expected}) do
    if rem(n, 5_000) == 0, do: IO.write("\r  collected #{n}/#{expected}")
  end

  # `--status-every S`: a line every S seconds with what a long run needs
  # watched — rate, the loss-relevant counters, the state dir on disk, memory.
  defp maybe_status(%{status_at: at} = tally, %{status_every: every} = cfg)
       when is_integer(every) and every > 0 do
    now = System.monotonic_time(:millisecond)

    if now - at >= every * 1_000 do
      rate = (tally.seen - tally.status_seen) * 1_000 / max(now - at, 1)

      IO.puts(
        "\n  [status] #{tally.seen}/#{tally.expected} collected, #{round(rate)} rec/s, " <>
          "dup=#{tally.duplicates}#{explained_dups(cfg)} skipped=#{tally.skipped} | state #{Tally.state_dir_summary(cfg.namespace)} | " <>
          "fsync p99 #{Tally.fsync_p99_ms()}ms | beam #{div(:erlang.memory(:total), 1_048_576)} MiB | " <>
          wal_signal_summary()
      )

      %{tally | status_at: now, status_seen: tally.seen}
    else
      tally
    end
  end

  defp maybe_status(tally, _cfg), do: tally

  # A retried batch the broker had in fact written lands in the input topic
  # twice, and the engine rightly emits both: up to `retries × batch` of the
  # duplicates are the generator's, anything beyond them is the engine's.
  def explained_dups(cfg) do
    case :counters.get(produce_retry_counter(), 1) do
      0 -> ""
      n -> " (≤#{n * cfg.batch} from #{n} generator retries)"
    end
  end

  defp wal_signal_summary do
    case :persistent_term.get({__MODULE__, :wal_counters}, nil) do
      nil ->
        ""

      c ->
        "seq_gap=#{:counters.get(c, 1)} credit_anomaly=#{:counters.get(c, 2)} " <>
          "stream_drop=#{:counters.get(c, 3)}"
    end
  end

  # Samples the busiest processes while the run is in flight. Mailbox depth
  # names the serialization point; reductions name where the CPU goes.
  # Counts the WAL writer's loss/anomaly signals for the run. A shed is the
  # writer dropping a Change past its hard in-memory bound; a credit anomaly is
  # a shed that happened even though the sender was credit-speaking, which the
  # writer treats as a protocol violation rather than back-pressure.
  def attach_wal_telemetry do
    counters = :counters.new(3, [])
    :persistent_term.put({__MODULE__, :wal_counters}, counters)

    :telemetry.attach_many(
      "bench-wal",
      [
        [:flezha, :state, :seq_gap],
        [:flezha, :state, :credit_anomaly],
        [:flezha, :stream, :drop]
      ],
      fn
        [:flezha, :state, :seq_gap], _m, _md, _ -> :counters.add(counters, 1, 1)
        [:flezha, :state, :credit_anomaly], _m, _md, _ -> :counters.add(counters, 2, 1)
        [:flezha, :stream, :drop], _m, _md, _ -> :counters.add(counters, 3, 1)
      end,
      nil
    )
  end

  def report_wal_telemetry do
    case :persistent_term.get({__MODULE__, :wal_counters}, nil) do
      nil ->
        :ok

      c ->
        IO.puts(
          "WAL signals: seq_gap=#{:counters.get(c, 1)} " <>
            "credit_anomaly=#{:counters.get(c, 2)} stream_drop=#{:counters.get(c, 3)}"
        )
    end

    # The data-stream drivers' own loss counter: a frame the peer did not
    # drain in time is dropped past the per-stream write-buffer cap, with no
    # seq gap and no telemetry to show for it.
    {dropped, written} =
      LivenessCounters.driver_refs()
      |> Enum.flat_map(fn {_key, ref} ->
        case Native.driver_stats(ref) do
          %{streams: streams} -> streams
          _ -> []
        end
      end)
      |> Enum.reduce({0, 0}, fn s, {d, w} ->
        {d + s.write_buffer_dropped, w + s.frames_written}
      end)

    IO.puts("Driver: frames_written=#{written} write_buffer_dropped=#{dropped}")
  end

  # tprof traces every process for the window, which also slows them: the
  # ranking is sound, the absolute times are not.
end

defmodule Mix.Tasks.Bench.Roundtrip do
  use Mix.Task

  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Tally

  @shortdoc "Kafka round-trip benchmark: Flezha vs Apache Flink"
  @moduledoc """
  Measures a full Kafka round trip — generator → `in` topic → engine →
  `out` topic → collector — with the engine as the only variable.

      mix bench.roundtrip --engine passthrough
      mix bench.roundtrip --engine flezha --workload identity -p 4 -m 5000
      mix bench.roundtrip --engine flink  --workload keyed --durability on
      mix bench.roundtrip --engine flezha --runtime java --workload keyed --durability on

  Requires the bench stack: `docker compose -f bench/flink/compose.yml up -d`.

  ## Engines

    * `flezha` — N operatives, one Kafka producer plugin each; `--runtime
      rust|java|elixir` (default `rust`) picks which SDK's runners serve them,
      launched from that SDK's `test/apps/<runtime>_runner/conformance.json`.
      A number is a number for the runtime that produced it — `--runtime elixir`
      carries only `--durability off`, because the Elixir SDK v1 defers the
      persistence family (`docs/sdk/elixir_sdk.md`)
    * `flink` — one submitted job (`--flink-jobs n` submits N instead)
    * `passthrough` — bare consume→produce, no engine. This is the broker +
      harness **floor**, not a competitor: it does none of the offset
      checkpointing a real source does. Report it as harness cost.

  ## Measurement

  Payload is ASCII `"<seq>,<send_ts_ns>,<pad>"` (keyed workload prefixes
  `"<key>:"`). The identity lives in the payload because operative metadata does
  not survive the round trip — the runner ingress reads five reserved keys and
  drops the rest, and `ProcedureCtx` exposes no input metadata at all.

  Generator and collector share this BEAM, so `System.monotonic_time/1` is
  directly comparable and there is no clock skew to correct.

  `--rate 0` (the default) generates unpaced, which measures **throughput**;
  the latency percentiles it reports include queueing at saturation. Pass a
  non-zero `--rate` for a sub-saturation latency measurement.
  """

  @in_prefix "flzbench.in"
  @out_prefix "flzbench.out"
  # Deliberately NOT "bench": `mix bench` owns that namespace, and an
  # effectively-once run here leaves per-operative WAL state behind that would
  # show up in its convergence intent set.
  @namespace "bench_rt"

  @switches [
    engine: :string,
    runtime: :string,
    workload: :string,
    durability: :string,
    pipelines: :integer,
    messages_per_pipeline: :integer,
    payload_bytes: :integer,
    rate: :integer,
    batch: :integer,
    runners: :integer,
    flink_jobs: :integer,
    broker: :string,
    flink_rest: :string,
    jar: :string,
    checkpoint_interval_ms: :integer,
    checkpoint_every: :integer,
    checkpoint_outputs: :string,
    checkpoint_delay_ms: :integer,
    linger_ms: :integer,
    window: :integer,
    acks: :integer,
    idempotent: :boolean,
    max_in_flight: :integer,
    keys: :integer,
    fsync_interval_ms: :integer,
    profile: :boolean,
    tprof: :boolean,
    stall_timeout: :integer,
    status_every: :integer,
    keep_topics: :boolean
  ]

  @aliases [e: :engine, r: :runtime, w: :workload, p: :pipelines, m: :messages_per_pipeline]

  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: @switches, aliases: @aliases)

    Process.flag(:trap_exit, true)

    cfg = build_config(opts)

    IO.puts("""
    === bench.roundtrip ===
    engine:      #{cfg.engine}#{if cfg.engine == :flezha, do: " (#{cfg.runtime})", else: ""}
    workload:    #{cfg.workload}
    durability:  #{cfg.durability}
    pipelines:   #{cfg.pipelines} (== partitions)
    messages:    #{cfg.messages_per_pipeline} per pipeline (#{cfg.total} total)
    payload:     #{cfg.payload_bytes} bytes
    rate:        #{if cfg.rate == 0, do: "unpaced", else: "#{cfg.rate}/s/partition"}
    broker:      #{cfg.broker_host}:#{cfg.broker_port}
    """)

    # `FLEZHA_BENCH_LOG=info` surfaces the controller's own diagnostics (credit
    # handshake, rebinds) which are otherwise below the bench's noise floor.
    log_level =
      case System.get_env("FLEZHA_BENCH_LOG") do
        nil -> :warning
        level -> String.to_existing_atom(level)
      end

    Logger.configure(level: log_level)
    Application.put_env(:logger, :level, log_level)

    # A run killed mid-flight (a timeout, a crash) never reaches teardown, so its
    # operatives stay in the hub's declared set and their state dirs stay on
    # disk. The convergence loop then spends every subsequent run trying to
    # restore dozens of dead operatives, which quietly skews the numbers. This
    # namespace exists only for this task, so reset it before the controller
    # boots and can restore anything.
    for dir <- [
          Path.join(["app_data", "state", @namespace]),
          Path.join(["app_data", "process_hub", "#{@namespace}_hub"])
        ] do
      File.rm_rf!(dir)
    end

    # Before app.start: FsyncBatcher validates and binds the tick at init/1.
    # This is the durability-batching interval for the per-Entity WAL and the
    # effectively-once input log, and the outbox release is gated on it.
    if cfg.fsync_interval_ms do
      Application.put_env(:flezha, :agent_fsync_interval_ms, cfg.fsync_interval_ms)
    end

    Mix.Task.run("app.start")
    Logger.configure(level: log_level)
    {:ok, _} = Application.ensure_all_started(:inets)

    Process.put(:bench_runtime, cfg.runtime)

    client = start_kafka_client!(cfg)
    create_topics!(client, cfg)

    Process.put(:bench_engine, nil)

    try do
      # Everything that is already standing when a Flink job is submitted — the
      # cluster processes, the uploaded jar — must also be standing for Flezha,
      # or the startup number compares a process spawn against a scheduling
      # decision. `prepare` is that pre-existing world; only `start_engine` is
      # inside the timed window.
      prepared = prepare_engine(cfg)

      # Records wait in the input topic before the engine exists, so time to
      # first record out measures bring-up, not generator warmup. Both engines
      # start from the earliest offset.
      prime(client, cfg)
      collector = start_collector(cfg)

      profiler = if cfg.profile, do: start_profiler(), else: nil
      if cfg.profile or cfg.status_every, do: attach_wal_telemetry()

      t0 = System.monotonic_time(:nanosecond)
      engine = start_engine(cfg, prepared)
      Process.put(:bench_engine, engine)

      first_out_ns = await_first_record(collector, cfg) - t0

      IO.puts("Time to first record out: #{fmt_ms(first_out_ns)} ms")
      IO.puts("  (engine bring-up detail: #{inspect(engine.startup)})")

      tprof = if cfg.tprof, do: start_tprof(), else: nil

      # The collector drains WHILE the generator runs: otherwise every batch of
      # the run waits in this process's mailbox until generation ends, which a
      # soak cannot afford and which would hide the status line until the end.
      gen_started = System.monotonic_time(:nanosecond)

      generator =
        Task.async(fn ->
          generate(client, cfg)
          System.monotonic_time(:nanosecond)
        end)

      {results, skipped, done_at} = await_completion(collector, cfg)
      gen_ended = Task.await(generator, :infinity)
      gen_ns = gen_ended - gen_started
      drain_ns = max(done_at - gen_ended, 0)
      if profiler, do: report_profile(profiler)
      if cfg.profile or cfg.status_every, do: report_wal_telemetry()
      if tprof, do: report_tprof(tprof)

      report(cfg, %{
        first_out_ns: first_out_ns,
        engine_startup: engine.startup,
        generate_ns: gen_ns,
        drain_ns: drain_ns,
        skipped: skipped,
        results: results
      })

      :ok
    after
      case Process.get(:bench_engine) do
        %{stop: stop} -> stop.()
        _ -> :ok
      end

      unless cfg.keep_topics, do: delete_topics(client, cfg)
      GenServer.stop(client, :normal, 5_000)
    end
  end

  # ---------------------------------------------------------------- config --

  defp build_config(opts) do
    engine = parse_enum(opts[:engine] || "passthrough", ~w(flezha flink passthrough), "--engine")

    workload =
      parse_enum(opts[:workload] || "identity", ~w(identity keyed), "--workload")

    durability =
      parse_enum(opts[:durability] || "off", ~w(off state on checkpoint), "--durability")

    pipelines = opts[:pipelines] || 4
    per_pipeline = opts[:messages_per_pipeline] || 5_000
    {broker_host, broker_port} = parse_broker(opts[:broker] || "localhost:39092")

    run_id = Integer.to_string(System.system_time(:second), 36)

    %{
      engine: engine,
      workload: workload,
      durability: durability,
      pipelines: pipelines,
      messages_per_pipeline: per_pipeline,
      total: pipelines * per_pipeline,
      payload_bytes: max(opts[:payload_bytes] || 128, 48),
      # Padding is sliced from one shared binary: building it per record made
      # the generator a measurable share of the box both engines run on.
      pad_pool: String.duplicate("x", max(opts[:payload_bytes] || 128, 48)),
      rate: opts[:rate] || 0,
      batch: opts[:batch] || 200,
      runners: opts[:runners] || 2,
      runtime: parse_runtime(opts[:runtime], durability),
      flink_jobs: opts[:flink_jobs] || 1,
      broker_host: broker_host,
      broker_port: broker_port,
      flink_rest: opts[:flink_rest] || "http://localhost:8083",
      jar: opts[:jar] || "bench/flink/job/target/flink-bench.jar",
      checkpoint_interval_ms: opts[:checkpoint_interval_ms] || 1_000,
      # `--durability checkpoint` only: how the interval closes, and whether it
      # holds its outputs until it does.
      checkpoint_every: opts[:checkpoint_every] || 500,
      checkpoint_outputs:
        parse_enum(
          opts[:checkpoint_outputs] || "as_produced",
          ~w(as_produced at_checkpoint),
          "--checkpoint-outputs"
        ),
      checkpoint_delay_ms: opts[:checkpoint_delay_ms] || 50,
      linger_ms: opts[:linger_ms] || 10,
      # Confirming ingests the bridge keeps in flight per pipeline.
      window: opts[:window] || 64,
      # Sink posture overrides, to measure the sink apart from the tier it
      # normally rides with. Unset, they follow `--durability`.
      acks: opts[:acks] || if(durability in [:on, :checkpoint], do: -1, else: 1),
      idempotent:
        if(is_nil(opts[:idempotent]),
          do: durability in [:on, :checkpoint],
          else: opts[:idempotent]
        ),
      # Produce requests outstanding per partition. Unset, the plugin's own
      # default applies (5 under idempotence, 1 without).
      max_in_flight: opts[:max_in_flight],
      keys: max(opts[:keys] || 1, 1),
      fsync_interval_ms: opts[:fsync_interval_ms],
      profile: opts[:profile] || false,
      # Two-second per-function CPU profile of every process, taken while the
      # generator is still producing. Distorts the run's own numbers — use it
      # to rank hot functions, never to report throughput.
      tprof: opts[:tprof] || false,
      stall_timeout: opts[:stall_timeout] || 30_000,
      status_every: opts[:status_every],
      keep_topics: opts[:keep_topics] || false,
      run_id: run_id,
      in_topic: "#{@in_prefix}.#{run_id}",
      out_topic: "#{@out_prefix}.#{run_id}",
      # The priming batch is excluded from every reported measurement; it only
      # exists so the engine has something to chew on the instant it starts.
      prime_count: pipelines
    }
  end

  # An unset axis leaves the option out entirely, so the plugin's own default
  # decides rather than the bench.
  defp put_unless_nil(map, _key, nil), do: map
  defp put_unless_nil(map, key, value), do: Map.put(map, key, value)

  defp parse_enum(value, allowed, flag) do
    if value in allowed do
      String.to_atom(value)
    else
      raise "#{flag} must be one of #{Enum.join(allowed, "|")}, got #{inspect(value)}"
    end
  end

  defp parse_broker(spec) do
    case String.split(spec, ":") do
      [host, port] -> {host, String.to_integer(port)}
      _ -> raise "--broker must be host:port, got #{inspect(spec)}"
    end
  end

  # ----------------------------------------------------------------- kafka --

  defp start_kafka_client!(cfg) do
    # Unnamed, and never routed through the plugin's WorkerRegistry: that
    # registry hands out refcounted shared clients, and stopping one from here
    # would take the producer plugins down with it.
    {:ok, pid} =
      KafkaEx.start_link_worker(:no_name, uris: [{cfg.broker_host, cfg.broker_port}])

    pid
  end

  defp create_topics!(client, cfg) do
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

  defp delete_topics(client, cfg) do
    KafkaEx.API.delete_topics(client, [cfg.in_topic, cfg.out_topic], 15_000)
  rescue
    _ -> :ok
  end

  # ------------------------------------------------------------- generator --

  defp prime(client, cfg) do
    # seq 0..pipelines-1, one per partition, so every partition has a record
    # waiting the moment the engine starts.
    Enum.each(0..(cfg.pipelines - 1), fn p ->
      produce_batch(client, cfg, p, [p])
    end)
  end

  defp generate(_client, cfg) do
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

  defp produce_retry_counter do
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

  # ------------------------------------------------------------- collector --

  defp start_collector(cfg) do
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

  defp await_first_record(_collector, cfg) do
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
  defp await_completion(collector, cfg) do
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
          "dup=#{tally.duplicates}#{explained_dups(cfg)} skipped=#{tally.skipped} | state #{Tally.state_dir_summary(@namespace)} | " <>
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
  defp explained_dups(cfg) do
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

  # --------------------------------------------------------------- profile --

  # Samples the busiest processes while the run is in flight. Mailbox depth
  # names the serialization point; reductions name where the CPU goes.
  # Counts the WAL writer's loss/anomaly signals for the run. A shed is the
  # writer dropping a Change past its hard in-memory bound; a credit anomaly is
  # a shed that happened even though the sender was credit-speaking, which the
  # writer treats as a protocol violation rather than back-pressure.
  defp attach_wal_telemetry do
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

  defp report_wal_telemetry do
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
      Flezha.Transport.LivenessCounters.driver_refs()
      |> Enum.flat_map(fn {_key, ref} ->
        case Flezha.Transport.Native.driver_stats(ref) do
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
  defp start_tprof do
    {:ok, _} = :tprof.start(%{type: :call_time})
    :tprof.set_pattern(:_, :_, :_)

    spawn_link(fn ->
      Process.sleep(1_500)
      _ = :tprof.enable_trace(:all)
      Process.sleep(2_000)
      _ = :tprof.disable_trace(:all)
    end)
  end

  defp report_tprof(_pid) do
    IO.puts("\n--- tprof: per-function CPU over a 2 s window, all processes (top 50 by time) ---")

    {_key, {_type, total_us, rows}} =
      :tprof.collect() |> :tprof.inspect(:total, :measurement) |> Enum.at(0)

    IO.puts("  total traced: #{div(total_us, 1000)} ms")

    rows
    |> Enum.sort_by(&(-elem(&1, 3)))
    |> Enum.take(50)
    |> Enum.each(fn {m, {f, a}, calls, us, per_call, pct} ->
      IO.puts(
        "  #{String.pad_trailing("#{inspect(m)}.#{f}/#{a}", 58)} " <>
          "#{String.pad_leading(Integer.to_string(calls), 9)} " <>
          "#{String.pad_leading(Integer.to_string(us), 9)}us " <>
          "#{String.pad_leading(:erlang.float_to_binary(per_call * 1.0, decimals: 2), 8)} " <>
          "#{:erlang.float_to_binary(pct * 1.0, decimals: 1)}%"
      )
    end)

    :tprof.stop()
  end

  defp start_profiler do
    parent = self()
    # Microstate accounting splits every BEAM thread's time into emulator /
    # gc / nif / port / sleep — the one view that says whether the controller's
    # CPU goes to Elixir code, to garbage, or to the transport and file NIFs.
    _ = :msacc.start()

    spawn_link(fn ->
      Process.flag(:trap_exit, true)
      profile_loop(parent, %{}, %{})
    end)
  end

  defp profile_loop(parent, queues, reds) do
    receive do
      {:report, from} -> send(from, {:profile, queues, reds})
    after
      0 ->
        # `proc_window` samples reductions over the window, so the figure is a
        # rate (per 2 s) and names who is CPU-bound, not who has lived longest.
        reds = merge_top(reds, :recon.proc_window(:reductions, 8, 2_000))
        queues = merge_top(queues, :recon.proc_count(:message_queue_len, 8))
        {:total, util, _} = List.keyfind(:scheduler.utilization(1), :total, 0)
        util = Float.round(util, 2)

        top =
          :recon.proc_count(:memory, 3)
          |> Enum.map(fn {pid, bytes, info} ->
            "#{label_for(pid, info)}=#{div(bytes, 1_048_576)}MB"
          end)
          |> Enum.join("  ")

        IO.puts(
          :stderr,
          "[mem] sched=#{util} total=#{div(:erlang.memory(:total), 1_048_576)}MB " <>
            "procs=#{length(:erlang.processes())} " <>
            "proc=#{div(:erlang.memory(:processes), 1_048_576)}MB " <>
            "bin=#{div(:erlang.memory(:binary), 1_048_576)}MB " <>
            "ets=#{div(:erlang.memory(:ets), 1_048_576)}MB  #{top}"
        )

        profile_loop(parent, queues, reds)
    end
  end

  defp merge_top(acc, samples) do
    Enum.reduce(samples, acc, fn {pid, value, info}, m ->
      Map.update(m, label_for(pid, info), value, &max(&1, value))
    end)
  end

  defp label_for(pid, info) do
    case Enum.find(info, &match?({:registered_name, _}, &1)) do
      {:registered_name, name} when is_atom(name) ->
        inspect(name)

      _ ->
        info
        |> Enum.find_value(inspect(pid), fn
          {:current_function, mfa} -> inspect(mfa)
          _ -> nil
        end)
    end
  end

  defp report_profile(profiler) do
    send(profiler, {:report, self()})

    receive do
      {:profile, queues, reds} ->
        IO.puts("\n--- peak mailbox depth (serialization points) ---")
        print_top(queues)
        IO.puts("--- peak reductions per 2 s window (CPU) ---")
        print_top(reds)
        _ = :msacc.stop()
        IO.puts("--- BEAM microstate accounting (whole run) ---")
        :msacc.print(:msacc.stats(), %{system: true})
    after
      2_000 -> IO.puts("(profiler did not report)")
    end
  end

  defp print_top(map) do
    map
    |> Enum.sort_by(fn {_k, v} -> -v end)
    |> Enum.take(8)
    |> Enum.each(fn {k, v} -> IO.puts("  #{String.pad_trailing(to_string(k), 60)} #{v}") end)
  end

  # ---------------------------------------------------------------- report --

  defp report(cfg, m) do
    # The priming records are excluded from the tally: they were produced
    # before the engine existed, so their latency is bring-up time.
    tally = m.results
    received = tally.received
    lost = cfg.total - received

    # Over the WHOLE window (first send -> last arrival), not the drain window
    # alone. Records stream out while the generator is still producing, so a
    # drain-only rate is inflated — badly so under `--rate`, where generation
    # deliberately takes most of the run.
    window_ns = m.generate_ns + m.drain_ns
    throughput = if window_ns > 0, do: received * 1_000_000_000 / window_ns, else: 0.0

    IO.puts(
      "\n\n=== Round-trip summary (#{cfg.engine} / #{cfg.workload} / durability=#{cfg.durability}) ==="
    )

    IO.puts("Records sent:        #{cfg.total}")

    IO.puts(
      "Records received:    #{received}#{if lost != 0, do: "  ** #{lost} LOST **", else: ""}"
    )

    IO.puts("Time to first out:   #{fmt_ms(m.first_out_ns)} ms")
    IO.puts("Generate window:     #{fmt_ms(m.generate_ns)} ms")
    IO.puts("Drain window:        #{fmt_ms(m.drain_ns)} ms")

    IO.puts(
      "Throughput:          #{Float.round(throughput, 2)} rec/s" <>
        if(cfg.rate > 0, do: " (rate-capped at ~#{cfg.rate * cfg.pipelines}/s)", else: "")
    )

    if m.skipped > 0 do
      IO.puts("Non-payload records: #{m.skipped} (transaction control markers)")
    end

    retries = :counters.get(produce_retry_counter(), 1)
    if retries > 0, do: IO.puts("Generator retries:   #{retries} (transient produce failures)")

    if tally.duplicates > retries * cfg.batch do
      IO.puts(
        "  ** #{tally.duplicates - retries * cfg.batch} DUPLICATES the generator cannot explain **"
      )
    end

    if tally.duplicates > 0 do
      IO.puts("Duplicates:          #{tally.duplicates}#{explained_dups(cfg)}")
    end

    if tally.foreign > 0 do
      IO.puts("Foreign records:     #{tally.foreign} (seq outside this run)")
    end

    pct = Tally.percentiles(tally, [0.50, 0.95, 0.99, 0.999])

    if received > 0 do
      IO.puts("Latency p50:         #{fmt_ms(pct[0.50])} ms")
      IO.puts("Latency p95:         #{fmt_ms(pct[0.95])} ms")
      IO.puts("Latency p99:         #{fmt_ms(pct[0.99])} ms")
      IO.puts("Latency p99.9:       #{fmt_ms(pct[0.999])} ms")
      IO.puts("Latency max:         #{fmt_ms(tally.max_ns)} ms")
    end

    write_report(cfg, m, tally, pct, received, lost, throughput)
  end

  defp write_report(cfg, m, tally, pct, received, lost, throughput) do
    path =
      Path.join(
        "bench/flink/reports",
        "#{cfg.engine}-#{cfg.workload}-#{cfg.durability}-#{cfg.run_id}.json"
      )

    File.mkdir_p!(Path.dirname(path))

    payload = %{
      engine: cfg.engine,
      workload: cfg.workload,
      durability: cfg.durability,
      pipelines: cfg.pipelines,
      messages_per_pipeline: cfg.messages_per_pipeline,
      payload_bytes: cfg.payload_bytes,
      rate: cfg.rate,
      # `bench.sh` stamps every report of one sweep, so `summarize.py` can fold
      # a `--repeat` series into one median row and keep stray runs apart.
      sweep: System.get_env("FLZ_BENCH_SWEEP"),
      sent: cfg.total,
      received: received,
      lost: lost,
      duplicates: tally.duplicates,
      generator_retries: :counters.get(produce_retry_counter(), 1),
      first_out_ms: fmt_ms(m.first_out_ns),
      engine_startup: inspect(m.engine_startup),
      generate_ms: fmt_ms(m.generate_ns),
      drain_ms: fmt_ms(m.drain_ns),
      throughput_per_sec: Float.round(throughput, 2),
      non_payload_records: m.skipped,
      latency_ms:
        if(received > 0,
          do: %{
            p50: fmt_ms(pct[0.50]),
            p95: fmt_ms(pct[0.95]),
            p99: fmt_ms(pct[0.99]),
            p999: fmt_ms(pct[0.999]),
            max: fmt_ms(tally.max_ns)
          },
          else: nil
        )
    }

    File.write!(path, Jason.encode!(payload, pretty: true))
    IO.puts("\nReport: #{path}")
  end

  defp fmt_ms(ns), do: Float.round(ns / 1_000_000, 3)

  # ---------------------------------------------------------------- engines --

  defp prepare_engine(%{engine: :passthrough}), do: nil

  defp prepare_engine(%{engine: :flezha} = cfg) do
    Fleet.setup(@namespace, cfg.runners, checkpoint_env(cfg))
    Fleet.preload_code()
    nil
  end

  defp prepare_engine(%{engine: :flink} = cfg), do: upload_jar!(cfg)

  # The checkpoint fixture DECLARES its interval, so the knobs have to reach the
  # runner before `channels()` runs — env, not a control frame.
  defp checkpoint_env(%{durability: :checkpoint} = cfg) do
    [
      {"FLEZHA_BENCH_CHECKPOINT_EVERY", to_string(cfg.checkpoint_every)},
      {"FLEZHA_BENCH_CHECKPOINT_OUTPUTS", to_string(cfg.checkpoint_outputs)},
      {"FLEZHA_BENCH_CHECKPOINT_DELAY_MS", to_string(cfg.checkpoint_delay_ms)}
    ]
  end

  defp checkpoint_env(_cfg), do: []

  defp start_engine(%{engine: :passthrough} = cfg, _prepared), do: start_passthrough(cfg)
  defp start_engine(%{engine: :flezha} = cfg, _prepared), do: start_flezha(cfg)
  defp start_engine(%{engine: :flink} = cfg, jar_id), do: start_flink(cfg, jar_id)

  @doc false
  def start_passthrough(cfg) do
    {startup_us, pids} =
      :timer.tc(fn ->
        Enum.map(0..(cfg.pipelines - 1), fn p ->
          spawn_link(fn -> passthrough_loop(cfg, p) end)
        end)
      end)

    %{startup: %{spawn_us: startup_us}, stop: fn -> Enum.each(pids, &Process.exit(&1, :kill)) end}
  end

  defp passthrough_loop(cfg, partition) do
    {:ok, client} =
      KafkaEx.start_link_worker(:no_name, uris: [{cfg.broker_host, cfg.broker_port}])

    passthrough_step(client, cfg, partition, 0)
  end

  defp passthrough_step(client, cfg, partition, offset) do
    opts = [max_wait_time: 50, min_bytes: 1, max_bytes: 5_000_000]

    case KafkaEx.API.fetch(client, cfg.in_topic, partition, offset, opts) do
      {:ok, %{records: []}} ->
        passthrough_step(client, cfg, partition, offset)

      {:ok, %{records: records}} ->
        messages = Enum.map(records, &%{value: &1.value})
        KafkaEx.API.produce(client, cfg.out_topic, partition, messages, required_acks: 1)
        passthrough_step(client, cfg, partition, List.last(records).offset + 1)

      {:error, _} ->
        Process.sleep(50)
        passthrough_step(client, cfg, partition, offset)
    end
  end

  @doc false
  def start_flezha(cfg) do
    factory = flezha_factory(cfg)

    # Both phases bring every pipeline up at once, the way a partition group's
    # members start; each number is that phase's wall time.
    {create_us, ids} =
      :timer.tc(fn ->
        Flezha.Parallel.map(0..(cfg.pipelines - 1), fn p ->
          id = "rt_#{cfg.run_id}_#{p}"

          {:ok, _} =
            OperativeManager.start_operative(@namespace, %Flezha.Operative.Builder{
              id: id,
              factory_id: factory
            })

          id
        end)
      end)

    {plugin_us, _} =
      :timer.tc(fn ->
        ids
        |> Enum.with_index()
        |> Flezha.Parallel.map(fn {id, p} -> enable_kafka_sink!(cfg, id, p) end)
      end)

    bridges =
      ids
      |> Enum.with_index()
      |> Enum.map(fn {id, p} -> spawn_link(fn -> bridge_loop(cfg, id, p) end) end)

    %{
      startup: %{operative_create_us: create_us, plugin_enable_us: plugin_us},
      stop: fn ->
        Enum.each(bridges, &Process.exit(&1, :kill))
        Fleet.cleanup(@namespace)
      end
    }
  end

  # `do_nothing` echoes the payload verbatim. `bench_keyed` / `tier_store` are
  # the same keyed per-key upsert; only the latter declares effectively-once,
  # which is a compile-time channel property rather than a runtime flag.
  # Tiers are matched to what the other engine is actually doing:
  #   off   — heap-only state, nothing leaves the runner (Flink: no checkpointing)
  #   state — every Change ships to the controller (Flink: checkpointing on)
  #   on    — effectively-once end to end (Flink: checkpointing + EXACTLY_ONCE)
  # `bench_keyed` at `off` would still ship every Change, which is why the
  # no-persist twin is the correct partner for an un-checkpointed Flink job.
  # An impossible axis is refused before a runner starts, by name: every SDK
  # that carries a tier has the fixture for it, and one that defers the tier has
  # nothing to measure.
  defp parse_runtime(nil, _durability), do: :rust

  defp parse_runtime(name, durability) do
    runtime = parse_enum(name, ~w(rust java elixir), "--runtime")

    if runtime == :elixir and durability != :off do
      Mix.raise(
        "--runtime elixir carries only --durability off: the Elixir SDK v1 defers state " <>
          "persistence and the effectively-once tier (see docs/sdk/elixir_sdk.md and " <>
          "test/apps/elixir_runner/conformance.json)"
      )
    end

    runtime
  end

  defp flezha_factory(%{workload: :identity}), do: "bench_echo"
  defp flezha_factory(%{workload: :keyed, durability: :off}), do: "bench_keyed_nopersist"
  defp flezha_factory(%{workload: :keyed, durability: :state}), do: "bench_keyed"
  defp flezha_factory(%{workload: :keyed, durability: :on}), do: "tier_store"
  # Exactly-once acknowledged per checkpoint instead of per record — the
  # amortised posture Flink's barrier is; Flink's side of this cell is `on`.
  defp flezha_factory(%{workload: :keyed, durability: :checkpoint}), do: "bench_checkpoint"

  defp enable_kafka_sink!(cfg, operative_id, partition) do
    plugin_id = "#{operative_id}_kafka"

    {:ok, _} =
      PluginManager.enable_plugin(plugin_id, %Flezha.Plugin{
        id: plugin_id,
        namespace: @namespace,
        operative_id: operative_id,
        provider: Flezha.Plugin.Provider.Kafka.Producer,
        opts:
          %{
            hosts: [{cfg.broker_host, cfg.broker_port}],
            topic: cfg.out_topic,
            # One plugin instance is one (topic, partition), so partitions ==
            # pipelines.
            partition: partition,
            linger_ms: cfg.linger_ms,
            queue_max: 100_000,
            # Blocking propagates back-pressure to the bridge, which is what we
            # want to measure; stated so the bench never depends on the default.
            queue_overflow: :block,
            queue_block_timeout_ms: 60_000,
            # Single-broker bench cluster; the plugin default of 2 fails.
            auto_create_replication_factor: 1,
            required_acks: cfg.acks,
            idempotent: cfg.idempotent
          }
          |> put_unless_nil(:max_in_flight, cfg.max_in_flight),
        subscriptions: [{:on_output, []}]
      })
  end

  defp bridge_loop(cfg, operative_id, partition) do
    {:ok, client} =
      KafkaEx.start_link_worker(:no_name, uris: [{cfg.broker_host, cfg.broker_port}])

    bridge_step(client, cfg, operative_id, partition, 0)
  end

  defp bridge_step(client, cfg, operative_id, partition, offset) do
    opts = [max_wait_time: 50, min_bytes: 1, max_bytes: 5_000_000]

    case KafkaEx.API.fetch(client, cfg.in_topic, partition, offset, opts) do
      {:ok, %{records: []}} ->
        bridge_step(client, cfg, operative_id, partition, offset)

      {:ok, %{records: records}} ->
        ingest!(cfg, operative_id, records)
        bridge_step(client, cfg, operative_id, partition, List.last(records).offset + 1)

      {:error, _} ->
        Process.sleep(50)
        bridge_step(client, cfg, operative_id, partition, offset)
    end
  end

  # One fetch, one pipelined window of confirming ingests — each record is its
  # own ingest, in order; only the round trips overlap. `:ok`, `{:ok, :absorbed}`
  # and `{:ok, :suspended}` are all terminal success — the operative owns the
  # payload. Same contract the HTTP-poll source uses.
  defp ingest!(cfg, operative_id, records) do
    payloads = Enum.map(records, &{&1.value, %{}})

    OperativeManager.send_io_confirming_many(
      operative_id,
      @namespace,
      payloads,
      30_000,
      cfg.window
    )
    |> Enum.each(fn
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> raise "ingest to #{operative_id} refused: #{inspect(reason)}"
    end)
  end

  @doc false
  def start_flink(cfg, jar_id) do
    args = [
      "--brokers",
      "kafka:9092",
      "--in-topic",
      cfg.in_topic,
      "--out-topic",
      cfg.out_topic,
      "--workload",
      to_string(cfg.workload),
      "--durability",
      if(cfg.durability == :checkpoint, do: "on", else: to_string(cfg.durability)),
      "--checkpoint-interval-ms",
      to_string(cfg.checkpoint_interval_ms)
    ]

    {submit_us, job_ids} =
      :timer.tc(fn ->
        for _ <- 1..cfg.flink_jobs do
          id = flink_run!(cfg, jar_id, args)
          await_running!(cfg, id)
          id
        end
      end)

    %{
      startup: %{submit_to_running_us: submit_us, jobs: length(job_ids)},
      stop: fn -> Enum.each(job_ids, &flink_cancel(cfg, &1)) end
    }
  end

  # Multipart upload via curl. Runs in `prepare_engine/1`, before t0, so startup
  # measures scheduling rather than jar transfer.
  defp upload_jar!(cfg) do
    unless File.exists?(cfg.jar) do
      raise "Flink job jar not found at #{cfg.jar}. Build it with: mvn -f bench/flink/job/pom.xml package"
    end

    {out, 0} =
      System.cmd("curl", [
        "-sS",
        "-X",
        "POST",
        "-H",
        "Expect:",
        "-F",
        "jarfile=@#{cfg.jar}",
        "#{cfg.flink_rest}/jars/upload"
      ])

    %{"filename" => filename} = Jason.decode!(out)
    Path.basename(filename)
  end

  defp flink_run!(cfg, jar_id, args) do
    body = Jason.encode!(%{programArgsList: args, parallelism: cfg.pipelines})

    case http(:post, "#{cfg.flink_rest}/jars/#{jar_id}/run", body) do
      {:ok, %{"jobid" => id}} -> id
      other -> raise "flink run failed: #{inspect(other)}"
    end
  end

  defp await_running!(cfg, job_id, tries \\ 300) do
    case http(:get, "#{cfg.flink_rest}/jobs/#{job_id}", nil) do
      {:ok, %{"state" => "RUNNING"}} ->
        :ok

      {:ok, %{"state" => state}} when state in ~w(FAILED CANCELED FINISHED) ->
        raise "flink job #{job_id} reached terminal state #{state} before RUNNING"

      _ when tries > 0 ->
        Process.sleep(100)
        await_running!(cfg, job_id, tries - 1)

      _ ->
        raise "flink job #{job_id} never reached RUNNING"
    end
  end

  defp flink_cancel(cfg, job_id) do
    http(:patch, "#{cfg.flink_rest}/jobs/#{job_id}?mode=cancel", nil)
  end

  defp http(:get, url, _body), do: do_http(:get, {String.to_charlist(url), []})

  defp http(method, url, body) do
    do_http(method, {String.to_charlist(url), [], ~c"application/json", body || ""})
  end

  defp do_http(method, request) do
    case :httpc.request(method, request, [{:timeout, 30_000}], body_format: :binary) do
      {:ok, {{_, status, _}, _headers, resp}} when status in 200..299 ->
        if resp == "", do: {:ok, %{}}, else: {:ok, Jason.decode!(resp)}

      other ->
        {:error, other}
    end
  end
end

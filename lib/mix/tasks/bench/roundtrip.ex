defmodule Mix.Tasks.Bench.Roundtrip do
  use Mix.Task

  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Report
  alias Mix.Tasks.Bench.Roundtrip.Engines
  alias Mix.Tasks.Bench.Roundtrip.Load
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

    * `flezha` — N operatives, one Kafka producer plugin each; `--runtime`
      (default `rust`) picks which SDK's runners serve them and accepts any SDK
      with a `test/apps/<runtime>_runner/conformance.json`, which is also what
      launches it. A number is a number for the runtime that produced it, so a
      `--durability` posture whose capability that manifest defers is refused by
      name before a runner starts
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
    open_report(cfg)
    boot(cfg)

    client = Load.start_kafka_client!(cfg)
    Load.create_topics!(client, cfg)

    Process.put(:bench_engine, nil)

    try do
      measure(client, cfg)
    after
      case Process.get(:bench_engine) do
        %{stop: stop} -> stop.()
        _ -> :ok
      end

      unless cfg.keep_topics, do: Load.delete_topics(client, cfg)
      GenServer.stop(client, :normal, 5_000)
    end
  end

  defp open_report(cfg) do
    Report.open("bench.roundtrip", [
      {"engine", to_string(cfg.engine)},
      {"runtime",
       if(cfg.engine == :flezha,
         do: "#{cfg.runtime} (#{Report.dispatch_note(cfg.runtime)})",
         else: "n/a"
       )},
      {"toolchain", if(cfg.engine == :flezha, do: Report.toolchain(cfg.runtime), else: "n/a")},
      {"workload", to_string(cfg.workload)},
      {"durability", to_string(cfg.durability)},
      {"pipelines", "#{cfg.pipelines} (== partitions)"},
      {"messages", "#{cfg.messages_per_pipeline} per pipeline (#{cfg.total} total)"},
      {"payload", "#{cfg.payload_bytes} bytes"},
      {"rate", if(cfg.rate == 0, do: "unpaced", else: "#{cfg.rate}/s/partition")},
      {"broker", "#{cfg.broker_host}:#{cfg.broker_port}"},
      {"mix env", to_string(Mix.env())}
    ])
  end

  defp boot(cfg) do
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
    Report.apply_log_level()
    {:ok, _} = Application.ensure_all_started(:inets)

    Process.put(:bench_runtime, cfg.runtime)
  end

  # Everything that is already standing when a Flink job is submitted — the
  # cluster processes, the uploaded jar — must also be standing for Flezha, or
  # the startup number compares a process spawn against a scheduling decision.
  # This is that pre-existing world; only `start_engine` is inside the timed
  # window. Records also wait in the input topic before the engine exists, so
  # time to first record out measures bring-up, not generator warmup.
  defp arm(client, cfg) do
    prepared = Engines.prepare_engine(cfg)
    Report.emit(%{durability: cfg.durability})

    Load.prime(client, cfg)
    collector = Load.start_collector(cfg)

    profiler = if cfg.profile, do: start_profiler(), else: nil
    if cfg.profile or cfg.status_every, do: Load.attach_wal_telemetry()

    {prepared, collector, profiler}
  end

  defp measure(client, cfg) do
    {prepared, collector, profiler} = arm(client, cfg)

    t0 = System.monotonic_time(:nanosecond)
    engine = Engines.start_engine(cfg, prepared)
    Process.put(:bench_engine, engine)

    first_out_ns = Load.await_first_record(collector, cfg) - t0

    IO.puts("Time to first record out: #{fmt_ms(first_out_ns)} ms")
    IO.puts("  (engine bring-up detail: #{inspect(engine.startup)})")

    tprof = if cfg.tprof, do: start_tprof(), else: nil

    # The collector drains WHILE the generator runs: otherwise every batch of
    # the run waits in this process's mailbox until generation ends, which a
    # soak cannot afford and which would hide the status line until the end.
    gen_started = System.monotonic_time(:nanosecond)

    generator =
      Task.async(fn ->
        Load.generate(client, cfg)
        System.monotonic_time(:nanosecond)
      end)

    {results, skipped, done_at} = Load.await_completion(collector, cfg)
    gen_ended = Task.await(generator, :infinity)
    gen_ns = gen_ended - gen_started
    drain_ns = max(done_at - gen_ended, 0)

    close_run(cfg, profiler, tprof, %{
      first_out_ns: first_out_ns,
      engine_startup: engine.startup,
      generate_ns: gen_ns,
      drain_ns: drain_ns,
      skipped: skipped,
      results: results
    })
  end

  defp close_run(cfg, profiler, tprof, metrics) do
    if profiler, do: report_profile(profiler)
    if cfg.profile or cfg.status_every, do: Load.report_wal_telemetry()
    if tprof, do: report_tprof(tprof)

    report(cfg, metrics)

    :ok
  end

  # --------------------------------------------------------------- config --

  defp build_config(opts) do
    engine = parse_enum(opts[:engine] || "passthrough", ~w(flezha flink passthrough), "--engine")

    workload =
      parse_enum(opts[:workload] || "identity", ~w(identity keyed), "--workload")

    durability =
      parse_enum(opts[:durability] || "off", ~w(off state on checkpoint), "--durability")

    pipelines = opts[:pipelines] || 4
    per_pipeline = opts[:messages_per_pipeline] || 5_000
    {broker_host, broker_port} = parse_broker(opts[:broker] || "localhost:39092")

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
      runtime:
        Fleet.parse_runtime(
          opts[:runtime],
          durability_capability(durability),
          "--durability #{durability}"
        ),
      flink_jobs: opts[:flink_jobs] || 1,
      broker_host: broker_host,
      broker_port: broker_port,
      flink_rest: opts[:flink_rest] || "http://localhost:8083",
      jar: opts[:jar] || "bench/flink/job/target/flink-bench.jar",
      keys: max(opts[:keys] || 1, 1),
      fsync_interval_ms: opts[:fsync_interval_ms],
      # The scratch namespace this task owns. It travels on the config because
      # the collector's status line and the flezha engine both name it, and
      # a module attribute cannot cross a module boundary.
      namespace: @namespace
    }
    |> Map.merge(run_identity(pipelines))
    |> Map.merge(durability_posture(opts, durability))
    |> Map.merge(observation_config(opts))
  end

  # Every run mints its own topic pair, so a leftover consumer group or an
  # unread backlog cannot reach the next one.
  defp run_identity(pipelines) do
    run_id = Integer.to_string(System.system_time(:second), 36)

    %{
      run_id: run_id,
      in_topic: "#{@in_prefix}.#{run_id}",
      out_topic: "#{@out_prefix}.#{run_id}",
      # The priming batch is excluded from every reported measurement; it only
      # exists so the engine has something to chew on the instant it starts.
      prime_count: pipelines
    }
  end

  # How the durable tier is driven, and how the sink is posted. Unset, the
  # sink axes follow `--durability`.
  defp durability_posture(opts, durability) do
    %{
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
      acks: opts[:acks] || if(durability in [:on, :checkpoint], do: -1, else: 1),
      idempotent:
        if(is_nil(opts[:idempotent]),
          do: durability in [:on, :checkpoint],
          else: opts[:idempotent]
        ),
      # Produce requests outstanding per partition. Unset, the plugin's own
      # default applies (5 under idempotence, 1 without).
      max_in_flight: opts[:max_in_flight]
    }
  end

  # What the run watches itself with, and what it leaves behind.
  defp observation_config(opts) do
    %{
      profile: opts[:profile] || false,
      # Two-second per-function CPU profile of every process, taken while the
      # generator is still producing. Distorts the run's own numbers — use it
      # to rank hot functions, never to report throughput.
      tprof: opts[:tprof] || false,
      stall_timeout: opts[:stall_timeout] || 30_000,
      status_every: opts[:status_every],
      keep_topics: opts[:keep_topics] || false
    }
  end

  # An unset axis leaves the option out entirely, so the plugin's own default
  # decides rather than the bench.
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

  # -------------------------------------------------------------- profile --

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

    IO.puts(
      Report.table(
        ["function", "calls", "us", "us/call", "%"],
        rows
        |> Enum.sort_by(&(-elem(&1, 3)))
        |> Enum.take(50)
        |> Enum.map(fn {m, {f, a}, calls, us, per_call, pct} ->
          [
            "#{inspect(m)}.#{f}/#{a}",
            Integer.to_string(calls),
            Integer.to_string(us),
            :erlang.float_to_binary(per_call * 1.0, decimals: 2),
            :erlang.float_to_binary(pct * 1.0, decimals: 1)
          ]
        end),
        [:left, :right, :right, :right, :right]
      )
    )

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
    IO.puts(
      Report.kv(
        map
        |> Enum.sort_by(fn {_k, v} -> -v end)
        |> Enum.take(8)
        |> Enum.map(fn {k, v} -> {to_string(k), to_string(v)} end)
      )
    )
  end

  # --------------------------------------------------------------- report --

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

    retries = :counters.get(Load.produce_retry_counter(), 1)
    pct = Tally.percentiles(tally, [0.50, 0.95, 0.99, 0.999])

    IO.puts("")

    records_section(cfg, tally, m, received, lost, retries)

    if tally.duplicates > retries * cfg.batch do
      IO.puts(
        "  ** #{tally.duplicates - retries * cfg.batch} DUPLICATES the generator cannot explain **\n"
      )
    end

    phase_sections(cfg, tally, m, pct, received, window_ns, throughput)

    write_report(cfg, m, tally, pct, received, lost, throughput)
  end

  defp records_section(cfg, tally, m, received, lost, retries) do
    Report.section(
      "records",
      Report.kv(
        [
          {"sent", Report.rate(cfg.total)},
          {"received",
           "#{Report.rate(received)}#{if lost != 0, do: "  ** #{lost} LOST **", else: ""}"}
        ] ++
          if(m.skipped > 0,
            do: [{"non-payload", "#{m.skipped} (transaction control markers)"}],
            else: []
          ) ++
          if(retries > 0,
            do: [{"generator retries", "#{retries} (transient produce failures)"}],
            else: []
          ) ++
          if(tally.duplicates > 0,
            do: [{"duplicates", "#{tally.duplicates}#{Load.explained_dups(cfg)}"}],
            else: []
          ) ++
          if(tally.foreign > 0,
            do: [{"foreign", "#{tally.foreign} (seq outside this run)"}],
            else: []
          )
      )
    )
  end

  defp phase_sections(cfg, tally, m, pct, received, window_ns, throughput) do
    Report.section(
      "phases",
      Report.table(
        ["phase", "elapsed"],
        [
          ["time to first record out", "#{Report.ms_from_ns(m.first_out_ns)} ms"],
          ["generate window", "#{Report.ms_from_ns(m.generate_ns)} ms"],
          ["drain window", "#{Report.ms_from_ns(m.drain_ns)} ms"]
        ],
        [:left, :right]
      )
    )

    Report.section(
      "throughput",
      Report.table(
        ["window", "elapsed", "rec/s"],
        [
          [
            "end-to-end (generate + drain)#{if cfg.rate > 0, do: " — rate-capped at ~#{cfg.rate * cfg.pipelines}/s", else: ""}",
            "#{Report.ms_from_ns(window_ns)} ms",
            Report.rate(throughput)
          ]
        ],
        [:left, :right, :right]
      )
    )

    if received > 0 do
      Report.section(
        "latency",
        Report.table(
          ["percentile", "ms"],
          [
            ["p50", Report.ms_from_ns(pct[0.50])],
            ["p95", Report.ms_from_ns(pct[0.95])],
            ["p99", Report.ms_from_ns(pct[0.99])],
            ["p99.9", Report.ms_from_ns(pct[0.999])],
            ["max", Report.ms_from_ns(tally.max_ns)]
          ],
          [:left, :right]
        )
      )
    end
  end

  defp write_report(cfg, m, tally, pct, received, lost, throughput) do
    Report.finish(
      # A fixed location: `bench/flink/bench.sh` and `lib/summarize.py` read it.
      Path.join(
        "bench/flink/reports",
        "#{cfg.engine}-#{cfg.workload}-#{cfg.durability}-#{cfg.run_id}.json"
      ),
      %{
        config: %{
          engine: cfg.engine,
          workload: cfg.workload,
          durability: cfg.durability,
          pipelines: cfg.pipelines,
          messages_per_pipeline: cfg.messages_per_pipeline,
          payload_bytes: cfg.payload_bytes,
          rate: cfg.rate,
          # `bench.sh` stamps every report of one sweep, so `summarize.py` can
          # fold a `--repeat` series into one median row and keep stray runs
          # apart.
          sweep: System.get_env("FLZ_BENCH_SWEEP")
        },
        metrics: %{
          sent: cfg.total,
          received: received,
          lost: lost,
          duplicates: tally.duplicates,
          generator_retries: :counters.get(Load.produce_retry_counter(), 1),
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
      }
    )
  end

  defp fmt_ms(ns), do: Float.round(ns / 1_000_000, 3)

  # ----------------------------------------------------------- durability --

  defp durability_capability(:off), do: nil
  defp durability_capability(:state), do: :state_persistence
  defp durability_capability(:on), do: :effectively_once
  defp durability_capability(:checkpoint), do: :effectively_once
end

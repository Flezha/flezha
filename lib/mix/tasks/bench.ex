defmodule Mix.Tasks.Bench do
  use Mix.Task
  alias Flezha.Config
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Inputs
  alias Mix.Tasks.Bench.Report
  alias Mix.Tasks.Bench.Tally

  @shortdoc "Run performance tests for Flezha relays"
  @moduledoc """
  Run performance tests for Flezha relays.

  ## Usage

      mix bench --num-relays 1000 --messages-per-relay 1000
      mix bench --runtime java -n 100 -m 1000
      mix bench --runtime rust --shape cell --pool-sizes 1,8
      mix bench --runtime java --shape timer
      mix bench --shape send

  ## Options

    * `--num-relays` / `-n` - Number of relays to create (default: 100)
    * `--messages-per-relay` / `-m` - Number of messages to send per relay (default: 100)
    * `--runtime` / `-r` - Which SDK to benchmark: `rust` (default), or any
      other SDK with a test-app manifest. Each is launched from that manifest
      (`test/apps/<runtime>_runner/conformance.json`, `bench` profile), which
      names the build command for a missing artifact.
    * `--shape` - Bench shape: `regression` (default; the apples-to-apples
      regression check, factory `do_nothing`, dimensions from `-n` / `-m`),
      `cell` (the cell-stress shape, factory `cell_stress`, dimensions
      fixed at `8 × 20_000` so the numbers stay comparable across runs, swept
      across the comma-list in
      `--pool-sizes`), `dynamic` (the dynamic cell-mode shape, factory
      `dynamic_cell`, lazy per-key Cells with 8 keys/relay,
      dimensions from `-n` / `-m`), `tier` (the effectively-once tier, factory
      `tier_store`, keyed like `dynamic`), `sleeping` (a persistent stream
      that sleeps when idle, factory `sleeping_counter`, keyed like `dynamic`),
      `timer` (factory `timer_churn`: per input a processing-time timer one
      millisecond ahead, cancelling the previous input's; the firings are not
      counted), or `send` (Rust only; factory `send_forward`, whose runner sends
      each input through the controller to its own `do_nothing` relay, the one
      the responses are counted from).
    * `--pool-sizes` - Comma-separated worker-pool sizes (default: `"1,8"`),
      only honored when `--shape cell`. Each value spawns a fresh
      Rust runner binary with `FLEZHA_WORKER_POOL_SIZE=<N>` and
      reports throughput per-size; the scaling ratio is carried in the report.
    * `--warmup` / `-w` - Untimed messages per relay sent (and their
      responses drained) before the measured phase, so one-time runner
      cold-start — class loading, JIT, lazy codec init — stays out of the
      numbers (default: 10, `0` disables).
    * `--hook-url` - Enable the `hook_webhook` controller plugin, with every
      hook, against this URL in the bench namespace before the relays
      start, so the run carries the hook path's cost.
    * `--via` - How each input reaches its relay: `cast` (default; an
      `{:io_in, ...}` cast to the relay's Overwatch) or `host_inject` (a
      `Flezha.Plugin.Host.inject/5` in `:verdict` mode carrying a `flz.msg_id`,
      the path a plugin source takes; each relay is fed by its own caller,
      since a verdict waits for its relay).
    * `--latency` - Stamp every input with its seq and send time and report
      p50 / p95 / p99 / p99.9 / max over the outputs that carry the stamp back.
      The inputs are then sent from a process of their own, so an output is
      never timed behind the bench's own send loop. Every fixture the bench
      drives passes its input through to its output except `sleeping_counter`,
      which folds an integer and so reports `n=0`; `cell_stress` emits eight
      outputs per input, and the first one is the one timed.
    * `--rate` - Send at this many inputs per second in total, round-robin over
      the relays, instead of one burst. Latency under a burst includes queueing
      behind every input sent before it; at a fixed rate below saturation it is
      the figure other systems publish. Needs `--via cast`; not for `cell`.
    * `--payload-bytes` - Pad every input to exactly this many bytes. Every run
      reports its input size, with or without it. Not for `sleeping`.
    * `--strict` - Refuse to measure when a box or build warning holds
      (governor, load, `MIX_ENV`, dirty tree, skipped build, fallback or debug
      artifact). Workload warnings such as the durability one never refuse.

  `FLEZHA_DURABILITY_BACKEND` selects the remote durability tier. A
  `local_path` store that `scripts/bench.sh` chose (`FLEZHA_BENCH_DURABILITY_OWNED`)
  is cleared with the namespace before each run; any other store is kept, with
  a warning. Every run reports what it persisted during the measured window.

  ## Comparing SDKs

      mix bench --runtime rust -n 100 -m 1000 | tee bench-rust.txt
      mix bench --runtime java -n 100 -m 1000 | tee bench-java.txt

  The runtime selector only governs how the compute engine processes are
  launched — the controller, protocol, and measurement code are identical
  across runs, so the throughput figures are directly comparable.

  ## Output

  Every run opens with a header naming the runtime and its dispatch model, the
  machine, and the artifact it launched, followed by any preflight warning that
  qualifies the number. Results are tables; the headline throughput is the
  **end-to-end** window (first input to last response) and the receive-only
  window is reported beside it.

  Every run also writes `local_dev/bench/reports/bench-<runtime>-<shape>-<run_id>.json`,
  which carries every header, warning and metric field. That report is the
  machine contract — CI reads `metrics.throughput_msg_per_sec` from it rather
  than matching a line of printed output.

  `FLEZHA_BENCH_LOG` sets the logger level for the run (default `warning`).
  """

  @default_namespace "default"
  @bench_namespace "bench"

  @switches [
    num_relays: :integer,
    messages_per_relay: :integer,
    runtime: :string,
    shape: :string,
    pool_sizes: :string,
    warmup: :integer,
    hook_url: :string,
    via: :string,
    latency: :boolean,
    rate: :integer,
    payload_bytes: :integer,
    strict: :boolean
  ]

  @aliases [
    n: :num_relays,
    m: :messages_per_relay,
    r: :runtime,
    w: :warmup
  ]

  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: @switches, aliases: @aliases)

    runtime = Fleet.parse_runtime(opts[:runtime])
    shape = parse_shape(opts[:shape])
    pool_sizes = parse_pool_sizes(opts[:pool_sizes])
    warmup = max(opts[:warmup] || 10, 0)
    via = parse_via(opts[:via])

    {num_relays, messages_per_relay} = dimensions_for_shape(shape, opts)
    inputs = Inputs.parse(opts, shape, via, {num_relays, messages_per_relay}, warmup)

    Process.put(:bench_runtime, runtime)
    Process.put(:bench_hook_url, opts[:hook_url])
    Process.put(:bench_inputs, inputs)
    Process.put(:bench_strict, opts[:strict] || false)

    # A run killed mid-flight (a timeout, Ctrl-C) never reaches the `after`
    # block, so its relays stay in the hub's declared set and their state
    # dirs stay on disk. The convergence loop then spends every later run trying
    # to restore dead relays, which slows relay creation and quietly
    # skews the numbers. `@bench_namespace` is this task's own scratch namespace,
    # so reset it before the controller boots and can restore anything. The
    # `default` namespace is left alone — it may hold runners this task reuses.
    Fleet.reset_namespace(@bench_namespace)

    open_report(runtime, shape, pool_sizes, warmup, {num_relays, messages_per_relay}, inputs)

    Mix.Task.run("app.start")
    Report.apply_log_level()

    case shape do
      :cell ->
        run_cell_shape(num_relays, messages_per_relay, pool_sizes, warmup)

      shape ->
        run_default_shape(runtime, num_relays, messages_per_relay, shape, warmup)
    end
  end

  defp open_report(runtime, shape, pool_sizes, warmup, {num_relays, messages_per_relay}, inputs) do
    Report.open(
      "mix bench",
      [
        {"runtime", "#{runtime} (#{Report.dispatch_note(runtime)})"},
        {"toolchain", Report.toolchain(runtime)},
        {"transport", to_string(Config.get(:transport))},
        {"shape", to_string(shape)},
        {"durability", durability_row()},
        {"relays", to_string(num_relays)},
        {"messages/relay", to_string(messages_per_relay)},
        {"warmup/relay", to_string(warmup)}
      ] ++
        Inputs.header_rows(inputs) ++
        if(shape == :cell, do: [{"pool sizes", Enum.join(pool_sizes, ",")}], else: []) ++
        if(inputs.via == :cast, do: [], else: [{"via", to_string(inputs.via)}]) ++
        [{"mix env", to_string(Mix.env())}]
    )
  end

  # ----- Single-namespace shapes: every shape but cell -----
  #
  # `:regression` is byte-identical to the historical default behaviour.
  # The keyed shapes reuse the same harness with their own factory and keyed
  # inputs (see `Mix.Tasks.Bench.Inputs`).

  defp run_default_shape(runtime, num_relays, messages_per_relay, shape, warmup) do
    # `:send` grants its runners a role, which only the scratch namespace takes.
    namespace =
      if runtime == :rust and shape != :send and Fleet.runners_available?(@default_namespace) do
        @default_namespace
      else
        Fleet.setup(@bench_namespace, 2, [])
        @bench_namespace
      end

    try do
      Report.emit(report_ctx())

      metrics =
        run_performance_test(namespace, num_relays, messages_per_relay, shape, warmup)

      Report.finish(report_name(shape), %{metrics: metrics})
    after
      if namespace == @bench_namespace do
        Fleet.cleanup(namespace)
      end
    end
  end

  # ----- Shape: cell (new — sweeps pool sizes, emits scaling summary) -----

  defp run_cell_shape(num_relays, messages_per_relay, pool_sizes, warmup) do
    results =
      Enum.map(pool_sizes, fn pool ->
        # Always bring up a fresh bench namespace per pool size — the default
        # namespace's compute engines were started without FLEZHA_WORKER_POOL_SIZE
        # threaded through, so they can't be reused for the per-pool sweep.
        Fleet.setup(@bench_namespace, 2, [{"FLEZHA_WORKER_POOL_SIZE", pool}])

        try do
          Report.emit(report_ctx())
          IO.puts("\ncell stress: worker pool = #{pool}\n")

          metrics =
            run_performance_test(
              @bench_namespace,
              num_relays,
              messages_per_relay,
              :cell,
              warmup
            )

          {pool, metrics}
        after
          Fleet.cleanup(@bench_namespace)
        end
      end)

    Report.finish(report_name(:cell), %{
      metrics: %{
        per_pool: Map.new(results, fn {pool, m} -> {Integer.to_string(pool), m} end),
        scaling_ratio: print_scaling_summary(results)
      }
    })
  end

  # The ratio the informational CI job reads, taken over the end-to-end window
  # like every other headline here.
  defp print_scaling_summary(results) do
    throughputs = Enum.map(results, fn {_pool, m} -> m.throughput_msg_per_sec end)

    ratio = Report.ratio(throughputs)
    # The report ships the float; the line prints it, so a round 1000.0 does not
    # reach a terminal as `1.0e3`.
    ratio_text = (ratio && Report.decimal(ratio, 2)) || "n/a (single pool size)"

    Report.section(
      "scaling (end-to-end window)",
      Report.table(
        ["worker pool", "msg/s"],
        Enum.map(results, fn {pool, m} ->
          [Integer.to_string(pool), Report.rate(m.throughput_msg_per_sec)]
        end),
        [:right, :right]
      ) <> "\n\nratio (max/min): #{ratio_text}"
    )

    ratio
  end

  defp durability_backend(), do: to_string(Config.get(:durability_backend))

  defp durability_row() do
    case Config.get(:durability_backend) do
      :local_path -> "local_path (#{Config.get(:durability_path)})"
      backend -> to_string(backend)
    end
  end

  defp report_ctx() do
    %{
      durability: durability_backend(),
      durability_kept: Fleet.durability_kept?(),
      strict: Process.get(:bench_strict, false)
    }
  end

  # `bench-<runtime>-<shape>`; `Report.finish/2` stamps the run id and places it.
  defp report_name(shape), do: "bench-#{Fleet.runtime()}-#{shape}"

  defp parse_shape(nil), do: :regression
  defp parse_shape("regression"), do: :regression
  defp parse_shape("cell"), do: :cell
  defp parse_shape("dynamic"), do: :dynamic
  defp parse_shape("tier"), do: :tier
  defp parse_shape("sleeping"), do: :sleeping
  defp parse_shape("timer"), do: :timer
  defp parse_shape("send"), do: :send

  defp parse_shape(other) do
    raise "Unknown --shape value: #{inspect(other)}. Expected \"regression\", \"cell\", " <>
            "\"dynamic\", \"tier\", \"sleeping\", \"timer\", or \"send\"."
  end

  defp parse_via(nil), do: :cast
  defp parse_via("cast"), do: :cast
  defp parse_via("host_inject"), do: :host_inject

  defp parse_via(other),
    do: raise("Unknown --via value: #{inspect(other)}. Expected \"cast\" or \"host_inject\".")

  defp parse_pool_sizes(nil), do: [1, 8]

  defp parse_pool_sizes(value) when is_binary(value) do
    value
    |> String.split(",", trim: true)
    |> Enum.map(fn s ->
      case Integer.parse(String.trim(s)) do
        {n, ""} when n > 0 ->
          n

        _ ->
          raise "Invalid --pool-sizes entry: #{inspect(s)}. Expected positive integers, e.g. \"1,8\"."
      end
    end)
  end

  defp factory_for_shape(:regression), do: "do_nothing"
  defp factory_for_shape(:cell), do: "cell_stress"
  defp factory_for_shape(:dynamic), do: "dynamic_cell"
  defp factory_for_shape(:tier), do: "tier_store"
  defp factory_for_shape(:sleeping), do: "sleeping_counter"
  defp factory_for_shape(:timer), do: "timer_churn"
  defp factory_for_shape(:send), do: "send_forward"

  defp dimensions_for_shape(:cell, opts) do
    if opts[:num_relays] || opts[:messages_per_relay] do
      IO.warn(
        "--shape cell ignores -n/-m; dimensions are fixed at 8 × 20_000 so the " <>
          "numbers stay comparable across runs. Use --shape regression for " <>
          "custom dimensions."
      )
    end

    {8, 20_000}
  end

  defp dimensions_for_shape(_shape, opts) do
    {opts[:num_relays] || 100, opts[:messages_per_relay] || 100}
  end

  defp run_performance_test(namespace, num_relays, test_messages_per_relay, shape, warmup) do
    factory = factory_for_shape(shape)
    inputs = Process.get(:bench_inputs)
    IO.puts("Starting #{num_relays} relays (factory=#{factory})...")

    enable_hooks(namespace, Process.get(:bench_hook_url))
    {creation_us, relay_infos, counted} = start_shape(namespace, shape, num_relays)
    plugin_setup_us = enable_forwarders(namespace, counted)

    warmup_total = length(relay_infos) * warmup
    total_messages = length(relay_infos) * test_messages_per_relay
    tally = if inputs.latency, do: Tally.new(warmup_total + total_messages, warmup_total)
    {warmup_us, tally} = warmup_pass(relay_infos, shape, warmup, inputs, tally)

    IO.puts("Sending #{total_messages} inputs (#{test_messages_per_relay} per relay)...")

    relay_ids = MapSet.new(relay_infos ++ counted, &elem(&1, 0))

    {send_us, receive_us, persisted, tally} =
      measured_pass(relay_infos, relay_ids, shape, test_messages_per_relay, warmup_total, tally)

    metrics =
      metrics(%{
        relays: length(relay_infos),
        total_messages: total_messages,
        expected_responses: expected_responses(shape, total_messages),
        creation_us: creation_us,
        plugin_setup_us: plugin_setup_us,
        warmup_us: warmup_us,
        send_us: send_us,
        receive_us: receive_us,
        inputs: inputs,
        persisted: persisted,
        tally: tally
      })

    print_results(metrics, warmup, inputs)

    RelayManager.stop_all_relays(namespace)

    if total_messages == 0 do
      exit("No messages were processed")
    end

    metrics
  end

  # The relays the inputs go to, and the ones whose outputs are counted: the
  # same relays, except under `:send`, whose relay `flow_test_<i>` sends each
  # input on to its own `do_nothing` relay `flow_test_<i>_sink`.
  defp start_shape(namespace, :send, num_relays) do
    Fleet.grant_send(namespace)
    {sinks_us, sinks} = start_relays(namespace, "do_nothing", num_relays, "_sink")
    {senders_us, senders} = start_relays(namespace, factory_for_shape(:send), num_relays)
    {sinks_us + senders_us, senders, sinks}
  end

  defp start_shape(namespace, shape, num_relays) do
    {elapsed, infos} = start_relays(namespace, factory_for_shape(shape), num_relays)
    {elapsed, infos, infos}
  end

  defp start_relays(namespace, factory, num_relays, suffix \\ "") do
    {elapsed, infos} =
      :timer.tc(fn ->
        Flezha.Parallel.map(1..num_relays, fn i ->
          relay_id = Inputs.relay_id(i, suffix)

          relay = %Flezha.Relay.Builder{
            id: relay_id,
            factory_id: factory
          }

          case RelayManager.start_relay(namespace, relay) do
            {:ok, info} ->
              {relay_id, info}

            error ->
              IO.puts("Failed to start relay #{relay_id}: #{inspect(error)}")
              nil
          end
        end)
        |> Enum.filter(&(&1 != nil))
      end)

    # Every start failed. Proceeding sends no inputs and then waits for
    # replies that nothing can produce, so the run dies in a warmup receive
    # and reports a timeout instead of the refusal that actually stopped it.
    if infos == [] do
      Mix.raise(
        "no relays started — every start was refused. The refusal is logged above; " <>
          "a placement refusal naming an sdk_lang means this namespace's factory state " <>
          "disagrees with the runners you started."
      )
    end

    {elapsed, infos}
  end

  defp enable_hooks(_namespace, nil), do: :ok

  defp enable_hooks(namespace, url) do
    hooks = for hook <- Flezha.Plugin.ProviderBehaviour.controller_callbacks(), do: {hook, []}

    {:ok, _} =
      PluginManager.enable_plugin("bench_hooks", %Flezha.Plugin{
        id: "bench_hooks",
        namespace: namespace,
        provider: Flezha.Plugin.Provider.HookWebhook,
        opts: %{url: url},
        hooks: hooks
      })
  end

  defp enable_forwarders(namespace, relay_infos) do
    recv_pid = self()

    {elapsed, _} =
      :timer.tc(fn ->
        # Every enable queues on the one hub coordinator, whose call budget is
        # 5 s per start; an unbounded fan-out over 1000 relays waits past it.
        Flezha.Parallel.each(relay_infos, fn {relay_id, _info} ->
          PluginManager.enable_plugin(
            relay_id <> "_beam_msg_forward",
            %Flezha.Plugin{
              id: relay_id <> "_beam_msg_forward",
              namespace: namespace,
              relay_id: relay_id,
              provider: Flezha.Plugin.Provider.BeamMsgForward,
              opts: %{recv_pid: recv_pid},
              hooks: [{:on_output, []}]
            }
          )

          PluginManager.enable_plugin(
            relay_id <> "_stdout_plugin",
            %Flezha.Plugin{
              id: relay_id <> "_stdout_plugin",
              namespace: namespace,
              relay_id: relay_id,
              provider: Flezha.Plugin.Provider.StdOut,
              opts: %{},
              hooks: []
            }
          )
        end)
      end)

    elapsed
  end

  defp warmup_pass(_relay_infos, _shape, warmup, _inputs, tally) when warmup <= 0,
    do: {nil, tally}

  # Warm-up takes the first seqs, which the tally's prime counts and never times.
  defp warmup_pass(relay_infos, shape, warmup, inputs, tally) do
    warmup_total = length(relay_infos) * warmup

    :timer.tc(fn ->
      Inputs.send_burst(inputs, relay_infos, shape, warmup, 0)
      await_responses(expected_responses(shape, warmup_total), "warmup", shape, tally)
    end)
  end

  # With latency measured, or a paced send, the inputs go out from a process of
  # their own while this one takes the outputs: timed here behind a send loop
  # still running, an output's latency would be the bench's own. The windows
  # keep their meaning: send ends when the sender is done, end-to-end at the
  # last response. Persistence is read around the same window.
  defp measured_pass(relay_infos, relay_ids, shape, messages_per_relay, base, tally) do
    inputs = Process.get(:bench_inputs)
    total = length(relay_infos) * messages_per_relay
    expected = expected_responses(shape, total)
    persisted_before = Tally.persisted(relay_ids)

    send_all = fn ->
      if inputs.rate,
        do: Inputs.send_paced(inputs, relay_infos, shape, messages_per_relay, base),
        else: Inputs.send_burst(inputs, relay_infos, shape, messages_per_relay, base)
    end

    started = System.monotonic_time(:microsecond)

    sent_at =
      if inputs.latency or inputs.rate do
        bench = self()

        {:ok, _} =
          Task.start_link(fn ->
            send_all.()
            send(bench, {:bench_sent, System.monotonic_time(:microsecond)})
          end)

        :pending
      else
        send_all.()
        System.monotonic_time(:microsecond)
      end

    tally = await_responses(expected, "message", shape, tally)
    done = System.monotonic_time(:microsecond)

    sent =
      with :pending <- sent_at do
        receive do
          {:bench_sent, at} -> at
        end
      end

    check_rate(inputs.rate, total, sent - started)
    {sent - started, done - sent, Tally.persisted(relay_ids, persisted_before), tally}
  end

  defp check_rate(nil, _total, _send_us), do: :ok

  defp check_rate(rate, total, send_us) do
    achieved = total * 1_000_000 / max(send_us, 1)

    if achieved < 0.95 * rate do
      Report.warn(
        "the paced sender reached #{Report.rate(achieved)}/s of the #{Report.rate(rate)}/s " <>
          "asked; the latency was taken at saturation, not at the asked rate"
      )
    end
  end

  # The send and receive windows run back to back, so their sum is
  # the wall time from the first input to the last response. That is the
  # honest window — responses stream in while the send loop is still casting,
  # so the receive window alone credits work the send window paid for.
  defp metrics(w) do
    end_to_end_us = w.send_us + w.receive_us
    {payload_min, payload_max} = w.inputs.payload

    %{
      relays: w.relays,
      inputs_dispatched: w.total_messages,
      responses_received: w.expected_responses,
      relay_creation_ms: rounded_ms(w.creation_us),
      plugin_setup_ms: rounded_ms(w.plugin_setup_us),
      warmup_ms: w.warmup_us && rounded_ms(w.warmup_us),
      send_ms: rounded_ms(w.send_us),
      receive_ms: rounded_ms(w.receive_us),
      end_to_end_ms: rounded_ms(end_to_end_us),
      throughput_msg_per_sec: round(w.total_messages * 1_000_000 / end_to_end_us),
      receive_window_msg_per_sec: round(w.total_messages * 1_000_000 / max(w.receive_us, 1)),
      payload_bytes: %{min: payload_min, max: payload_max},
      persisted: w.persisted
    }
    |> Map.merge(if w.tally, do: %{latency_ms: latency_metrics(w.tally)}, else: %{})
    |> Map.merge(
      if w.inputs.rate,
        do: %{send_rate_msg_per_sec: round(w.total_messages * 1_000_000 / max(w.send_us, 1))},
        else: %{}
    )
  end

  defp latency_metrics(tally) do
    Map.merge(Tally.latency_ms(tally), %{
      unstamped: tally.foreign,
      duplicates: tally.duplicates
    })
  end

  defp rounded_ms(microseconds), do: Float.round(microseconds / 1000, 1)

  # Both windows are reported, each labelled with what it measures: they differ
  # by the send window and are not interchangeable, and only the label says
  # which one a quoted figure came from.
  defp print_results(m, warmup, inputs) do
    IO.puts("")

    Report.section(
      "totals",
      Report.kv([
        {"relays", Integer.to_string(m.relays)},
        {"inputs dispatched", Report.rate(m.inputs_dispatched)},
        {"responses received", Report.rate(m.responses_received)}
      ])
    )

    phases_section(m, warmup)

    Report.section(
      "throughput",
      Report.table(
        ["window", "elapsed", "msg/s"],
        [
          [
            "end-to-end (first input → last response)",
            "#{m.end_to_end_ms} ms",
            Report.rate(m.throughput_msg_per_sec)
          ],
          [
            "receive only (send loop returned → last response)",
            "#{m.receive_ms} ms",
            Report.rate(m.receive_window_msg_per_sec)
          ]
        ],
        [:left, :right, :right]
      )
    )

    latency_section(m[:latency_ms], inputs)
    persisted_section(m.persisted)
  end

  defp latency_section(nil, _inputs), do: :ok

  defp latency_section(l, inputs) do
    mode =
      if inputs.rate,
        do: "paced at #{Report.rate(inputs.rate)}/s",
        else: "burst, includes queueing behind earlier inputs"

    Report.section(
      "latency (#{mode})",
      Tally.render_latency(l) <> "\nunstamped #{l.unstamped}, duplicates #{l.duplicates}"
    )
  end

  # A shape whose outputs do not wait on its writes can persist after the window
  # closes, so the label names the window.
  @persisted_title "persisted (within the measured window)"

  defp persisted_section(%{changes: 0, bytes: 0, fsyncs: 0}),
    do: Report.section(@persisted_title, "nothing persisted")

  defp persisted_section(p) do
    Report.section(
      @persisted_title,
      Report.kv([
        {"changes", Report.rate(p.changes)},
        {"bytes", Report.bytes(p.bytes)},
        {"fsyncs", Report.rate(p.fsyncs)}
      ])
    )
  end

  defp phases_section(m, warmup) do
    phases =
      [
        {"relay creation", m.relay_creation_ms, ""},
        {"plugin setup", m.plugin_setup_ms, ""}
      ] ++
        if(m.warmup_ms,
          do: [{"warmup", m.warmup_ms, "#{warmup}/relay, excluded from the windows below"}],
          else: []
        ) ++
        [
          {"input send", m.send_ms, ""},
          {"response drain", m.receive_ms, ""}
        ]

    Report.section(
      "phases",
      Report.table(
        ["phase", "elapsed", "note"],
        Enum.map(phases, fn {name, ms, note} -> [name, "#{ms} ms", note] end),
        [:left, :right, :left]
      )
    )
  end

  defp await_responses(expected, _phase, _shape, tally) when expected <= 0, do: tally

  defp await_responses(expected, phase, shape, tally) do
    Enum.reduce(1..expected//1, tally, &await_response(&1, expected, phase, shape, &2))
  end

  # A `timer_churn` firing lands whenever its timer was not cancelled in time,
  # so its count is not the inputs'; it is drained and not counted.
  defp await_response(x, expected, phase, shape, tally) do
    receive do
      {:message_forward, "fired:" <> _} when shape == :timer ->
        await_response(x, expected, phase, shape, tally)

      {:message_forward, msg} ->
        time_output(tally, msg)
    after
      10_000 ->
        IO.puts("Timeout waiting for #{phase} #{x}/#{expected}")
        exit("Timeout waiting for messages")
    end
  end

  # `do_nothing` echoes once per input (single tasklet, single cell,
  # one plugin-bound emit), so each dispatched input produces one
  # `:on_output` event. `cell_stress` chains four tasklets × 8
  # cells; only TaskletD emits to a plugin, and only its `p0` cell
  # is targeted by the pipeline routing so it runs once per TaskletC emit.
  # The entry tasklet fans 8-wide → TaskletA runs 8× → 8 emits to B[p0] →
  # 8 emits to C[p0] → 8 emits to D[p0] → 8 plugin-bound emits per input.
  # Every other shape's factory emits once per input.
  defp expected_responses(:cell, total_messages), do: total_messages * 8

  defp expected_responses(_shape, total_messages), do: total_messages

  # The receive time is taken first, so parsing is not part of the latency.
  defp time_output(nil, _msg), do: nil

  defp time_output(tally, msg) do
    now = System.monotonic_time(:nanosecond)

    case is_binary(msg) && Tally.parse_stamp(msg) do
      {seq, sent_ns} -> Tally.record({seq, max(now - sent_ns, 0)}, tally)
      _ -> %{tally | foreign: tally.foreign + 1}
    end
  end
end

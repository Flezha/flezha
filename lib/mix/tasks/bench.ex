defmodule Mix.Tasks.Bench do
  use Mix.Task
  alias Flezha.Config
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Report

  @shortdoc "Run performance tests for Flezha relays"
  @moduledoc """
  Run performance tests for Flezha relays.

  ## Usage

      mix bench --num-relays 1000 --messages-per-relay 1000
      mix bench --runtime java -n 100 -m 1000
      mix bench --runtime rust --shape cell --pool-sizes 1,8

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
      `tier_store`, keyed like `dynamic`), or `sleeping` (a persistent stream
      that sleeps when idle, factory `sleeping_counter`, keyed like `dynamic`).
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

  Every run also writes `bench/reports/bench-<runtime>-<shape>-<run_id>.json`,
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
    hook_url: :string
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

    {num_relays, messages_per_relay} = dimensions_for_shape(shape, opts)

    Process.put(:bench_runtime, runtime)
    Process.put(:bench_hook_url, opts[:hook_url])

    open_report(runtime, shape, pool_sizes, warmup, {num_relays, messages_per_relay})

    # A run killed mid-flight (a timeout, Ctrl-C) never reaches the `after`
    # block, so its relays stay in the hub's declared set and their state
    # dirs stay on disk. The convergence loop then spends every later run trying
    # to restore dead relays, which slows relay creation and quietly
    # skews the numbers. `@bench_namespace` is this task's own scratch namespace,
    # so reset it before the controller boots and can restore anything. The
    # `default` namespace is left alone — it may hold runners this task reuses.
    Fleet.reset_namespace(@bench_namespace)

    Mix.Task.run("app.start")
    Report.apply_log_level()

    case shape do
      :cell ->
        run_cell_shape(num_relays, messages_per_relay, pool_sizes, warmup)

      shape ->
        run_default_shape(runtime, num_relays, messages_per_relay, shape, warmup)
    end
  end

  defp open_report(runtime, shape, pool_sizes, warmup, {num_relays, messages_per_relay}) do
    Report.open(
      "mix bench",
      [
        {"runtime", "#{runtime} (#{Report.dispatch_note(runtime)})"},
        {"toolchain", Report.toolchain(runtime)},
        {"transport", to_string(Config.get(:transport))},
        {"shape", to_string(shape)},
        {"durability", durability_backend()},
        {"relays", to_string(num_relays)},
        {"messages/relay", to_string(messages_per_relay)},
        {"warmup/relay", to_string(warmup)}
      ] ++
        if(shape == :cell, do: [{"pool sizes", Enum.join(pool_sizes, ",")}], else: []) ++
        [{"mix env", to_string(Mix.env())}]
    )
  end

  # ----- Single-namespace shapes: every shape but cell -----
  #
  # `:regression` is byte-identical to the historical default behaviour.
  # The keyed shapes reuse the same harness with their own factory and keyed
  # inputs (see `input_for_shape/3`).

  defp run_default_shape(runtime, num_relays, messages_per_relay, shape, warmup) do
    namespace =
      cond do
        runtime != :rust ->
          Fleet.setup(@bench_namespace, 2, [])
          @bench_namespace

        Fleet.runners_available?(@default_namespace) ->
          @default_namespace

        true ->
          Fleet.setup(@bench_namespace, 2, [])
          @bench_namespace
      end

    try do
      Report.emit(%{durability: durability_backend()})

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
          Report.emit(%{durability: durability_backend()})
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

  # `bench-<runtime>-<shape>`; `Report.finish/2` stamps the run id and places it.
  defp report_name(shape), do: "bench-#{Fleet.runtime()}-#{shape}"

  defp parse_shape(nil), do: :regression
  defp parse_shape("regression"), do: :regression
  defp parse_shape("cell"), do: :cell
  defp parse_shape("dynamic"), do: :dynamic
  defp parse_shape("tier"), do: :tier
  defp parse_shape("sleeping"), do: :sleeping

  defp parse_shape(other) do
    raise "Unknown --shape value: #{inspect(other)}. Expected \"regression\", \"cell\", " <>
            "\"dynamic\", \"tier\", or \"sleeping\"."
  end

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
    IO.puts("Starting #{num_relays} relays (factory=#{factory})...")

    enable_hooks(namespace, Process.get(:bench_hook_url))
    {creation_us, relay_infos} = start_relays(namespace, factory, num_relays)
    plugin_setup_us = enable_forwarders(namespace, relay_infos)
    warmup_us = warmup_pass(relay_infos, shape, warmup)

    total_messages = length(relay_infos) * test_messages_per_relay

    IO.puts("Sending #{total_messages} inputs (#{test_messages_per_relay} per relay)...")

    {send_us, _} =
      :timer.tc(fn -> send_inputs(relay_infos, shape, test_messages_per_relay) end)

    expected_responses = expected_responses(shape, total_messages)

    {receive_us, _} =
      :timer.tc(fn -> await_responses(expected_responses, "message") end)

    metrics =
      metrics(%{
        relays: length(relay_infos),
        total_messages: total_messages,
        expected_responses: expected_responses,
        creation_us: creation_us,
        plugin_setup_us: plugin_setup_us,
        warmup_us: warmup_us,
        send_us: send_us,
        receive_us: receive_us
      })

    print_results(metrics, warmup)

    RelayManager.stop_all_relays(namespace)

    if total_messages == 0 do
      exit("No messages were processed")
    end

    metrics
  end

  defp start_relays(namespace, factory, num_relays) do
    {elapsed, infos} =
      :timer.tc(fn ->
        Flezha.Parallel.map(1..num_relays, fn i ->
          relay_id = "flow_test_#{i}"

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
        Flezha.Parallel.map(relay_infos, fn {relay_id, _info} ->
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

  defp warmup_pass(_relay_infos, _shape, warmup) when warmup <= 0, do: nil

  defp warmup_pass(relay_infos, shape, warmup) do
    warmup_total = length(relay_infos) * warmup

    {elapsed, _} =
      :timer.tc(fn ->
        send_inputs(relay_infos, shape, warmup)
        await_responses(expected_responses(shape, warmup_total), "warmup")
      end)

    elapsed
  end

  # The send and receive `:timer.tc` blocks run back to back, so their sum is
  # the wall time from the first input to the last response. That is the
  # honest window — responses stream in while the send loop is still casting,
  # so the receive window alone credits work the send window paid for.
  defp metrics(w) do
    end_to_end_us = w.send_us + w.receive_us

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
      receive_window_msg_per_sec: round(w.total_messages * 1_000_000 / w.receive_us)
    }
  end

  defp rounded_ms(microseconds), do: Float.round(microseconds / 1000, 1)

  # Both windows are reported, each labelled with what it measures: they differ
  # by the send window and are not interchangeable, and only the label says
  # which one a quoted figure came from.
  defp print_results(m, warmup) do
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

  defp send_inputs(relay_infos, shape, messages_per_relay) do
    Enum.each(relay_infos, fn {relay_id, %{pid: relay_pid}} ->
      for msg_num <- 1..messages_per_relay do
        GenServer.cast(
          relay_pid,
          {:io_in, input_for_shape(shape, relay_id, msg_num), %{}}
        )
      end
    end)
  end

  defp await_responses(expected, _phase) when expected <= 0, do: []

  defp await_responses(expected, phase) do
    for x <- 1..expected//1 do
      receive do
        {:message_forward, _msg} ->
          :ok
      after
        10_000 ->
          IO.puts("Timeout waiting for #{phase} #{x}/#{expected}")
          exit("Timeout waiting for messages")
      end
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

  # Per-shape wire input. The keyed shapes prefix a bounded key (`k0`..`k7`)
  # so a steady burst exercises 8 Cells per relay without tripping the
  # dynamic soft cap; `sleeping_counter` folds the value as an integer delta.
  defp input_for_shape(:sleeping, _relay_id, msg_num), do: "k#{rem(msg_num, 8)}:1"

  defp input_for_shape(shape, relay_id, msg_num) when shape in [:dynamic, :tier] do
    "k#{rem(msg_num, 8)}:test_msg_#{relay_id}_#{msg_num}"
  end

  defp input_for_shape(_shape, relay_id, msg_num) do
    "test_msg_#{relay_id}_#{msg_num}"
  end
end

defmodule Mix.Tasks.Bench do
  use Mix.Task
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Report

  @shortdoc "Run performance tests for Flezha operatives"
  @moduledoc """
  Run performance tests for Flezha operatives.

  ## Usage

      mix bench --num-operatives 1000 --messages-per-operative 1000
      mix bench --runtime java -n 100 -m 1000
      mix bench --runtime rust --shape entity --pool-sizes 1,8

  ## Options

    * `--num-operatives` / `-n` - Number of operatives to create (default: 100)
    * `--messages-per-operative` / `-m` - Number of messages to send per operative (default: 100)
    * `--runtime` / `-r` - Which SDK to benchmark: `rust` (default), `java`
      or `elixir`. Each is launched from its test app's manifest
      (`test/apps/<runtime>_runner/conformance.json`, `bench` profile), which
      names the build command for a missing artifact.
    * `--shape` - Bench shape: `regression` (default; the apples-to-apples
      regression check, factory `do_nothing`, dimensions from `-n` / `-m`),
      `entity` (the `operative-bench-harness` entity-stress shape, factory
      `entity_stress`, dimensions hard-coded at `8 × 20_000` per
      `operative-bench-harness/design.md` Decision 6, swept across the comma-list in
      `--pool-sizes`), or `dynamic` (the dynamic entity-mode shape, factory
      `dynamic_entity`, lazy per-key Entities with 8 keys/operative,
      dimensions from `-n` / `-m`).
    * `--pool-sizes` - Comma-separated worker-pool sizes (default: `"1,8"`),
      only honored when `--shape entity`. Each value spawns a fresh
      Rust runner binary with `FLEZHA_WORKER_POOL_SIZE=<N>` and
      reports throughput per-size; the scaling ratio is carried in the report.
    * `--warmup` / `-w` - Untimed messages per operative sent (and their
      responses drained) before the measured phase, so one-time runner
      cold-start — class loading, JIT, lazy codec init — stays out of the
      numbers (default: 10, `0` disables).

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

  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [
          num_operatives: :integer,
          messages_per_operative: :integer,
          runtime: :string,
          shape: :string,
          pool_sizes: :string,
          warmup: :integer
        ],
        aliases: [
          n: :num_operatives,
          m: :messages_per_operative,
          r: :runtime,
          w: :warmup
        ]
      )

    runtime = parse_runtime(opts[:runtime])
    shape = parse_shape(opts[:shape])
    pool_sizes = parse_pool_sizes(opts[:pool_sizes])
    warmup = max(opts[:warmup] || 10, 0)

    {num_operatives, messages_per_operative} = dimensions_for_shape(shape, opts)

    Process.put(:bench_runtime, runtime)

    Report.open(
      "mix bench",
      [
        {"runtime", "#{runtime} (#{Report.dispatch_note(runtime)})"},
        {"runner", runner_mode()},
        {"toolchain", Report.toolchain(runtime)},
        {"transport", to_string(Application.get_env(:flezha, :transport, :uds))},
        {"shape", to_string(shape)},
        {"durability", durability_backend()},
        {"operatives", to_string(num_operatives)},
        {"messages/operative", to_string(messages_per_operative)},
        {"warmup/operative", to_string(warmup)}
      ] ++
        if(shape == :entity, do: [{"pool sizes", Enum.join(pool_sizes, ",")}], else: []) ++
        [{"mix env", to_string(Mix.env())}]
    )

    # A run killed mid-flight (a timeout, Ctrl-C) never reaches the `after`
    # block, so its operatives stay in the hub's declared set and their state
    # dirs stay on disk. The convergence loop then spends every later run trying
    # to restore dead operatives, which slows operative creation and quietly
    # skews the numbers. `@bench_namespace` is this task's own scratch namespace,
    # so reset it before the controller boots and can restore anything. The
    # `default` namespace is left alone — it may hold runners this task reuses.
    reset_bench_namespace()

    Mix.Task.run("app.start")
    Report.apply_log_level()

    case shape do
      :regression ->
        run_default_shape(runtime, num_operatives, messages_per_operative, :regression, warmup)

      :dynamic ->
        run_default_shape(runtime, num_operatives, messages_per_operative, :dynamic, warmup)

      :entity ->
        run_entity_shape(num_operatives, messages_per_operative, pool_sizes, warmup)
    end
  end

  # ----- Single-namespace shapes: regression (default) and dynamic -----
  #
  # `:regression` is byte-identical to the historical default behaviour.
  # `:dynamic` reuses the same harness with the `dynamic_entity`
  # factory and keyed inputs (see `input_for_shape/3`).

  defp reset_bench_namespace do
    for dir <- [
          Path.join(["app_data", "state", @bench_namespace]),
          Path.join(["app_data", "process_hub", "#{@bench_namespace}_hub"])
        ] do
      File.rm_rf!(dir)
    end
  end

  defp run_default_shape(runtime, num_operatives, messages_per_operative, shape, warmup) do
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
        run_performance_test(namespace, num_operatives, messages_per_operative, shape, warmup)

      Report.finish(report_name(shape), %{metrics: metrics})
    after
      if namespace == @bench_namespace do
        Fleet.cleanup(namespace)
      end
    end
  end

  # ----- Shape: entity (new — sweeps pool sizes, emits scaling summary) -----

  defp run_entity_shape(num_operatives, messages_per_operative, pool_sizes, warmup) do
    results =
      Enum.map(pool_sizes, fn pool ->
        # Always bring up a fresh bench namespace per pool size — the default
        # namespace's compute engines were started without FLEZHA_WORKER_POOL_SIZE
        # threaded through, so they can't be reused for the per-pool sweep.
        Fleet.setup(@bench_namespace, 2, [{"FLEZHA_WORKER_POOL_SIZE", pool}])

        try do
          Report.emit(%{durability: durability_backend()})
          IO.puts("\nentity stress: worker pool = #{pool}\n")

          metrics =
            run_performance_test(
              @bench_namespace,
              num_operatives,
              messages_per_operative,
              :entity,
              warmup
            )

          {pool, metrics}
        after
          Fleet.cleanup(@bench_namespace)
        end
      end)

    Report.finish(report_name(:entity), %{
      metrics: %{
        per_pool: Map.new(results, fn {pool, m} -> {Integer.to_string(pool), m} end),
        scaling_ratio: print_scaling_summary(results)
      }
    })
  end

  # The ratio the informational CI job reads. Taken over the end-to-end window
  # like every other headline here, so a pool comparison and a floor check are
  # the same measurement at different pool sizes.
  defp print_scaling_summary(results) do
    throughputs = Enum.map(results, fn {_pool, m} -> m.throughput_msg_per_sec end)

    ratio = Report.ratio(throughputs)

    Report.section(
      "scaling (end-to-end window)",
      Report.table(
        ["worker pool", "msg/s"],
        Enum.map(results, fn {pool, m} ->
          [Integer.to_string(pool), Report.rate(m.throughput_msg_per_sec)]
        end),
        [:right, :right]
      ) <> "\n\nratio (max/min): #{ratio || "n/a (single pool size)"}"
    )

    ratio
  end

  # `:native` is the in-process Elixir SDK: there is no runner process to
  # spawn, and the distinction is a property of the transport, not of the SDK.
  defp runner_mode() do
    if Application.get_env(:flezha, :transport, :uds) == :native,
      do: "embedded (in-process)",
      else: "spawned"
  end

  defp durability_backend(), do: System.get_env("FLEZHA_DURABILITY_BACKEND") || "none"

  # `bench-<runtime>-<shape>`; `Report.finish/2` stamps the run id and places it.
  defp report_name(shape), do: "bench-#{Fleet.runtime()}-#{shape}"

  # Every SDK with a test-app manifest can serve this bench: the three shapes
  # drive `do_nothing`, `entity_stress` and `dynamic_entity`, which the
  # conformance gate already requires every runtime to carry or defer by name.
  # Read from the manifests rather than a clause per runtime, so a new SDK is
  # benchable the moment its app exists.
  defp parse_runtime(nil), do: :rust

  defp parse_runtime(name) do
    runtimes = Flezha.Service.PortPilot.runtimes()

    Enum.find(runtimes, &(Atom.to_string(&1) == name)) ||
      raise "Unknown --runtime value: #{inspect(name)}. Expected one of: " <>
              Enum.map_join(runtimes, ", ", &inspect/1)
  end

  defp parse_shape(nil), do: :regression
  defp parse_shape("regression"), do: :regression
  defp parse_shape("entity"), do: :entity
  defp parse_shape("dynamic"), do: :dynamic

  defp parse_shape(other) do
    raise "Unknown --shape value: #{inspect(other)}. Expected \"regression\", \"entity\", or \"dynamic\"."
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
  defp factory_for_shape(:entity), do: "entity_stress"
  defp factory_for_shape(:dynamic), do: "dynamic_entity"

  defp dimensions_for_shape(:regression, opts) do
    {opts[:num_operatives] || 100, opts[:messages_per_operative] || 100}
  end

  defp dimensions_for_shape(:dynamic, opts) do
    {opts[:num_operatives] || 100, opts[:messages_per_operative] || 100}
  end

  defp dimensions_for_shape(:entity, opts) do
    if opts[:num_operatives] || opts[:messages_per_operative] do
      IO.warn(
        "--shape entity ignores -n/-m; dimensions are hard-coded at 8 × 20_000 " <>
          "per operative-bench-harness/design.md Decision 6. Use --shape regression for " <>
          "custom dimensions."
      )
    end

    {8, 20_000}
  end

  defp run_performance_test(namespace, num_operatives, test_messages_per_operative, shape, warmup) do
    factory = factory_for_shape(shape)
    IO.puts("Starting #{num_operatives} operatives (factory=#{factory})...")

    {operative_creation_time, operative_infos} =
      :timer.tc(fn ->
        Flezha.Parallel.map(1..num_operatives, fn i ->
          operative_id = "flow_test_#{i}"

          operative = %Flezha.Operative.Builder{
            id: operative_id,
            factory_id: factory
          }

          case OperativeManager.start_operative(namespace, operative) do
            {:ok, info} ->
              {operative_id, info}

            error ->
              IO.puts("Failed to start operative #{operative_id}: #{inspect(error)}")
              nil
          end
        end)
        |> Enum.filter(&(&1 != nil))
      end)

    # Every start failed. Proceeding sends no inputs and then waits for
    # replies that nothing can produce, so the run dies in a warmup receive
    # and reports a timeout instead of the refusal that actually stopped it.
    if operative_infos == [] do
      Mix.raise(
        "no operatives started — every start was refused. The refusal is logged above; " <>
          "a placement refusal naming an sdk_lang means this namespace's factory state " <>
          "disagrees with the runners you started."
      )
    end

    recv_pid = self()

    {plugin_setup_time, _} =
      :timer.tc(fn ->
        Flezha.Parallel.map(operative_infos, fn {operative_id, _info} ->
          PluginManager.enable_plugin(
            operative_id <> "_beam_msg_forward",
            %Flezha.Plugin{
              id: operative_id <> "_beam_msg_forward",
              namespace: namespace,
              operative_id: operative_id,
              provider: Flezha.Plugin.Provider.BeamMsgForward,
              opts: %{recv_pid: recv_pid},
              subscriptions: [{:on_output, []}]
            }
          )

          PluginManager.enable_plugin(
            operative_id <> "_stdout_plugin",
            %Flezha.Plugin{
              id: operative_id <> "_stdout_plugin",
              namespace: namespace,
              operative_id: operative_id,
              provider: Flezha.Plugin.Provider.StdOut,
              opts: %{},
              subscriptions: []
            }
          )
        end)
      end)

    warmup_time =
      if warmup > 0 do
        warmup_total = length(operative_infos) * warmup

        {elapsed, _} =
          :timer.tc(fn ->
            send_inputs(operative_infos, shape, warmup)
            await_responses(expected_responses(shape, warmup_total), "warmup")
          end)

        elapsed
      end

    total_messages = length(operative_infos) * test_messages_per_operative

    IO.puts("Sending #{total_messages} inputs (#{test_messages_per_operative} per operative)...")

    {message_send_time, _} =
      :timer.tc(fn -> send_inputs(operative_infos, shape, test_messages_per_operative) end)

    expected_responses = expected_responses(shape, total_messages)

    {message_receive_time, _} =
      :timer.tc(fn -> await_responses(expected_responses, "message") end)

    # The send and receive `:timer.tc` blocks run back to back, so their sum is
    # the wall time from the first input to the last response. That is the
    # honest window — responses stream in while the send loop is still casting,
    # so the receive window alone credits work the send window paid for.
    end_to_end_time = message_send_time + message_receive_time

    metrics = %{
      operatives: length(operative_infos),
      inputs_dispatched: total_messages,
      responses_received: expected_responses,
      operative_creation_ms: rounded_ms(operative_creation_time),
      plugin_setup_ms: rounded_ms(plugin_setup_time),
      warmup_ms: warmup_time && rounded_ms(warmup_time),
      send_ms: rounded_ms(message_send_time),
      receive_ms: rounded_ms(message_receive_time),
      end_to_end_ms: rounded_ms(end_to_end_time),
      throughput_msg_per_sec: round(total_messages * 1_000_000 / end_to_end_time),
      receive_window_msg_per_sec: round(total_messages * 1_000_000 / message_receive_time)
    }

    print_results(metrics, warmup)

    OperativeManager.stop_all_operatives(namespace)

    if length(operative_infos) == 0 do
      exit("No operatives were created successfully")
    end

    if total_messages == 0 do
      exit("No messages were processed")
    end

    metrics
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
        {"operatives", Integer.to_string(m.operatives)},
        {"inputs dispatched", Report.rate(m.inputs_dispatched)},
        {"responses received", Report.rate(m.responses_received)}
      ])
    )

    phases =
      [
        {"operative creation", m.operative_creation_ms, ""},
        {"plugin setup", m.plugin_setup_ms, ""}
      ] ++
        if(m.warmup_ms,
          do: [{"warmup", m.warmup_ms, "#{warmup}/operative, excluded from the windows below"}],
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

  defp send_inputs(operative_infos, shape, messages_per_operative) do
    Enum.each(operative_infos, fn {operative_id, %{pid: operative_pid}} ->
      for msg_num <- 1..messages_per_operative do
        GenServer.cast(
          operative_pid,
          {:io_in, input_for_shape(shape, operative_id, msg_num), %{}}
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

  # `do_nothing` echoes once per input (single procedure, single entity,
  # one plugin-bound emit), so each dispatched input produces one
  # `:on_output` event. `entity_stress` chains four procedures × 8
  # entities; only ProcedureD emits to a plugin, and only its `p0` entity
  # is targeted by the pipeline routing so it runs once per ProcedureC emit.
  # The entry procedure fans 8-wide → ProcedureA runs 8× → 8 emits to B[p0] →
  # 8 emits to C[p0] → 8 emits to D[p0] → 8 plugin-bound emits per input.
  defp expected_responses(:regression, total_messages), do: total_messages
  defp expected_responses(:entity, total_messages), do: total_messages * 8
  # `dynamic_entity` emits once per input (one plugin-bound emit),
  # so each dispatched input produces one `:on_output` event.
  defp expected_responses(:dynamic, total_messages), do: total_messages

  # Per-shape wire input. The dynamic shape prefixes a workflow key
  # (`k0`..`k7`) so a steady burst exercises 8 dynamic Entities per operative
  # without tripping the soft cap; other shapes are key-agnostic.
  defp input_for_shape(:dynamic, operative_id, msg_num) do
    "k#{rem(msg_num, 8)}:test_msg_#{operative_id}_#{msg_num}"
  end

  defp input_for_shape(_shape, operative_id, msg_num) do
    "test_msg_#{operative_id}_#{msg_num}"
  end
end

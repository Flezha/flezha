defmodule Mix.Tasks.Bench do
  use Mix.Task
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Flezha.Runner.FleetManager

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
    * `--runtime` / `-r` - Which SDK to benchmark: `rust` (default) or `java`.
      The `java` runtime requires the test jar at
      `test/apps/java_runner/target/java_runner.jar`. Build it
      with `mix test.java_sdk` (builds host `flz_jni` + Maven) or
      `mvn -f test/apps/java_runner/pom.xml package`.
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
      reports throughput per-size; the final scaling summary is parsed by CI.

  ## Comparing SDKs

      mix bench --runtime rust -n 100 -m 1000 | tee bench-rust.txt
      mix bench --runtime java -n 100 -m 1000 | tee bench-java.txt

  The runtime selector only governs how the compute engine processes are
  launched — the controller, protocol, and measurement code are identical
  across runs, so the Throughput / Message processing numbers are directly
  comparable.
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
          pool_sizes: :string
        ],
        aliases: [
          n: :num_operatives,
          m: :messages_per_operative,
          r: :runtime
        ]
      )

    runtime = parse_runtime(opts[:runtime])
    shape = parse_shape(opts[:shape])
    pool_sizes = parse_pool_sizes(opts[:pool_sizes])

    {num_operatives, messages_per_operative} = dimensions_for_shape(shape, opts)

    IO.puts("Starting Flezha performance test...")
    IO.puts("Runtime: #{runtime}")
    IO.puts("Shape: #{shape}")
    IO.puts("Operatives: #{num_operatives}")
    IO.puts("Messages per operative: #{messages_per_operative}")
    if shape == :entity, do: IO.puts("Pool sizes: #{Enum.join(pool_sizes, ",")}")
    IO.puts("")

    Process.put(:bench_runtime, runtime)

    Logger.configure(level: :warning)
    Application.put_env(:logger, :level, :warning)
    Mix.Task.run("app.start")
    Logger.configure(level: :warning)

    case shape do
      :regression ->
        run_default_shape(runtime, num_operatives, messages_per_operative, :regression)

      :dynamic ->
        run_default_shape(runtime, num_operatives, messages_per_operative, :dynamic)

      :entity ->
        run_entity_shape(num_operatives, messages_per_operative, pool_sizes)
    end
  end

  # ----- Single-namespace shapes: regression (default) and dynamic -----
  #
  # `:regression` is byte-identical to the historical default behaviour.
  # `:dynamic` reuses the same harness with the `dynamic_entity`
  # factory and keyed inputs (see `input_for_shape/3`).

  defp run_default_shape(runtime, num_operatives, messages_per_operative, shape) do
    namespace =
      cond do
        runtime != :rust ->
          setup_test_environment(2, [])
          @bench_namespace

        runners_available?(@default_namespace) ->
          @default_namespace

        true ->
          setup_test_environment(2, [])
          @bench_namespace
      end

    try do
      run_performance_test(namespace, num_operatives, messages_per_operative, shape)
    after
      if namespace == @bench_namespace do
        cleanup_test_environment(namespace)
      end
    end
  end

  # ----- Shape: entity (new — sweeps pool sizes, emits scaling summary) -----

  defp run_entity_shape(num_operatives, messages_per_operative, pool_sizes) do
    results =
      Enum.map(pool_sizes, fn pool ->
        IO.puts("\n=== Entity Stress: pool=#{pool} ===")
        # Always bring up a fresh bench namespace per pool size — the default
        # namespace's compute engines were started without FLEZHA_WORKER_POOL_SIZE
        # threaded through, so they can't be reused for the per-pool sweep.
        setup_test_environment(2, [{"FLEZHA_WORKER_POOL_SIZE", pool}])

        try do
          throughput =
            run_performance_test(
              @bench_namespace,
              num_operatives,
              messages_per_operative,
              :entity
            )

          {pool, throughput}
        after
          cleanup_test_environment(@bench_namespace)
        end
      end)

    print_scaling_summary(results)
  end

  defp print_scaling_summary(results) do
    IO.puts("\n=== Scaling Summary ===")

    Enum.each(results, fn {pool, throughput} ->
      IO.puts("pool=#{pool}: #{Float.round(throughput, 2)} msg/s")
    end)

    throughputs = Enum.map(results, fn {_pool, t} -> t end)

    case throughputs do
      [_single] ->
        IO.puts("ratio (max/min): n/a (single pool size)")

      _ ->
        max_t = Enum.max(throughputs)
        min_t = Enum.min(throughputs)
        ratio = if min_t > 0, do: max_t / min_t, else: 0.0
        IO.puts("ratio (max/min): #{Float.round(ratio, 2)}")
    end
  end

  defp runners_available?(namespace) do
    case FleetManager.active_runners(namespace) do
      engines when is_list(engines) and length(engines) > 0 -> true
      _ -> false
    end
  end

  defp setup_test_environment(count, extra_env) do
    IO.puts("Setting up test environment with compute engines...")

    runner_ids = start_runners(@bench_namespace, count, extra_env)

    IO.puts("Started #{length(runner_ids)} compute engines for performance testing")

    Process.put(:bench_runners, runner_ids)
  end

  defp cleanup_test_environment(namespace) do
    IO.puts("Cleaning up test environment...")

    OperativeManager.stop_all_operatives(namespace)

    case Process.get(:bench_runners) do
      engines when is_list(engines) ->
        Enum.each(engines, fn engine_id ->
          FleetManager.disconnect_runner(namespace, engine_id)
        end)

      _ ->
        :ok
    end

    FleetManager.stop_namespace(namespace)
    Process.delete(:bench_runners)

    IO.puts("Test environment cleanup completed")
  end

  defp start_runners(namespace, count, extra_env) do
    {:ok, current_path} = :file.get_cwd()
    runtime = Process.get(:bench_runtime, :rust)

    for i <- 1..count do
      command = build_command(runtime, current_path, namespace)

      Flezha.Service.EventManager.add_handler(:runner_connected_event)

      Flezha.Service.PortPilot.open_port(command, env: extra_env)

      runner_id =
        receive do
          {:runner_connected_event, {^namespace, id}} -> id
        after
          5000 -> raise "Performance test compute engine #{i} failed to connect"
        end

      Flezha.Service.EventManager.rem_handler(:runner_connected_event)

      runner_id
    end
  end

  defp parse_runtime(nil), do: :rust
  defp parse_runtime("rust"), do: :rust
  defp parse_runtime("java"), do: :java

  defp parse_runtime(other) do
    raise "Unknown --runtime value: #{inspect(other)}. Expected \"rust\" or \"java\"."
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

  defp build_command(:rust, current_path, namespace) do
    candidates = [
      "#{current_path}/target/release/rust_runner",
      "#{current_path}/test/apps/rust_runner/target/release/rust_runner",
      "#{current_path}/target/debug/rust_runner",
      "#{current_path}/test/apps/rust_runner/target/debug/rust_runner"
    ]

    case Enum.find(candidates, &File.regular?/1) do
      nil ->
        "cargo run --release --manifest-path=#{current_path}/test/apps/rust_runner/Cargo.toml --target-dir=./target -- #{namespace}"

      path ->
        if String.contains?(path, "/debug/") do
          IO.warn(
            "mix bench is using the DEBUG Rust runner binary at #{path} — " <>
              "this is ~5× slower than release. Build with " <>
              "`cargo build -p rust_runner --release` for representative numbers."
          )
        end

        "#{path} #{namespace}"
    end
  end

  defp build_command(:java, current_path, namespace) do
    jar_path =
      Path.join([
        current_path,
        "test/apps/java_runner/target/java_runner.jar"
      ])

    if not File.exists?(jar_path) do
      raise """
      Java compute engine jar not found at #{jar_path}.

      Build it first with one of:
        mix test.java_sdk   # full pipeline: build flz_jni + Maven + run tests
        mvn -f test/apps/java_runner/pom.xml package

      Then retry: mix bench --runtime java ...
      """
    end

    "java -jar #{jar_path} #{namespace}"
  end

  defp run_performance_test(namespace, num_operatives, test_messages_per_operative, shape) do
    factory = factory_for_shape(shape)
    IO.puts("Starting flow test with #{num_operatives} operatives (factory=#{factory})...")

    {operative_creation_time, operative_infos} =
      :timer.tc(fn ->
        for i <- 1..num_operatives do
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
        end
        |> Enum.filter(&(&1 != nil))
      end)

    IO.puts(
      "Operative creation took: #{operative_creation_time / 1000} ms for #{length(operative_infos)} operatives"
    )

    {plugin_setup_time, _} =
      :timer.tc(fn ->
        Enum.each(operative_infos, fn {operative_id, _info} ->
          PluginManager.enable_plugin(
            operative_id <> "_beam_msg_forward",
            %Flezha.Plugin{
              id: operative_id <> "_beam_msg_forward",
              namespace: namespace,
              operative_id: operative_id,
              provider: Flezha.Plugin.Provider.BeamMsgForward,
              opts: %{recv_pid: self()},
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

    IO.puts(
      "Plugin setup took: #{plugin_setup_time / 1000} ms for #{length(operative_infos)} operatives"
    )

    total_messages = length(operative_infos) * test_messages_per_operative

    IO.puts(
      "Sending #{total_messages} total messages (#{test_messages_per_operative} per operative)..."
    )

    {message_send_time, _} =
      :timer.tc(fn ->
        Enum.each(operative_infos, fn {operative_id, %{pid: operative_pid}} ->
          for msg_num <- 1..test_messages_per_operative do
            GenServer.cast(
              operative_pid,
              {:io_in, input_for_shape(shape, operative_id, msg_num), %{}}
            )
          end
        end)
      end)

    IO.puts("Message sending took: #{message_send_time / 1000} ms")

    expected_responses = expected_responses(shape, total_messages)

    {message_receive_time, _} =
      :timer.tc(fn ->
        for x <- 1..expected_responses do
          receive do
            {:message_forward, _msg} ->
              :ok
          after
            10_000 ->
              IO.puts("Timeout waiting for message #{x}/#{expected_responses}")
              exit("Timeout waiting for messages")
          end
        end
      end)

    IO.puts(
      "Message processing took: #{message_receive_time / 1000} ms for #{expected_responses} messages"
    )

    total_time =
      operative_creation_time + plugin_setup_time + message_send_time + message_receive_time

    throughput_msgs_per_sec = total_messages * 1_000_000 / message_receive_time

    IO.puts("\n=== Performance Summary ===")
    IO.puts("Operatives created: #{length(operative_infos)}")
    IO.puts("Total inputs dispatched: #{total_messages}")
    IO.puts("Plugin-forwarded responses received: #{expected_responses}")
    IO.puts("Operative creation: #{operative_creation_time / 1000} ms")
    IO.puts("Plugin setup: #{plugin_setup_time / 1000} ms")
    IO.puts("Message sending: #{message_send_time / 1000} ms")
    IO.puts("Message processing: #{message_receive_time / 1000} ms")
    IO.puts("Total time: #{total_time / 1000} ms")
    IO.puts("Throughput: #{Float.round(throughput_msgs_per_sec, 2)} messages/second")
    IO.puts("Average time per operative: #{total_time / length(operative_infos) / 1000} ms")

    OperativeManager.stop_all_operatives(namespace)

    if length(operative_infos) == 0 do
      exit("No operatives were created successfully")
    end

    if total_messages == 0 do
      exit("No messages were processed")
    end

    IO.puts("\nPerformance test completed successfully!")

    throughput_msgs_per_sec
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

  # Per-shape wire input. The dynamic shape prefixes a bounded key
  # (`k0`..`k7`) so a steady burst exercises 8 dynamic Entities per operative
  # without tripping the soft cap; other shapes are key-agnostic.
  defp input_for_shape(:dynamic, operative_id, msg_num) do
    "k#{rem(msg_num, 8)}:test_msg_#{operative_id}_#{msg_num}"
  end

  defp input_for_shape(_shape, operative_id, msg_num) do
    "test_msg_#{operative_id}_#{msg_num}"
  end
end

defmodule Mix.Tasks.Bench do
  use Mix.Task
  require Logger
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.Executor.FleetManager

  @shortdoc "Run performance tests for Flezha relays"
  @moduledoc """
  Run performance tests for Flezha relays.

  ## Usage

      mix bench --num-relays 1000 --messages-per-relay 1000
      mix bench --runtime java -n 100 -m 1000
      mix bench --runtime rust --shape partitioning --pool-sizes 1,8

  ## Options

    * `--num-relays` / `-n` - Number of relays to create (default: 100)
    * `--messages-per-relay` / `-m` - Number of messages to send per relay (default: 100)
    * `--runtime` / `-r` - Which SDK to benchmark: `rust` (default) or `java`.
      The `java` runtime requires the test jar at
      `test/apps/java_executor/target/java_executor.jar`. Build it
      with `mix test.java_sdk` (builds host `flz_jni` + Maven) or
      `mvn -f test/apps/java_executor/pom.xml package`.
    * `--shape` - Bench shape: `regression` (default; the apples-to-apples
      regression check, factory `do_nothing`, dimensions from `-n` / `-m`),
      `partitioning` (the `relay-bench-harness` partitioning-stress shape, factory
      `partitioning_stress`, dimensions hard-coded at `8 × 20_000` per
      `relay-bench-harness/design.md` Decision 6, swept across the comma-list in
      `--pool-sizes`), or `dynamic` (the `relay-dynamic-partitioning` shape, factory
      `dynamic_partition`, lazy per-key Partitions with 8 keys/relay,
      dimensions from `-n` / `-m`).
    * `--pool-sizes` - Comma-separated worker-pool sizes (default: `"1,8"`),
      only honored when `--shape partitioning`. Each value spawns a fresh
      Rust executor binary with `FLEZHA_WORKER_POOL_SIZE=<N>` and
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
          num_relays: :integer,
          messages_per_relay: :integer,
          runtime: :string,
          shape: :string,
          pool_sizes: :string
        ],
        aliases: [
          n: :num_relays,
          m: :messages_per_relay,
          r: :runtime
        ]
      )

    runtime = parse_runtime(opts[:runtime])
    shape = parse_shape(opts[:shape])
    pool_sizes = parse_pool_sizes(opts[:pool_sizes])

    {num_relays, messages_per_relay} = dimensions_for_shape(shape, opts)

    IO.puts("Starting Flezha performance test...")
    IO.puts("Runtime: #{runtime}")
    IO.puts("Shape: #{shape}")
    IO.puts("Relays: #{num_relays}")
    IO.puts("Messages per relay: #{messages_per_relay}")
    if shape == :partitioning, do: IO.puts("Pool sizes: #{Enum.join(pool_sizes, ",")}")
    IO.puts("")

    Process.put(:bench_runtime, runtime)

    Logger.configure(level: :warning)
    Application.put_env(:logger, :level, :warning)
    Mix.Task.run("app.start")
    Logger.configure(level: :warning)

    case shape do
      :regression -> run_default_shape(runtime, num_relays, messages_per_relay, :regression)
      :dynamic -> run_default_shape(runtime, num_relays, messages_per_relay, :dynamic)
      :partitioning -> run_partitioning_shape(num_relays, messages_per_relay, pool_sizes)
    end
  end

  # ----- Single-namespace shapes: regression (default) and dynamic -----
  #
  # `:regression` is byte-identical to the historical default behaviour.
  # `:dynamic` reuses the same harness with the `dynamic_partition`
  # factory and keyed inputs (see `input_for_shape/3`).

  defp run_default_shape(runtime, num_relays, messages_per_relay, shape) do
    namespace =
      cond do
        runtime != :rust ->
          setup_test_environment(2, [])
          @bench_namespace

        executors_available?(@default_namespace) ->
          @default_namespace

        true ->
          setup_test_environment(2, [])
          @bench_namespace
      end

    try do
      run_performance_test(namespace, num_relays, messages_per_relay, shape)
    after
      if namespace == @bench_namespace do
        cleanup_test_environment(namespace)
      end
    end
  end

  # ----- Shape: partitioning (new — sweeps pool sizes, emits scaling summary) -----

  defp run_partitioning_shape(num_relays, messages_per_relay, pool_sizes) do
    results =
      Enum.map(pool_sizes, fn pool ->
        IO.puts("\n=== Partitioning Stress: pool=#{pool} ===")
        # Always bring up a fresh bench namespace per pool size — the default
        # namespace's compute engines were started without FLEZHA_WORKER_POOL_SIZE
        # threaded through, so they can't be reused for the per-pool sweep.
        setup_test_environment(2, [{"FLEZHA_WORKER_POOL_SIZE", pool}])

        try do
          throughput =
            run_performance_test(
              @bench_namespace,
              num_relays,
              messages_per_relay,
              :partitioning
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

  defp executors_available?(namespace) do
    case FleetManager.active_executors(namespace) do
      engines when is_list(engines) and length(engines) > 0 -> true
      _ -> false
    end
  end

  defp setup_test_environment(count, extra_env) do
    IO.puts("Setting up test environment with compute engines...")

    executor_ids = start_executors(@bench_namespace, count, extra_env)

    IO.puts("Started #{length(executor_ids)} compute engines for performance testing")

    Process.put(:bench_executors, executor_ids)
  end

  defp cleanup_test_environment(namespace) do
    IO.puts("Cleaning up test environment...")

    RelayManager.stop_all_relays(namespace)

    case Process.get(:bench_executors) do
      engines when is_list(engines) ->
        Enum.each(engines, fn engine_id ->
          FleetManager.disconnect_executor(namespace, engine_id)
        end)

      _ ->
        :ok
    end

    FleetManager.stop_namespace(namespace)
    Process.delete(:bench_executors)

    IO.puts("Test environment cleanup completed")
  end

  defp start_executors(namespace, count, extra_env) do
    {:ok, current_path} = :file.get_cwd()
    runtime = Process.get(:bench_runtime, :rust)

    for i <- 1..count do
      command = build_command(runtime, current_path, namespace)

      Flezha.Service.EventManager.add_handler(:executor_connected_event)

      Flezha.Service.PortPilot.open_port(command, env: extra_env)

      executor_id =
        receive do
          {:executor_connected_event, {^namespace, id}} -> id
        after
          5000 -> raise "Performance test compute engine #{i} failed to connect"
        end

      Flezha.Service.EventManager.rem_handler(:executor_connected_event)

      executor_id
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
  defp parse_shape("partitioning"), do: :partitioning
  defp parse_shape("dynamic"), do: :dynamic

  defp parse_shape(other) do
    raise "Unknown --shape value: #{inspect(other)}. Expected \"regression\", \"partitioning\", or \"dynamic\"."
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
  defp factory_for_shape(:partitioning), do: "partitioning_stress"
  defp factory_for_shape(:dynamic), do: "dynamic_partition"

  defp dimensions_for_shape(:regression, opts) do
    {opts[:num_relays] || 100, opts[:messages_per_relay] || 100}
  end

  defp dimensions_for_shape(:dynamic, opts) do
    {opts[:num_relays] || 100, opts[:messages_per_relay] || 100}
  end

  defp dimensions_for_shape(:partitioning, opts) do
    if opts[:num_relays] || opts[:messages_per_relay] do
      IO.warn(
        "--shape partitioning ignores -n/-m; dimensions are hard-coded at 8 × 20_000 " <>
          "per relay-bench-harness/design.md Decision 6. Use --shape regression for " <>
          "custom dimensions."
      )
    end

    {8, 20_000}
  end

  defp build_command(:rust, current_path, namespace) do
    candidates = [
      "#{current_path}/target/release/rust_executor",
      "#{current_path}/test/apps/rust_executor/target/release/rust_executor",
      "#{current_path}/target/debug/rust_executor",
      "#{current_path}/test/apps/rust_executor/target/debug/rust_executor"
    ]

    case Enum.find(candidates, &File.regular?/1) do
      nil ->
        "cargo run --release --manifest-path=#{current_path}/test/apps/rust_executor/Cargo.toml --target-dir=./target -- #{namespace}"

      path ->
        if String.contains?(path, "/debug/") do
          IO.warn(
            "mix bench is using the DEBUG Rust executor binary at #{path} — " <>
              "this is ~5× slower than release. Build with " <>
              "`cargo build -p rust_executor --release` for representative numbers."
          )
        end

        "#{path} #{namespace}"
    end
  end

  defp build_command(:java, current_path, namespace) do
    jar_path =
      Path.join([
        current_path,
        "test/apps/java_executor/target/java_executor.jar"
      ])

    if not File.exists?(jar_path) do
      raise """
      Java compute engine jar not found at #{jar_path}.

      Build it first with one of:
        mix test.java_sdk   # full pipeline: build flz_jni + Maven + run tests
        mvn -f test/apps/java_executor/pom.xml package

      Then retry: mix bench --runtime java ...
      """
    end

    "java -jar #{jar_path} #{namespace}"
  end

  defp run_performance_test(namespace, num_relays, test_messages_per_relay, shape) do
    factory = factory_for_shape(shape)
    IO.puts("Starting flow test with #{num_relays} relays (factory=#{factory})...")

    {relay_creation_time, relay_infos} =
      :timer.tc(fn ->
        for i <- 1..num_relays do
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
        end
        |> Enum.filter(&(&1 != nil))
      end)

    IO.puts(
      "Relay creation took: #{relay_creation_time / 1000} ms for #{length(relay_infos)} relays"
    )

    {plugin_setup_time, _} =
      :timer.tc(fn ->
        Enum.each(relay_infos, fn {relay_id, _info} ->
          PluginManager.enable_plugin(
            relay_id <> "_beam_msg_forward",
            %Flezha.Plugin{
              id: relay_id <> "_beam_msg_forward",
              namespace: namespace,
              relay_id: relay_id,
              provider: Flezha.Plugin.Provider.BeamMsgForward,
              opts: %{recv_pid: self()},
              subscriptions: [{:on_output, []}]
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
              subscriptions: []
            }
          )
        end)
      end)

    IO.puts("Plugin setup took: #{plugin_setup_time / 1000} ms for #{length(relay_infos)} relays")

    total_messages = length(relay_infos) * test_messages_per_relay

    IO.puts("Sending #{total_messages} total messages (#{test_messages_per_relay} per relay)...")

    {message_send_time, _} =
      :timer.tc(fn ->
        Enum.each(relay_infos, fn {relay_id, %{pid: relay_pid}} ->
          for msg_num <- 1..test_messages_per_relay do
            GenServer.cast(
              relay_pid,
              {:io_in, input_for_shape(shape, relay_id, msg_num), %{}}
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
      relay_creation_time + plugin_setup_time + message_send_time + message_receive_time

    throughput_msgs_per_sec = total_messages * 1_000_000 / message_receive_time

    IO.puts("\n=== Performance Summary ===")
    IO.puts("Relays created: #{length(relay_infos)}")
    IO.puts("Total inputs dispatched: #{total_messages}")
    IO.puts("Plugin-forwarded responses received: #{expected_responses}")
    IO.puts("Relay creation: #{relay_creation_time / 1000} ms")
    IO.puts("Plugin setup: #{plugin_setup_time / 1000} ms")
    IO.puts("Message sending: #{message_send_time / 1000} ms")
    IO.puts("Message processing: #{message_receive_time / 1000} ms")
    IO.puts("Total time: #{total_time / 1000} ms")
    IO.puts("Throughput: #{Float.round(throughput_msgs_per_sec, 2)} messages/second")
    IO.puts("Average time per relay: #{total_time / length(relay_infos) / 1000} ms")

    RelayManager.stop_all_relays(namespace)

    if length(relay_infos) == 0 do
      exit("No relays were created successfully")
    end

    if total_messages == 0 do
      exit("No messages were processed")
    end

    IO.puts("\nPerformance test completed successfully!")

    throughput_msgs_per_sec
  end

  # `do_nothing` echoes once per input (single shard, single partition,
  # one plugin-bound emit), so each dispatched input produces one
  # `:on_output` event. `partitioning_stress` chains four shards × 8
  # partitions; only ShardD emits to a plugin, and only its `p0` partition
  # is targeted by the pipeline routing so it runs once per ShardC emit.
  # The entry shard fans 8-wide → ShardA runs 8× → 8 emits to B[p0] →
  # 8 emits to C[p0] → 8 emits to D[p0] → 8 plugin-bound emits per input.
  defp expected_responses(:regression, total_messages), do: total_messages
  defp expected_responses(:partitioning, total_messages), do: total_messages * 8
  # `dynamic_partition` emits once per input (one plugin-bound emit),
  # so each dispatched input produces one `:on_output` event.
  defp expected_responses(:dynamic, total_messages), do: total_messages

  # Per-shape wire input. The dynamic shape prefixes a bounded key
  # (`k0`..`k7`) so a steady burst exercises 8 dynamic Partitions per relay
  # without tripping the soft cap; other shapes are key-agnostic.
  defp input_for_shape(:dynamic, relay_id, msg_num) do
    "k#{rem(msg_num, 8)}:test_msg_#{relay_id}_#{msg_num}"
  end

  defp input_for_shape(_shape, relay_id, msg_num) do
    "test_msg_#{relay_id}_#{msg_num}"
  end
end

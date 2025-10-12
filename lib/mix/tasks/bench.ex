defmodule Mix.Tasks.Bench do
  use Mix.Task
  require Logger
  alias Flezha.Service.ProcessorManager
  alias Flezha.Service.PluginManager
  alias Flezha.ComputeEngine.FleetManager

  @shortdoc "Run performance tests for Flezha processors"
  @moduledoc """
  Run performance tests for Flezha processors.

  ## Usage

      mix bench --num-processors 1000 --messages-per-processor 1000

  ## Options

    * `--num-processors` - Number of processors to create (default: 1000)
    * `--messages-per-processor` - Number of messages to send per processor (default: 1000)

  """

  @default_namespace "default"
  @bench_namespace "bench"
  @test_processor_factory "do_nothing"

  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [
          num_processors: :integer,
          messages_per_processor: :integer
        ],
        aliases: [
          n: :num_processors,
          m: :messages_per_processor
        ]
      )

    num_processors = opts[:num_processors] || 1000
    test_messages_per_processor = opts[:messages_per_processor] || 1000

    IO.puts("Starting Flezha performance test...")
    IO.puts("Processors: #{num_processors}")
    IO.puts("Messages per processor: #{test_messages_per_processor}")
    IO.puts("")

    # Set logging to only show warnings and errors BEFORE starting application
    Logger.configure(level: :warning)

    # Also configure logger through Application environment
    Application.put_env(:logger, :level, :warning)

    # Ensure application is started
    Mix.Task.run("app.start")

    # Re-configure logger after app start to ensure it sticks
    Logger.configure(level: :warning)

    # Check if we can use the default namespace or need to set up a test environment
    namespace =
      if compute_engines_available?(@default_namespace) do
        @default_namespace
      else
        setup_test_environment()
        @bench_namespace
      end

    try do
      run_performance_test(namespace, num_processors, test_messages_per_processor)
    after
      if namespace == @bench_namespace do
        cleanup_test_environment(namespace)
      end
    end
  end

  defp compute_engines_available?(namespace) do
    case FleetManager.active_compute_engines(namespace) do
      engines when is_list(engines) and length(engines) > 0 -> true
      _ -> false
    end
  end

  defp setup_test_environment() do
    IO.puts("Setting up test environment with compute engines...")

    # Start compute engines using the same approach as the test helper
    compute_engine_ids = start_compute_engines(@bench_namespace, 2)

    IO.puts("Started #{length(compute_engine_ids)} compute engines for performance testing")

    # Store compute engine IDs for cleanup
    Process.put(:bench_compute_engines, compute_engine_ids)
  end

  defp cleanup_test_environment(namespace) do
    IO.puts("Cleaning up test environment...")

    # Stop all processors in the namespace
    ProcessorManager.stop_all_processors(namespace)

    # Stop compute engines if we started them
    case Process.get(:bench_compute_engines) do
      engines when is_list(engines) ->
        Enum.each(engines, fn engine_id ->
          FleetManager.disconnect_compute_engine(namespace, engine_id)
        end)

      _ ->
        :ok
    end

    # Stop the namespace
    FleetManager.stop_namespace(namespace)

    IO.puts("Test environment cleanup completed")
  end

  defp start_compute_engines(namespace, count) do
    # Get current working directory for the cargo command
    {:ok, current_path} = :file.get_cwd()

    # Start multiple compute engines
    for i <- 1..count do
      command =
        "cargo run --manifest-path=#{current_path}/test/apps/rust_compute_engine/Cargo.toml --target-dir=./target -- #{namespace}"

      # Add event handler to wait for connection
      Flezha.Service.EventManager.add_handler(:compute_engine_connected_event)

      # Start the compute engine process
      Flezha.Service.PortPilot.open_port(command)

      # Wait for connection
      compute_engine_id =
        receive do
          {:compute_engine_connected_event, {^namespace, id}} -> id
        after
          5000 -> raise "Performance test compute engine #{i} failed to connect"
        end

      # Remove handler to avoid duplicate events
      Flezha.Service.EventManager.rem_handler(:compute_engine_connected_event)

      compute_engine_id
    end
  end

  defp run_performance_test(namespace, num_processors, test_messages_per_processor) do
    IO.puts("Starting flow test with #{num_processors} processors...")

    {processor_creation_time, processor_infos} =
      :timer.tc(fn ->
        for i <- 1..num_processors do
          processor_id = "flow_test_#{i}"

          processor = %Flezha.Processor.Builder{
            id: processor_id,
            factory_id: @test_processor_factory
          }

          case ProcessorManager.start_processor(namespace, processor) do
            {:ok, info} ->
              {processor_id, info}

            error ->
              IO.puts("Failed to start processor #{processor_id}: #{inspect(error)}")
              nil
          end
        end
        |> Enum.filter(&(&1 != nil))
      end)

    IO.puts(
      "Processor creation took: #{processor_creation_time / 1000} ms for #{length(processor_infos)} processors"
    )

    # Step 2: Enable plugins for all processors
    {plugin_setup_time, _} =
      :timer.tc(fn ->
        Enum.each(processor_infos, fn {processor_id, _info} ->
          # Enable BeamMsgForward plugin
          PluginManager.enable_plugin(
            processor_id <> "_beam_msg_forward",
            %Flezha.Plugin.Driver.IOBindingPlugin{
              namespace: namespace,
              processor_id: processor_id,
              trigger: :on_output,
              provider: %Flezha.Plugin.Provider.BeamMsgForward{
                recv_pid: self()
              }
            }
          )

          # Enable StdOut plugin
          PluginManager.enable_plugin(
            processor_id <> "_stdout_plugin",
            %Flezha.Plugin.Driver.IOBindingPlugin{
              namespace: namespace,
              processor_id: processor_id,
              trigger: :proactive,
              provider: %Flezha.Plugin.Provider.StdOut{}
            }
          )
        end)
      end)

    IO.puts(
      "Plugin setup took: #{plugin_setup_time / 1000} ms for #{length(processor_infos)} processors"
    )

    # Step 3: Send test messages to all processors
    total_messages = length(processor_infos) * test_messages_per_processor

    IO.puts(
      "Sending #{total_messages} total messages (#{test_messages_per_processor} per processor)..."
    )

    {message_send_time, _} =
      :timer.tc(fn ->
        Enum.each(processor_infos, fn {processor_id, %{pid: processor_pid}} ->
          for msg_num <- 1..test_messages_per_processor do
            GenServer.cast(
              processor_pid,
              {:io_in, "test_msg_#{processor_id}_#{msg_num}", %{}}
            )
          end
        end)
      end)

    IO.puts("Message sending took: #{message_send_time / 1000} ms")

    # Step 4: Receive and measure response time
    {message_receive_time, _} =
      :timer.tc(fn ->
        for x <- 1..total_messages do
          receive do
            {:message_forward, _msg} ->
              :ok
          after
            10_000 ->
              IO.puts("Timeout waiting for message #{x}/#{total_messages}")
              exit("Timeout waiting for messages")
          end
        end
      end)

    IO.puts(
      "Message processing took: #{message_receive_time / 1000} ms for #{total_messages} messages"
    )

    # Calculate throughput metrics
    total_time =
      processor_creation_time + plugin_setup_time + message_send_time + message_receive_time

    throughput_msgs_per_sec = total_messages * 1_000_000 / message_receive_time

    IO.puts("\n=== Performance Summary ===")
    IO.puts("Processors created: #{length(processor_infos)}")
    IO.puts("Total messages processed: #{total_messages}")
    IO.puts("Processor creation: #{processor_creation_time / 1000} ms")
    IO.puts("Plugin setup: #{plugin_setup_time / 1000} ms")
    IO.puts("Message sending: #{message_send_time / 1000} ms")
    IO.puts("Message processing: #{message_receive_time / 1000} ms")
    IO.puts("Total time: #{total_time / 1000} ms")
    IO.puts("Throughput: #{Float.round(throughput_msgs_per_sec, 2)} messages/second")
    IO.puts("Average time per processor: #{total_time / length(processor_infos) / 1000} ms")

    # Cleanup
    ProcessorManager.stop_all_processors(namespace)

    if length(processor_infos) == 0 do
      exit("No processors were created successfully")
    end

    if total_messages == 0 do
      exit("No messages were processed")
    end

    IO.puts("\nPerformance test completed successfully!")
  end
end

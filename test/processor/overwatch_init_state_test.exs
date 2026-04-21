defmodule FlezhaTest.Processor.OverwatchInitStateTest do
  alias Flezha.Processor.Builder
  alias Flezha.Processor.InitStatePlugin
  alias Flezha.Service.ProcessorManager

  use ExUnit.Case

  @namespace "default"
  @factory "store_input"

  defmodule ProbeProvider do
    @behaviour Flezha.Processor.InitStateProvider
    @impl true
    def resolve(context) do
      pid = Map.fetch!(context.opts, :report_to)
      send(pid, {:probe, :called})
      {:ok, Map.get(context.opts, :return, %{})}
    end
  end

  defmodule FailProvider do
    @behaviour Flezha.Processor.InitStateProvider
    @impl true
    def resolve(_context), do: {:error, :intentional_failure}
  end

  setup do
    on_exit(fn -> ProcessorManager.stop_all_processors(@namespace) end)
    :ok
  end

  test "4.2.1 cold start with empty inline init_values runs chain and SDK observes resolved map" do
    processor_id = "init_state_cold_start_test"

    plugin =
      InitStatePlugin.new(
        provider: ProbeProvider,
        opts: %{
          report_to: self(),
          return: %{"binary_inputs" => [], "text_inputs" => ["seeded_by_plugin"]}
        }
      )

    processor = %Builder{
      id: processor_id,
      factory_id: @factory,
      opts: [init_state_plugins: [plugin]]
    }

    assert {:ok, _} = ProcessorManager.start_processor(@namespace, processor)
    assert_receive {:probe, :called}, 500

    {:ok, json_state} =
      ProcessorManager.get_processor_state(@namespace, processor_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "binary_inputs" => [],
             "text_inputs" => ["seeded_by_plugin"]
           }
  end

  test "4.2.2 cold start with non-empty inline init_values bypasses chain" do
    processor_id = "init_state_inline_bypass_test"

    plugin =
      InitStatePlugin.new(
        provider: ProbeProvider,
        opts: %{
          report_to: self(),
          return: %{"binary_inputs" => [], "text_inputs" => ["FROM_PLUGIN"]}
        }
      )

    processor = %Builder{
      id: processor_id,
      factory_id: @factory,
      init_values: %{"binary_inputs" => [], "text_inputs" => ["FROM_INLINE"]},
      opts: [init_state_plugins: [plugin]]
    }

    assert {:ok, _} = ProcessorManager.start_processor(@namespace, processor)
    refute_receive {:probe, :called}, 200

    {:ok, json_state} =
      ProcessorManager.get_processor_state(@namespace, processor_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "binary_inputs" => [],
             "text_inputs" => ["FROM_INLINE"]
           }
  end

  test "4.2.3 migration path does not invoke resolver even when plugins are configured" do
    processor_id = "init_state_migration_test"

    plugin =
      InitStatePlugin.new(
        provider: ProbeProvider,
        opts: %{report_to: self(), return: %{"binary_inputs" => [], "text_inputs" => ["x"]}}
      )

    processor = %Builder{
      id: processor_id,
      factory_id: @factory,
      opts: [init_state_plugins: [plugin]]
    }

    assert {:ok, %{pid: pid, compute_engine_id: current_id}} =
             ProcessorManager.start_processor(@namespace, processor)

    assert_receive {:probe, :called}, 500

    try do
      GenServer.call(pid, {:compute_engine_migrate, current_id})
    catch
      :exit, _ -> :ok
    end

    refute_receive {:probe, :called}, 300
  end

  test "4.2.4 {:error, reason} from resolver surfaces and processor is not registered" do
    processor_id = "init_state_error_surfaces_test"

    plugin = InitStatePlugin.new(provider: FailProvider)

    processor = %Builder{
      id: processor_id,
      factory_id: @factory,
      opts: [init_state_plugins: [plugin]]
    }

    assert {:error, %{reason: :intentional_failure}} =
             ProcessorManager.start_processor(@namespace, processor)

    assert ProcessorManager.exist?(@namespace, processor_id) == false
  end
end

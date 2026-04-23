defmodule FlezhaTest.Plugin.MessageHandlerIntegrationTest do
  @moduledoc """
  End-to-end integration tests for `processor-message-handlers` and the
  `Sibling` init-state provider under the unified extension-system.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MessageForward
  alias Flezha.Processor.Builder
  alias Flezha.Processor.InitState.Providers.Sibling
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ProcessorManager

  use ExUnit.Case

  @namespace "default"

  setup do
    on_exit(fn -> ProcessorManager.stop_all_processors(@namespace) end)
    :ok
  end

  defp request(pid, key, payload, timeout_ms \\ 2_000) do
    ref = make_ref()
    send(pid, {:flezha_request, key, payload, self(), ref})

    receive do
      {:flezha_response, ^ref, result} -> result
    after
      timeout_ms -> {:error, :test_timeout}
    end
  end

  test "factory handle_message echoes payload round-trips via MessageForward" do
    processor_id = "mh_int_echo"

    assert {:ok, %{pid: overwatch_pid}} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: processor_id,
               factory_id: "echo_message"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_echo_plugin", %Plugin{
               id: "mh_int_echo_plugin",
               namespace: @namespace,
               processor_id: processor_id,
               provider: MessageForward,
               opts: %{},
               subscriptions: [{:on_message, [key: :sdk_echo]}]
             })

    payload = <<1, 2, 3, 4, 5>>
    assert {:ok, ^payload} = request(overwatch_pid, :sdk_echo, payload)
  end

  test "factory handle_message returns error string surfaced to caller" do
    processor_id = "mh_int_error"

    assert {:ok, %{pid: overwatch_pid}} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: processor_id,
               factory_id: "error_message"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_error_plugin", %Plugin{
               id: "mh_int_error_plugin",
               namespace: @namespace,
               processor_id: processor_id,
               provider: MessageForward,
               opts: %{},
               subscriptions: [{:on_message, [key: :sdk_err]}]
             })

    assert {:error, "handle_message_error"} = request(overwatch_pid, :sdk_err, "ignored")
  end

  test "factory without handle_message returns non-empty error and stays healthy" do
    processor_id = "mh_int_unsupported"

    assert {:ok, %{pid: overwatch_pid}} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: processor_id,
               factory_id: "store_input"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_unsupported_plugin", %Plugin{
               id: "mh_int_unsupported_plugin",
               namespace: @namespace,
               processor_id: processor_id,
               provider: MessageForward,
               opts: %{},
               subscriptions: [{:on_message, [key: :unsupported]}]
             })

    assert {:error, msg} = request(overwatch_pid, :unsupported, <<>>)
    assert is_binary(msg) and msg != ""

    assert Process.alive?(overwatch_pid)
    assert {:ok, _json_state} = ProcessorManager.get_processor_state(@namespace, processor_id)
  end

  test "Sibling init-state seeds a new processor from a running source" do
    source_id = "mh_int_source"
    consumer_id = "mh_int_consumer"

    assert {:ok, _} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: source_id,
               factory_id: "json_state_message"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_source_plugin", %Plugin{
               id: "mh_int_source_plugin",
               namespace: @namespace,
               processor_id: source_id,
               provider: MessageForward,
               opts: %{},
               subscriptions: [{:on_message, [key: :fetch_init_state]}]
             })

    from_plugin = %Plugin{
      id: "from-processor",
      provider: Sibling,
      opts: %{
        source_processor_id: source_id,
        handler_key: :fetch_init_state,
        request_timeout_ms: 2_000
      },
      subscriptions: [{:on_processor_init, []}]
    }

    assert {:ok, _} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: consumer_id,
               factory_id: "store_input",
               opts: [plugins: [from_plugin]]
             })

    assert {:ok, json_state} =
             ProcessorManager.get_processor_state(@namespace, consumer_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "binary_inputs" => [],
             "text_inputs" => ["seeded_from_source"]
           }
  end

  test "Sibling on_error: :empty with unreachable source lets consumer start empty" do
    consumer_id = "mh_int_consumer_degraded"

    from_plugin = %Plugin{
      id: "from-processor-empty",
      provider: Sibling,
      opts: %{
        source_processor_id: "does_not_exist",
        handler_key: :fetch_init_state,
        request_timeout_ms: 500
      },
      subscriptions: [{:on_processor_init, [on_error: :empty]}]
    }

    assert {:ok, _} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: consumer_id,
               factory_id: "store_input",
               opts: [plugins: [from_plugin]]
             })

    assert {:ok, json_state} =
             ProcessorManager.get_processor_state(@namespace, consumer_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "binary_inputs" => [],
             "text_inputs" => []
           }
  end

  test "handler is still reachable under the same key after compute-engine migration" do
    processor_id = "mh_int_migrated"

    assert {:ok, %{pid: pid, compute_engine_id: current_id}} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: processor_id,
               factory_id: "echo_message"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_migrated_plugin", %Plugin{
               id: "mh_int_migrated_plugin",
               namespace: @namespace,
               processor_id: processor_id,
               provider: MessageForward,
               opts: %{},
               subscriptions: [{:on_message, [key: :sdk_echo]}]
             })

    assert {:ok, "before"} = request(pid, :sdk_echo, "before")

    try do
      GenServer.call(pid, {:compute_engine_migrate, current_id})
    catch
      :exit, _ -> :ok
    end

    assert {:ok, "after"} = request(pid, :sdk_echo, "after")
  end
end

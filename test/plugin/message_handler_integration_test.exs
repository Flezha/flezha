defmodule FlezhaTest.Plugin.MessageHandlerIntegrationTest do
  @moduledoc """
  End-to-end integration tests for `processor-message-handlers` and the
  `FromProcessor` init-state provider. These exercise a real
  `rust_compute_engine` via the default testnet compute-engines — the test
  binary registers `echo_message`, `error_message`, and
  `json_state_message` factories that override `handle_message` with
  deterministic replies.
  """

  alias Flezha.Plugin.Driver.MessageHandlerPlugin
  alias Flezha.Plugin.Provider.MessageForward
  alias Flezha.Processor.Builder
  alias Flezha.Processor.InitStatePlugin
  alias Flezha.Processor.InitState.Providers.FromProcessor
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ProcessorManager

  use ExUnit.Case

  @namespace "default"

  setup do
    on_exit(fn -> ProcessorManager.stop_all_processors(@namespace) end)
    :ok
  end

  defp request(pid, handler_key, payload, timeout_ms \\ 2_000) do
    ref = make_ref()
    send(pid, {:flezha_request, handler_key, payload, self(), ref})

    receive do
      {:flezha_response, ^ref, result} -> result
    after
      timeout_ms -> {:error, :test_timeout}
    end
  end

  test "11.1 factory handle_message echoes payload round-trips via MessageForward" do
    processor_id = "mh_int_echo"

    assert {:ok, %{pid: overwatch_pid}} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: processor_id,
               factory_id: "echo_message"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_echo_plugin", %MessageHandlerPlugin{
               namespace: @namespace,
               processor_id: processor_id,
               handler_key: :sdk_echo,
               provider: MessageForward
             })

    payload = <<1, 2, 3, 4, 5>>
    assert {:ok, ^payload} = request(overwatch_pid, :sdk_echo, payload)
  end

  test "11.2 factory handle_message returns error string surfaced to caller" do
    processor_id = "mh_int_error"

    assert {:ok, %{pid: overwatch_pid}} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: processor_id,
               factory_id: "error_message"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_error_plugin", %MessageHandlerPlugin{
               namespace: @namespace,
               processor_id: processor_id,
               handler_key: :sdk_err,
               provider: MessageForward
             })

    assert {:error, "handle_message_error"} = request(overwatch_pid, :sdk_err, "ignored")
  end

  test "11.3 factory without handle_message returns non-empty error and stays healthy" do
    processor_id = "mh_int_unsupported"

    assert {:ok, %{pid: overwatch_pid}} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: processor_id,
               factory_id: "store_input"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_unsupported_plugin", %MessageHandlerPlugin{
               namespace: @namespace,
               processor_id: processor_id,
               handler_key: :unsupported,
               provider: MessageForward
             })

    assert {:error, msg} = request(overwatch_pid, :unsupported, <<>>)
    assert is_binary(msg) and msg != ""

    # Processor is still healthy: existing control commands (get state)
    # keep working and the Overwatch pid is still alive.
    assert Process.alive?(overwatch_pid)
    assert {:ok, _json_state} = ProcessorManager.get_processor_state(@namespace, processor_id)
  end

  test "11.4 FromProcessor init-state seeds a new processor from a running source" do
    source_id = "mh_int_source"
    consumer_id = "mh_int_consumer"

    assert {:ok, _} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: source_id,
               factory_id: "json_state_message"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_source_plugin", %MessageHandlerPlugin{
               namespace: @namespace,
               processor_id: source_id,
               handler_key: :fetch_init_state,
               provider: MessageForward
             })

    from_plugin =
      InitStatePlugin.new(
        provider: FromProcessor,
        opts: %{
          source_processor_id: source_id,
          handler_key: :fetch_init_state,
          request_timeout_ms: 2_000
        }
      )

    assert {:ok, _} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: consumer_id,
               factory_id: "store_input",
               opts: [init_state_plugins: [from_plugin]]
             })

    assert {:ok, json_state} =
             ProcessorManager.get_processor_state(@namespace, consumer_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "binary_inputs" => [],
             "text_inputs" => ["seeded_from_source"]
           }
  end

  test "11.5 FromProcessor on_error: :empty with unreachable source lets consumer start empty" do
    consumer_id = "mh_int_consumer_degraded"

    from_plugin =
      InitStatePlugin.new(
        provider: FromProcessor,
        on_error: :empty,
        opts: %{
          source_processor_id: "does_not_exist",
          handler_key: :fetch_init_state,
          request_timeout_ms: 500
        }
      )

    assert {:ok, _} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: consumer_id,
               factory_id: "store_input",
               opts: [init_state_plugins: [from_plugin]]
             })

    assert {:ok, json_state} =
             ProcessorManager.get_processor_state(@namespace, consumer_id)

    # Empty init_values → SDK keeps the factory default (empty StoreInputState).
    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "binary_inputs" => [],
             "text_inputs" => []
           }
  end

  test "11.6 handler is still reachable under the same key after compute-engine migration" do
    processor_id = "mh_int_migrated"

    assert {:ok, %{pid: pid, compute_engine_id: current_id}} =
             ProcessorManager.start_processor(@namespace, %Builder{
               id: processor_id,
               factory_id: "echo_message"
             })

    assert {:ok, "plugin enabled"} =
             PluginManager.enable_plugin("mh_int_migrated_plugin", %MessageHandlerPlugin{
               namespace: @namespace,
               processor_id: processor_id,
               handler_key: :sdk_echo,
               provider: MessageForward
             })

    # Sanity check before migration.
    assert {:ok, "before"} = request(pid, :sdk_echo, "before")

    # Drive the migration code path (alter_handover_state) on the same
    # Overwatch pid — this exercises `sync_handlers/1` which re-registers
    # the MessageHandlerPlugin with PluginManager after handover.
    try do
      GenServer.call(pid, {:compute_engine_migrate, current_id})
    catch
      :exit, _ -> :ok
    end

    assert {:ok, "after"} = request(pid, :sdk_echo, "after")
  end
end

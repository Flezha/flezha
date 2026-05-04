defmodule FlezhaTest.Plugin.MessageHandlerIntegrationTest do
  @moduledoc """
  End-to-end integration tests for `processor-message-handlers` under
  the unified extension-system.

  RELAY_MESSAGE_HANDLERS_PENDING — these tests exercise the
  `EchoMessageProcessor` / `ErrorMessageProcessor` / `JsonStateMessageProcessor`
  factories that `relay-mvp-rust` removed from the Rust test app
  (per `relay-mvp-rust/design.md` Decision 9). The matching agent path
  is preserved; the Rust SDK side falls through to the canonical
  `processor does not support handle_message` error. `relay-message-handlers`
  re-introduces per-Shard `handle_message` and re-enables this module by
  removing the `@moduletag :skip` tag below.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MessageForward
  alias Flezha.Processor.Builder
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ProcessorManager

  use ExUnit.Case
  @moduletag :skip

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

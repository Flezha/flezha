defmodule FlezhaTest.Plugin.MessageHandlerDispatchTest do
  @moduledoc """
  Covers the Overwatch-side dispatch of `{:flezha_request, ...}` envelopes,
  the `MessageHandlerPlugin` enable/disable conflict handling, and the
  migration-race outcome where a caller resolves the source pid *before*
  the old Overwatch is terminated and sends *after* it dies.
  """

  alias Flezha.Plugin.Driver.MessageHandlerPlugin
  alias Flezha.Processor.Builder
  alias Flezha.Service.ProcessorManager

  use ExUnit.Case

  @namespace "default"
  @factory "store_input"

  defmodule OkProvider do
    @behaviour Flezha.Plugin.MessageHandlerBehaviour
    @impl true
    def handle(payload, _context), do: {:ok, "ok:" <> payload}
  end

  defmodule ErrorProvider do
    @behaviour Flezha.Plugin.MessageHandlerBehaviour
    @impl true
    def handle(_payload, _context), do: {:error, :deliberate}
  end

  defmodule RaisingProvider do
    @behaviour Flezha.Plugin.MessageHandlerBehaviour
    @impl true
    def handle(_payload, _context), do: raise("boom")
  end

  defmodule SlowProvider do
    @behaviour Flezha.Plugin.MessageHandlerBehaviour
    @impl true
    def handle(payload, _context) do
      Process.sleep(200)
      {:ok, "slow:" <> payload}
    end
  end

  setup do
    on_exit(fn -> ProcessorManager.stop_all_processors(@namespace) end)
    :ok
  end

  defp start_processor(id) do
    processor = %Builder{id: id, factory_id: @factory}
    assert {:ok, %{pid: pid}} = ProcessorManager.start_processor(@namespace, processor)
    pid
  end

  defp enable_handler(overwatch_pid, plugin_id, handler_key, provider, opts \\ %{}) do
    plugin = %MessageHandlerPlugin{
      namespace: @namespace,
      processor_id: "ignored-by-direct-enable",
      handler_key: handler_key,
      provider: provider,
      opts: opts
    }

    GenServer.call(overwatch_pid, {:enable_message_handler, plugin_id, plugin})
  end

  defp request(pid, handler_key, payload, timeout_ms \\ 1_000) do
    ref = make_ref()
    send(pid, {:flezha_request, handler_key, payload, self(), ref})

    receive do
      {:flezha_response, ^ref, result} -> result
    after
      timeout_ms -> {:error, :test_timeout}
    end
  end

  test "10.1 valid handler_key routes to plugin and replies {:ok, bytes}" do
    pid = start_processor("mh_dispatch_ok")
    :ok = enable_handler(pid, "plugin-ok", :echo, OkProvider)

    assert {:ok, "ok:hi"} = request(pid, :echo, "hi")
  end

  test "10.2 unknown handler_key replies {:error, {:unknown_handler_key, key}}" do
    pid = start_processor("mh_dispatch_unknown")

    assert {:error, {:unknown_handler_key, :nope}} = request(pid, :nope, <<>>)
  end

  test "10.3 long-running handler runs off-Overwatch and does not block IO casts" do
    pid = start_processor("mh_dispatch_nonblocking")
    :ok = enable_handler(pid, "plugin-slow", :slow, SlowProvider)

    ref = make_ref()
    send(pid, {:flezha_request, :slow, "x", self(), ref})

    # While the handler is asleep for ~200ms, IO casts and pings must still
    # be served by the Overwatch GenServer, otherwise they'd queue behind
    # the handler call.
    GenServer.cast(pid, {:io_in, "probe", %{}})
    assert :pong = GenServer.call(pid, :ping, 100)

    assert_receive {:flezha_response, ^ref, {:ok, "slow:x"}}, 500
  end

  test "10.4 duplicate handler_key returns conflict and keeps existing handler" do
    pid = start_processor("mh_dispatch_conflict")
    :ok = enable_handler(pid, "plugin-a", :shared, OkProvider)

    assert {:error, {:handler_key_conflict, :shared}} =
             enable_handler(pid, "plugin-b", :shared, ErrorProvider)

    # Original handler still wins.
    assert {:ok, "ok:ping"} = request(pid, :shared, "ping")
  end

  test "10.5 handler that raises surfaces as {:error, {:exception, _, _}} and Overwatch stays up" do
    pid = start_processor("mh_dispatch_raise")
    :ok = enable_handler(pid, "plugin-raise", :boom, RaisingProvider)
    :ok = enable_handler(pid, "plugin-ok", :ok, OkProvider)

    assert {:error, {:exception, :error, %RuntimeError{}}} = request(pid, :boom, "x")

    # Overwatch is still serving other requests.
    assert {:ok, "ok:after"} = request(pid, :ok, "after")
    assert :pong = GenServer.call(pid, :ping)
  end

  test "10.6 handler returning {:error, reason} is propagated verbatim" do
    pid = start_processor("mh_dispatch_error")
    :ok = enable_handler(pid, "plugin-err", :err, ErrorProvider)

    assert {:error, :deliberate} = request(pid, :err, <<>>)
  end

  test "6.4 sending after the old Overwatch pid dies surfaces as caller timeout" do
    pid = start_processor("mh_dispatch_dead_pid")
    :ok = enable_handler(pid, "plugin-ok", :echo, OkProvider)

    # Stop the processor cleanly so the caller is holding a stale pid
    # when it eventually sends — mirroring the "resolve before / send after
    # handover" migration-race outcome described in design.md.
    :ok = ProcessorManager.stop_processor(@namespace, "mh_dispatch_dead_pid")
    refute Process.alive?(pid)

    # Use a short timeout because the message is dropped, not answered.
    assert {:error, :test_timeout} = request(pid, :echo, "x", 100)
  end
end

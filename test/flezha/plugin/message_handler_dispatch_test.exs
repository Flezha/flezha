defmodule FlezhaTest.Plugin.MessageHandlerDispatchTest do
  @moduledoc """
  Covers Overwatch-side dispatch of `{:flezha_request, ...}` envelopes under
  the unified extension-system contract: `on_message/2` subscribers,
  per-key uniqueness enforced by `Extension.Table`, and Task-supervised
  handler invocation.
  """

  alias Flezha.Relay.Builder
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case

  # A key conflict is driven on purpose and the table reports the halt at
  # error level; the refusal itself is asserted as a return value.
  @moduletag :capture_log

  @namespace "default"
  @factory "store_input"

  defmodule OkProvider do
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_message(%Ctx.Msg{payload: p}, _opts), do: {:ok, "ok:" <> p}
  end

  defmodule ErrorProvider do
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_message(_ctx, _opts), do: {:error, :deliberate}
  end

  defmodule RaisingProvider do
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_message(_ctx, _opts), do: raise("boom")
  end

  # Holds its answer until the test releases it, so the Overwatch is proved
  # responsive while a handler is genuinely still running.
  defmodule SlowProvider do
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_message(%Ctx.Msg{payload: p}, _opts) do
      Process.register(self(), :slow_handler_under_test)
      NodeTestHelper.park()
      {:ok, "slow:" <> p}
    end
  end

  setup do
    on_exit(fn -> RelayManager.stop_all_relays(@namespace) end)
    :ok
  end

  defp start_relay(id) do
    relay = %Builder{id: id, factory_id: @factory}
    assert {:ok, %{pid: pid}} = RelayManager.start_relay(@namespace, relay)
    pid
  end

  defp enable_handler(overwatch_pid, plugin_id, key, provider, opts \\ %{}) do
    plugin = %Plugin{
      id: plugin_id,
      namespace: @namespace,
      relay_id: "ignored-by-direct-enable",
      provider: provider,
      opts: opts,
      hooks: [{:on_message, [key: key]}]
    }

    # The plugin is its own entry: nothing enables it by name here.
    GenServer.call(overwatch_pid, {:plugin_subscribe, plugin_id, plugin, plugin})
  end

  defp request(pid, key, payload, timeout_ms \\ 1_000) do
    ref = make_ref()
    send(pid, {:flezha_request, key, payload, self(), ref})

    receive do
      {:flezha_response, ^ref, result} -> result
    after
      timeout_ms -> {:error, :test_timeout}
    end
  end

  test "valid key routes to plugin and replies {:ok, bytes}" do
    pid = start_relay("mh_dispatch_ok")
    :ok = enable_handler(pid, "plugin-ok", "echo", OkProvider)

    assert {:ok, "ok:hi"} = request(pid, "echo", "hi")
  end

  test "unknown key replies {:error, {:unknown_key, :on_message, key}}" do
    pid = start_relay("mh_dispatch_unknown")

    assert {:error, {:unknown_key, :on_message, "nope"}} = request(pid, "nope", <<>>)
  end

  test "long-running handler runs off-Overwatch and does not block IO casts" do
    pid = start_relay("mh_dispatch_nonblocking")
    :ok = enable_handler(pid, "plugin-slow", "slow", SlowProvider)

    ref = make_ref()
    send(pid, {:flezha_request, "slow", "x", self(), ref})

    handler =
      NodeTestHelper.wait_until(
        fn -> Process.whereis(:slow_handler_under_test) end,
        1_000
      )

    assert is_pid(handler), "the handler never started"

    GenServer.cast(pid, {:io_in, "probe", %{}})
    assert :pong = GenServer.call(pid, :ping, 100)

    send(handler, :unpark)
    assert_receive {:flezha_response, ^ref, {:ok, "slow:x"}}, 500
  end

  test "duplicate key returns conflict and keeps existing handler" do
    pid = start_relay("mh_dispatch_conflict")
    :ok = enable_handler(pid, "plugin-a", "shared", OkProvider)

    assert {:error, {:key_conflict, :on_message, "shared"}} =
             enable_handler(pid, "plugin-b", "shared", ErrorProvider)

    assert {:ok, "ok:ping"} = request(pid, "shared", "ping")
  end

  test "handler that raises surfaces as {:error, {:exception, _, _}} and Overwatch stays up" do
    pid = start_relay("mh_dispatch_raise")
    :ok = enable_handler(pid, "plugin-raise", "boom", RaisingProvider)
    :ok = enable_handler(pid, "plugin-ok", "ok", OkProvider)

    assert {:error, {:exception, :error, %RuntimeError{}}} = request(pid, "boom", "x")

    assert {:ok, "ok:after"} = request(pid, "ok", "after")
    assert :pong = GenServer.call(pid, :ping)
  end

  test "handler returning {:error, reason} is propagated verbatim" do
    pid = start_relay("mh_dispatch_error")
    :ok = enable_handler(pid, "plugin-err", "err", ErrorProvider)

    assert {:error, :deliberate} = request(pid, "err", <<>>)
  end

  test "sending after the old Overwatch pid dies surfaces as caller timeout" do
    pid = start_relay("mh_dispatch_dead_pid")
    :ok = enable_handler(pid, "plugin-ok", "echo", OkProvider)

    :ok = RelayManager.stop_relay(@namespace, "mh_dispatch_dead_pid")
    refute Process.alive?(pid)

    assert {:error, :test_timeout} = request(pid, "echo", "x", 100)
  end
end

defmodule FlezhaTest.System.HttpPushE2ETest do
  @moduledoc """
  `relay_http_push` end to end against live runners: a plain flow relay
  (`store_input`) sends every output and drops a refused one; two
  effectively-once relays (`tier_store`) bound to one shared plugin send over
  one connection, and while the server fails their outputs wait and arrive in
  order once it recovers, leaving the outbox owing nothing, and a refusal parks
  the output as a dead letter.

  Tagged `:system` (boots real subprocesses); run with
  `mix test --include system test/system/http_push_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :effectively_once

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Dlq.Store
  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.HttpPush
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.WebhookServer

  @moduletag timeout: 300_000

  setup do
    dir = Path.join(System.tmp_dir!(), "flezha_push_e2e_#{System.unique_integer([:positive])}")
    original = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.put(original, :dir, dir))

    on_exit(fn ->
      Application.put_env(:flezha, :dlq, original)
      File.rm_rf!(dir)
    end)

    :ok
  end

  defp start!(namespace, relay_id, factory) do
    builder = %Builder{id: relay_id, factory_id: factory, init_values: %{}, opts: []}
    assert {:ok, _info} = RelayManager.start_relay(namespace, builder)
  end

  # The bodies the server received, in order, a retried one read once.
  defp bodies_until(last) do
    receive do
      {:request, _line, _headers, ^last} -> [last]
      {:request, _line, _headers, body} -> [body | bodies_until(last)]
    after
      30_000 -> flunk("#{inspect(last)} never arrived")
    end
  end

  # One namespace, one runner and one `push` plugin on `server`, with
  # `relays` started from `factory` and bound to it.
  defp bound!(factory, relays, server) do
    namespace = unique("push")
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, nil, id_path) end)
    {_port, _exec} = boot_runner(namespace, id_path)
    Enum.each(relays, &start!(namespace, &1, factory))

    {:ok, _} =
      PluginManager.enable_plugin("push", %Plugin{
        id: "push",
        provider: HttpPush,
        namespace: namespace,
        opts: %{
          url: "http://127.0.0.1:#{server.port}/in",
          channel: "sink",
          retries: 100,
          retry_backoff_ms: 50,
          retry_max_backoff_ms: 200
        }
      })

    on_exit(fn -> PluginManager.disable_plugin(namespace, "push") end)

    for relay <- relays,
        do: :ok = PluginManager.bind_plugin(namespace, "push", relay, hooks: [{:on_output, []}])

    namespace
  end

  describe "a plain flow relay" do
    @describetag capability: :state_persistence,
                 skip: FlezhaTest.SystemCase.deferral(:state_persistence)

    test "sends every output, and a refused one is dropped" do
      server = WebhookServer.start([200])
      plain = unique("plain")
      namespace = bound!("store_input", [plain], server)

      send_inputs(plain, namespace, ["a", "b"])
      assert_receive {:request, "POST /in " <> _, headers, "a"}, 30_000
      assert_receive {:request, _, _, "b"}, 30_000
      refute headers =~ "idempotency-key"

      WebhookServer.set(server, [422])
      restore = NodeTestHelper.override_log_level(:warning)

      log =
        capture_log(fn ->
          send_inputs(plain, namespace, ["c"])
          assert_receive {:request, _, _, "c"}, 30_000
          WebhookServer.set(server, [200])
          send_inputs(plain, namespace, ["d"])
          assert_receive {:request, _, _, "d"}, 30_000
        end)

      restore.()

      assert log =~ "dropped a request"
      refute_received {:request, _, _, "c"}
    end
  end

  test "two guaranteed relays share one connection, outlast an outage and park a refusal" do
    server = WebhookServer.start([200])
    [first, second] = [unique("eo_a"), unique("eo_b")]
    namespace = bound!("tier_store", [first, second], server)

    for relay <- [first, second],
        do: assert(wait_until(fn -> ClassRegistry.effectively_once?(relay) end, 30_000))

    send_inputs(first, namespace, ["k1:a"])
    assert_receive {:request, _, headers, "k1=a"}, 30_000
    assert headers =~ ~r/idempotency-key: .+\/.+\/0/
    send_inputs(second, namespace, ["j1:a"])
    assert_receive {:request, _, _, "j1=a"}, 30_000
    assert WebhookServer.connections(server) == 1

    # An outage of two seconds: the guaranteed outputs wait and arrive in order.
    WebhookServer.set(server, [503])
    send_inputs(first, namespace, ["k2:a", "k3:a", "k4:a"])
    assert_receive {:request, _, _, "k2=a"}, 30_000
    Process.sleep(2_000)
    WebhookServer.set(server, [200])
    assert Enum.dedup(bodies_until("k4=a")) == ["k2=a", "k3=a", "k4=a"]
    assert await_outbox_drained(first)

    # A refusal parks the output with its status.
    WebhookServer.set(server, [422])

    log =
      capture_log(fn ->
        send_inputs(first, namespace, ["k5:a"])
        assert_receive {:request, _, _, "k5=a"}, 30_000
        assert wait_until(fn -> Store.list(first) != [] end, 30_000)
        # The park confirms the output only after logging it, so a drained
        # outbox means the log line is in the capture.
        assert await_outbox_drained(first)
      end)

    assert log =~ "parked undeliverable output"
    assert log =~ "http_push.rejected"
    assert [entry] = Store.list(first)

    assert %{"code" => "http_push.rejected", "details" => %{"status" => 422}} =
             Jason.decode!(entry["error"])
  end
end

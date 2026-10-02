defmodule FlezhaTest.Plugin.Provider.HookWebhookTest do
  @moduledoc """
  `hook_webhook` against a local HTTP server: one JSON POST per event, in order
  on one connection, a transient answer retried and a refusal dropped at once,
  and a server that never answers falling behind alone.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  import ExUnit.CaptureLog

  alias Flezha.Cluster.Directory
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.HookWebhook
  alias Flezha.Plugin.Shared
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.HookCopy
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry
  alias FlezhaTest.Helper.WebhookServer

  @namespace "hook_webhook_ns"

  setup_all do
    TestHelpers.reset_all_cells()
    runner_id = NodeTestHelper.start_runner(@namespace)

    on_exit(fn ->
      NodeTestHelper.stop_runner(@namespace, runner_id)
      FleetManager.stop_namespace(@namespace)
    end)

    :ok
  end

  setup do
    Telemetry.capture(
      for verb <- [:sent, :failed, :dropped], do: [:flezha, :plugin, :hook_webhook, verb]
    )

    on_exit(fn -> PluginManager.disable_plugin(@namespace, "hook") end)
    :ok
  end

  defp enable(server, opts \\ %{}) do
    enabled =
      PluginManager.enable_plugin("hook", %Plugin{
        id: "hook",
        provider: HookWebhook,
        namespace: @namespace,
        opts:
          Map.merge(%{url: "http://127.0.0.1:#{server.port}/hooks/secret-path?token=t"}, opts),
        hooks: [{"on_dead_letter", []}]
      })

    with {:ok, _} <- enabled, do: HookCopy.await_listening()
    enabled
  end

  defp dead_letter(n) do
    :telemetry.execute([:flezha, :dlq, :dead_lettered], %{count: 1}, %{
      namespace: @namespace,
      operator: "op#{n}",
      entry: "op#{n}/t/e#{n}",
      channel: "in",
      error: "boom"
    })
  end

  defp code(reason), do: Flezha.Error.from_reason(reason, :"plugin.enable").code

  test "an event is one JSON POST with the configured headers and a text field" do
    server = WebhookServer.start([200])
    {:ok, _} = enable(server, %{headers: ~s({"x-token": "s3"})})
    dead_letter(1)

    assert_receive {:request, request_line, headers, body}, 5_000
    assert request_line =~ "POST /hooks/secret-path?token=t HTTP/1.1"
    assert headers =~ ~r/content-type: application\/json/i
    assert headers =~ ~r/x-token: s3/i

    assert %{"hook" => "on_dead_letter", "operator" => "op1", "text" => text} =
             Jason.decode!(body)

    assert is_binary(text) and text != ""
    assert_receive {:telemetry, [:flezha, :plugin, :hook_webhook, :sent], _, _}
  end

  test "a 503 then a 200 is sent twice and dropped never" do
    server = WebhookServer.start([503, 200])
    {:ok, _} = enable(server)
    dead_letter(1)

    assert_receive {:request, _, _, first}, 5_000
    assert_receive {:request, _, _, ^first}, 5_000
    assert_receive {:telemetry, [:flezha, :plugin, :hook_webhook, :sent], _, _}, 5_000
    refute_received {:telemetry, [:flezha, :plugin, :hook_webhook, :dropped], _, _}
  end

  test "a 400 is sent once, dropped once and warned without the URL's path" do
    server = WebhookServer.start([400])
    {:ok, _} = enable(server)
    restore = NodeTestHelper.override_log_level(:warning)

    log =
      capture_log(fn ->
        dead_letter(1)
        assert_receive {:request, _, _, _}, 5_000
        assert_receive {:telemetry, [:flezha, :plugin, :hook_webhook, :dropped], _, _}, 5_000
      end)

    restore.()

    assert log =~ "hook"
    assert log =~ "127.0.0.1"
    assert log =~ "400"
    refute log =~ "secret-path"
    refute log =~ "token=t"
    refute_receive {:request, _, _, _}, 300
  end

  test "a server that never answers: every emit returns and the plugin lags" do
    Application.put_env(:flezha, :plugin_backlog_frames, 10)
    server = WebhookServer.start([:hang])
    {:ok, _} = enable(server, %{timeout_ms: 500, retries: 0})
    [copy] = Directory.plugin_hosts(@namespace, "hook")

    capture_log(fn ->
      for n <- 1..50, do: assert(:ok = dead_letter(n))
      HookCopy.assert_lagging(copy, 10)
    end)
  end

  test "a header value reaches neither a restart's warning nor the copy's status" do
    server = WebhookServer.start([200])
    secret = "s3cr3t-#{System.unique_integer([:positive])}"
    {:ok, _} = enable(server, %{headers: ~s({"authorization": "#{secret}"})})
    [copy] = Directory.plugin_hosts(@namespace, "hook")

    refute inspect(:sys.get_status(copy)) =~ secret
    {:ok, %Plugin{} = entry} = Flezha.Service.HubManager.plugin_entry(@namespace, "hook")
    restore = NodeTestHelper.override_log_level(:warning)

    log =
      capture_log(fn ->
        Shared.update(
          copy,
          %Plugin{entry | opts: %{entry.opts | url: "ftp://hooks.test/x"}},
          :restart
        )

        :sys.get_state(copy)
      end)

    restore.()
    assert log =~ "did not restart"
    refute log =~ secret
    refute inspect(:sys.get_status(copy)) =~ secret
  end

  test "each option refusal answers its code before anything connects" do
    server = WebhookServer.start([200])

    assert {:error, reason} = enable(server, %{url: "https://user:pw@hooks.test/x"})
    assert code(reason) == "plugin.url_has_credentials"

    assert {:error, reason} = enable(server, %{url: "ftp://hooks.test/x"})
    assert code(reason) == "plugin.url_scheme_unsupported"

    assert {:error, reason} = enable(server, %{url: "http://"})
    assert code(reason) == "plugin.url_without_host"

    assert {:error, reason} = enable(server, %{headers: "[1]"})
    assert code(reason) == "plugin.headers_invalid"

    assert {:error, _} = enable(server, %{timeout_ms: 0})
    assert {:error, _} = enable(server, %{retries: -1})
    refute_receive {:request, _, _, _}, 200
  end
end

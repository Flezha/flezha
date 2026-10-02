defmodule FlezhaTest.Plugin.Provider.HttpPushTest do
  @moduledoc """
  `operator_http_push`'s options: each refusal answers its code, a header the
  request sets itself or could not carry is refused by name, the declaration
  is a shareable row of the provider table, and nothing connects while it is
  checked.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.HttpPush
  alias Flezha.Plugin.Providers
  alias FlezhaTest.Helper.ProviderPrepare
  alias FlezhaTest.Helper.WebhookServer

  defp prepare(server, opts) do
    ProviderPrepare.prepare(
      HttpPush,
      Map.merge(%{url: "http://127.0.0.1:#{server.port}/in", source_plugin_id: "push"}, opts)
    )
  end

  defp refusal(server, opts) do
    assert {:error, reason} = prepare(server, opts)
    Flezha.Error.from_reason(reason, :"plugin.enable")
  end

  test "it is the shareable provider operator_http_push" do
    assert Providers.fetch("operator_http_push") == {:ok, HttpPush}
    assert HttpPush.__provider__().shareable
  end

  test "a valid set of options prepares with its defaults" do
    server = WebhookServer.start([200])

    assert {:ok, opts} = prepare(server, %{headers: ~s({"authorization": "Bearer t"})})
    assert opts.method == "POST"
    assert opts.content_type == "application/octet-stream"
    assert opts.queue_overflow == :drop_nothing
    assert opts.headers == %{"authorization" => "Bearer t"}
  end

  test "each URL refusal answers its code" do
    server = WebhookServer.start([200])

    assert refusal(server, %{url: "https://u:p@api.test/in"}).code ==
             "plugin.url_has_credentials"

    assert refusal(server, %{url: "ftp://api.test/in"}).code == "plugin.url_scheme_unsupported"
    assert refusal(server, %{url: "http://"}).code == "plugin.url_without_host"
  end

  test "a header the request sets itself, or could not carry, is refused by name" do
    server = WebhookServer.start([200])

    for {headers, name} <- [
          {~s({"flz-key": "x"}), "flz-key"},
          {~s({"Content-Type": "text/plain"}), "Content-Type"},
          {~s({"idempotency-key": "k"}), "idempotency-key"},
          {~s({"host": "elsewhere"}), "host"},
          {~s({"bad name": "x"}), "bad name"},
          {~s({"x-token": "café"}), "x-token"}
        ] do
      error = refusal(server, %{headers: headers})
      assert error.code == "plugin.headers_invalid"
      assert error.details["header"] == name
    end

    assert refusal(server, %{headers: "[1]"}).code == "plugin.headers_invalid"

    error = refusal(server, %{content_type: "text/plain\r\nx-injected: 1"})
    assert {error.code, error.details["header"]} == {"plugin.headers_invalid", "content-type"}
  end

  test "every bounded option refuses what is out of its bounds" do
    server = WebhookServer.start([200])

    for opts <- [
          %{method: "GET"},
          %{queue_overflow: "drop_some"},
          %{max_in_flight: 0},
          %{retries: -1},
          %{timeout_ms: 0},
          %{retry_backoff_ms: 0},
          %{delivery_timeout_ms: 0},
          %{queue_max: 0},
          %{queue_max_bytes: 0},
          %{queue_block_timeout_ms: 0},
          %{failure_threshold: 0}
        ] do
      assert {:error, _} = prepare(server, opts), "#{inspect(opts)} was accepted"
    end
  end

  test "checking the options opens no connection" do
    server = WebhookServer.start([200])

    assert {:ok, _} = prepare(server, %{})
    assert {:error, _} = prepare(server, %{headers: ~s({"flz-seq": "1"})})
    refute_receive {:request, _, _, _}, 200
    assert WebhookServer.connections(server) == 0
  end
end

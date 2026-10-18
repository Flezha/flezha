defmodule FlezhaTest.Plugin.Provider.HttpPushWorkerTest do
  @moduledoc """
  `relay_http_push` against a local HTTP server, through its worker
  (`Flezha.Plugin.Provider.HttpSend`): one request per payload in order on one
  connection, the identity headers, a guaranteed output confirmed on a 2xx and
  parked when it is refused or its retries run out, a plain one dropped and
  counted, the three overflow policies, a stop parking what it holds, health
  following the endpoint, and no URL path or header value shown.

  Every ladder here is short: the defaults would make a run of retries take
  minutes.
  """

  use FlezhaTest.Case, fixtures: [:isolated_dlq_dir], async: false

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HttpPush
  alias Flezha.Plugin.Provider.HttpSend
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ProviderPrepare
  alias FlezhaTest.Helper.Telemetry
  alias FlezhaTest.Helper.WebhookServer

  @path "/in/secret-path?token=t"

  setup do
    Telemetry.capture(
      for verb <- [:shipped, :retried, :failed, :overflowed],
          do: [:flezha, :plugin, :http_push, verb]
    )

    rc = "push_#{System.unique_integer([:positive])}"
    cell = {rc, "task", "k1"}
    :ok = ProviderPrepare.stub_outbox_owner(cell)
    {:ok, cell: cell, rc: rc}
  end

  defp start!(server, extra \\ %{}) do
    opts =
      Map.merge(
        %{
          url: "http://127.0.0.1:#{server.port}#{@path}",
          source_plugin_id: "push_#{System.unique_integer([:positive])}",
          subscribed_extensions: [:on_output],
          retry_backoff_ms: 1,
          retry_max_backoff_ms: 5,
          timeout_ms: 2_000
        },
        extra
      )

    {:ok, opts, pid} = ProviderPrepare.prepare_and_activate(HttpPush, opts)
    ProviderPrepare.stop_on_exit(HttpPush, pid)
    {opts, pid}
  end

  defp out(opts, body, metadata \\ %{"channel" => "out"}),
    do: HttpPush.on_output(%Ctx.IO{message: body, metadata: metadata}, opts)

  defp guaranteed(cell, seq, extra \\ %{}), do: ProviderPrepare.guaranteed_metadata(cell, seq, extra)

  defp header(headers, name) do
    Enum.find_value(String.split(headers, "\n"), fn line ->
      case String.split(line, ": ", parts: 2) do
        [key, value] -> if String.downcase(key) == name, do: value
        _ -> nil
      end
    end)
  end

  defp dead_letters(rc), do: DlqStore.list(rc)

  defp error_of(entry), do: Jason.decode!(entry["error"])

  # Bodies the server received, each retry of one body read as that body once.
  defp received_bodies(until) do
    receive do
      {:request, _line, _headers, ^until} -> [until]
      {:request, _line, _headers, body} -> [body | received_bodies(until)]
    after
      5_000 -> flunk("#{inspect(until)} never arrived")
    end
  end

  test "each output is one request, in order, on one connection, with its identity" do
    server = WebhookServer.start([200])
    {opts, _pid} = start!(server)

    for n <- 1..3 do
      metadata = %{
        "channel" => "out",
        "flz.key" => "k#{n}",
        "flz.cell" => "c/t/к",
        "flz.seq" => "#{n}.0",
        "flz.epoch" => "2"
      }

      out(opts, "body#{n}", metadata)
    end

    for n <- 1..3 do
      assert_receive {:request, line, headers, body}, 5_000
      assert line == "POST #{@path} HTTP/1.1"
      assert body == "body#{n}"
      assert header(headers, "content-type") == "application/octet-stream"
      assert header(headers, "flz-key") == "k#{n}"
      assert header(headers, "flz-seq") == "#{n}.0"
      assert header(headers, "flz-epoch") == "2"
      assert header(headers, "flz-cell") == "c/t/%D0%BA"
      assert header(headers, "idempotency-key") == "c/t/%D0%BA/#{n}.0/2"
    end

    assert WebhookServer.connections(server) == 1
    assert_receive {:telemetry, [:flezha, :plugin, :http_push, :shipped], %{count: 1}, _}
  end

  test "PUT and a content type are sent as configured" do
    server = WebhookServer.start([200])
    {opts, _pid} = start!(server, %{method: "PUT", content_type: "application/json"})
    out(opts, ~s({"a":1}))

    assert_receive {:request, "PUT " <> _, headers, ~s({"a":1})}, 5_000
    assert header(headers, "content-type") == "application/json"
  end

  test "a channel the plugin does not send is not sent" do
    server = WebhookServer.start([200])
    {opts, _pid} = start!(server, %{channel: "alerts"})

    out(opts, "price", %{"channel" => "prices"})
    out(opts, "alert", %{"channel" => "alerts"})

    assert_receive {:request, _, _, "alert"}, 5_000
    refute_received {:request, _, _, "price"}
  end

  test "a retried guaranteed output keeps its idempotency-key and is confirmed",
       %{cell: cell} do
    server = WebhookServer.start([503, 200])
    {opts, _pid} = start!(server)
    out(opts, "v1", guaranteed(cell, 1))

    assert_receive {:request, _, first, "v1"}, 5_000
    assert_receive {:request, _, second, "v1"}, 5_000
    assert header(first, "idempotency-key") == header(second, "idempotency-key")
    assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 2_000)
    assert_receive {:telemetry, [:flezha, :plugin, :http_push, :retried], _, %{reason: 503}}
  end

  test "a 422 is sent once and parked as http_push.rejected", %{cell: cell, rc: rc} do
    server = WebhookServer.start([422])
    {opts, _pid} = start!(server)

    log =
      capture_log(fn ->
        out(opts, "v1", guaranteed(cell, 1))
        assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 5_000)
      end)

    assert_received {:request, _, _, "v1"}
    refute_received {:request, _, _, _}
    assert [entry] = dead_letters(rc)
    assert %{"code" => "http_push.rejected", "details" => %{"status" => 422}} = error_of(entry)
    assert entry["attempts"] == 1
    assert log =~ "parked undeliverable output"
    refute log =~ "secret-path"
  end

  test "a redirect is not followed and parks the output", %{cell: cell, rc: rc} do
    elsewhere = WebhookServer.start([200])
    location = "http://127.0.0.1:#{elsewhere.port}/stolen"
    server = WebhookServer.start([{307, [{"location", location}]}])
    {opts, _pid} = start!(server)

    capture_log(fn ->
      out(opts, "v1", guaranteed(cell, 1))
      assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 5_000)
    end)

    assert [entry] = dead_letters(rc)
    assert %{"code" => "http_push.rejected", "details" => %{"status" => 307}} = error_of(entry)
    assert WebhookServer.connections(elsewhere) == 0
  end

  test "a retry-after in seconds is waited out", %{cell: cell} do
    server = WebhookServer.start([{503, [{"retry-after", "1"}]}, 200])
    {opts, _pid} = start!(server, %{retry_max_backoff_ms: 2_000})
    out(opts, "v1", guaranteed(cell, 1))

    assert_receive {:request, _, _, "v1"}, 5_000
    first = System.monotonic_time(:millisecond)
    assert_receive {:request, _, _, "v1"}, 5_000
    assert System.monotonic_time(:millisecond) - first >= 1_000
    assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 2_000)
  end

  test "retries that run out park the output as http_push.exhausted", %{cell: cell, rc: rc} do
    server = WebhookServer.start([503])
    {opts, _pid} = start!(server, %{retries: 2})

    capture_log(fn ->
      out(opts, "v1", guaranteed(cell, 1))
      assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 5_000)
    end)

    for _ <- 1..3, do: assert_received({:request, _, _, "v1"})
    refute_received {:request, _, _, _}
    assert [entry] = dead_letters(rc)
    assert %{"code" => "http_push.exhausted", "details" => %{"reason" => 503}} = error_of(entry)
    assert entry["attempts"] == 3
  end

  test "a guaranteed output whose time budget runs out is parked", %{cell: cell, rc: rc} do
    server = WebhookServer.start([503])

    {opts, _pid} =
      start!(server, %{
        retries: 100,
        retry_backoff_ms: 50,
        retry_max_backoff_ms: 50,
        delivery_timeout_ms: 300
      })

    capture_log(fn ->
      out(opts, "v1", guaranteed(cell, 1))
      assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 5_000)
    end)

    assert [entry] = dead_letters(rc)
    assert entry["attempts"] < 100
    assert %{"code" => "http_push.exhausted"} = error_of(entry)
  end

  test "plain outputs to a failing endpoint are queued at once, dropped and counted once each" do
    server = WebhookServer.start([500])
    {opts, pid} = start!(server, %{retries: 1})
    restore = NodeTestHelper.override_log_level(:warning)

    log =
      capture_log(fn ->
        for n <- 1..100, do: assert({:ok, _} = out(opts, "p#{n}"))
        assert wait_until(fn -> HttpSend.stats(pid)[:dropped] == 100 end, 10_000)
      end)

    restore.()
    assert [_one] = Regex.scan(~r/dropped a request/, log)
    assert log =~ "127.0.0.1"
    assert log =~ "500"
    refute log =~ "secret-path"
    refute log =~ "token=t"
  end

  test "a dispatch does not wait on an endpoint that never answers" do
    server = WebhookServer.start([:hang])
    {opts, pid} = start!(server, %{timeout_ms: 60_000})

    for n <- 1..100, do: out(opts, "p#{n}")

    assert_receive {:request, _, _, "p1"}, 5_000
    assert %{queued: 99, in_flight: 1} = HttpSend.stats(pid)
  end

  test "max_in_flight 2 sends two at once on two connections" do
    server = WebhookServer.start([:hang])
    {opts, _pid} = start!(server, %{max_in_flight: 2, timeout_ms: 60_000})
    out(opts, "a")
    out(opts, "b")

    assert_receive {:request, _, _, "a"}, 5_000
    assert_receive {:request, _, _, "b"}, 5_000
    assert WebhookServer.connections(server) == 2
  end

  describe "a full queue" do
    # The first payload stays in flight, failing every try, until `set/2` lets
    # it through; what is queued behind it meets the policy.
    defp stalled(policy, extra \\ %{}) do
      server = WebhookServer.start([503])

      {opts, pid} =
        start!(
          server,
          Map.merge(
            %{
              queue_max: 10,
              queue_overflow: policy,
              retries: 100,
              retry_backoff_ms: 20,
              retry_max_backoff_ms: 20,
              delivery_timeout_ms: 60_000
            },
            extra
          )
        )

      out(opts, "0")
      assert_receive {:request, _, _, "0"}, 5_000
      {server, opts, pid}
    end

    test "drop_oldest keeps the last ten" do
      {server, opts, pid} = stalled(:drop_oldest)
      for n <- 1..29, do: out(opts, "#{n}")

      assert %{queued: 10, overflowed: 19} = HttpSend.stats(pid)
      WebhookServer.set(server, [200])

      assert Enum.dedup(received_bodies("29")) == ["0" | Enum.map(20..29, &"#{&1}")]

      assert_received {:telemetry, [:flezha, :plugin, :http_push, :overflowed], _,
                       %{policy: :drop_oldest}}
    end

    test "drop_newest keeps the first ten" do
      {server, opts, pid} = stalled(:drop_newest)
      for n <- 1..29, do: out(opts, "#{n}")

      assert %{queued: 10, overflowed: 19} = HttpSend.stats(pid)
      WebhookServer.set(server, [200])
      assert "10" in received_bodies("10")
      refute_receive {:request, _, _, "11"}, 300
    end

    test "drop_oldest parks a guaranteed output only when no plain one is left",
         %{cell: cell, rc: rc} do
      {server, opts, pid} = stalled(:drop_oldest, %{queue_max: 2})

      capture_log(fn ->
        out(opts, "g1", guaranteed(cell, 1))
        out(opts, "plain")
        out(opts, "g2", guaranteed(cell, 2))
        assert %{queued: 2, overflowed: 1} = HttpSend.stats(pid)
        out(opts, "g3", guaranteed(cell, 3))
        assert %{queued: 2, overflowed: 2} = HttpSend.stats(pid)
        assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 5_000)
      end)

      assert [entry] = dead_letters(rc)
      assert %{"details" => %{"reason" => "queue_full"}} = error_of(entry)
      WebhookServer.set(server, [200])
      assert received_bodies("g3") |> Enum.dedup() |> Enum.take(-2) == ["g2", "g3"]
      assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 2}, {:cursor, 3}], 5_000)
    end

    test "drop_nothing makes the dispatch wait until there is room" do
      {server, opts, pid} =
        stalled(:drop_nothing, %{queue_max: 2, queue_block_timeout_ms: 30_000})

      out(opts, "1")
      out(opts, "2")
      assert %{queued: 2} = HttpSend.stats(pid)

      test = self()
      spawn(fn -> send(test, {:dispatched, out(opts, "3")}) end)
      assert wait_until(fn -> HttpSend.stats(pid)[:waiting] == 1 end, 2_000)
      refute_received {:dispatched, _}

      WebhookServer.set(server, [200])
      assert_receive {:dispatched, {:ok, _}}, 5_000
      assert "3" in received_bodies("3")
      assert %{overflowed: 0} = HttpSend.stats(pid)
    end

    test "drop_nothing overflows a payload that waited past its timeout",
         %{cell: cell, rc: rc} do
      {_server, opts, pid} = stalled(:drop_nothing, %{queue_max: 1, queue_block_timeout_ms: 200})
      out(opts, "1")

      capture_log(fn ->
        started = System.monotonic_time(:millisecond)
        out(opts, "g", guaranteed(cell, 1))
        assert System.monotonic_time(:millisecond) - started >= 200
        assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 5_000)
      end)

      assert %{overflowed: 1, queued: 1} = HttpSend.stats(pid)
      assert [entry] = dead_letters(rc)
      assert %{"details" => %{"reason" => "queue_full"}} = error_of(entry)
    end
  end

  test "a stop parks the guaranteed outputs it holds", %{cell: cell, rc: rc} do
    server = WebhookServer.start([:hang])
    {opts, pid} = start!(server, %{timeout_ms: 60_000})
    out(opts, "g1", guaranteed(cell, 1))
    out(opts, "g2", guaranteed(cell, 2))
    out(opts, "plain")
    assert_receive {:request, _, _, "g1"}, 5_000
    assert %{queued: 2} = HttpSend.stats(pid)

    capture_log(fn ->
      ProviderPrepare.stop(HttpPush, pid)
      assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}, {:cursor, 2}], 5_000)
    end)

    assert entries = dead_letters(rc)
    assert length(entries) == 2
    assert Enum.all?(entries, &(error_of(&1)["details"] == %{"reason" => "stopped"}))
  end

  test "with no worker running a guaranteed output is held and a plain one counted",
       %{cell: cell} do
    {:ok, opts} =
      ProviderPrepare.prepare(HttpPush, %{
        url: "http://127.0.0.1:1/in",
        source_plugin_id: "absent"
      })

    out(opts, "plain")

    assert_receive {:telemetry, [:flezha, :plugin, :http_push, :failed], _,
                    %{reason: :not_running}}

    out(opts, "g", guaranteed(cell, 1))
    assert_receive {:"$gen_cast", {:outbox_hold, {:cursor, 1}}}
    refute_receive {:telemetry, [:flezha, :plugin, :http_push, :failed], _, _}, 100
  end

  test "health turns after three failed requests and back on a 2xx, and shows no secret" do
    server = WebhookServer.start([500])
    secret = "s3cr3t-#{System.unique_integer([:positive])}"

    {opts, pid} =
      start!(server, %{
        retries: 100,
        retry_backoff_ms: 50,
        retry_max_backoff_ms: 50,
        headers: ~s({"authorization": "#{secret}"})
      })

    out(opts, "p")
    assert wait_until(fn -> HttpPush.health(opts) == {:unhealthy, :send_stalled} end, 5_000)
    assert HttpSend.stats(pid).failures >= 3

    status = inspect(:sys.get_status(pid))
    refute status =~ secret
    refute status =~ "secret-path"
    refute status =~ "token=t"

    WebhookServer.set(server, [200])
    assert wait_until(fn -> HttpPush.health(opts) == :healthy end, 5_000)
  end
end

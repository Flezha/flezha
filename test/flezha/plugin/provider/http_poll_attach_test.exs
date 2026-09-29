defmodule Flezha.Plugin.Provider.HttpPollAttachTest do
  @moduledoc """
  Attach-time contract for `Plugin.Provider.HttpPoll`: what the declaration and
  `init_provider/1` refuse before a poller exists, what `build/1` refuses on the
  CLI path, and the partition placeholders a URL and header set may carry.
  """

  # async: false: global telemetry handlers.

  use FlezhaTest.HttpPollCase, async: false

  alias Flezha.Plugin.Provider.HttpPoll
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  # Preparation as the extension table performs it: the declaration normalises
  # the options, then the provider interprets them. Named apart from the case's
  # own `prepare/2`, which starts from a live origin.
  defp attach(opts) do
    with {:ok, opts} <- ProviderOptions.normalise_opts(HttpPoll, opts),
         do: HttpPoll.init_provider(opts)
  end

  describe "attach-time validation" do
    test "rejects a non-http scheme, a hostless url, and embedded credentials" do
      assert {:error, {:url_scheme_unsupported, "http_poll", :url, _}} =
               attach(%{url: "ftp://x/y", channel: @channel})

      assert {:error, {:url_scheme_unsupported, "http_poll", :url, _}} =
               attach(%{url: "/relative", channel: @channel})

      assert {:error, {:url_without_host, "http_poll", :url, _}} =
               attach(%{url: "http://", channel: @channel})

      assert {:error, {:url_has_credentials, "http_poll", :url, :headers} = reason} =
               attach(%{url: "https://u:hunter2@api/c", channel: @channel})

      refute inspect(reason) =~ "hunter2"
    end

    test "rejects a missing url or channel" do
      assert {:error, {:options_required, "http_poll", [:url]}} = attach(%{channel: @channel})

      # Either spelling names the entry channel, so the refusal for naming
      # neither names both rather than one row.
      assert {:error, {:entry_channel_required, "http_poll"}} = attach(%{url: "http://x/y"})

      assert {:error, _} = attach(%{url: "http://x/y", channel: ""})
      assert {:ok, _} = attach(%{url: "http://x/y", channel_in: @channel})
    end

    test "accepts an explicit idle window and rejects a malformed one" do
      assert {:ok, opts} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 idle_after_ms: 5_000
               })

      assert opts.idle_after_ms == 5_000

      assert {:error, {:option_below_minimum, :idle_after_ms, -1, 0}} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 idle_after_ms: -1
               })

      assert {:error, {:invalid_option_value, :idle_after_ms, "soon"}} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 idle_after_ms: "soon"
               })
    end

    test "rejects an interval below the minimum and a non-positive tunable" do
      assert {:error, {:option_below_minimum, :interval_ms, 500, 1000}} =
               attach(%{url: "http://x/y", channel: @channel, interval_ms: 500})

      assert {:error, {:option_below_minimum, :timeout_ms, 0, 1}} =
               attach(%{url: "http://x/y", channel: @channel, timeout_ms: 0})
    end

    test "rejects a timeout that can outlast its own interval" do
      assert {:error, {:timeout_not_below_interval, 10_000, 1_000}} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 interval_ms: 1_000,
                 timeout_ms: 10_000
               })
    end

    test "rejects a body cap above the ceiling and non-numeric tunables" do
      assert {:error, {:option_above_maximum, :max_body_bytes, 1_073_741_824, 67_108_864}} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 max_body_bytes: 1_073_741_824
               })

      assert {:error, {:invalid_option_value, :interval_ms, "soon"}} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 interval_ms: "soon"
               })
    end

    test "rejects malformed headers and accepts a JSON object" do
      assert {:error, {:invalid_option_value, :headers, "nope"}} =
               attach(%{url: "http://x/y", channel: @channel, headers: "nope"})

      assert {:error, {:headers_invalid, "http_poll", :headers}} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 headers: %{"a" => %{"nested" => true}}
               })

      assert {:ok, opts} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 headers: ~s({"authorization":"Bearer t"})
               })

      assert opts.headers == %{"authorization" => "Bearer t"}
    end

    test "refuses an unknown option on the path a user actually uses" do
      # `SpecDecoder` hands a `--plugins` spec straight to `init_provider/1`
      # without ever building the provider struct, so a check that lived only
      # in `build/1` would never run in production.
      assert {:error, {:unknown_options, "http_poll", ["method"]}} =
               attach(%{url: "http://x/y", channel: @channel, method: "POST"})

      assert {:error, {:unknown_options, "http_poll", ["method"]}} =
               HttpPoll.build(%{"url" => "http://x/y", "channel" => @channel, "method" => "POST"})
    end

    test "accepts the identity the host merges in" do
      assert {:ok, _} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 source_namespace: "default",
                 source_operator_id: "detector"
               })
    end

    test "build/1 coerces string tunables" do
      assert {:ok, spec} =
               HttpPoll.build(%{
                 "url" => "http://x/y",
                 "channel" => @channel,
                 "interval_ms" => "90000"
               })

      assert spec.interval_ms == 90_000
      assert spec.url == "http://x/y"
    end

    test "the default plugin_id strips credentials and query" do
      assert HttpPoll.redact("https://api.internal/cfg?token=s3cret") ==
               "https://api.internal/cfg"

      refute HttpPoll.redact("https://u:p@api.internal/cfg") =~ "p@"
    end

    test "static headers reach the origin" do
      origin = start_origin(body: "cfg-v1")

      start_poller(origin, %{headers: %{"x-flezha-test" => "yes"}})
      assert {"cfg-v1", _} = take_ingest()

      assert last_request_headers(origin) =~ "x-flezha-test: yes"
    end
  end

  # ---------- poller helpers ----------

  # A worker that answers the health probe with a chosen verdict. The idle
  # window is a clock, and driving a real poller past it would mean sleeping
  # through real intervals.

  describe "partition placeholders" do
    test "are rendered once at init, so the poll worker needs no partition knowledge" do
      assert {:ok, opts} =
               attach(%{
                 url:
                   "https://api.example/trades?shard={{partition_index}}&of={{partition_count}}",
                 channel: @channel,
                 partition_index: 1,
                 partition_count: 3,
                 key_group_from: 85,
                 key_group_to: 169
               })

      assert opts.url == "https://api.example/trades?shard=1&of=3"
    end

    test "a placeholder with no partition identity is refused, naming it" do
      assert {:error, {:partition_placeholder_unfilled, :url, "{{partition_index}}"}} =
               attach(%{
                 url: "https://api.example/trades?shard={{partition_index}}",
                 channel: @channel
               })
    end

    test "validation sees the rendered url, not the template" do
      # The substitution lands in the scheme, so only a check that runs AFTER
      # rendering can catch it — at init, not at request time.
      assert {:error, {:url_scheme_unsupported, "http_poll", :url, _}} =
               attach(%{
                 url: "{{partition_index}}://api.example/x",
                 channel: @channel,
                 partition_index: 1,
                 partition_count: 3
               })
    end

    test "a url without placeholders is untouched by a partitioned plugin" do
      assert {:ok, opts} =
               attach(%{
                 url: "https://api.example/trades",
                 channel: @channel,
                 partition_index: 2,
                 partition_count: 3
               })

      assert opts.url == "https://api.example/trades"
    end
  end
end

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
      assert {:error, message} = attach(%{url: "ftp://x/y", channel: @channel})
      assert message =~ "http or https"

      assert {:error, message} = attach(%{url: "/relative", channel: @channel})
      assert message =~ "http or https"

      assert {:error, message} = attach(%{url: "http://", channel: @channel})
      assert message =~ "no host"

      assert {:error, message} =
               attach(%{url: "https://u:hunter2@api/c", channel: @channel})

      assert message =~ "must not embed credentials"
      refute message =~ "hunter2"
    end

    test "rejects a missing url or channel" do
      assert {:error, message} = attach(%{channel: @channel})
      assert message =~ ":url"

      # Either spelling names the entry channel, so the refusal for naming
      # neither names both rather than one row.
      assert {:error, message} = attach(%{url: "http://x/y"})
      assert message =~ "requires an entry channel"

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

      assert {:error, message} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 idle_after_ms: -1
               })

      assert message =~ "idle_after_ms"

      assert {:error, message} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 idle_after_ms: "soon"
               })

      assert message =~ "idle_after_ms"
    end

    test "rejects an interval below the minimum and a non-positive tunable" do
      assert {:error, message} =
               attach(%{url: "http://x/y", channel: @channel, interval_ms: 500})

      assert message =~ "is below the minimum 1000"

      assert {:error, message} =
               attach(%{url: "http://x/y", channel: @channel, timeout_ms: 0})

      assert message =~ "is below the minimum 1"
    end

    test "rejects a timeout that can outlast its own interval" do
      assert {:error, message} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 interval_ms: 1_000,
                 timeout_ms: 10_000
               })

      assert message =~ "must be below interval_ms"
    end

    test "rejects a body cap above the ceiling and non-numeric tunables" do
      assert {:error, message} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 max_body_bytes: 1_073_741_824
               })

      assert message =~ "is above the maximum 67108864"

      assert {:error, message} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 interval_ms: "soon"
               })

      assert message =~ "interval_ms"
    end

    test "rejects malformed headers and accepts a JSON object" do
      assert {:error, message} =
               attach(%{url: "http://x/y", channel: @channel, headers: "nope"})

      assert message =~ "Invalid value for headers"

      assert {:error, message} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 headers: %{"a" => %{"nested" => true}}
               })

      assert message =~ "scalar"

      assert {:ok, opts} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 headers: ~s({"authorization":"Bearer t"})
               })

      assert opts.headers == %{"authorization" => "Bearer t"}
    end

    test "refuses an unknown option on the path an operator actually uses" do
      # `SpecDecoder` hands a `--plugins` spec straight to `init_provider/1`
      # without ever building the provider struct, so a check that lived only
      # in `build/1` would never run in production.
      assert {:error, message} =
               attach(%{url: "http://x/y", channel: @channel, method: "POST"})

      assert message =~ "unknown option(s) for http_poll: method"

      assert {:error, message} =
               HttpPoll.build(%{"url" => "http://x/y", "channel" => @channel, "method" => "POST"})

      assert message =~ "unknown option(s) for http_poll: method"
    end

    test "accepts the identity the host merges in" do
      assert {:ok, _} =
               attach(%{
                 url: "http://x/y",
                 channel: @channel,
                 source_namespace: "default",
                 source_operative_id: "detector"
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
      assert {:error, message} =
               attach(%{
                 url: "https://api.example/trades?shard={{partition_index}}",
                 channel: @channel
               })

      assert message =~ "{{partition_index}}"
      assert message =~ "no partition identity"
    end

    test "validation sees the rendered url, not the template" do
      # The substitution lands in the scheme, so only a check that runs AFTER
      # rendering can catch it — at init, not at request time.
      assert {:error, message} =
               attach(%{
                 url: "{{partition_index}}://api.example/x",
                 channel: @channel,
                 partition_index: 1,
                 partition_count: 3
               })

      assert message =~ "http or https"
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

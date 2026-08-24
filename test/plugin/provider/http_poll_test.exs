defmodule Flezha.Plugin.Provider.HttpPollTest do
  @moduledoc """
  Contract for `Plugin.Provider.HttpPoll` per `add-http-poll-source-provider`.

  Driven against a real `:gen_tcp` origin so `:httpc`, the conditional GET, the
  streaming size cap, and the status classification are exercised rather than
  mocked. The host operative is stood in for by the test process: injection is
  a `{:ingest, body, metadata}` **call**, so the test answers it explicitly and
  can hand back any verdict a real Overwatch would — including the refusals
  that must leave the content undelivered and re-offered.

  Polls after the first are driven by sending `:poll`, with `:interval_ms`
  parked at a minute, so each scenario is a deterministic sequence rather than
  a race against a timer.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.HttpPoll

  @channel "config_in"

  @catalog Host.build_catalog(
             [{:on_terminate, :fold, Ctx.Terminate}],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  setup do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach_many(
      ref,
      [
        [:flezha, :plugin, :http_poll, :injected],
        [:flezha, :plugin, :http_poll, :unchanged],
        [:flezha, :plugin, :http_poll, :failed]
      ],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:poll_event, List.last(event), measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(ref) end)
    :ok
  end

  describe "hash gating" do
    test "a steady endpoint injects the seed and nothing more" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin)

      assert {"cfg-v1", %{"channel" => @channel}} = take_ingest()
      assert_receive {:poll_event, :injected, _, %{seed: true}}

      for _ <- 1..3 do
        send(poller, :poll)
        assert_receive {:poll_event, :unchanged, _, _}, 2_000
      end

      refute_ingest()
    end

    test "changed content injects exactly once" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin)
      assert {"cfg-v1", _} = take_ingest()

      set(origin, body: "cfg-v2")
      send(poller, :poll)

      assert {"cfg-v2", %{"channel" => @channel}} = take_ingest()
      assert_receive {:poll_event, :injected, _, %{seed: false}}

      send(poller, :poll)
      assert_receive {:poll_event, :unchanged, _, _}, 2_000
      refute_ingest()
    end

    test "a re-applied plugin seeds again on unchanged content" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin)
      assert {"cfg-v1", _} = take_ingest()

      send(poller, :poll)
      assert_receive {:poll_event, :unchanged, _, _}, 2_000

      # Handover / plugin restart: `init_provider` + `activate` run again and
      # the fresh poller carries no hash, so it re-seeds the rejoining
      # operative with content that never changed.
      _second = start_poller(origin)

      assert {"cfg-v1", %{"channel" => @channel}} = take_ingest()
      assert_receive {:poll_event, :injected, _, %{seed: true}}
    end

    test "an ETag 304 is a successful unchanged poll" do
      origin = start_origin(body: "cfg-v1", etag: "\"abc\"")
      poller = start_poller(origin)
      assert {"cfg-v1", _} = take_ingest()

      send(poller, :poll)
      assert_receive {:poll_event, :unchanged, _, _}, 2_000

      assert conditional_requests(origin) >= 1
      refute_ingest()
    end
  end

  describe "delivery is confirmed before the hash advances" do
    test "a refused ingest re-offers the same content on the next poll" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin)

      assert {"cfg-v1", _} = take_ingest({:error, :refused})

      assert_receive {:poll_event, :failed, %{consecutive_failures: 1},
                      %{reason: {:ingest, :refused}}},
                     2_000

      # Nothing about the endpoint changed, but the operative never took the
      # payload — so the poller must offer it again rather than read it as
      # delivered and go silent forever.
      send(poller, :poll)
      assert {"cfg-v1", _} = take_ingest()
      assert_receive {:poll_event, :injected, _, %{seed: true}}
    end

    test "an absorbed or suspended verdict counts as delivered" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin)

      assert {"cfg-v1", _} = take_ingest({:ok, :absorbed})
      assert_receive {:poll_event, :injected, _, _}, 2_000

      send(poller, :poll)
      assert_receive {:poll_event, :unchanged, _, _}, 2_000
      refute_ingest()
    end
  end

  describe "failures" do
    test "outage flips health unhealthy and recovery injects the missed change" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin, %{failure_threshold: 3})
      assert {"cfg-v1", _} = take_ingest()

      set(origin, status: 503, body: "nope")

      for expected <- 1..3 do
        send(poller, :poll)

        assert_receive {:poll_event, :failed, %{consecutive_failures: ^expected},
                        %{reason: {:http_status, 503}}},
                       2_000
      end

      assert {:unhealthy, {:poll_failing, {:http_status, 503}}} =
               HttpPoll.health(%{owned_pid: poller, failure_threshold: 3})

      refute_ingest()

      set(origin, status: 200, body: "cfg-v2")
      send(poller, :poll)

      assert {"cfg-v2", %{"channel" => @channel}} = take_ingest()
      assert :healthy = HttpPoll.health(%{owned_pid: poller, failure_threshold: 3})
      refute_ingest()
    end

    test "a body over the cap fails every poll and never resets via a 304" do
      # The cap refusal must not commit the validator: if it did, the next
      # request would be conditional, the server would answer 304, that would
      # read as an unchanged poll, and the failure run would reset — leaving a
      # plugin that is permanently healthy, permanently silent, and has never
      # delivered a seed.
      origin = start_origin(body: String.duplicate("x", 4_000), etag: "\"big\"")
      poller = start_poller(origin, %{max_body_bytes: 64, failure_threshold: 3})

      for expected <- 1..3 do
        assert_receive {:poll_event, :failed, %{consecutive_failures: ^expected},
                        %{reason: {:oversized_body, _seen, 64}}},
                       2_000

        send(poller, :poll)
      end

      assert {:unhealthy, {:poll_failing, {:oversized_body, _, 64}}} =
               HttpPoll.health(%{owned_pid: poller, failure_threshold: 3})

      refute_ingest()
    end

    test "a non-UTF-8 body fails every poll and never resets via a 304" do
      origin = start_origin(body: <<0xFF, 0xFE, 0x00>>, etag: "\"bin\"")
      poller = start_poller(origin, %{failure_threshold: 2})

      for expected <- 1..2 do
        assert_receive {:poll_event, :failed, %{consecutive_failures: ^expected},
                        %{reason: {:invalid_utf8, 3}}},
                       2_000

        send(poller, :poll)
      end

      assert {:unhealthy, {:poll_failing, {:invalid_utf8, 3}}} =
               HttpPoll.health(%{owned_pid: poller, failure_threshold: 2})

      refute_ingest()
    end

    test "a redirect is not followed — the configured host is the only one read" do
      secret = start_origin(body: "SHOULD-NOT-BE-INJECTED")
      origin = start_origin(status: 302, location: "http://127.0.0.1:#{secret.port}/meta")

      start_poller(origin)

      assert_receive {:poll_event, :failed, _, %{reason: {:http_status, 302}}}, 2_000
      refute_ingest()
      assert requests(secret) == 0
    end

    test "an unreachable endpoint counts toward health and keeps the last content" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin, %{failure_threshold: 1})
      assert {"cfg-v1", _} = take_ingest()

      stop_origin(origin)
      send(poller, :poll)
      assert_receive {:poll_event, :failed, %{consecutive_failures: 1}, _}, 2_000

      assert {:unhealthy, {:poll_failing, _}} =
               HttpPoll.health(%{owned_pid: poller, failure_threshold: 1})

      refute_ingest()
    end

    test "an https endpoint is attempted with verification and fails cleanly when absent" do
      # Exercises the TLS branch end to end (`:public_key.cacerts_get/0` must
      # resolve) without needing a certificate: nothing is listening, so the
      # poll fails at connect rather than at option construction.
      origin = start_origin(body: "unused")
      port = origin.port
      stop_origin(origin)

      {:ok, opts} =
        HttpPoll.init_provider(%{
          url: "https://127.0.0.1:#{port}/config",
          channel: @channel,
          interval_ms: 60_000,
          timeout_ms: 2_000
        })

      {:ok, poller} = HttpPoll.activate(opts)
      kill_on_exit(poller)

      assert_receive {:poll_event, :failed, _, %{reason: reason}}, 5_000
      refute match?({:no_ca_store, _}, reason)
      refute_ingest()
    end

    test "health reports a dead or absent poller" do
      assert {:unhealthy, :no_poller} = HttpPoll.health(%{})

      dead = spawn(fn -> :ok end)
      ref = Process.monitor(dead)
      assert_receive {:DOWN, ^ref, :process, ^dead, _}

      assert {:unhealthy, :poller_dead} = HttpPoll.health(%{owned_pid: dead})
    end
  end

  describe "lifecycle" do
    test "init_provider performs no upstream I/O" do
      origin = start_origin(body: "cfg-v1")

      assert {:ok, opts} = HttpPoll.init_provider(base_opts(origin))
      Process.sleep(60)

      assert requests(origin) == 0
      assert opts.overwatcher_pid == self()
      assert opts.plugin_id == opts.url
    end

    test "activate returns without waiting on the request" do
      origin = start_origin(body: "cfg-v1", delay_ms: 250)
      {:ok, opts} = HttpPoll.init_provider(base_opts(origin))

      {elapsed_us, {:ok, poller}} = :timer.tc(fn -> HttpPoll.activate(opts) end)
      kill_on_exit(poller)

      assert elapsed_us < 100_000

      # The host keeps serving its own mailbox while that request is in
      # flight — the wait belongs to the fetch child, not to the poller and
      # certainly not to the dispatch path.
      Enum.each(1..50, fn n -> send(self(), {:host_work, n}) end)
      Enum.each(1..50, fn n -> assert_received {:host_work, ^n} end)

      assert {:ok, {0, nil, _last_settled_at}} = probe(poller)
      assert {"cfg-v1", _} = take_ingest(:ok, 5_000)
    end

    test "the health probe answers while a request is outstanding" do
      origin = start_origin(body: "cfg-v1", delay_ms: 250)
      poller = start_poller(origin)

      # Mid-flight: the request is blocking its own child, not the worker, so
      # the verdict is available for exactly the slow endpoint it must report.
      assert {:ok, {0, nil, _last_settled_at}} = probe(poller)
      assert :healthy = HttpPoll.health(%{owned_pid: poller, failure_threshold: 3})

      assert {"cfg-v1", _} = take_ingest(:ok, 5_000)
    end

    test "the poller stops when the host operative goes down" do
      origin = start_origin(body: "cfg-v1")
      parent = self()

      host =
        spawn(fn ->
          {:ok, opts} = HttpPoll.init_provider(base_opts(origin))
          {:ok, poller} = HttpPoll.activate(opts)
          send(parent, {:poller, poller})
          receive do: (:stop -> :ok)
        end)

      assert_receive {:poller, poller}, 2_000
      ref = Process.monitor(poller)

      send(host, :stop)
      assert_receive {:DOWN, ^ref, :process, ^poller, :normal}, 2_000
    end

    test "subscribes, activates, and is stopped by the host on unsubscribe" do
      origin = start_origin(body: "cfg-v1")

      plugin = %Plugin{
        id: "cfg",
        provider: HttpPoll,
        opts: base_opts(origin),
        subscriptions: [{:on_terminate, []}],
        namespace: "default",
        operative_id: "detector"
      }

      assert {:ok, table} = Table.subscribe(Table.new(@catalog), "cfg", plugin)
      assert Table.phase(table, "cfg") == :prepared

      {table, _} = Table.bulk_activate(table)
      assert Table.phase(table, "cfg") == :running

      assert {"cfg-v1", %{"channel" => @channel}} = take_ingest()

      poller = owned_pid(table, "cfg")
      assert is_pid(poller) and Process.alive?(poller)
      assert Table.health(table) == %{"cfg" => :healthy}

      ref = Process.monitor(poller)
      Table.unsubscribe(table, "cfg")
      assert_receive {:DOWN, ^ref, :process, ^poller, _}, 2_000
    end
  end

  describe "a poller that stopped settling polls says so" do
    test "a worker idle past the window is unhealthy with a zero failure run" do
      poller = stub_worker(failures: 0, last_error: nil, settled_ago_ms: 400)

      assert {:unhealthy, {:poll_idle, age}} =
               HttpPoll.health(%{owned_pid: poller, failure_threshold: 3, idle_after_ms: 200})

      assert age >= 400
    end

    test "unchanged content on cadence stays healthy" do
      # `settle/3` stamps on EVERY outcome, so a content-gated poller that has
      # injected nothing for many intervals is still settling and still healthy.
      poller = stub_worker(failures: 0, last_error: nil, settled_ago_ms: 10)

      assert :healthy =
               HttpPoll.health(%{owned_pid: poller, failure_threshold: 3, idle_after_ms: 200})
    end

    test "a poll in flight inside the window stays healthy" do
      origin = start_origin(body: "cfg-v1", delay_ms: 250)
      poller = start_poller(origin)

      assert :healthy = HttpPoll.health(%{owned_pid: poller, failure_threshold: 3})
      assert {"cfg-v1", _} = take_ingest(:ok, 5_000)
    end

    test "a failure run outranks the idle age" do
      poller = stub_worker(failures: 5, last_error: :econnrefused, settled_ago_ms: 10_000)

      assert {:unhealthy, {:poll_failing, :econnrefused}} =
               HttpPoll.health(%{owned_pid: poller, failure_threshold: 3, idle_after_ms: 200})
    end

    test "the window defaults to three intervals when unset" do
      # 3 x 1_000 ms = 3_000 ms, so a 2 s-old settle is healthy and a 4 s-old
      # one is not, without the option ever being supplied.
      assert :healthy =
               HttpPoll.health(%{
                 owned_pid: stub_worker(settled_ago_ms: 2_000),
                 interval_ms: 1_000
               })

      assert {:unhealthy, {:poll_idle, _}} =
               HttpPoll.health(%{
                 owned_pid: stub_worker(settled_ago_ms: 4_000),
                 interval_ms: 1_000
               })
    end

    test "a wedged worker is reported as uncertain, not as a verdict" do
      wedged = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> if Process.alive?(wedged), do: Process.exit(wedged, :kill) end)

      # A worker that is alive but not answering is neither healthy nor
      # unhealthy. The provider says so rather than swallowing it as `:healthy`;
      # `Extension.Table` is what turns this into telemetry, because it is the
      # layer that knows which plugin the provider belongs to.
      assert {:fail_open, :probe_timeout} =
               HttpPoll.health(%{owned_pid: wedged, failure_threshold: 3})
    end
  end

  describe "attach-time validation" do
    test "rejects a non-http scheme, a hostless url, and embedded credentials" do
      assert {:error, message} = HttpPoll.init_provider(%{url: "ftp://x/y", channel: @channel})
      assert message =~ "http or https"

      assert {:error, message} = HttpPoll.init_provider(%{url: "/relative", channel: @channel})
      assert message =~ "http or https"

      assert {:error, message} = HttpPoll.init_provider(%{url: "http://", channel: @channel})
      assert message =~ "no host"

      assert {:error, message} =
               HttpPoll.init_provider(%{url: "https://u:hunter2@api/c", channel: @channel})

      assert message =~ "must not embed credentials"
      refute message =~ "hunter2"
    end

    test "rejects a missing url or channel" do
      assert {:error, message} = HttpPoll.init_provider(%{channel: @channel})
      assert message =~ ":url"

      assert {:error, message} = HttpPoll.init_provider(%{url: "http://x/y"})
      assert message =~ ":channel"

      assert {:error, _} = HttpPoll.init_provider(%{url: "http://x/y", channel: ""})
    end

    test "accepts an explicit idle window and rejects a malformed one" do
      assert {:ok, opts} =
               HttpPoll.init_provider(%{
                 url: "http://x/y",
                 channel: @channel,
                 idle_after_ms: 5_000
               })

      assert opts.idle_after_ms == 5_000

      assert {:error, message} =
               HttpPoll.init_provider(%{
                 url: "http://x/y",
                 channel: @channel,
                 idle_after_ms: -1
               })

      assert message =~ "idle_after_ms"

      assert {:error, message} =
               HttpPoll.init_provider(%{
                 url: "http://x/y",
                 channel: @channel,
                 idle_after_ms: "soon"
               })

      assert message =~ "idle_after_ms"
    end

    test "rejects an interval below the minimum and a non-positive tunable" do
      assert {:error, message} =
               HttpPoll.init_provider(%{url: "http://x/y", channel: @channel, interval_ms: 500})

      assert message =~ "1000 ms minimum"

      assert {:error, message} =
               HttpPoll.init_provider(%{url: "http://x/y", channel: @channel, timeout_ms: 0})

      assert message =~ "must be positive"
    end

    test "rejects a timeout that can outlast its own interval" do
      assert {:error, message} =
               HttpPoll.init_provider(%{
                 url: "http://x/y",
                 channel: @channel,
                 interval_ms: 1_000,
                 timeout_ms: 10_000
               })

      assert message =~ "must be below interval_ms"
    end

    test "rejects a body cap above the ceiling and non-numeric tunables" do
      assert {:error, message} =
               HttpPoll.init_provider(%{
                 url: "http://x/y",
                 channel: @channel,
                 max_body_bytes: 1_073_741_824
               })

      assert message =~ "ceiling"

      assert {:error, message} =
               HttpPoll.init_provider(%{
                 url: "http://x/y",
                 channel: @channel,
                 interval_ms: "soon"
               })

      assert message =~ "interval_ms"
    end

    test "rejects malformed headers and accepts a JSON object" do
      assert {:error, message} =
               HttpPoll.init_provider(%{url: "http://x/y", channel: @channel, headers: "nope"})

      assert message =~ "JSON object"

      assert {:error, message} =
               HttpPoll.init_provider(%{
                 url: "http://x/y",
                 channel: @channel,
                 headers: %{"a" => %{"nested" => true}}
               })

      assert message =~ "scalar"

      assert {:ok, opts} =
               HttpPoll.init_provider(%{
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
               HttpPoll.init_provider(%{url: "http://x/y", channel: @channel, method: "POST"})

      assert message =~ "unknown option(s) for http_poll: method"

      assert {:error, message} =
               HttpPoll.build(%{"url" => "http://x/y", "channel" => @channel, "method" => "POST"})

      assert message =~ "unknown option(s) for http_poll: method"
    end

    test "accepts the identity the host merges in" do
      assert {:ok, _} =
               HttpPoll.init_provider(%{
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
  defp stub_worker(fields) do
    failures = Keyword.get(fields, :failures, 0)
    last_error = Keyword.get(fields, :last_error, nil)
    settled_ago = Keyword.get(fields, :settled_ago_ms, 0)
    settled_at = :erlang.monotonic_time(:millisecond) - settled_ago

    pid =
      spawn(fn ->
        recv = fn recv ->
          receive do
            {:health_probe, from, ref} ->
              send(from, {ref, {failures, last_error, settled_at}})
              recv.(recv)
          end
        end

        recv.(recv)
      end)

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    pid
  end

  defp base_opts(origin, extra \\ %{}) do
    Map.merge(
      %{
        url: "http://127.0.0.1:#{origin.port}/config",
        channel: @channel,
        interval_ms: 60_000,
        timeout_ms: 2_000
      },
      extra
    )
  end

  defp start_poller(origin, extra \\ %{}) do
    {:ok, opts} = HttpPoll.init_provider(base_opts(origin, extra))
    {:ok, poller} = HttpPoll.activate(opts)
    kill_on_exit(poller)
    poller
  end

  defp probe(poller), do: Flezha.Plugin.Provider.HttpPoll.PollWorker.probe(poller)

  defp kill_on_exit(pid) do
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
  end

  defp owned_pid(table, plugin_id) do
    case Map.get(table.owned_pids, plugin_id) do
      {pid, _ref} -> pid
      _ -> nil
    end
  end

  # The test process IS the host, so it answers the confirming ingest call the
  # way an Overwatch would — including the refusals.
  defp take_ingest(verdict \\ :ok, timeout \\ 2_000) do
    receive do
      {:"$gen_call", from, {:ingest, body, metadata}} ->
        GenServer.reply(from, verdict)
        {body, metadata}
    after
      timeout -> flunk("no ingest call within #{timeout}ms")
    end
  end

  defp refute_ingest(), do: refute_received({:"$gen_call", _from, {:ingest, _, _}})

  # ---------- fake HTTP origin ----------

  defp start_origin(config) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true, backlog: 16])

    {:ok, port} = :inet.port(listen)

    {:ok, agent} =
      Agent.start(fn ->
        Map.merge(
          %{
            status: 200,
            body: "",
            etag: nil,
            location: nil,
            delay_ms: 0,
            requests: 0,
            conditional: 0,
            last: ""
          },
          Map.new(config)
        )
      end)

    acceptor = spawn(fn -> accept_loop(listen, agent) end)
    origin = %{port: port, listen: listen, agent: agent, acceptor: acceptor}

    on_exit(fn -> stop_origin(origin) end)
    origin
  end

  defp stop_origin(origin) do
    if Process.alive?(origin.acceptor), do: Process.exit(origin.acceptor, :kill)
    :gen_tcp.close(origin.listen)
    :ok
  end

  defp set(origin, config), do: Agent.update(origin.agent, &Map.merge(&1, Map.new(config)))
  defp requests(origin), do: Agent.get(origin.agent, & &1.requests)
  defp conditional_requests(origin), do: Agent.get(origin.agent, & &1.conditional)
  defp last_request_headers(origin), do: Agent.get(origin.agent, & &1.last)

  defp accept_loop(listen, agent) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        spawn(fn -> serve(socket, agent) end)
        accept_loop(listen, agent)

      {:error, _closed} ->
        :ok
    end
  end

  defp serve(socket, agent) do
    with {:ok, request} <- read_request(socket) do
      config =
        Agent.get_and_update(agent, fn state ->
          conditional? = String.contains?(String.downcase(request), "if-none-match:")

          state = %{
            state
            | requests: state.requests + 1,
              conditional: state.conditional + if(conditional?, do: 1, else: 0),
              last: request
          }

          {Map.put(state, :conditional?, conditional?), state}
        end)

      if config.delay_ms > 0, do: Process.sleep(config.delay_ms)
      :gen_tcp.send(socket, response(config))
    end

    :gen_tcp.close(socket)
  end

  defp read_request(socket, acc \\ "") do
    if String.contains?(acc, "\r\n\r\n") do
      {:ok, acc}
    else
      case :gen_tcp.recv(socket, 0, 2_000) do
        {:ok, chunk} -> read_request(socket, acc <> chunk)
        {:error, _} -> :error
      end
    end
  end

  # A conditional request whose validator the origin still holds is answered
  # 304, which is what makes the ETag pass-through observable — and what a
  # failure path must never be able to trigger for itself.
  defp response(%{conditional?: true, etag: etag, status: 200}) when is_binary(etag) do
    "HTTP/1.1 304 Not Modified\r\netag: #{etag}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
  end

  defp response(config) do
    optional =
      [
        if(config.etag, do: "etag: #{config.etag}\r\n"),
        if(config.location, do: "location: #{config.location}\r\n")
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join()

    "HTTP/1.1 #{config.status} #{reason_phrase(config.status)}\r\n" <>
      optional <>
      "Content-Type: application/octet-stream\r\n" <>
      "Content-Length: #{byte_size(config.body)}\r\n" <>
      "Connection: close\r\n\r\n" <> config.body
  end

  describe "partition placeholders" do
    test "are rendered once at init, so the poll worker needs no partition knowledge" do
      assert {:ok, opts} =
               HttpPoll.init_provider(%{
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
               HttpPoll.init_provider(%{
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
               HttpPoll.init_provider(%{
                 url: "{{partition_index}}://api.example/x",
                 channel: @channel,
                 partition_index: 1,
                 partition_count: 3
               })

      assert message =~ "http or https"
    end

    test "a url without placeholders is untouched by a partitioned plugin" do
      assert {:ok, opts} =
               HttpPoll.init_provider(%{
                 url: "https://api.example/trades",
                 channel: @channel,
                 partition_index: 2,
                 partition_count: 3
               })

      assert opts.url == "https://api.example/trades"
    end
  end

  defp reason_phrase(200), do: "OK"
  defp reason_phrase(302), do: "Found"
  defp reason_phrase(503), do: "Service Unavailable"
  defp reason_phrase(_), do: "Status"
end

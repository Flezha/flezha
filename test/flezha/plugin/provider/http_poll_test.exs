defmodule Flezha.Plugin.Provider.HttpPollTest do
  @moduledoc """
  Polling contract for `Plugin.Provider.HttpPoll`: hash gating, the
  confirmation that gates a hash advance, the status and transport failure
  classification, the worker lifecycle, and the idle window that makes a
  poller that stopped settling say so.

  Polls after the first are driven by sending `:poll`, with `:interval_ms`
  parked at a minute, so each scenario is a deterministic sequence rather
  than a race against a timer.
  """

  # async: false: global telemetry handlers.

  use FlezhaTest.HttpPollCase, async: false

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.HttpPoll
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.WebhookServer

  @catalog Host.build_catalog(
             [{:on_terminate, :fold, Ctx.Terminate}],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  describe "hash gating" do
    test "a steady endpoint injects the seed and nothing more" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin)

      assert {"cfg-v1", %{"channel" => @channel}} = take_ingest()
      assert_receive {:poll_event, :shipped, _, %{seed: true}}

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
      assert_receive {:poll_event, :shipped, _, %{seed: false}}

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

      # Handover / plugin restart: the table stops the worker it started and the
      # next activation starts a fresh one, which carries no hash, so it
      # re-seeds the rejoining relay with content that never changed.
      ref = Process.monitor(poller)
      :ok = stop_worker(poller)
      assert_receive {:DOWN, ^ref, :process, ^poller, _}, 2_000

      _second = start_poller(origin)

      assert {"cfg-v1", %{"channel" => @channel}} = take_ingest()
      assert_receive {:poll_event, :shipped, _, %{seed: true}}
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

      # Nothing about the endpoint changed, but the relay never took the
      # payload — so the poller must offer it again rather than read it as
      # delivered and go silent forever.
      send(poller, :poll)
      assert {"cfg-v1", _} = take_ingest()
      assert_receive {:poll_event, :shipped, _, %{seed: true}}
    end

    test "an absorbed or suspended verdict counts as delivered" do
      origin = start_origin(body: "cfg-v1")
      poller = start_poller(origin)

      assert {"cfg-v1", _} = take_ingest({:ok, :absorbed})
      assert_receive {:poll_event, :shipped, _, _}, 2_000

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
               HttpPoll.health(health_opts(%{owned_pid: poller, failure_threshold: 3}))

      refute_ingest()

      set(origin, status: 200, body: "cfg-v2")
      send(poller, :poll)

      assert {"cfg-v2", %{"channel" => @channel}} = take_ingest()
      assert :healthy = HttpPoll.health(health_opts(%{owned_pid: poller, failure_threshold: 3}))
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
               HttpPoll.health(health_opts(%{owned_pid: poller, failure_threshold: 3}))

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
               HttpPoll.health(health_opts(%{owned_pid: poller, failure_threshold: 2}))

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

      WebhookServer.stop(origin)
      send(poller, :poll)
      assert_receive {:poll_event, :failed, %{consecutive_failures: 1}, _}, 2_000

      assert {:unhealthy, {:poll_failing, _}} =
               HttpPoll.health(health_opts(%{owned_pid: poller, failure_threshold: 1}))

      refute_ingest()
    end

    test "an https endpoint is attempted with verification and fails cleanly when absent" do
      # Exercises the TLS branch end to end (`:public_key.cacerts_get/0` must
      # resolve) without needing a certificate: nothing is listening, so the
      # poll fails at connect rather than at option construction.
      origin = start_origin(body: "unused")
      port = origin.port
      WebhookServer.stop(origin)

      {:ok, opts} =
        ProviderOptions.normalise_opts(HttpPoll, %{
          url: "https://127.0.0.1:#{port}/config",
          channel: @channel,
          interval_ms: 60_000,
          timeout_ms: 2_000,
          overwatcher_pid: self()
        })
        |> then(fn {:ok, o} -> HttpPoll.init_provider(o) end)

      {:ok, poller} = start_worker(opts)
      stop_on_exit(poller)

      assert_receive {:poll_event, :failed, _, %{reason: reason}}, 5_000
      refute match?({:no_ca_store, _}, reason)
      refute_ingest()
    end

    test "health reports a dead or absent poller" do
      assert {:unhealthy, :no_poller} = HttpPoll.health(health_opts(%{}))

      dead = spawn(fn -> :ok end)
      ref = Process.monitor(dead)
      assert_receive {:DOWN, ^ref, :process, ^dead, _}

      assert {:unhealthy, :poller_dead} = HttpPoll.health(health_opts(%{owned_pid: dead}))
    end
  end

  describe "lifecycle" do
    test "init_provider performs no upstream I/O" do
      origin = start_origin(body: "cfg-v1")

      # `init_provider/1` starts nothing — the poller is `activate/1`'s — so
      # there is no process that could reach the origin later.
      assert {:ok, opts} = prepare(origin)

      assert requests(origin) == 0
      assert opts.overwatcher_pid == self()
      assert opts.plugin_id == opts.url
    end

    test "with no plugin_id, the poller is named by the id the plugin was enabled under" do
      origin = start_origin(body: "cfg-v1")

      assert {:ok, opts} = prepare(origin, %{source_plugin_id: "config#p1"})
      assert opts.plugin_id == "config#p1"
    end

    test "activate returns without waiting on the request" do
      origin = start_origin(body: "cfg-v1", hold: self())
      {:ok, opts} = prepare(origin)

      {elapsed_us, {:ok, poller}} = :timer.tc(fn -> start_worker(opts) end)
      stop_on_exit(poller)

      assert elapsed_us < 100_000

      # The host keeps serving its own mailbox while that request is in
      # flight — the wait belongs to the fetch child, not to the poller and
      # certainly not to the dispatch path.
      Enum.each(1..50, fn n -> send(self(), {:host_work, n}) end)
      Enum.each(1..50, fn n -> assert_received {:host_work, ^n} end)

      assert {:ok, {0, nil, _last_settled_at}} = probe(poller)
      WebhookServer.release()
      assert {"cfg-v1", _} = take_ingest(:ok, 5_000)
    end

    test "the health probe answers while a request is outstanding" do
      origin = start_origin(body: "cfg-v1", hold: self())
      poller = start_poller(origin)

      # Mid-flight: the request is blocking its own child, not the worker, so
      # the verdict is available for exactly the slow endpoint it must report.
      assert {:ok, {0, nil, _last_settled_at}} = probe(poller)
      assert :healthy = HttpPoll.health(health_opts(%{owned_pid: poller, failure_threshold: 3}))

      WebhookServer.release()
      assert {"cfg-v1", _} = take_ingest(:ok, 5_000)
    end

    test "the poller stops when the host relay goes down" do
      origin = start_origin(body: "cfg-v1")
      parent = self()

      host =
        spawn(fn ->
          {:ok, opts} = prepare(origin)
          {:ok, poller} = start_worker(opts)
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
        hooks: [{:on_terminate, []}],
        namespace: "default",
        relay_id: "detector"
      }

      assert {:ok, table} = Table.bulk_subscribe(Table.new(@catalog), [{"cfg", plugin}])
      assert Table.phase(table, "cfg") == :prepared

      {table, _} = Table.bulk_activate(table)
      assert Table.phase(table, "cfg") == :running

      assert {"cfg-v1", %{"channel" => @channel}} = take_ingest()

      poller = Table.owned_pid(table, "cfg")
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
               HttpPoll.health(
                 health_opts(%{owned_pid: poller, failure_threshold: 3, idle_after_ms: 200})
               )

      assert age >= 400
    end

    test "unchanged content on cadence stays healthy" do
      # `settle/3` stamps on EVERY outcome, so a content-gated poller that has
      # injected nothing for many intervals is still settling and still healthy.
      poller = stub_worker(failures: 0, last_error: nil, settled_ago_ms: 10)

      assert :healthy =
               HttpPoll.health(
                 health_opts(%{owned_pid: poller, failure_threshold: 3, idle_after_ms: 200})
               )
    end

    test "a poll in flight inside the window stays healthy" do
      origin = start_origin(body: "cfg-v1", hold: self())
      poller = start_poller(origin)

      # The request is in flight for as long as the origin holds it.
      assert_receive {:holding, conn}, 5_000
      assert :healthy = HttpPoll.health(health_opts(%{owned_pid: poller, failure_threshold: 3}))
      send(conn, :unpark)
      assert {"cfg-v1", _} = take_ingest(:ok, 5_000)
    end

    test "a failure run outranks the idle age" do
      poller = stub_worker(failures: 5, last_error: :econnrefused, settled_ago_ms: 10_000)

      assert {:unhealthy, {:poll_failing, :econnrefused}} =
               HttpPoll.health(
                 health_opts(%{owned_pid: poller, failure_threshold: 3, idle_after_ms: 200})
               )
    end

    test "the window defaults to three intervals when unset" do
      # 3 x 1_000 ms = 3_000 ms, so a 2 s-old settle is healthy and a 4 s-old
      # one is not, without the option ever being supplied.
      assert :healthy =
               HttpPoll.health(
                 health_opts(%{
                   owned_pid: stub_worker(settled_ago_ms: 2_000),
                   interval_ms: 1_000
                 })
               )

      assert {:unhealthy, {:poll_idle, _}} =
               HttpPoll.health(
                 health_opts(%{
                   owned_pid: stub_worker(settled_ago_ms: 4_000),
                   interval_ms: 1_000
                 })
               )
    end

    test "a wedged worker is reported as uncertain, not as a verdict" do
      wedged = spawn(&NodeTestHelper.park/0)
      on_exit(fn -> if Process.alive?(wedged), do: Process.exit(wedged, :kill) end)

      # A worker that is alive but not answering is neither healthy nor
      # unhealthy. The provider says so rather than swallowing it as `:healthy`;
      # `Extension.Table` is what turns this into telemetry, because it is the
      # layer that knows which plugin the provider belongs to.
      assert {:fail_open, :probe_timeout} =
               HttpPoll.health(health_opts(%{owned_pid: wedged, failure_threshold: 3}))
    end
  end

  # A worker that answers the health probe with a chosen verdict. The idle
  # window is a clock, and driving a real poller past it would mean sleeping
  # through real intervals.
  # `health/1` reads declared fields, so a probe is driven with the declaration's
  # own defaults and only the values a case is about.
  defp health_opts(extra),
    do: Map.merge(ProviderOptions.defaults(HttpPoll), extra)

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
end

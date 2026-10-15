defmodule FlezhaTest.System.BackoffTableDrillTest do
  @moduledoc """
  Every ladder row driven through the site that walks it, read off telemetry.

  The row set is the controller's `Backoff.ladders/0`, so a new ladder is
  covered with no test edit: a row this module neither drives nor accounts for
  fails `every row is accounted for` by name.

  A driven row induces the failure at the edge where its site meets the outside
  world — a socket that answers `429`, an endpoint that answers `503`, a
  refusing backend, an unanswered hub call —
  and asserts the delays announced on `[:flezha, :backoff, :wait]` are the
  row's, that the attempts ascend, and that a success announces
  `[:flezha, :backoff, :reset]`.

  An accounted row is one whose wait is a single fixed cadence armed inside a
  private path (a port restart, a hub retry, an outbox retry, a broker-replace
  throttle): the value is one number, `backoff_seam_test.exs`
  proves no site holds an arithmetic of its own, and the suite named beside the
  row drives the behaviour that arms it. Each says which.

  Tagged `:system`; run with
  `mix test --include system test/system/backoff_table_drill_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :retry_ladder
  use Mimic

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Config.Backoff
  alias Flezha.Config.Durations
  alias Flezha.Observability.Export.OtlpClient
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HookWebhook
  alias Flezha.Plugin.Provider.HttpPush
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Service.RelayManager.Init
  alias Flezha.State.Watchdog
  alias FlezhaTest.Helper.ProviderPrepare
  alias FlezhaTest.Helper.WebhookServer
  alias FlezhaTest.Helper.WebsocketServer

  setup :set_mimic_global_until_exit

  @moduletag timeout: 300_000

  # The rows whose wait is one fixed cadence armed inside a private path. Each
  # names the suite that drives the behaviour arming it; the value itself is
  # the row's, and no site may hold a second one (backoff_seam_test.exs).
  @accounted %{
    hub_retry: "test/flezha/schedule/hub_test.exs — the schedule hub retries its start",
    port_restart: "no suite drives the crash-loop path",
    postgres_flush:
      "test/flezha/plugin/provider/postgres_sink_test.exs — a failing flush retries on its timer",
    file_flush:
      "test/flezha/plugin/provider/file_sink_test.exs drives it: a failed write drops nothing and retries on its timer",
    forward_deliver:
      "test/flezha/plugin/provider/relay_forward_test.exs — an unconfirmed delivery retries",
    durability_ship: "test/flezha/state/durability_tier_test.exs — a refused ship retries",
    schedule_spawn:
      "test/flezha/schedule/job_test.exs — a failed spawn retries until the next slot"
  }

  describe "the table is the test's row set" do
    test "every row is either driven here or accounted for by the suite that drives it" do
      driven = MapSet.new(Map.keys(devices()))
      accounted = MapSet.new(Map.keys(@accounted))

      unaccounted =
        for key <- Map.keys(Backoff.ladders()),
            not MapSet.member?(driven, key),
            not MapSet.member?(accounted, key),
            do: key

      assert unaccounted == [],
             "ladder rows with no device and no account: #{inspect(unaccounted)}"
    end

    test "every account names a suite that exists" do
      missing =
        for {key, account} <- @accounted,
            [path] = Regex.run(~r|^\S+|, account),
            String.starts_with?(path, "test/"),
            not File.exists?(path),
            do: {key, path}

      assert missing == [], "an account names a file that is not there: #{inspect(missing)}"
    end

    test "no row is both driven and accounted for" do
      overlap =
        MapSet.intersection(MapSet.new(Map.keys(devices())), MapSet.new(Map.keys(@accounted)))

      assert MapSet.to_list(overlap) == []
    end
  end

  describe "every driven row announces its own schedule" do
    for key <- [
          :heal,
          :compaction,
          :otlp_post,
          :hook_webhook_post,
          :http_push,
          :no_response,
          :cluster_reconnect,
          :ws_reconnect,
          :ws_sink_ack
        ] do
      @key key
      test "#{key}" do
        {driver, attempts, knobs} = Map.fetch!(devices(), @key)
        collect(@key)

        driver.()

        waits = drain_waits(@key, attempts)

        assert length(waits) >= attempts,
               "#{@key}: expected #{attempts} waits, saw #{length(waits)}"

        # Each wait carries the row's delay for the attempt it names — the
        # claim that the site reads its row and holds no schedule of its own.
        for %{delay_ms: delay, attempt: attempt} <- waits do
          assert attempt >= 1, "#{@key}: an attempt is 1-based"
          assert_in_band(@key, attempt, delay, knobs)
        end

        # And the ladder advances rather than repeating one rung: which
        # counter a site keeps is its own, so the drill asserts the walk, not
        # the index.
        seen = waits |> Enum.map(& &1.attempt) |> Enum.uniq()

        assert length(seen) >= 2, "#{@key}: the ladder did not advance: #{inspect(seen)}"
        assert List.first(seen) < List.last(seen), "#{@key}: attempts must ascend"
        assert List.first(seen) == 1, "#{@key}: a ladder starts at attempt 1"
      end
    end
  end

  describe "a success after a retry announces the reset" do
    # The upstream's echo is this ladder's success. A frame answered on its
    # first send walked no rung, so only a retried one has a reset to announce.
    test "ws_sink_ack" do
      collect(:ws_sink_ack)
      server = ws_sink_ack_device()

      assert [%{attempt: 1}] = drain_waits(:ws_sink_ack, 1)
      assert_receive {:ws_frame, frame}, 5_000
      WebsocketServer.ack(server, Jason.decode!(frame)["req_id"])

      assert_receive {:backoff, :reset, %{attempt: 0}}, 5_000
    end
  end

  # ---------- devices ----------

  defp devices() do
    %{
      # The call `Convergence.Loop.execute/2` makes on its own table for a
      # remedy that did not repair its target; the fleet-level drive of the
      # same loop is remedy_drill_test.exs.
      heal: {&heal_device/0, 4, []},
      # The Watchdog's own cast, the entry point a failing compaction takes.
      compaction: {&compaction_device/0, 3, []},
      # A real endpoint that answers 503 on every post.
      otlp_post: {&otlp_device/0, 4, []},
      # A receiver that answers 503 to every event a controller hook posts.
      hook_webhook_post: {&hook_webhook_device/0, 2, []},
      # A receiver that answers 503 to every output, under the plugin's own
      # `retry_backoff_ms`/`retry_max_backoff_ms`, which bound the row.
      http_push: {&http_push_device/0, 3, [base: 100, cap: 1_000]},
      # A hub whose start never answers: the retry the init path walks.
      no_response: {&no_response_device/0, 3, []},
      # A peer that never answers Node.connect/1.
      cluster_reconnect: {&cluster_reconnect_device/0, 3, []},
      # A tasklet that returns an error for every input.
      # A socket that answers every upgrade with 429.
      ws_reconnect: {&ws_reconnect_device/0, 3, []},
      # A socket whose peer takes every frame and acknowledges none.
      ws_sink_ack: {&ws_sink_ack_device/0, 2, []}
    }
  end

  defp heal_device() do
    key = {:rebind_relay, "drill_#{System.unique_integer([:positive])}"}

    Enum.reduce(1..4, Backoff.new(:heal), fn _, table ->
      Backoff.update(table, key, :error, 0)
    end)
  end

  defp compaction_device() do
    cell_id = {"drill", "task", "k#{System.unique_integer([:positive])}"}

    for _ <- 1..3 do
      GenServer.cast(Watchdog, {:compaction_failed, cell_id})
      _ = :sys.get_state(Watchdog)
    end
  end

  defp otlp_device() do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    spawn_link(fn ->
      answer_http(listener, "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")
    end)

    previous = Application.get_env(:flezha, :observability_export, [])

    Application.put_env(
      :flezha,
      :observability_export,
      Keyword.put(previous, :otlp_endpoint, "http://127.0.0.1:#{port}")
    )

    on_exit(fn ->
      Application.put_env(:flezha, :observability_export, previous)
      :gen_tcp.close(listener)
    end)

    # The client is a named singleton; the drill owns it for the duration.
    if pid = Process.whereis(OtlpClient), do: Supervisor.stop(pid)
    {:ok, _} = OtlpClient.start_link([])
    OtlpClient.post(:logs, "{}")
  end

  # `call/2` holds its caller until the event is sent or given up, so the hook
  # runs off the test process; the worker stops with the test, mid-ladder.
  defp hook_webhook_device() do
    server = WebhookServer.start([503])

    {:ok, opts, worker} =
      ProviderPrepare.prepare_and_activate(HookWebhook, %{
        url: "http://127.0.0.1:#{server.port}/hook",
        source_plugin_id: "hook_drill_#{System.unique_integer([:positive])}",
        retries: 5
      })

    ProviderPrepare.stop_on_exit(HookWebhook, worker)
    spawn(fn -> HookWebhook.on_dead_letter(%{relay: "drill"}, opts) end)
    :ok
  end

  defp http_push_device() do
    server = WebhookServer.start([503])

    {:ok, opts, worker} =
      ProviderPrepare.prepare_and_activate(HttpPush, %{
        url: "http://127.0.0.1:#{server.port}/push",
        source_plugin_id: "push_drill_#{System.unique_integer([:positive])}",
        subscribed_extensions: [:on_output],
        retries: 10,
        retry_backoff_ms: 100,
        retry_max_backoff_ms: 1_000
      })

    ProviderPrepare.stop_on_exit(HttpPush, worker)
    {:ok, _ctx} = HttpPush.on_output(%Ctx.IO{message: "drill", metadata: %{}}, opts)
    :ok
  end

  defp no_response_device() do
    id = "drill_#{System.unique_integer([:positive])}"
    relay = %{id: id, factory_id: "drill_factory", opts: %{}}

    stub(ProcessHub, :start_child, fn _hub, _spec, _opts -> :drill_future end)

    stub(ProcessHub.Future, :await, fn :drill_future ->
      %ProcessHub.StartResult{started: [], errors: [{id, :no_response}]}
    end)

    spawn(fn -> Init.init_relay(false, "drill_ns", relay) end)
    :ok
  end

  defp cluster_reconnect_device() do
    node = :"never_answers_#{System.unique_integer([:positive])}@127.0.0.1"
    send(Process.whereis(Flezha.Cluster.Orchestrator), {:nodedown, node})
    :ok
  end

  defp ws_reconnect_device() do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    spawn_link(fn ->
      answer_http(listener, "HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\n\r\n")
    end)

    {:ok, _opts, client} =
      ProviderPrepare.prepare_and_activate(Websocket, %{
        uri: "ws://127.0.0.1:#{port}/",
        plugin_id: "ws_drill"
      })

    ProviderPrepare.stop_on_exit(Websocket, client)
    on_exit(fn -> :gen_tcp.close(listener) end)

    :ok
  end

  # One output under `sink_ack_mode: "required"`: the socket re-sends it on
  # every rung until an acknowledgement settles it. Answers the peer, so a test
  # can acknowledge.
  defp ws_sink_ack_device() do
    server = WebsocketServer.start()

    {:ok, opts, client} =
      ProviderPrepare.prepare_and_activate(Websocket, %{
        uri: "ws://127.0.0.1:#{server.port}/",
        plugin_id: "ws_sink_drill_#{System.unique_integer([:positive])}",
        subscribed_extensions: [:on_output],
        sink_ack_mode: "required",
        sink_template: ~s({"req_id":"{{req_id}}","d":"{{payload}}"})
      })

    ProviderPrepare.stop_on_exit(Websocket, client)
    {:ok, _ctx} = Websocket.on_output(%Ctx.IO{message: "drill"}, opts)
    server
  end

  # ---------- observation ----------

  defp collect(ladder) do
    parent = self()
    id = "backoff-drill-#{ladder}-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      id,
      [[:flezha, :backoff, :wait], [:flezha, :backoff, :reset]],
      fn event, measurements, metadata, _ ->
        if metadata.ladder == ladder, do: send(parent, {:backoff, List.last(event), measurements})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp drain_waits(_ladder, expected, acc \\ []) do
    receive do
      {:backoff, :wait, measurements} ->
        acc = acc ++ [measurements]
        if length(acc) >= expected, do: acc, else: drain_waits(nil, expected, acc)

      {:backoff, :reset, _} ->
        drain_waits(nil, expected, acc)
    after
      20_000 -> acc
    end
  end

  # The row's own arithmetic is the expectation: `rand: fn _ -> 1 end` pins the
  # jittered floor, and the band is what the one jitter rule allows above it.
  # `knobs` are the caller's `base`/`cap` where the row's bound is a per-plugin
  # setting, so the band is the one that site actually walks.
  defp assert_in_band(ladder, attempt, delay, knobs) do
    row = Backoff.ladder(ladder)
    floor = Backoff.delay(ladder, attempt, Keyword.put(knobs, :rand, fn _ -> 1 end))

    if row.jitter do
      cap =
        case knobs[:cap] || row.cap do
          name when is_atom(name) -> Durations.get(name)
          ms -> ms
        end

      plain = unjittered(ladder, attempt, knobs)

      assert delay >= div(plain, 2) + 1 and delay <= min(div(plain, 2) + plain, cap),
             "#{ladder} attempt #{attempt}: #{delay} is outside the row's jitter band"
    else
      assert delay == floor,
             "#{ladder} attempt #{attempt}: #{delay} is not the row's #{floor}"
    end
  end

  # The rung before jitter, read out of the row's own arithmetic: `jitter/2`
  # is handed `max(plain, 1)`, so the injected `rand` reports it.
  defp unjittered(ladder, attempt, knobs) do
    parent = self()

    rand = fn max ->
      send(parent, {:unjittered, max})
      1
    end

    _ = Backoff.delay(ladder, attempt, Keyword.put(knobs, :rand, rand))

    receive do
      {:unjittered, max} -> max
    after
      0 -> raise "#{ladder} declares jitter but its delay took no random value"
    end
  end

  defp answer_http(listener, response) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        :gen_tcp.recv(socket, 0, 1_000)
        :gen_tcp.send(socket, response)
        :gen_tcp.close(socket)
        answer_http(listener, response)

      _ ->
        :ok
    end
  end
end

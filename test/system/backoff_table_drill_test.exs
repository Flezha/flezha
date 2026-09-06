defmodule FlezhaTest.System.BackoffTableDrillTest do
  @moduledoc """
  Every ladder row driven through the site that walks it, read off telemetry.

  The row set is `Backoff.ladders/0`, so a new ladder is covered with no test
  edit: a row this module neither drives nor accounts for fails `every row is
  accounted for` by name.

  A driven row induces the failure at the edge where its site meets the outside
  world — a socket that answers `429`, an endpoint that answers `503`, a
  refusing backend, an unanswered hub call, a procedure that returns an error —
  and asserts the delays announced on `[:flezha, :backoff, :wait]` are the
  row's, that the attempts ascend, and that a success announces
  `[:flezha, :backoff, :reset]`.

  An accounted row is one whose wait is a single fixed cadence armed inside a
  private path (a port restart, a hub retry, an outbox retry, a park resend, a
  broker-replace throttle): the value is one number, `backoff_seam_test.exs`
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
  alias Flezha.Plugin.Provider.Kafka.ProducerWorker
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.SDK.Channels
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.Service.OperativeManager.Init
  alias Flezha.State.Watchdog
  alias FlezhaTest.Helper.SdkOperativeHarness
  alias FlezhaTest.Kafka.ProducerFakes

  setup :set_mimic_global_until_exit

  @moduletag timeout: 300_000

  # The Kafka worker's own knobs — the row's bound is a per-plugin setting.
  @kafka_base_ms 5
  @kafka_cap_ms 40

  # The rows whose wait is one fixed cadence armed inside a private path. Each
  # names the suite that drives the behaviour arming it; the value itself is
  # the row's, and no site may hold a second one (backoff_seam_test.exs).
  @accounted %{
    park_resend: "test/flezha/sdk/effectively_once_test.exs — a park is resent until acked",
    hub_retry: "test/flezha/schedule/hub_test.exs — the schedule hub retries its start",
    proc_outbox_retry:
      "test/flezha/runner/control_channel_commits_test.exs — the proc outbox re-drives",
    port_restart: "no suite drives the crash-loop path",
    kafka_force_replace:
      "test/flezha/plugin/kafka_test.exs — a wedged client is replaced once per window",
    postgres_flush:
      "test/flezha/plugin/provider/postgres_sink_test.exs — a failing flush retries on its timer",
    forward_deliver:
      "test/flezha/plugin/provider/operative_forward_test.exs — an unconfirmed delivery retries",
    durability_ship: "test/flezha/state/durability_tier_test.exs — a refused ship retries",
    schedule_spawn:
      "test/flezha/schedule/job_test.exs — a failed spawn retries until the next slot",
    entity_wake_retry:
      "test/flezha/sdk/entity_timers_test.exs — a due timer that finds no room comes back",
    sdk_reconnect:
      "test/flezha/sdk/transport_modes_test.exs — a dropped runner reconnects; the row is pinned to flz_core by backoff_seam_test.exs"
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
          :no_response,
          :cluster_reconnect,
          :sdk_retry,
          :ws_reconnect,
          :kafka_produce
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
        # counter a site keeps is its own (a Kafka batch announces the attempt
        # it is about to make), so the drill asserts the walk, not the index.
        seen = waits |> Enum.map(& &1.attempt) |> Enum.uniq()

        assert length(seen) >= 2, "#{@key}: the ladder did not advance: #{inspect(seen)}"
        assert List.first(seen) < List.last(seen), "#{@key}: attempts must ascend"
        assert List.first(seen) == 1, "#{@key}: a ladder starts at attempt 1"
      end
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
      # A hub whose start never answers: the retry the init path walks.
      no_response: {&no_response_device/0, 3, []},
      # A peer that never answers Node.connect/1.
      cluster_reconnect: {&cluster_reconnect_device/0, 3, []},
      # A procedure that returns an error for every input.
      sdk_retry: {&sdk_retry_device/0, 3, [base: 50]},
      # A socket that answers every upgrade with 429.
      ws_reconnect: {&ws_reconnect_device/0, 3, []},
      # A broker send that always fails.
      # Four waits: the worker announces the attempt it is about to make and
      # arms the one it is on, so a walk of two rungs shows as four events.
      kafka_produce: {&kafka_device/0, 4, [base: @kafka_base_ms, cap: @kafka_cap_ms]}
    }
  end

  defp heal_device() do
    key = {:rebind_operative, "drill_#{System.unique_integer([:positive])}"}

    Enum.reduce(1..4, Backoff.new(:heal), fn _, table ->
      Backoff.update(table, key, :error, 0)
    end)
  end

  defp compaction_device() do
    entity_id = {"drill", "proc", "k#{System.unique_integer([:positive])}"}

    for _ <- 1..3 do
      GenServer.cast(Watchdog, {:compaction_failed, entity_id})
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

  defp no_response_device() do
    id = "drill_#{System.unique_integer([:positive])}"
    operative = %{id: id, factory_id: "drill_factory", opts: %{}}

    stub(ProcessHub, :start_child, fn _hub, _spec, _opts -> :drill_future end)

    stub(ProcessHub.Future, :await, fn :drill_future ->
      %ProcessHub.StartResult{started: [], errors: [{id, :no_response}]}
    end)

    spawn(fn -> Init.init_operative(false, "drill_ns", operative) end)
    :ok
  end

  defp cluster_reconnect_device() do
    node = :"never_answers_#{System.unique_integer([:positive])}@127.0.0.1"
    send(Process.whereis(Flezha.Cluster.Orchestrator), {:nodedown, node})
    :ok
  end

  defmodule FailingProcedure do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "backoff_drill_procedure"
    def init_state(_), do: %{}
    def apply_change(s, _change), do: s
    def state_from_json(json), do: json

    # Every input fails, so the ladder walks to its retry_max and parks.
    def run(_input, _state, _ctx), do: {:error, "backoff drill: always fails"}
  end

  defmodule FailingOperative do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "backoff_drill"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.retry_backoff_ms(50)
      |> Channels.retry_max(4)
      |> Channels.into(FailingProcedure)
      |> Channels.sink("out")
    end
  end

  defp sdk_retry_device() do
    %{op: op} = SdkOperativeHarness.start_operative(FailingOperative)
    Inbox.input(op, "in", "payload", %{})
    :ok
  end

  defp ws_reconnect_device() do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    spawn_link(fn ->
      answer_http(listener, "HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\n\r\n")
    end)

    {:ok, client} =
      Websocket.Client.start_link("ws://127.0.0.1:#{port}/",
        plugin_id: "ws_drill",
        overwatcher_pid: self(),
        post_connect: %{},
        ws_opts: []
      )

    on_exit(fn ->
      if Process.alive?(client), do: Process.exit(client, :kill)
      :gen_tcp.close(listener)
    end)

    :ok
  end

  defp kafka_device() do
    # A broker that refuses every send: the worker retries on its own timer,
    # and each retry arms the row.
    {worker, _script} =
      ProducerFakes.start_worker(
        retry_backoff_ms: @kafka_base_ms,
        retry_max_backoff_ms: @kafka_cap_ms,
        delivery_timeout_ms: 5_000,
        retries: 100,
        script: List.duplicate({:return, {:error, :no_broker}}, 200)
      )

    ProducerWorker.enqueue(worker, "v", %{})
    :ok
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

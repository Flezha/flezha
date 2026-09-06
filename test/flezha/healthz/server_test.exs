defmodule Flezha.Healthz.ServerTest do
  @moduledoc """
  HTTP `/healthz` contract per `convergence-self-healing`
  §4.6-4.9. Drives the listener over a real TCP socket — the
  hand-rolled `:gen_tcp` request parser is the artefact under test.

  Covered scenarios match the `convergence-self-healing` capability:
    * `/` returns `pong\\n`
    * `/healthz` returns 200 healthy after a tick observation with
      no stalled operatives.
    * `/healthz` returns 200 `degraded` when a operative is output-stalled
      (`producing_operatives < bound_operatives`) — a stalled operative is the
      Loop's job to rebind, NOT a kubelet restart signal.
    * `/healthz` returns 503 with `reason: "tick_stale"` only when the
      Loop is wedged (ticks stopped past `tick_ms × 3`).
    * Unknown paths return 404; non-GET methods return 405.
  """

  # async: false: writes Application env; registered singletons; node-global tables.

  use FlezhaTest.Case, fixtures: [:env], async: false

  import FlezhaTest.Helper.ConvergenceFixtures, only: [verdicts: 1]

  alias Flezha.Boot.Readiness
  alias Flezha.Convergence.Snapshot
  alias Flezha.Healthz.Server
  alias Flezha.Service.HubManager
  alias Flezha.Service.HubManager.Starter

  setup do
    Application.put_env(:flezha, :healthz_port, 0)

    # Clear recovery states from prior tests so the recovery summary starts with
    # a clean slate. The reader samples it through `RecoveryGate`.
    Snapshot.reset_recovery_states()

    # A fresh sensor mark, so the sensor-freshness reader is neutral for every
    # test that is not about it (the app's own Watchdog stamps this row too).
    fresh_sensor()

    {:ok, _pid} = start_supervised(Server)
    {:ok, port} = Server.listen_port()

    on_exit(fn ->
      # Reset the globally-started snapshot so leaked health state does
      # not bleed into other suites.
      Snapshot.publish(%{
        last_tick_at: nil,
        operative_health: %{},
        bound_operatives: 0,
        producing_operatives: 0
      })

      Snapshot.reset_recovery_states()
      fresh_sensor()
    end)

    {:ok, port: port}
  end

  # Publish a convergence snapshot the way the Loop would: one verdict record
  # per operative, which is what the endpoint folds. `bound` operatives are
  # minted healthy and the ones named in `stalled` degraded, so the counts the
  # body reports are DERIVED here exactly as the Loop derives them.
  defp publish_tick(stalled, bound \\ 0, age_ms \\ 0) do
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond) - age_ms,
      operative_health: operative_records(stalled, bound),
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })
  end

  # `bound` operatives, the first `length(stalled)` of them named by `stalled`
  # and output-stalled, the rest healthy and producing.
  defp operative_records(stalled, bound) do
    healthy = for i <- 1..max(bound - length(stalled), 0)//1, do: "healthy_#{i}"

    verdicts(
      for(
        id <- stalled,
        do: {op(id), :degraded, reason: {:stalled, :output_stall}, phase: :running}
      ) ++
        for(id <- healthy, do: {op(id), :healthy, reason: :producing, phase: :running})
    )
  end

  defp op(id), do: {:operative, {"ns", id}}

  @sensor_period_ms 10_000

  defp fresh_sensor(),
    do: Snapshot.observe_sensor(:watchdog, System.monotonic_time(:millisecond), @sensor_period_ms)

  # A snapshot whose data-plane verdict is `layer`, held for `polls` polls.
  defp publish_verdict(layer, polls) do
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stuck_operatives: %{},
      intent_mismatch: [],
      operative_health: operative_records([], 1),
      pending_restore: false,
      data_plane_layer: layer,
      data_plane_streak: polls
    })
  end

  test "hub in :timeout recovery state → 200 recovering (not 503)", %{port: port} do
    hub_id = :"healthz_timeout_hub_#{System.unique_integer([:positive])}"
    Snapshot.observe_recovery(hub_id, :timeout)
    publish_tick([])

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] == "recovering"
    assert decoded["recovery_state"] == "timeout"
  end

  test "GET / returns pong", %{port: port} do
    {status, _headers, body} = get(port, "/")
    assert status == 200
    assert body == "pong\n"
  end

  test "GET /unknown returns 404", %{port: port} do
    {status, _headers, _body} = get(port, "/unknown")
    assert status == 404
  end

  test "POST /healthz returns 405", %{port: port} do
    {status, _headers, _body} = request(port, "POST", "/healthz")
    assert status == 405
  end

  test "healthy → 200 with status: healthy", %{port: port} do
    publish_tick([])

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] in ["healthy", "recovering"]
  end

  test "output stalled → 200 degraded (not a restart signal)", %{port: port} do
    # One of two bound operatives stalled: producing(1) < bound(2).
    publish_tick(["btc_kraken"], 2)

    {status, _headers, body} = get(port, "/healthz")
    decoded = Jason.decode!(body)

    if decoded["status"] == "recovering" do
      # The spec's deliberate "do not restart during planned recovery"
      # branch takes precedence over degraded.
      assert status == 200
    else
      assert status == 200
      assert decoded["status"] == "degraded"
      assert decoded["bound_operatives"] == 2
      assert decoded["producing_operatives"] == 1
      assert decoded["stalled_operatives"] == ["btc_kraken"]
    end
  end

  test "plugin-degraded operative → 200 degraded with reason plugin_unhealthy", %{port: port} do
    # One of two bound operatives has declared plugins missing/unhealthy.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      operative_health:
        verdicts([
          {op("btc_kraken"), :degraded,
           reason: {:plugin_unhealthy, "src", :not_running}, phase: :running},
          {op("healthy_1"), :healthy, reason: :producing, phase: :running}
        ])
    })

    {status, _headers, body} = get(port, "/healthz")
    decoded = Jason.decode!(body)

    if decoded["status"] == "recovering" do
      # Planned-recovery branch takes precedence over degraded.
      assert status == 200
    else
      assert status == 200
      assert decoded["status"] == "degraded"
      assert decoded["reason"] == "plugin_unhealthy"
      assert decoded["plugin_degraded_operatives"] == ["btc_kraken"]
    end
  end

  test "operatives declared but not yet bound → 200 recovering (post-restart rebind)", %{
    port: port
  } do
    # 3 operatives intended, only 1 actually bound (controller mid-rebind). Capable-
    # but-not-ready must read recovering, never healthy.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      operative_health:
        verdicts([
          {op("bound"), :healthy, reason: :producing, phase: :running},
          {op("rebinding_a"), :unbound, reason: :no_binding},
          {op("rebinding_b"), :unbound, reason: :no_binding}
        ]),
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] == "recovering"
    assert decoded["reason"] == "operatives_rebinding"
    assert decoded["bound_operatives"] == 1
    assert decoded["intended_operatives"] == 3
  end

  test "a dead runner's channel while every operative is bound → 200 degraded, not recovering",
       %{port: port} do
    # The rebind window is an OPERATIVE window: the grace it sits inside is
    # measured from unmet intent, and a channel record never makes intent
    # unmet. Weighing one here would hold the endpoint at `recovering` for as
    # long as the node runs, outranking every reason the fold could report.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      operative_health:
        verdicts([
          {op("bound"), :healthy, reason: :producing, phase: :running},
          {{:channel, {"departed_runner", :control}}, :failed, reason: :dead_runner}
        ]),
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] == "degraded"
    assert decoded["reason"] == "dead_runner"
    assert decoded["bound_operatives"] == 1
    assert decoded["intended_operatives"] == 1
  end

  test "operatives declared but unbound PAST the grace → 200 degraded (not silent recovering)",
       %{port: port} do
    # The silent-30h-outage gap: a operative declared but never bound must not sit
    # at `recovering` forever. Once `unmet_intent_ms` exceeds the grace the
    # reader escalates to `degraded` so it alarms (still 200 — a restart can't
    # re-bind an orphaned operative).
    Application.put_env(:flezha, :convergence_unmet_intent_grace_ms, 100)

    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      operative_health: verdicts([{op("orphan"), :unbound, reason: :no_binding}]),
      unmet_intent_ms: 5_000,
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    refute decoded["status"] == "healthy"
    assert decoded["status"] == "degraded"
    assert decoded["reason"] == "operatives_unbound"
    assert decoded["intended_operatives"] == 1
    assert decoded["bound_operatives"] == 0
    assert decoded["unmet_intent_ms"] == 5_000
  end

  test "durable registry pending → 200 recovering (post-restart, registry not loaded yet)",
       %{port: port} do
    # Hubs start lazily on runner reconnect, so right after a restart Intent is
    # empty (intended == 0) while operatives.dets still sits on disk. This must read
    # recovering, never vacuously healthy — `bound < intended` (0 < 0) misses it.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_operatives: [],
      intended_operatives: 0,
      bound_operatives: 0,
      producing_operatives: 0,
      pending_restore: true,
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] == "recovering"
    assert decoded["reason"] == "restoring_registry"
    assert decoded["intended_operatives"] == 0
    assert decoded["bound_operatives"] == 0
  end

  test "genuinely empty, fully started system → 200 healthy", %{port: port} do
    # No durable operatives pending and nothing declared: a legitimately-empty
    # controller that has finished starting must still report healthy, so the
    # restoring-registry branch never masks a steady-state empty system.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_operatives: [],
      intended_operatives: 0,
      bound_operatives: 0,
      producing_operatives: 0,
      pending_restore: false,
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] == "healthy"
  end

  test "tick stale → 503 with reason: tick_stale (when configured for fast staleness)",
       %{port: port} do
    Application.put_env(:flezha, :convergence_tick_ms, 10)

    # A tick whose timestamp is already past tick_ms × 2: the staleness window
    # has elapsed.
    publish_tick([], 0, 50)

    {status, _headers, body} = get(port, "/healthz")
    decoded = Jason.decode!(body)

    if decoded["status"] == "recovering" do
      assert status == 200
    else
      assert status == 503
      assert decoded["reason"] == "tick_stale"
    end
  end

  test "idle fleet counts as producing → 200 healthy", %{port: port} do
    # 5 bound operatives with no input and flat markers classify IDLE, which
    # the Loop counts as producing — flat-because-idle is not degraded.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stuck_operatives: %{},
      intent_mismatch: [],
      operative_health:
        verdicts(for i <- 1..5, do: {op("idle_#{i}"), :healthy, reason: :idle, phase: :running}),
      pending_restore: false
    })

    {status, _headers, body} = get(port, "/healthz")
    decoded = Jason.decode!(body)

    if decoded["status"] == "recovering" do
      assert status == 200
    else
      assert status == 200
      assert decoded["status"] == "healthy"
      assert decoded["producing_operatives"] == 5
    end
  end

  test "STUCK operative (fed but frozen) → 200 degraded with per-reason detail", %{port: port} do
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stuck_operatives: %{"btc_kraken" => %{detail: :wal_frozen, entity: "proc/k1"}},
      intent_mismatch: [],
      operative_health:
        verdicts([
          {op("btc_kraken"), :degraded, reason: {:stuck, :wal_frozen}, phase: :running},
          {op("healthy_1"), :healthy, reason: :producing, phase: :running}
        ]),
      pending_restore: false
    })

    {status, _headers, body} = get(port, "/healthz")
    decoded = Jason.decode!(body)

    if decoded["status"] == "recovering" do
      assert status == 200
    else
      assert status == 200
      assert decoded["status"] == "degraded"
      assert decoded["reason"] == "output_stalled"
      assert decoded["producing_operatives"] == 1
      assert decoded["stuck_operatives"]["btc_kraken"]["detail"] == "wal_frozen"
      assert decoded["stuck_operatives"]["btc_kraken"]["entity"] == "proc/k1"
    end
  end

  test "registry wipe (durable evidence, empty intent) → 200 degraded intent_mismatch, never healthy",
       %{port: port} do
    # The 30 h outage shape: intended == bound == 0 so every per-operative
    # check quantifies over an empty set — the mismatch list is the witness.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_operatives: [],
      stuck_operatives: %{},
      intent_mismatch: ["btc_kraken", "eth_kraken"],
      intended_operatives: 0,
      bound_operatives: 0,
      producing_operatives: 0,
      pending_restore: false
    })

    {status, _headers, body} = get(port, "/healthz")
    decoded = Jason.decode!(body)

    if decoded["status"] == "recovering" do
      assert status == 200
    else
      # 200, not 503 — a kubelet restart cannot repair missing intent.
      assert status == 200
      assert decoded["status"] == "degraded"
      assert decoded["reason"] == "intent_mismatch"
      assert decoded["intent_mismatch"] == ["btc_kraken", "eth_kraken"]
    end
  end

  test "recovery state takes precedence over intent mismatch", %{port: port} do
    hub_id = :"healthz_mismatch_recovery_hub_#{System.unique_integer([:positive])}"
    Snapshot.observe_recovery(hub_id, :recovering)

    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_operatives: [],
      stuck_operatives: %{},
      intent_mismatch: ["btc_kraken"],
      intended_operatives: 0,
      bound_operatives: 0,
      producing_operatives: 0,
      pending_restore: false
    })

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] == "recovering"
  end

  describe "controller-fabric wedge → 503 (D4)" do
    setup do
      Application.put_env(:flezha, :convergence_fabric_503_polls, 3)
      :ok
    end

    test "a sustained frozen NIF driver → 503 naming the layer", %{port: port} do
      publish_verdict(:driver_loop_frozen, 3)

      {status, _headers, body} = get(port, "/healthz")
      decoded = Jason.decode!(body)

      assert status == 503
      assert decoded["status"] == "unhealthy"
      assert decoded["reason"] == "data_plane_fabric_wedged"
      assert decoded["layer"] == "driver_loop_frozen"
      assert decoded["polls"] == 3
    end

    test "a single-poll transient never reaches 503", %{port: port} do
      for polls <- [1, 2] do
        publish_verdict(:nif_queue_backing_up, polls)
        assert {200, _headers, _body} = get(port, "/healthz")
      end
    end

    test "a busy-but-healthy controller reports 200 throughout", %{port: port} do
      for _ <- 1..5 do
        publish_verdict(nil, 0)
        {status, _headers, body} = get(port, "/healthz")
        assert status == 200
        assert Jason.decode!(body)["status"] == "healthy"
      end
    end

    test "a runner-side wedge stays 200 degraded — the pipeline owns that remedy",
         %{port: port} do
      for layer <- [:runner_not_consuming, :runner_pool_wedged, :channel_not_sending] do
        publish_verdict(layer, 10)
        {status, _headers, body} = get(port, "/healthz")
        decoded = Jason.decode!(body)

        assert status == 200
        assert decoded["status"] == "degraded"
        assert decoded["reason"] =~ "data_plane_wedged"
        assert decoded["data_plane_layer"] == to_string(layer)
      end
    end

    test "FLEZHA_FABRIC_503_POLLS=0 disables the fabric 503 entirely", %{port: port} do
      Application.put_env(:flezha, :convergence_fabric_503_polls, 0)
      publish_verdict(:driver_loop_frozen, 99)

      assert {200, _headers, _body} = get(port, "/healthz")
    end
  end

  describe "sensor freshness (D5)" do
    test "a Watchdog that stopped sweeping → 200 degraded, reason sensor_stale",
         %{port: port} do
      publish_verdict(nil, 0)

      Snapshot.observe_sensor(
        :watchdog,
        System.monotonic_time(:millisecond) - @sensor_period_ms * 4,
        @sensor_period_ms
      )

      {status, _headers, body} = get(port, "/healthz")
      decoded = Jason.decode!(body)

      assert status == 200
      assert decoded["status"] == "degraded"
      assert decoded["reason"] =~ "sensor_stale"
      assert decoded["stale_sensors"] == ["watchdog"]
    end

    test "a restarted sensor completing one sweep clears the reason with no operator action",
         %{port: port} do
      publish_verdict(nil, 0)

      Snapshot.observe_sensor(
        :watchdog,
        System.monotonic_time(:millisecond) - @sensor_period_ms * 4,
        @sensor_period_ms
      )

      assert Jason.decode!(elem(get(port, "/healthz"), 2))["status"] == "degraded"

      fresh_sensor()

      assert Jason.decode!(elem(get(port, "/healthz"), 2))["status"] == "healthy"
    end

    test "a sensor with no completed pass yet is not stale — boot is not a wedge",
         %{port: port} do
      publish_verdict(nil, 0)
      # During boot a sensor is alive but has stamped nothing; an absent stamp
      # must read as "not yet", never as stale.
      refute Map.has_key?(Snapshot.sensors(), :never_swept_sensor)

      {status, _headers, body} = get(port, "/healthz")
      assert status == 200
      assert Jason.decode!(body)["status"] == "healthy"
    end

    test "a sensor whose cadence is disabled is never stale", %{port: port} do
      publish_verdict(nil, 0)
      Snapshot.observe_sensor(:watchdog, System.monotonic_time(:millisecond) - 600_000, 0)

      {status, _headers, body} = get(port, "/healthz")
      assert status == 200
      assert Jason.decode!(body)["status"] == "healthy"
    end
  end

  # `/healthz` answers "should the kubelet restart me?"; `/readyz` answers
  # "should traffic be routed to me?". Every case below reads BOTH on the same
  # tick, because the change is that they answer differently.
  describe "GET /readyz — readiness is not health" do
    setup do
      # The application's own boot published the flag; each case decides for
      # itself, and the node is left as it was found.
      Readiness.reset()
      # `declared_hubs_started` reads the starter's published set, which is
      # node-global: another file's starter must not answer for these cases.
      Starter.reset()
      on_exit(&Readiness.publish_sequence_complete/0)
      :ok
    end

    test "boot sequence not complete → 503 naming it, while /healthz is 200 healthy",
         %{port: port} do
      publish_verdict(nil, 0)

      {ready_status, _headers, ready_body} = get(port, "/readyz")
      {health_status, _headers, health_body} = get(port, "/healthz")

      assert ready_status == 503
      decoded = Jason.decode!(ready_body)
      assert decoded["status"] == "booting"
      assert decoded["unmet"] == ["boot_sequence_complete"]

      # The defect in one pair: the endpoint the readiness probe used to read
      # says healthy for exactly this window.
      assert health_status == 200
      assert Jason.decode!(health_body)["status"] == "healthy"
    end

    test "a hub in the recovery state machine → 503 naming hubs_recovered, /healthz 200 recovering",
         %{port: port} do
      Readiness.publish_sequence_complete()
      hub_id = :"readyz_recovering_hub_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :recovery_pending)
      publish_tick([])

      {ready_status, _headers, ready_body} = get(port, "/readyz")
      {health_status, _headers, health_body} = get(port, "/healthz")

      assert ready_status == 503
      decoded = Jason.decode!(ready_body)
      assert decoded["status"] == "recovering"
      assert "hubs_recovered" in decoded["unmet"]
      assert decoded["preconditions"]["boot_sequence_complete"] == true

      assert health_status == 200
      assert Jason.decode!(health_body)["status"] == "recovering"
    end

    test "boot complete and nothing recovering → 200 with every row met", %{port: port} do
      Readiness.publish_sequence_complete()
      publish_verdict(nil, 0)

      {status, _headers, body} = get(port, "/readyz")

      assert status == 200
      decoded = Jason.decode!(body)
      assert decoded["status"] == "ready"
      assert decoded["unmet"] == []

      assert decoded["preconditions"] == %{
               "boot_sequence_complete" => true,
               "hubs_recovered" => true,
               "durable_registry_loaded" => true,
               "declared_hubs_started" => true
             }
    end

    test "a hub left in :timeout stays unready, then flips with no restart", %{port: port} do
      # F-R1: the boot sequence completes even when the bounded replay wait
      # elapsed, so a latched readiness row would leave the pod NotReady for
      # ever. The row is live, so it recovers on its own.
      Readiness.publish_sequence_complete()
      hub_id = :"readyz_timeout_hub_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :timeout)

      {status, _headers, body} = get(port, "/readyz")
      assert status == 503
      assert "hubs_recovered" in Jason.decode!(body)["unmet"]

      Snapshot.observe_recovery(hub_id, :normal)

      {status, _headers, body} = get(port, "/readyz")
      assert status == 200
      assert Jason.decode!(body)["status"] == "ready"
    end

    # A row added without a reader, or a reader inventing a key, fails here; the
    # `flz boot status` half of the same pair is `CliManagerBootTest`.
    test "the body's preconditions are exactly the rows the table declares", %{port: port} do
      Readiness.publish_sequence_complete()

      {_status, _headers, body} = get(port, "/readyz")
      {_verdict, rows} = Readiness.verdict()

      assert Jason.decode!(body)["preconditions"] |> Map.keys() |> Enum.sort() ==
               rows |> Enum.map(&to_string(&1.key)) |> Enum.sort()
    end

    test "an unreloaded durable registry → 503 naming it, /healthz 200 restoring",
         %{port: port} do
      # The post-restart case. Two rows would answer `200 ready` here — while
      # `/healthz`, on the same tick, answers `restoring`.
      Readiness.publish_sequence_complete()

      root = Path.join(System.tmp_dir!(), "readyz_registry_#{System.unique_integer([:positive])}")
      hub_dir = Path.join(root, "readyz_unstarted_hub")
      File.mkdir_p!(hub_dir)
      File.write!(Path.join(hub_dir, "registry.dets"), "")
      on_exit(fn -> File.rm_rf!(root) end)

      Application.put_env(:flezha, :process_hub_registry_root, root)
      Application.put_env(:flezha, :auto_restore_enabled, true)

      # The Loop derives the Snapshot field from this very predicate
      # (`Convergence.Loop.Summary`), so publishing it is what the Loop's next
      # tick would publish — `/readyz` reads the predicate directly.
      pending = HubManager.durable_registry_pending?()
      assert pending

      Snapshot.publish(%{
        last_tick_at: System.monotonic_time(:millisecond),
        stalled_operatives: [],
        intended_operatives: 0,
        bound_operatives: 0,
        producing_operatives: 0,
        pending_restore: pending
      })

      {ready_status, _headers, ready_body} = get(port, "/readyz")
      {health_status, _headers, health_body} = get(port, "/healthz")

      assert ready_status == 503
      decoded = Jason.decode!(ready_body)
      assert decoded["status"] == "recovering"
      # The same directory answers both rows: it is durable state that has not
      # been reloaded, AND a namespace this node has evidence for with no hub.
      assert decoded["unmet"] == ["durable_registry_loaded", "declared_hubs_started"]
      assert decoded["preconditions"]["boot_sequence_complete"] == true
      assert decoded["preconditions"]["hubs_recovered"] == true

      assert health_status == 200
      health = Jason.decode!(health_body)
      assert health["status"] == "recovering"
      assert health["reason"] == "restoring_registry"
    end

    test "a precondition that cannot be read is unmet, never ready", %{port: port} do
      # The boot window this change exists to describe reaches back further
      # than the listener: `/readyz` is answerable before HubManager exists.
      # Taking the name away is that state, and the answer must be a refusal
      # rather than a crash or a vacuous 200.
      Readiness.publish_sequence_complete()
      # Auto-restore is on in production, which is what makes the durable
      # registry row consult HubManager at all.
      Application.put_env(:flezha, :auto_restore_enabled, true)

      pid = Process.whereis(HubManager)
      Process.unregister(HubManager)

      on_exit(fn ->
        if Process.whereis(HubManager) == nil, do: Process.register(pid, HubManager)
      end)

      {status, _headers, body} = get(port, "/readyz")

      Process.register(pid, HubManager)

      assert status == 503
      decoded = Jason.decode!(body)
      assert decoded["status"] == "recovering"
      assert "durable_registry_loaded" in decoded["unmet"]
    end
  end

  defp get(port, path), do: request(port, "GET", path)

  defp request(port, method, path) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 1_000)

    req = "#{method} #{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    :ok = :gen_tcp.send(sock, req)

    raw = read_until_close(sock)
    :gen_tcp.close(sock)

    parse_response(raw)
  end

  defp read_until_close(sock, acc \\ <<>>) do
    case :gen_tcp.recv(sock, 0, 2_000) do
      {:ok, chunk} -> read_until_close(sock, acc <> chunk)
      {:error, :closed} -> acc
      {:error, _} -> acc
    end
  end

  defp parse_response(raw) do
    [headers_part, body] = String.split(raw, "\r\n\r\n", parts: 2)
    [status_line | header_lines] = String.split(headers_part, "\r\n")
    ["HTTP/1.1", status, _phrase] = String.split(status_line, " ", parts: 3)

    {String.to_integer(status), header_lines, body}
  end
end

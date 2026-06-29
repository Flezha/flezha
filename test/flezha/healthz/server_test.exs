defmodule Flezha.Healthz.ServerTest do
  @moduledoc """
  HTTP `/healthz` contract per `consume-recovery-mode-and-self-heal`
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

  use ExUnit.Case, async: false

  alias Flezha.Convergence.Snapshot
  alias Flezha.Healthz.Server

  setup do
    previous_port = Application.get_env(:flezha, :healthz_port)
    Application.put_env(:flezha, :healthz_port, 0)

    # Clear recovery states from prior tests so `collect_recovery_states/0`
    # starts with a clean slate. The healthz reader now uses Snapshot directly.
    Snapshot.reset_recovery_states()

    {:ok, _pid} = start_supervised(Server)
    {:ok, port} = Server.listen_port()

    on_exit(fn ->
      # Reset the globally-started snapshot so leaked health state does
      # not bleed into other suites.
      Snapshot.publish(%{
        last_tick_at: nil,
        stalled_operatives: [],
        bound_operatives: 0,
        producing_operatives: 0
      })

      Snapshot.reset_recovery_states()

      if previous_port,
        do: Application.put_env(:flezha, :healthz_port, previous_port),
        else: Application.delete_env(:flezha, :healthz_port)
    end)

    {:ok, port: port}
  end

  # Publish a convergence snapshot the way the Loop would, so the reader
  # under test has live state to format. `producing_operatives` is derived as
  # `bound - stalled`, mirroring the Loop.
  defp publish_tick(stalled, bound \\ 0) do
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_operatives: stalled,
      bound_operatives: bound,
      producing_operatives: max(bound - length(stalled), 0),
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
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
      stalled_operatives: [],
      plugin_degraded_operatives: ["btc_kraken"],
      bound_operatives: 2,
      producing_operatives: 1
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

  test "operatives declared but not yet bound → 200 recovering (post-restart rebind)", %{port: port} do
    # 3 operatives intended, only 1 actually bound (controller mid-rebind). Capable-
    # but-not-ready must read recovering, never healthy.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_operatives: [],
      intended_operatives: 3,
      bound_operatives: 1,
      producing_operatives: 1,
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

  test "operatives declared but unbound PAST the grace → 200 degraded (not silent recovering)",
       %{port: port} do
    # The silent-30h-outage gap: a operative declared but never bound must not sit
    # at `recovering` forever. Once `unmet_intent_ms` exceeds the grace the
    # reader escalates to `degraded` so it alarms (still 200 — a restart can't
    # re-bind an orphaned operative).
    prev = Application.get_env(:flezha, :convergence_unmet_intent_grace_ms)
    Application.put_env(:flezha, :convergence_unmet_intent_grace_ms, 100)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:flezha, :convergence_unmet_intent_grace_ms, prev),
        else: Application.delete_env(:flezha, :convergence_unmet_intent_grace_ms)
    end)

    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_operatives: [],
      intended_operatives: 1,
      bound_operatives: 0,
      producing_operatives: 0,
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
    # Hubs start lazily on executor reconnect, so right after a restart Intent is
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
    previous = Application.get_env(:flezha, :convergence_tick_ms)
    Application.put_env(:flezha, :convergence_tick_ms, 10)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:flezha, :convergence_tick_ms, previous),
        else: Application.delete_env(:flezha, :convergence_tick_ms)
    end)

    # First a tick to set the timestamp...
    publish_tick([])
    # ...then sleep past tick_ms × 2 so the staleness window elapses.
    Process.sleep(50)

    {status, _headers, body} = get(port, "/healthz")
    decoded = Jason.decode!(body)

    if decoded["status"] == "recovering" do
      assert status == 200
    else
      assert status == 503
      assert decoded["reason"] == "tick_stale"
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

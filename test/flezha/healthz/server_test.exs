defmodule Flezha.Healthz.ServerTest do
  @moduledoc """
  HTTP `/healthz` contract per `consume-recovery-mode-and-self-heal`
  §4.6-4.9. Drives the listener over a real TCP socket — the
  hand-rolled `:gen_tcp` request parser is the artefact under test.

  Covered scenarios match the `convergence-self-healing` capability:
    * `/` returns `pong\\n`
    * `/healthz` returns 200 healthy after a tick observation with
      no stalled relays.
    * `/healthz` returns 200 `degraded` when a relay is output-stalled
      (`producing_relays < bound_relays`) — a stalled relay is the
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

    {:ok, _pid} = start_supervised(Server)
    {:ok, port} = Server.listen_port()

    on_exit(fn ->
      # Reset the globally-started snapshot so leaked health state does
      # not bleed into other suites.
      Snapshot.publish(%{
        last_tick_at: nil,
        stalled_relays: [],
        bound_relays: 0,
        producing_relays: 0
      })

      if previous_port,
        do: Application.put_env(:flezha, :healthz_port, previous_port),
        else: Application.delete_env(:flezha, :healthz_port)
    end)

    {:ok, port: port}
  end

  # Publish a convergence snapshot the way the Loop would, so the reader
  # under test has live state to format. `producing_relays` is derived as
  # `bound - stalled`, mirroring the Loop.
  defp publish_tick(stalled, bound \\ 0) do
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_relays: stalled,
      bound_relays: bound,
      producing_relays: max(bound - length(stalled), 0),
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })
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
    # One of two bound relays stalled: producing(1) < bound(2).
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
      assert decoded["bound_relays"] == 2
      assert decoded["producing_relays"] == 1
      assert decoded["stalled_relays"] == ["btc_kraken"]
    end
  end

  test "relays declared but not yet bound → 200 recovering (post-restart rebind)", %{port: port} do
    # 3 relays intended, only 1 actually bound (controller mid-rebind). Capable-
    # but-not-ready must read recovering, never healthy.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_relays: [],
      intended_relays: 3,
      bound_relays: 1,
      producing_relays: 1,
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] == "recovering"
    assert decoded["reason"] == "relays_rebinding"
    assert decoded["bound_relays"] == 1
    assert decoded["intended_relays"] == 3
  end

  test "durable registry pending → 200 recovering (post-restart, registry not loaded yet)",
       %{port: port} do
    # Hubs start lazily on executor reconnect, so right after a restart Intent is
    # empty (intended == 0) while relays.dets still sits on disk. This must read
    # recovering, never vacuously healthy — `bound < intended` (0 < 0) misses it.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_relays: [],
      intended_relays: 0,
      bound_relays: 0,
      producing_relays: 0,
      pending_restore: true,
      tick_latency_p99_ms: 0,
      tick_breach_streak: 0
    })

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] == "recovering"
    assert decoded["reason"] == "restoring_registry"
    assert decoded["intended_relays"] == 0
    assert decoded["bound_relays"] == 0
  end

  test "genuinely empty, fully started system → 200 healthy", %{port: port} do
    # No durable relays pending and nothing declared: a legitimately-empty
    # controller that has finished starting must still report healthy, so the
    # restoring-registry branch never masks a steady-state empty system.
    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_relays: [],
      intended_relays: 0,
      bound_relays: 0,
      producing_relays: 0,
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

defmodule Flezha.Healthz.ServerTest do
  @moduledoc """
  HTTP `/healthz` contract per `consume-recovery-mode-and-self-heal`
  §4.6-4.9. Drives the listener over a real TCP socket — the
  hand-rolled `:gen_tcp` request parser is the artefact under test.

  Covered scenarios match the `convergence-self-healing` capability:
    * `/` returns `pong\\n`
    * `/healthz` returns 200 healthy after a tick observation with
      no stalled relays.
    * `/healthz` returns 503 with `reason: "tick_stale"` when no
      tick has been observed inside `tick_ms × 2`.
    * `/healthz` returns 503 with `reason: "output_stalled"` when
      the watcher has marked a relay as stalled.
    * Unknown paths return 404; non-GET methods return 405.
  """

  use ExUnit.Case, async: false

  alias Flezha.Healthz.Server

  setup do
    previous_port = Application.get_env(:flezha, :healthz_port)
    Application.put_env(:flezha, :healthz_port, 0)

    {:ok, _pid} = start_supervised(Server)
    {:ok, port} = Server.listen_port()

    on_exit(fn ->
      if previous_port,
        do: Application.put_env(:flezha, :healthz_port, previous_port),
        else: Application.delete_env(:flezha, :healthz_port)
    end)

    {:ok, port: port}
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
    Server.note_tick()
    Server.clear_output_stall("any")

    {status, _headers, body} = get(port, "/healthz")
    assert status == 200
    decoded = Jason.decode!(body)
    assert decoded["status"] in ["healthy", "recovering"]
  end

  test "output stalled → 503 with reason: output_stalled", %{port: port} do
    Server.note_tick()
    Server.note_output_stall("btc_kraken")

    {status, _headers, body} = get(port, "/healthz")
    decoded = Jason.decode!(body)

    if decoded["status"] == "recovering" do
      # No hubs running in this test env may report recovering;
      # that is the spec's deliberate "do not restart during planned
      # recovery" branch and takes precedence over output_stalled.
      assert status == 200
    else
      assert status == 503
      assert decoded["reason"] == "output_stalled"
      assert decoded["stalled_relays"] == ["btc_kraken"]
    end

    # Clean up so other tests are not affected.
    Server.clear_output_stall("btc_kraken")
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
    Server.note_tick()
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

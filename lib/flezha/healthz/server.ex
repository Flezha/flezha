defmodule Flezha.Healthz.Server do
  @moduledoc """
  Opt-in HTTP `/healthz` listener. Hand-rolled `:gen_tcp` HTTP/1.1
  server following the pattern in
  `Flezha.Observability.Export.PrometheusHttp` — same idle-timeout
  hygiene, same single-socket accept loop, no Cowboy/Plug dep.

  This is a thin reader of `Flezha.Convergence.Snapshot.current/0` (the
  single published source of truth, design.md invariant I7) plus the
  per-hub recovery cache. It holds no health state of its own and runs
  no telemetry handler — the convergence `Loop` publishes the snapshot
  this server formats.

  Endpoint contract:

    * `GET /healthz` — body JSON, carrying `bound_relays` and
      `producing_relays` as distinct counters.
      - `200 OK` `{"status":"healthy", ...}` when every namespace hub is
        `:normal` and `producing_relays == bound_relays`.
      - `200 OK` `{"status":"degraded", ...}` when
        `producing_relays < bound_relays` (one or more relays output-
        stalled). A stalled relay is NOT a restart signal — kubelet must
        leave the pod alone while the Loop rebinds it.
      - `200 OK` `{"status":"recovering", ...}` when any hub is in
        `:recovery_pending | :recovering` (kubelet must NOT restart
        during a planned recovery).
      - `503 Service Unavailable` `{"status":"unhealthy", ...}` ONLY when
        the convergence Loop itself is wedged (`reason` ∈
        `tick_stale | recovery_failed`): it has stopped ticking
        (`wall - last_tick > tick_ms × 3`) or blew the per-tick latency
        budget for ≥ 3 consecutive ticks.

    * `GET /` — `pong\\n` (cheap probe target).
    * Anything else → `404 / 405 / 400`.

  Gated by `:healthz_enabled` (default `false`); when off the
  application supervisor omits this child entirely.
  """

  use GenServer

  alias Flezha.Convergence.Snapshot
  alias Flezha.Service.HubManager

  @idle_timeout_ms 5_000
  @max_request_line_bytes 8_192

  # ---------- Public API ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Return the listening port — useful for tests that bind to port 0."
  @spec listen_port() :: {:ok, :inet.port_number()} | :error
  def listen_port() do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) -> GenServer.call(pid, :listen_port)
      _ -> :error
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    port = Application.get_env(:flezha, :healthz_port, 9569)
    bind = Application.get_env(:flezha, :healthz_bind, "127.0.0.1")

    {:ok, ip} = :inet.parse_address(String.to_charlist(bind))

    listen_opts = [
      :binary,
      packet: :raw,
      active: false,
      reuseaddr: true,
      ip: ip,
      backlog: 128
    ]

    case :gen_tcp.listen(port, listen_opts) do
      {:ok, listen_socket} ->
        {:ok, actual_port} = :inet.port(listen_socket)
        send(self(), :accept_loop)
        {:ok, %{listen_socket: listen_socket, port: actual_port}}

      {:error, reason} ->
        {:stop, {:listen_failed, reason}}
    end
  end

  @impl true
  def handle_call(:listen_port, _from, %{port: port} = state) do
    {:reply, {:ok, port}, state}
  end

  @impl true
  def handle_info(:accept_loop, %{listen_socket: listen_socket} = state) do
    parent = self()
    spawn(fn -> accept_loop(parent, listen_socket) end)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{listen_socket: ls}) do
    _ = :gen_tcp.close(ls)
    :ok
  end

  # ---------- Acceptor / handler ----------

  defp accept_loop(parent, listen_socket) do
    case :gen_tcp.accept(listen_socket, 1_000) do
      {:ok, socket} ->
        spawn(fn ->
          try do
            serve(socket)
          catch
            _, _ -> :gen_tcp.close(socket)
          end
        end)

        accept_loop(parent, listen_socket)

      {:error, :timeout} ->
        if Process.alive?(parent), do: accept_loop(parent, listen_socket), else: :ok

      {:error, :closed} ->
        :ok

      {:error, _other} ->
        :ok
    end
  end

  defp serve(socket) do
    case read_request_line(socket) do
      {:ok, line} ->
        _ = drain_headers(socket, 5)
        handle_request(socket, line)

      {:error, :timeout} ->
        :gen_tcp.close(socket)

      {:error, _} ->
        _ = send_response(socket, 400, "text/plain", "bad request\n")
        :gen_tcp.close(socket)
    end
  end

  defp read_request_line(socket) do
    :inet.setopts(socket, packet: :line)

    case :gen_tcp.recv(socket, 0, @idle_timeout_ms) do
      {:ok, line} when byte_size(line) > @max_request_line_bytes -> {:error, :too_long}
      {:ok, line} -> {:ok, line}
      err -> err
    end
  end

  defp drain_headers(socket, attempts) when attempts > 0 do
    case :gen_tcp.recv(socket, 0, 500) do
      {:ok, "\r\n"} -> :ok
      {:ok, "\n"} -> :ok
      {:ok, _other} -> drain_headers(socket, attempts - 1)
      _ -> :ok
    end
  end

  defp drain_headers(_socket, _), do: :ok

  defp handle_request(socket, line) do
    case parse_request_line(line) do
      {:get, "/healthz"} -> respond_healthz(socket)
      {:get, "/"} -> send_response(socket, 200, "text/plain", "pong\n")
      {:get, _} -> send_response(socket, 404, "text/plain", "not found\n")
      {:method_not_allowed, _} -> send_response(socket, 405, "text/plain", "method not allowed\n")
      :bad -> send_response(socket, 400, "text/plain", "bad request\n")
    end

    :gen_tcp.close(socket)
  end

  defp parse_request_line(line) do
    case String.split(String.trim(line)) do
      [method, path, "HTTP/" <> _] ->
        case String.upcase(method) do
          "GET" -> {:get, path}
          _ -> {:method_not_allowed, path}
        end

      _ ->
        :bad
    end
  end

  defp respond_healthz(socket) do
    case compute_health() do
      {:healthy, body} -> send_response(socket, 200, "application/json", body)
      {:degraded, body} -> send_response(socket, 200, "application/json", body)
      {:recovering, body} -> send_response(socket, 200, "application/json", body)
      {:unhealthy, body} -> send_response(socket, 503, "application/json", body)
    end
  end

  # Reads the single published convergence snapshot and the per-hub
  # recovery cache. No local state, no telemetry.
  defp compute_health() do
    current = Snapshot.current()
    recovery_states = collect_recovery_states()
    bound = Map.get(current, :bound_relays, 0)
    intended = Map.get(current, :intended_relays, bound)
    producing = Map.get(current, :producing_relays, 0)
    stalled = Map.get(current, :stalled_relays, [])
    last_tick_at = Map.get(current, :last_tick_at)
    breach_streak = Map.get(current, :tick_breach_streak, 0)
    p99 = Map.get(current, :tick_latency_p99_ms)

    cond do
      any_recovering?(recovery_states) ->
        {:recovering,
         encode_json(%{
           status: "recovering",
           recovery_state: pick_recovery_state(recovery_states)
         })}

      any_recovery_failed?(recovery_states) ->
        {:unhealthy, encode_json(%{status: "unhealthy", reason: "recovery_failed"})}

      loop_wedged?(last_tick_at, breach_streak) ->
        {:unhealthy,
         encode_json(%{
           status: "unhealthy",
           reason: "tick_stale",
           tick_age_ms: tick_age(last_tick_at),
           tick_latency_p99_ms: p99,
           tick_breach_streak: breach_streak
         })}

      bound < intended ->
        # Declared relays are not all bound yet (e.g. controller just restarted
        # and the Loop is still rebinding them). Capable-but-not-ready ⇒
        # recovering, never "healthy": kubelet must not restart mid-rebind.
        {:recovering,
         encode_json(%{
           status: "recovering",
           reason: "relays_rebinding",
           intended_relays: intended,
           bound_relays: bound,
           producing_relays: producing
         })}

      producing < bound ->
        {:degraded,
         encode_json(%{
           status: "degraded",
           tick_age_ms: tick_age(last_tick_at),
           bound_relays: bound,
           producing_relays: producing,
           stalled_relays: stalled
         })}

      true ->
        {:healthy,
         encode_json(%{
           status: "healthy",
           tick_age_ms: tick_age(last_tick_at),
           bound_relays: bound,
           producing_relays: producing
         })}
    end
  end

  defp collect_recovery_states() do
    HubManager.list_running_hub_ids()
    |> Enum.map(fn hub_id ->
      case HubManager.recovery_state(hub_id) do
        {:ok, state} -> state
        _ -> :unknown
      end
    end)
  end

  defp any_recovering?(states), do: Enum.any?(states, &(&1 in [:recovery_pending, :recovering]))

  defp any_recovery_failed?(states), do: Enum.any?(states, &(&1 == :timeout))

  defp pick_recovery_state([]), do: "unknown"

  defp pick_recovery_state(states) do
    cond do
      :recovering in states -> "recovering"
      :recovery_pending in states -> "recovery_pending"
      :timeout in states -> "timeout"
      true -> Atom.to_string(List.first(states))
    end
  end

  # 503 ONLY when the Loop is genuinely wedged. Two independent signals,
  # either of which means kubelet should restart the pod:
  #   * it has stopped ticking (`wall - last_tick > tick_ms × 3`), or
  #   * it blew the per-tick latency budget for ≥ 3 consecutive ticks.
  # A merely output-stalled relay is `degraded`, never wedged — that
  # hair-trigger SIGTERM'd healthy pods (proposal §2).
  defp loop_wedged?(nil, _streak), do: false

  defp loop_wedged?(last_tick_at, breach_streak) do
    tick_ms = Application.get_env(:flezha, :convergence_tick_ms, 5_000)
    now_ms() - last_tick_at > tick_ms * 3 or breach_streak >= 3
  end

  defp tick_age(nil), do: nil
  defp tick_age(last_tick_at), do: now_ms() - last_tick_at

  defp now_ms(), do: System.monotonic_time(:millisecond)

  defp send_response(socket, status, content_type, body) do
    body_bin = IO.iodata_to_binary(body)
    size = byte_size(body_bin)

    response = [
      "HTTP/1.1 ",
      Integer.to_string(status),
      " ",
      status_text(status),
      "\r\n",
      "Content-Type: ",
      content_type,
      "\r\n",
      "Content-Length: ",
      Integer.to_string(size),
      "\r\n",
      "Connection: close\r\n",
      "\r\n",
      body_bin
    ]

    _ = :gen_tcp.send(socket, response)
    :ok
  end

  defp status_text(200), do: "OK"
  defp status_text(400), do: "Bad Request"
  defp status_text(404), do: "Not Found"
  defp status_text(405), do: "Method Not Allowed"
  defp status_text(503), do: "Service Unavailable"

  defp encode_json(map), do: Jason.encode!(map)
end

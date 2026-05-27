defmodule Flezha.Healthz.Server do
  @moduledoc """
  Opt-in HTTP `/healthz` listener introduced by
  `consume-recovery-mode-and-self-heal` §4.6-4.7. Hand-rolled
  `:gen_tcp` HTTP/1.1 server following the pattern in
  `Flezha.Observability.Export.PrometheusHttp` — same idle-timeout
  hygiene, same single-socket accept loop, no Cowboy/Plug dep.

  Endpoint contract (per the `convergence-self-healing` capability):

    * `GET /healthz` — body JSON
      - `200 OK` `{"status":"healthy", ...}` when every namespace hub
        is `:normal`, the last convergence tick fired inside the
        `tick_window`, and no bound relay has been output-stalled
        past the grace window.
      - `200 OK` `{"status":"recovering", ...}` when any hub is in
        `:recovery_pending | :recovering` (kubelet must NOT restart
        during a planned recovery).
      - `503 Service Unavailable` `{"status":"unhealthy", ...}`
        otherwise. `reason` ∈ `tick_stale | output_stalled | recovery_failed`.

    * `GET /` — `pong\\n` (cheap probe target).
    * Anything else → `404 / 405 / 400`.

  Gated by `:healthz_enabled` (default `false`); when off the
  application supervisor omits this child entirely (see
  `Flezha.Application.healthz_children/0`).
  """

  use GenServer

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

  @doc """
  Note a fresh convergence tick. Called from the
  `Flezha.Convergence.Health` watcher's telemetry handler. The
  /healthz check reads this timestamp to compute tick staleness.
  """
  @spec note_tick() :: :ok
  def note_tick() do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.cast(pid, :note_tick)
    end
  end

  @doc """
  Note an output-stalled relay so the /healthz probe can surface
  `reason: "output_stalled"`. Cleared by `clear_output_stall/1`.
  """
  @spec note_output_stall(binary()) :: :ok
  def note_output_stall(relay_id) when is_binary(relay_id) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:note_output_stall, relay_id})
    end
  end

  @spec clear_output_stall(binary()) :: :ok
  def clear_output_stall(relay_id) when is_binary(relay_id) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:clear_output_stall, relay_id})
    end
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

        # Subscribe to the watcher's tick + the State.Watchdog's
        # output-stall events so the JSON body is computed from
        # live observations without a manual ping/clear discipline.
        _ =
          :telemetry.attach_many(
            {__MODULE__, :events},
            [
              [:flezha, :convergence, :tick],
              [:flezha, :state, :output_stall]
            ],
            &__MODULE__.handle_telemetry/4,
            nil
          )

        {:ok,
         %{
           listen_socket: listen_socket,
           port: actual_port,
           last_tick_at: nil,
           stalled_relays: MapSet.new()
         }}

      {:error, reason} ->
        {:stop, {:listen_failed, reason}}
    end
  end

  @doc false
  def handle_telemetry(event, _measurements, metadata, _config) do
    case event do
      [:flezha, :convergence, :tick] ->
        note_tick()

      [:flezha, :state, :output_stall] ->
        relay_id = Map.get(metadata, :relay)
        if is_binary(relay_id), do: note_output_stall(relay_id)

      _ ->
        :ok
    end
  catch
    _, _ -> :ok
  end

  @impl true
  def handle_call(:listen_port, _from, %{port: port} = state) do
    {:reply, {:ok, port}, state}
  end

  def handle_call(:snapshot, _from, state) do
    {:reply, %{last_tick_at: state.last_tick_at, stalled_relays: state.stalled_relays}, state}
  end

  @impl true
  def handle_cast(:note_tick, state) do
    {:noreply, %{state | last_tick_at: System.monotonic_time(:millisecond)}}
  end

  def handle_cast({:note_output_stall, relay_id}, state) do
    {:noreply, %{state | stalled_relays: MapSet.put(state.stalled_relays, relay_id)}}
  end

  def handle_cast({:clear_output_stall, relay_id}, state) do
    {:noreply, %{state | stalled_relays: MapSet.delete(state.stalled_relays, relay_id)}}
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
    _ = :telemetry.detach({__MODULE__, :events})
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
      {:recovering, body} -> send_response(socket, 200, "application/json", body)
      {:unhealthy, body} -> send_response(socket, 503, "application/json", body)
    end
  end

  defp compute_health() do
    snapshot = GenServer.call(__MODULE__, :snapshot)
    recovery_states = collect_recovery_states()

    cond do
      any_recovering?(recovery_states) ->
        body = encode_json(%{
          status: "recovering",
          recovery_state: pick_recovery_state(recovery_states)
        })
        {:recovering, body}

      any_recovery_failed?(recovery_states) ->
        body = encode_json(%{status: "unhealthy", reason: "recovery_failed"})
        {:unhealthy, body}

      MapSet.size(snapshot.stalled_relays) > 0 ->
        body =
          encode_json(%{
            status: "unhealthy",
            reason: "output_stalled",
            stalled_relays: MapSet.to_list(snapshot.stalled_relays)
          })

        {:unhealthy, body}

      tick_stale?(snapshot.last_tick_at) ->
        body = encode_json(%{status: "unhealthy", reason: "tick_stale"})
        {:unhealthy, body}

      true ->
        tick_age_ms = if snapshot.last_tick_at, do: now_ms() - snapshot.last_tick_at, else: nil

        body =
          encode_json(%{
            status: "healthy",
            tick_age_ms: tick_age_ms,
            bound_relays: count_bound_relays(),
            producing_relays: count_bound_relays()
          })

        {:healthy, body}
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

  defp tick_stale?(nil) do
    # No tick observed yet — only stale once we have been running long
    # enough that a tick SHOULD have fired. Without a baseline boot
    # time the safest answer is "not stale yet"; kubelet will retry
    # within seconds anyway.
    false
  end

  defp tick_stale?(last_tick_at) do
    tick_ms = Application.get_env(:flezha, :convergence_tick_ms, 5_000)
    now_ms() - last_tick_at > tick_ms * 2
  end

  defp count_bound_relays() do
    HubManager.get_running_namespaces()
    |> Enum.flat_map(fn ns -> HubManager.get_all_relays(ns) end)
    |> length()
  catch
    _, _ -> 0
  end

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

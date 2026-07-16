defmodule Flezha.Healthz.Server do
  @moduledoc """
  Opt-in HTTP `/healthz` listener, built on `Flezha.Http.Listener`. A thin
  reader of `Flezha.Convergence.Snapshot.current/0` (the single published
  source of truth) plus the per-hub recovery cache; it holds no health
  state of its own.

  Endpoint contract:

    * `GET /healthz` — JSON body carrying `bound_operatives` and
      `producing_operatives` as distinct counters.
      - `200 OK` `{"status":"healthy", ...}` when every namespace hub is
        `:normal` and `producing_operatives == bound_operatives`.
        `producing` derives from durable progress markers with the fed
        precondition: an IDLE operative (no input, flat markers) counts as
        producing; a STUCK one (fed but flat past grace) does not.
      - `200 OK` `{"status":"degraded", ...}` when
        `producing_operatives < bound_operatives` (operatives output-stalled or
        STUCK — per-operative detail `wal_frozen | watermark_frozen |
        outbox_flat` in `stuck_operatives`) or when `intent_mismatch` is
        non-empty (durable state evidence for operatives absent from the
        live intended set — a wiped registry must never read healthy).
        Neither is a restart signal — kubelet must leave the pod alone
        while the Loop rebinds / the operator resumes or deletes state.
      - `200 OK` `{"status":"recovering", ...}` when any hub is in
        `:recovery_pending | :recovering` (kubelet must NOT restart
        during a planned recovery).
      - `503 Service Unavailable` `{"status":"unhealthy", ...}` ONLY when
        the convergence Loop itself is wedged (`reason` ∈
        `tick_stale | recovery_failed`).

    * `GET /` — `pong\\n` (cheap probe target).
    * Anything else → `404 / 405 / 400`.

  Gated by `:healthz_enabled` (default `false`); when off the
  application supervisor omits this child entirely.
  """

  use GenServer

  alias Flezha.Convergence.Snapshot
  alias Flezha.Http.Listener

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

    case Listener.listen(port, bind) do
      {:ok, listen_socket, actual_port} ->
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
    spawn(fn -> Listener.accept_loop(parent, listen_socket, &route/2) end)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{listen_socket: ls}) do
    _ = :gen_tcp.close(ls)
    :ok
  end

  # ---------- routing ----------

  defp route(socket, {:get, "/healthz"}), do: respond_healthz(socket)
  defp route(socket, {:get, "/"}), do: Listener.send_response(socket, 200, "text/plain", "pong\n")

  defp route(socket, {:get, _}),
    do: Listener.send_response(socket, 404, "text/plain", "not found\n")

  defp route(socket, {:method_not_allowed, _}),
    do: Listener.send_response(socket, 405, "text/plain", "method not allowed\n")

  defp route(socket, :bad), do: Listener.send_response(socket, 400, "text/plain", "bad request\n")

  defp respond_healthz(socket) do
    {status, body} = compute_health()
    code = if status == :unhealthy, do: 503, else: 200
    Listener.send_response(socket, code, "application/json", body)
  end

  # Reads the single published convergence snapshot and the per-hub
  # recovery cache. No local state, no telemetry.
  defp compute_health() do
    current = Snapshot.current()
    recovery_states = collect_recovery_states()
    bound = Map.get(current, :bound_operatives, 0)
    intended = Map.get(current, :intended_operatives, bound)
    producing = Map.get(current, :producing_operatives, 0)
    stalled = Map.get(current, :stalled_operatives, [])
    stuck = Map.get(current, :stuck_operatives, %{})
    plugin_degraded = Map.get(current, :plugin_degraded_operatives, [])
    mismatch = Map.get(current, :intent_mismatch, [])
    pending_restore = Map.get(current, :pending_restore, false)
    unmet_intent_ms = Map.get(current, :unmet_intent_ms)
    last_tick_at = Map.get(current, :last_tick_at)

    cond do
      any_recovering?(recovery_states) ->
        {:recovering,
         encode_json(%{
           status: "recovering",
           recovery_state: pick_recovery_state(recovery_states)
         })}

      loop_hard_wedged?(last_tick_at) ->
        {:unhealthy,
         encode_json(%{
           status: "unhealthy",
           reason: "tick_stale",
           tick_age_ms: tick_age(last_tick_at)
         })}

      loop_tick_gapped?(last_tick_at) ->
        # One large tick gap means the whole BEAM was suspended (host
        # sleep/hibernate, `docker pause`, long GC) — the Loop is resuming, not
        # wedged. Report recovering so kubelet does not restart during a planned
        # wake; this escalates to 503 only once the gap exceeds the hard bound.
        {:recovering,
         encode_json(%{
           status: "recovering",
           reason: "tick_resuming",
           tick_age_ms: tick_age(last_tick_at)
         })}

      bound < intended ->
        # Declared but not all bound: recovering within the grace, degraded
        # past it. Always 200 — a restart can't re-bind a runner-orphaned operative.
        if is_integer(unmet_intent_ms) and unmet_intent_ms > unmet_intent_grace_ms() do
          {:degraded,
           encode_json(%{
             status: "degraded",
             reason: "operatives_unbound",
             intended_operatives: intended,
             bound_operatives: bound,
             producing_operatives: producing,
             unmet_intent_ms: unmet_intent_ms
           })}
        else
          {:recovering,
           encode_json(%{
             status: "recovering",
             reason: "operatives_rebinding",
             intended_operatives: intended,
             bound_operatives: bound,
             producing_operatives: producing
           })}
        end

      pending_restore ->
        # Intent is empty (intended == 0) only because hubs start lazily on
        # runner reconnect and the durable registry has not been read back
        # since this controller (re)started — operatives.dets exists on disk for a
        # hub that is not running yet. `bound < intended` cannot catch this
        # (0 < 0 is false), so without this the system reports vacuously
        # `healthy`. Recovering, never healthy, until the registry is loaded.
        {:recovering,
         encode_json(%{
           status: "recovering",
           reason: "restoring_registry",
           intended_operatives: intended,
           bound_operatives: bound,
           producing_operatives: producing
         })}

      # Never 503 on intent mismatch: the data plane may be legitimately
      # stopped, and a kubelet restart cannot repair missing intent.
      producing < bound or mismatch != [] ->
        {:degraded,
         encode_json(%{
           status: "degraded",
           reason: degraded_reason(stalled, plugin_degraded, stuck, mismatch),
           tick_age_ms: tick_age(last_tick_at),
           bound_operatives: bound,
           producing_operatives: producing,
           stalled_operatives: stalled,
           stuck_operatives: stuck,
           plugin_degraded_operatives: plugin_degraded,
           intent_mismatch: mismatch
         })}

      true ->
        {:healthy,
         encode_json(%{
           status: "healthy",
           tick_age_ms: tick_age(last_tick_at),
           bound_operatives: bound,
           producing_operatives: producing
         })}
    end
  end

  # One composed reason string; "output_stalled" is the fallback when a
  # producing shortfall carries no per-operative attribution.
  defp degraded_reason(stalled, plugin_degraded, stuck, mismatch) do
    [
      if(stalled != [] or map_size(stuck) > 0, do: "output_stalled"),
      if(plugin_degraded != [], do: "plugin_unhealthy"),
      if(mismatch != [], do: "intent_mismatch")
    ]
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> "output_stalled"
      parts -> Enum.join(parts, "+")
    end
  end

  defp collect_recovery_states() do
    Snapshot.recovery_states() |> Map.values()
  end

  defp any_recovering?(states),
    do: Enum.any?(states, &(&1 in [:recovery_pending, :recovering, :timeout]))

  defp pick_recovery_state([]), do: "unknown"

  defp pick_recovery_state(states) do
    cond do
      :recovering in states -> "recovering"
      :recovery_pending in states -> "recovery_pending"
      :timeout in states -> "timeout"
      true -> Atom.to_string(List.first(states))
    end
  end

  # 503 only when the Loop has genuinely STOPPED ticking far longer than any
  # pause/wake could explain. Tick age is the only authoritative liveness
  # signal: a slow-but-completing tick is alive, not wedged, so escalating it
  # would make kubelet kill a producing pod. Shorter freezes are `recovering`
  # via the gapped grace below; a merely output-stalled operative is `degraded`.
  defp loop_hard_wedged?(nil), do: false
  defp loop_hard_wedged?(last_tick_at), do: now_ms() - last_tick_at > tick_ms() * 24

  # A single tick gap past the normal cadence but within the hard bound — treat
  # as a freeze/wake in progress (recovering), not a wedge.
  defp loop_tick_gapped?(nil), do: false
  defp loop_tick_gapped?(last_tick_at), do: now_ms() - last_tick_at > tick_ms() * 3

  defp tick_ms(), do: Application.get_env(:flezha, :convergence_tick_ms, 5_000)

  defp unmet_intent_grace_ms(),
    do: Application.get_env(:flezha, :convergence_unmet_intent_grace_ms, 120_000)

  defp tick_age(nil), do: nil
  defp tick_age(last_tick_at), do: now_ms() - last_tick_at

  defp now_ms(), do: System.monotonic_time(:millisecond)

  defp encode_json(map), do: Jason.encode!(map)
end

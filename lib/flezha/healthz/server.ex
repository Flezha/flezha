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
        outbox_flat` in `stuck_operatives`), when `intent_mismatch` is
        non-empty (durable state evidence for operatives absent from the
        live intended set — a wiped registry must never read healthy), or
        when a periodic sensor has stopped completing passes
        (`sensor_stale`). None is a restart signal — kubelet must leave the
        pod alone while the Loop rebinds / the operator resumes or deletes
        state.
      - `200 OK` `{"status":"recovering", ...}` when any hub is in
        `:recovery_pending | :recovering` (kubelet must NOT restart
        during a planned recovery).
      - `503 Service Unavailable` `{"status":"unhealthy", ...}` ONLY when
        the convergence Loop itself is wedged (`reason` ∈
        `tick_stale | recovery_failed`) or the controller's own data-plane
        fabric is (`data_plane_fabric_wedged` — a NIF driver thread no
        BEAM-level heal can reach, sustained for
        `:convergence_fabric_503_polls` classifier polls). A verdict the
        convergence pipeline can heal stays `degraded`.

    * `GET /` — `pong\\n` (cheap probe target).
    * Anything else → `404 / 405 / 400`.

  Every response also carries `durability`: `remote_tier` when a durability
  backend is configured, `local_only` when this node's disk is the only copy of
  its durable state. It never changes the verdict — local-only is a supported
  posture — but a fleet audit has to be able to find it.

  Gated by `:healthz_enabled` (default `false`); when off the
  application supervisor omits this child entirely.
  """

  use GenServer

  alias Flezha.Convergence.{Snapshot, StallClassifier}
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

  # `compute_health/0` decides the verdict and the fields that explain it; the
  # facts every response carries whatever the verdict are merged in here, so a
  # branch cannot forget one. `durability` names the posture rather than the
  # backend: `local_only` is a supported choice, and one an operator must be
  # able to find across a fleet without inspecting each node's configuration.
  defp respond_healthz(socket) do
    {status, body} = compute_health()
    code = if status == :unhealthy, do: 503, else: 200

    body =
      body
      |> Map.put(:durability, to_string(Flezha.State.DurabilityTier.posture()))
      |> Jason.encode!()

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
    rpo_breaches = Flezha.State.DurabilityTier.breaches()
    stale_sensors = stale_sensors()
    data_plane = data_plane_verdict(current)

    degraded =
      degraded_reasons(stalled, plugin_degraded, stuck, mismatch, stale_sensors, data_plane)

    cond do
      any_recovering?(recovery_states) ->
        {:recovering,
         %{
           status: "recovering",
           recovery_state: pick_recovery_state(recovery_states)
         }}

      loop_hard_wedged?(last_tick_at) ->
        {:unhealthy,
         %{
           status: "unhealthy",
           reason: "tick_stale",
           tick_age_ms: tick_age(last_tick_at)
         }}

      # The controller's own driver thread is wedged and has stayed wedged.
      # Ranked with `tick_stale`, above every degraded case: a heal issued from
      # this controller would travel through the very thread that is not
      # moving, so only the orchestrator's restart can clear it.
      match?({:fabric, _layer, _polls}, data_plane) ->
        {:fabric, layer, polls} = data_plane

        {:unhealthy,
         %{
           status: "unhealthy",
           reason: "data_plane_fabric_wedged",
           layer: to_string(layer),
           polls: polls
         }}

      loop_tick_gapped?(last_tick_at) ->
        # One large tick gap means the whole BEAM was suspended (host
        # sleep/hibernate, `docker pause`, long GC) — the Loop is resuming, not
        # wedged. Report recovering so kubelet does not restart during a planned
        # wake; this escalates to 503 only once the gap exceeds the hard bound.
        {:recovering,
         %{
           status: "recovering",
           reason: "tick_resuming",
           tick_age_ms: tick_age(last_tick_at)
         }}

      bound < intended ->
        # Declared but not all bound: recovering within the grace, degraded
        # past it. Always 200 — a restart can't re-bind a runner-orphaned operative.
        if is_integer(unmet_intent_ms) and unmet_intent_ms > unmet_intent_grace_ms() do
          {:degraded,
           %{
             status: "degraded",
             reason: "operatives_unbound",
             intended_operatives: intended,
             bound_operatives: bound,
             producing_operatives: producing,
             unmet_intent_ms: unmet_intent_ms
           }}
        else
          {:recovering,
           %{
             status: "recovering",
             reason: "operatives_rebinding",
             intended_operatives: intended,
             bound_operatives: bound,
             producing_operatives: producing
           }}
        end

      pending_restore ->
        # Intent is empty (intended == 0) only because hubs start lazily on
        # runner reconnect and the durable registry has not been read back
        # since this controller (re)started — operatives.dets exists on disk for a
        # hub that is not running yet. `bound < intended` cannot catch this
        # (0 < 0 is false), so without this the system reports vacuously
        # `healthy`. Recovering, never healthy, until the registry is loaded.
        {:recovering,
         %{
           status: "recovering",
           reason: "restoring_registry",
           intended_operatives: intended,
           bound_operatives: bound,
           producing_operatives: producing
         }}

      # A remote-durability RPO breach is degraded, never 503: local durability
      # is intact and a kubelet restart cannot reach the backend that is not
      # answering. The alarm belongs on the probe all the same — a widening RPO
      # that is only visible in the backend is the failure this tier exists to
      # prevent.
      rpo_breaches != [] ->
        {:degraded,
         %{
           status: "degraded",
           reason: "durability_rpo_exceeded",
           rpo_breached_refs: length(rpo_breaches),
           bound_operatives: bound,
           producing_operatives: producing
         }}

      # Never 503 on intent mismatch or a dead sensor: the data plane may be
      # legitimately stopped, and a kubelet restart repairs neither missing
      # intent nor a supervisor-restartable sensor.
      producing < bound or degraded != [] ->
        {:degraded,
         %{
           status: "degraded",
           reason: degraded_reason(degraded),
           tick_age_ms: tick_age(last_tick_at),
           bound_operatives: bound,
           producing_operatives: producing,
           stalled_operatives: stalled,
           stuck_operatives: stuck,
           plugin_degraded_operatives: plugin_degraded,
           intent_mismatch: mismatch,
           stale_sensors: stale_sensors,
           data_plane_layer: pipeline_layer(data_plane)
         }}

      true ->
        {:healthy,
         %{
           status: "healthy",
           tick_age_ms: tick_age(last_tick_at),
           bound_operatives: bound,
           producing_operatives: producing
         }}
    end
  end

  # Every reason this tick is degraded. The list IS the branch condition, so
  # the condition and the reported reason cannot drift apart.
  defp degraded_reasons(stalled, plugin_degraded, stuck, mismatch, stale_sensors, data_plane) do
    [
      if(stalled != [] or map_size(stuck) > 0, do: "output_stalled"),
      if(plugin_degraded != [], do: "plugin_unhealthy"),
      if(mismatch != [], do: "intent_mismatch"),
      if(stale_sensors != [], do: "sensor_stale"),
      if(match?({:pipeline, _layer}, data_plane), do: "data_plane_wedged")
    ]
    |> Enum.reject(&is_nil/1)
  end

  # A producing shortfall with no per-operative attribution still owes a reason.
  defp degraded_reason([]), do: "output_stalled"
  defp degraded_reason(parts), do: Enum.join(parts, "+")

  # A sensor is stale once its last completed pass is older than 3× its own
  # cadence — enough headroom for one missed pass under load, short enough that
  # a wedged sensor shows up within a poll. A sensor that has never stamped is
  # NOT stale (it may not have run yet, which boot legitimately looks like), and
  # neither is one whose cadence is 0 (deliberately disabled).
  defp stale_sensors() do
    now = now_ms()

    for {name, {at, period}} <- Snapshot.sensors(),
        period > 0,
        now - at > period * 3,
        do: to_string(name)
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

  # This tick's data-plane verdict, split by who owns the remedy: `:fabric` is
  # the orchestrator's (a 503), `:pipeline` is the convergence loop's (degraded,
  # never a restart signal — that would interrupt the heal already under way).
  #
  # A fabric verdict counts only once it has held for the configured number of
  # consecutive classifier polls; `0` disables the fabric 503 entirely, which is
  # also the rollback switch for the whole behaviour.
  defp data_plane_verdict(current) do
    layer = Map.get(current, :data_plane_layer)
    polls = Map.get(current, :data_plane_streak, 0)
    window = fabric_503_polls()

    case StallClassifier.layer_scope(layer) do
      nil -> nil
      :fabric -> if window > 0 and polls >= window, do: {:fabric, layer, polls}
      _ -> {:pipeline, layer}
    end
  end

  defp pipeline_layer({:pipeline, layer}), do: to_string(layer)
  defp pipeline_layer(_verdict), do: nil

  defp fabric_503_polls(),
    do: Application.get_env(:flezha, :convergence_fabric_503_polls, 3)

  defp tick_ms(), do: Application.get_env(:flezha, :convergence_tick_ms, 5_000)

  defp unmet_intent_grace_ms(),
    do: Application.get_env(:flezha, :convergence_unmet_intent_grace_ms, 120_000)

  defp tick_age(nil), do: nil
  defp tick_age(last_tick_at), do: now_ms() - last_tick_at

  defp now_ms(), do: System.monotonic_time(:millisecond)
end

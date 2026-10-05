defmodule Flezha.Healthz.Server do
  @moduledoc """
  Opt-in HTTP `/healthz` listener, built on `Flezha.Http.Listener`. A thin
  reader of `Flezha.Convergence.Snapshot.current/0` (the single published
  source of truth) plus the per-hub recovery cache; it holds no health
  state of its own.

  Endpoint contract:

    * `GET /healthz` — JSON body carrying `bound_relays` and
      `producing_relays` as distinct counters.
      - `200 OK` `{"status":"healthy", ...}` when every namespace hub is
        `:normal` and `producing_relays == bound_relays`.
        `producing` derives from durable progress markers with the fed
        precondition: an IDLE relay (no input, flat markers) counts as
        producing; a STUCK one (fed but flat past grace) does not.
      - `200 OK` `{"status":"degraded", ...}` when
        `producing_relays < bound_relays` (relays output-stalled or
        STUCK — per-relay detail `wal_frozen | watermark_frozen |
        outbox_flat` in `stuck_relays`), when `intent_mismatch` is
        non-empty (durable state evidence for relays absent from the
        live intended set — a wiped registry must never read healthy), or
        when a periodic sensor has stopped completing passes
        (`sensor_stale`). None is a restart signal — kubelet must leave the
        pod alone while the Loop rebinds / the user resumes or deletes
        state.
      - `200 OK` `{"status":"recovering", ...}` when any hub is still in the
        recovery state machine (kubelet must NOT restart during a planned
        recovery).
      - `503 Service Unavailable` `{"status":"unhealthy", ...}` ONLY when
        the convergence Loop itself is wedged (`reason` ∈
        `tick_stale | recovery_failed`) or the controller's own data-plane
        fabric is (`data_plane_fabric_wedged` — a NIF driver thread no
        BEAM-level heal can reach, sustained for
        `:convergence_fabric_503_polls` classifier polls). A verdict the
        convergence pipeline can heal stays `degraded`.

        The fabric clause needs a NIF driver to judge. The UDS and the QUIC
        server driver both produce the self-report, so a controller on either
        transport is covered. The `:native` in-node transport runs no driver,
        so `data_plane_fabric_wedged` cannot be minted on a native-only
        controller and a `200` there is evidence about the loop, the
        relays and the plugins — not about the data plane.

    * `GET /readyz` — the readiness verdict of `Flezha.Boot.Readiness`, a
      different question with a different answer: health says whether the
      kubelet should restart this node, readiness says whether traffic should
      be routed to it. `200 OK` only on `:ready`; `503` while the boot
      sequence is running, while a hub is in the recovery state machine, and
      while the durable registry is unreloaded — exactly the window `/healthz`
      must keep answering `200`. Shares this listener, port and enable switch,
      and reads nothing from the health ladder.

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

  alias Flezha.Boot.Readiness
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Convergence.{RecoveryGate, Snapshot, StallClassifier, Verdict}
  alias Flezha.Http.Listener
  alias Flezha.SDK.SafeCall
  alias Flezha.State.DurabilityTier

  # ---------- Public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  The bind address the endpoint listens on when nothing configures one.

  `0.0.0.0` so a kubelet `httpGet` probe reaches the pod IP; it is the ONE
  default, read here and by `config/runtime.exs`. A test environment states
  its loopback bind in `config/test.exs` rather than shadowing this.
  """
  @spec default_bind() :: binary()
  def default_bind(), do: "0.0.0.0"

  @doc "Return the listening port — useful for tests that bind to port 0."
  @spec listen_port() :: {:ok, :inet.port_number()} | :error
  def listen_port() do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        SafeCall.bounded(pid, :listen_port, Durations.get(:listen_port_call_ms), on_error: :error)

      _ ->
        :error
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    port = Config.get(:healthz_port)
    bind = Config.get(:healthz_bind)

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
  defp route(socket, {:get, "/readyz"}), do: respond_readyz(socket)
  defp route(socket, {:get, "/"}), do: Listener.send_response(socket, 200, "text/plain", "pong\n")

  defp route(socket, {:get, _}),
    do: Listener.send_response(socket, 404, "text/plain", "not found\n")

  defp route(socket, {:method_not_allowed, _}),
    do: Listener.send_response(socket, 405, "text/plain", "method not allowed\n")

  defp route(socket, :bad), do: Listener.send_response(socket, 400, "text/plain", "bad request\n")

  # `compute_health/0` decides the verdict and the fields that explain it; the
  # facts every response carries whatever the verdict are merged in here, so a
  # branch cannot forget one. `durability` names the posture rather than the
  # backend: `local_only` is a supported choice, and one a user must be
  # able to find across a fleet without inspecting each node's configuration.
  defp respond_healthz(socket) do
    {status, body} = compute_health()
    code = Verdict.http_status(status)

    body =
      body
      |> Map.put(:durability, to_string(DurabilityTier.posture()))
      |> Jason.encode!()

    Listener.send_response(socket, code, "application/json", body)
  end

  # The readiness answer, folded from `Boot.Readiness`'s precondition table.
  # It reads no fact `compute_health/0` reads and shares no arm with it: the
  # two endpoints answer different questions, and the body names every row so
  # a user can see which precondition is holding the node back.
  defp respond_readyz(socket) do
    {verdict, rows} = Readiness.verdict()
    code = if verdict == :ready, do: 200, else: 503

    body =
      Jason.encode!(%{
        status: to_string(verdict),
        unmet: for(%{key: key, met: false} <- rows, do: to_string(key)),
        preconditions: Map.new(rows, fn %{key: key, met: met} -> {key, met} end)
      })

    Listener.send_response(socket, code, "application/json", body)
  end

  # Reads the single published convergence snapshot and the per-hub recovery
  # cache, and RENDERS what `Verdict.fold/2` decides. No local state, no
  # telemetry, and no verdict word: the ranking, the words and the reasons are
  # the record module's, so this endpoint and the CLI can never drift apart.
  defp compute_health() do
    current = Snapshot.current()
    Verdict.fold(Map.get(current, :relay_health, %{}), facts(current))
  end

  # The tick's NON-verdict facts: everything the fold weighs that is about the
  # node rather than about a subject. Sampled once so no two arms can disagree
  # about the same tick.
  defp facts(current) do
    recovery = RecoveryGate.recovery_summary()
    last_tick_at = Map.get(current, :last_tick_at)

    %{
      recovery_states: recovery.states,
      recovering?: recovery.recovering?,
      tick_stale?: loop_hard_wedged?(last_tick_at),
      tick_gapped?: loop_tick_gapped?(last_tick_at),
      tick_age_ms: tick_age(last_tick_at),
      stuck: Map.get(current, :stuck_relays, %{}),
      intent_mismatch: Map.get(current, :intent_mismatch, []),
      pending_restore: Map.get(current, :pending_restore, false),
      rpo_breaches: DurabilityTier.breaches(),
      stale_sensors: stale_sensors(),
      data_plane: data_plane_verdict(current)
    }
    |> Map.merge(Snapshot.unmet_intent(current))
  end

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

  # 503 only when the Loop has genuinely STOPPED ticking far longer than any
  # pause/wake could explain. Tick age is the only authoritative liveness
  # signal: a slow-but-completing tick is alive, not wedged, so escalating it
  # would make kubelet kill a producing pod. Shorter freezes are `recovering`
  # via the gapped grace below; a merely output-stalled relay is `degraded`.
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

  defp fabric_503_polls(),
    do: Config.get(:convergence_fabric_503_polls)

  defp tick_ms(), do: Durations.get(:convergence_tick_ms)

  defp tick_age(nil), do: nil
  defp tick_age(last_tick_at), do: now_ms() - last_tick_at

  defp now_ms(), do: System.monotonic_time(:millisecond)
end

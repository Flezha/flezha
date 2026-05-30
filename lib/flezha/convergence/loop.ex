defmodule Flezha.Convergence.Loop do
  @moduledoc """
  The single convergence GenServer (design.md §1) — the four-seam
  declarative reconciler. Every tick (and on declared events) it reads
  `Intent`,
  probes `Probe` reality, computes a `Diff`, the `Planner` turns the diff
  into backoff-filtered primitive ops, and `Heal` executes them. The
  resulting reality + health summary is published to
  `Flezha.Convergence.Snapshot` so `/healthz`, the CLI, and telemetry
  all read one source of truth (invariant I7).

  Reality-matches-intent is the *event*, not the default: the loop heals
  every divergence it finds, not only ones triggered by a specific
  signal. Declared events (`:output_stall`, `:executor_connected`) just
  shorten detection latency by triggering an immediate tick.

  Heal primitives are observable no-ops this session (see
  `Flezha.Convergence.Heal`); behaviour matches the chaos baseline until
  Sessions 3-4 fill them in.
  """

  use GenServer

  alias Flezha.Convergence.{Backoff, Diff, Heal, Intent, Planner, Probe, Snapshot}
  alias Flezha.Service.{EventManager, HubManager}

  require Logger

  @default_tick_ms 5_000
  @default_output_stall_grace_ms 60_000
  @default_tick_latency_threshold_ms 1_000
  @latency_window 20

  @telemetry_subs [[:flezha, :state, :output_stall]]
  @handler_id {__MODULE__, :events}

  defstruct tick_ms: @default_tick_ms,
            output_stall_grace_ms: @default_output_stall_grace_ms,
            tick_latency_threshold_ms: @default_tick_latency_threshold_ms,
            recovery_awaited?: false,
            backoff: Backoff.new(),
            # relay_instance => monotonic_ms when last flagged stalled.
            # Entries age out once they cross the output-stall grace,
            # so a relay that resumes producing auto-clears (no manual
            # clear) — Phase F invariant I3.
            stalled_relays: %{},
            tick_latencies: [],
            tick_breach_streak: 0

  # ---------- public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run a single tick synchronously; for tests + debugging."
  @spec tick_now() :: :ok
  def tick_now(), do: GenServer.call(__MODULE__, :tick_now, 30_000)

  # ---------- GenServer ----------

  @impl true
  def init(opts) do
    tick_ms =
      Keyword.get(
        opts,
        :tick_ms,
        Application.get_env(:flezha, :convergence_tick_ms, @default_tick_ms)
      )

    grace_ms =
      Keyword.get(
        opts,
        :output_stall_grace_ms,
        Application.get_env(
          :flezha,
          :convergence_output_stall_grace_ms,
          @default_output_stall_grace_ms
        )
      )

    latency_threshold_ms =
      Keyword.get(
        opts,
        :tick_latency_threshold_ms,
        Application.get_env(
          :flezha,
          :convergence_tick_latency_threshold_ms,
          @default_tick_latency_threshold_ms
        )
      )

    _ =
      :telemetry.attach_many(@handler_id, @telemetry_subs, &__MODULE__.handle_telemetry/4, self())

    # Shorten executor-takeover detection from `tick_ms` to ~ms.
    _ = EventManager.add_handler(:executor_connected_event)

    # First tick immediately so the recovery gate + first publish happen
    # without waiting `tick_ms`.
    Process.send_after(self(), :tick, 0)

    {:ok,
     %__MODULE__{
       tick_ms: tick_ms,
       output_stall_grace_ms: grace_ms,
       tick_latency_threshold_ms: latency_threshold_ms
     }}
  end

  @impl true
  def handle_info(:tick, %__MODULE__{} = state) do
    state = run_or_await(state)
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  def handle_info({:executor_connected_event, {_namespace, _executor_id}}, %__MODULE__{} = state) do
    {:noreply, run_or_await(state)}
  end

  def handle_info({:output_stall, relay_id}, %__MODULE__{} = state) when is_binary(relay_id) do
    now = System.monotonic_time(:millisecond)
    {:noreply, %{state | stalled_relays: Map.put(state.stalled_relays, relay_id, now)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call(:tick_now, _from, %__MODULE__{} = state) do
    {:reply, :ok, run_or_await(state)}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :telemetry.detach(@handler_id)
    :ok
  end

  # ---------- telemetry callback ----------

  @doc false
  def handle_telemetry([:flezha, :state, :output_stall], _measurements, metadata, loop_pid) do
    relay_id = Map.get(metadata, :relay_instance)
    if is_binary(relay_id) and is_pid(loop_pid), do: send(loop_pid, {:output_stall, relay_id})
    :ok
  catch
    _, _ -> :ok
  end

  def handle_telemetry(_event, _measurements, _metadata, _config), do: :ok

  # ---------- tick body ----------

  # Gate every emission on recovery completion (the upstream
  # `recovery-mode-opt-in` contract): no heal fires against a
  # half-restored registry.
  defp run_or_await(%__MODULE__{recovery_awaited?: true} = state), do: run_tick(state)

  defp run_or_await(%__MODULE__{} = state) do
    case Probe.await_recovery_complete(0) do
      :ok ->
        run_tick(%{state | recovery_awaited?: true})

      {:error, {:recovery_timeout, hub_ids}} ->
        :telemetry.execute(
          [:flezha, :convergence, :tick],
          %{relays_total: 0, actions_issued: 0},
          %{state: :awaiting_recovery, unfinished_hubs: hub_ids}
        )

        state
    end
  end

  defp run_tick(%__MODULE__{} = state) do
    started_at = System.monotonic_time(:millisecond)

    # Age out stalled entries the Watchdog has stopped re-flagging: a
    # relay that resumed producing leaves the set without a manual clear
    # (invariant I3 auto-clear).
    stalled = expire_stalled(state.stalled_relays, started_at, state.output_stall_grace_ms)
    state = %{state | stalled_relays: stalled}

    intent = Intent.declared()
    reality = Map.put(Probe.snapshot(), :stalled_relays, stalled)
    delta = Diff.compute(intent, reality)
    plan = Planner.plan(delta, state.backoff)

    {backoff, issued} = execute_plan(plan, state.backoff)

    elapsed = System.monotonic_time(:millisecond) - started_at

    {latencies, p99, streak} =
      record_latency(
        state.tick_latencies,
        elapsed,
        state.tick_breach_streak,
        state.tick_latency_threshold_ms
      )

    state = %{state | backoff: backoff, tick_latencies: latencies, tick_breach_streak: streak}

    publish(intent, reality, state, p99)

    :telemetry.execute(
      [:flezha, :convergence, :tick],
      %{relays_total: length(intent.relays), actions_issued: issued, duration_ms: elapsed},
      %{state: :ticking}
    )

    state
  end

  defp expire_stalled(stalled, now, grace_ms) do
    :maps.filter(fn _relay, since -> now - since < grace_ms end, stalled)
  end

  # Rolling window of the last `@latency_window` tick durations → p99;
  # `streak` counts consecutive ticks over the budget (reset on a fast
  # tick) so `/healthz` can tell a wedged loop from a single slow tick.
  defp record_latency(latencies, elapsed, streak, threshold_ms) do
    latencies = Enum.take([elapsed | latencies], @latency_window)
    streak = if elapsed > threshold_ms, do: streak + 1, else: 0
    {latencies, percentile(latencies, 99), streak}
  end

  defp percentile([], _p), do: 0

  defp percentile(values, p) do
    sorted = Enum.sort(values)
    idx = (round(p / 100 * length(sorted)) - 1) |> max(0) |> min(length(sorted) - 1)
    Enum.at(sorted, idx)
  end

  defp execute_plan(plan, backoff) do
    now = System.monotonic_time(:millisecond)

    Enum.reduce(plan, {backoff, 0}, fn op, {acc_backoff, count} ->
      result = Heal.execute(op)
      {Backoff.update(acc_backoff, Planner.key(op), result, now), count + 1}
    end)
  end

  defp publish(%Intent{relays: relays}, reality, %__MODULE__{} = state, p99) do
    # `intended` = declared relays (what should be bound); `bound` = relays
    # actually observed running on executors. After a restart the controller is
    # up but its relays are not yet rebound (bound < intended) — that is a
    # recovery window, NOT healthy, even though nothing is "stalled".
    intended = length(relays)
    bound = map_size(Map.get(reality, :relays, %{}))
    stalled = Map.keys(state.stalled_relays)
    producing = max(bound - length(stalled), 0)

    # Hubs are started lazily on executor reconnect, so immediately after a
    # restart the durable registry has not been loaded yet and Intent is empty
    # (intended == 0). Surfacing the on-disk pending-restore signal lets
    # `/healthz` report `recovering` instead of vacuously `healthy` until the
    # registry is read back. Only consulted when nothing is declared yet (the
    # disk check is cheap, but irrelevant once relays are loaded).
    pending_restore = intended == 0 and HubManager.durable_registry_pending?()

    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_relays: stalled,
      intended_relays: intended,
      bound_relays: bound,
      producing_relays: producing,
      pending_restore: pending_restore,
      tick_latency_p99_ms: p99,
      tick_breach_streak: state.tick_breach_streak
    })
  end
end

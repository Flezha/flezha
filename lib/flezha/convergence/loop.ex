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
  alias Flezha.Service.EventManager

  require Logger

  @default_tick_ms 5_000
  @default_output_stall_grace_ms 60_000

  @telemetry_subs [[:flezha, :state, :output_stall]]
  @handler_id {__MODULE__, :events}

  defstruct tick_ms: @default_tick_ms,
            output_stall_grace_ms: @default_output_stall_grace_ms,
            recovery_awaited?: false,
            backoff: Backoff.new(),
            stalled_relays: MapSet.new()

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
      Keyword.get(opts, :tick_ms, Application.get_env(:flezha, :convergence_tick_ms, @default_tick_ms))

    grace_ms =
      Keyword.get(
        opts,
        :output_stall_grace_ms,
        Application.get_env(:flezha, :convergence_output_stall_grace_ms, @default_output_stall_grace_ms)
      )

    _ =
      :telemetry.attach_many(@handler_id, @telemetry_subs, &__MODULE__.handle_telemetry/4, self())

    # Shorten executor-takeover detection from `tick_ms` to ~ms.
    _ = EventManager.add_handler(:executor_connected_event)

    # First tick immediately so the recovery gate + first publish happen
    # without waiting `tick_ms`.
    Process.send_after(self(), :tick, 0)

    {:ok, %__MODULE__{tick_ms: tick_ms, output_stall_grace_ms: grace_ms}}
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
    {:noreply, %{state | stalled_relays: MapSet.put(state.stalled_relays, relay_id)}}
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
    relay_id = Map.get(metadata, :relay_instance) || Map.get(metadata, :relay)
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
    intent = Intent.declared()
    reality = Map.put(Probe.snapshot(), :stalled_relays, state.stalled_relays)
    delta = Diff.compute(intent, reality)
    plan = Planner.plan(delta, state.backoff)

    {backoff, issued} = execute_plan(plan, state.backoff)

    publish(intent, state)

    :telemetry.execute(
      [:flezha, :convergence, :tick],
      %{relays_total: length(intent.relays), actions_issued: issued},
      %{state: :ticking}
    )

    %{state | backoff: backoff}
  end

  defp execute_plan(plan, backoff) do
    now = System.monotonic_time(:millisecond)

    Enum.reduce(plan, {backoff, 0}, fn op, {acc_backoff, count} ->
      result = Heal.execute(op)
      {Backoff.update(acc_backoff, Planner.key(op), result, now), count + 1}
    end)
  end

  defp publish(%Intent{relays: relays}, %__MODULE__{} = state) do
    bound = length(relays)

    Snapshot.publish(%{
      last_tick_at: System.monotonic_time(:millisecond),
      stalled_relays: MapSet.to_list(state.stalled_relays),
      bound_relays: bound,
      producing_relays: bound
    })
  end
end

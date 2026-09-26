defmodule Flezha.Convergence.Snapshot do
  @moduledoc """
  The single published source of truth read by `/healthz`, the CLI
  status surface, and telemetry consumers.

  Owns one public ETS table holding three kinds of row:

    * `{:recovery, hub_id} => state` — the per-hub recovery lifecycle
      state, cached from ProcessHub's `recovery_state_changed` hook and
      seeded by `HubManager.start_hub/1` via `observe_recovery/2`.

    * `{:sensor, name} => {at_ms, period_ms}` — the last completed pass of
      a periodic sensor, stamped by the sensor itself (`observe_sensor/3`).
      A sensor that wedges without crashing stops stamping, which is how
      `/healthz` sees detection die. Written by the sensor rather than
      polled from it, so reading it never blocks on the wedged process.

    * `:current => map` — the latest reality + health summary published
      by `Flezha.Convergence.Loop` once per tick.

  Started unconditionally by `Flezha.Application` so recovery-state
  reads work even when the optional `Loop` is disabled (e.g. tests).
  Reads run in the caller against the public table; the GenServer only
  owns the table and the telemetry handler lifecycle.
  """

  use GenServer

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Verdict
  alias ProcessHub.Constant.Hook

  @table __MODULE__

  @default_current %{
    last_tick_at: nil,
    stuck_operators: %{},
    intent_mismatch: [],
    operator_health: %{}
  }

  # ---------- public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Latest published reality/health summary, or a safe default before the first tick."
  @spec current() :: map()
  def current() do
    case lookup(:current) do
      nil -> @default_current
      map -> map
    end
  end

  @doc "Publish the per-tick reality/health summary. Called by the Loop."
  @spec publish(map()) :: :ok
  def publish(map) when is_map(map) do
    insert(:current, map)
  end

  @doc """
  The unmet-intent clock the Loop published in `current` beside the configured
  grace: the two facts `Verdict` weighs to tell a rebind still inside its
  window from one past it.
  """
  @spec unmet_intent(map()) :: %{
          unmet_intent_ms: non_neg_integer() | nil,
          unmet_intent_grace_ms: non_neg_integer()
        }
  def unmet_intent(current \\ current()) do
    %{
      unmet_intent_ms: Map.get(current, :unmet_intent_ms),
      unmet_intent_grace_ms: Durations.get(:convergence_unmet_intent_grace_ms)
    }
  end

  @doc """
  The Loop's verdict record for `{namespace, operator_id}`, or `nil` for an
  operator it has not judged yet (before the first tick, or a durable-registry
  row with no live placement).
  """
  @spec verdict(String.t(), term()) :: Verdict.t() | nil
  def verdict(namespace, operator_id), do: record({:operator, {namespace, operator_id}})

  @doc """
  The Loop's verdict record for any subject — a runner, a plugin, a channel or
  a driver as well as an operator — or `nil` for one it has not judged. The
  one read of the published health map, so no reader digs into the snapshot's
  shape for itself.
  """
  @spec record({Verdict.kind(), term()}) :: Verdict.t() | nil
  def record(subject) do
    current() |> Map.get(:operator_health, %{}) |> Map.get(subject)
  end

  @doc """
  Per-operator health for `{namespace, operator_id}`: the state of the one
  verdict record the Loop published for it. `:unknown` for an operator it has
  not judged. One of `:healthy | :degraded | :failed | :unbound | :parked |
  :suspended | :unknown`.
  """
  @spec operator_health(String.t(), term()) :: Verdict.state()
  def operator_health(namespace, operator_id) do
    case verdict(namespace, operator_id) do
      %Verdict{state: state} -> state
      _ -> :unknown
    end
  end

  @doc """
  Stamp a periodic sensor's completed pass: `at_ms` is monotonic and
  `period_ms` is the sensor's own cadence, so a reader can bound staleness
  without knowing which sensor it is looking at.
  """
  @spec observe_sensor(atom(), integer(), non_neg_integer()) :: :ok
  def observe_sensor(name, at_ms, period_ms) when is_atom(name) do
    insert({:sensor, name}, {at_ms, period_ms})
  end

  @doc "Every stamped sensor as `{name, {at_ms, period_ms}}`."
  @spec sensors() :: %{atom() => {integer(), non_neg_integer()}}
  def sensors(), do: tagged_rows(:sensor)

  @doc "Last known recovery state for `hub_id`; `:unknown` if never observed."
  @spec recovery_state(atom()) :: atom()
  def recovery_state(hub_id) when is_atom(hub_id) do
    lookup({:recovery, hub_id}) || :unknown
  end

  @doc "All observed `{hub_id => state}` recovery entries."
  @spec recovery_states() :: %{atom() => atom()}
  def recovery_states(), do: tagged_rows(:recovery)

  @doc """
  Record an observed recovery transition from a non-telemetry source —
  used by `HubManager.start_hub/1` so a hub booting straight into
  `:normal` (marker present, no replay, no telemetry) is still seen.
  """
  @spec observe_recovery(atom(), atom()) :: :ok
  def observe_recovery(hub_id, state) when is_atom(hub_id) and is_atom(state) do
    insert({:recovery, hub_id}, state)
  end

  @doc """
  Drop every observed `{:recovery, hub_id}` row. The cache otherwise lives
  for the life of the BEAM, so a hub left mid-recovery would keep gating
  `RecoveryGate.await_recovery_complete/2` forever. Used by tests that need the
  gate to weigh only their own hubs; harmless in prod (rows are re-seeded
  by `HubManager.start_hub/1` + recovery telemetry).
  """
  @spec reset_recovery_states() :: :ok
  def reset_recovery_states() do
    case :ets.whereis(@table) do
      :undefined -> :ok
      tid -> :ets.match_delete(tid, {{:recovery, :_}, :_}) && :ok
    end
  end

  # ---------- recovery_state_changed hook callback ----------

  @doc """
  Handler for ProcessHub's `recovery_state_changed` hook. Registered per-hub
  by `HubManager` (see `hook_handlers/1`). The hook payload carries only the
  transition (`%{from, to, reason}` — no hub identity), so the hub_id is
  bound as a literal arg at registration, the same way `alter_child_data`
  gets its hub. Maps the lifecycle transition to the cached recovery state.
  """
  def handle_recovery_transition(hub_id, %{to: to, reason: reason}) when is_atom(hub_id) do
    state =
      case {to, reason} do
        {:recovering, _} -> :recovering
        {:normal, :recovery_timeout} -> :timeout
        {:normal, :marker_present} -> :skipped
        {:normal, _} -> :normal
      end

    observe_recovery(hub_id, state)
  end

  def handle_recovery_transition(_hub_id, _data), do: :ok

  @doc """
  Hook config injected into `hub_id`'s `%ProcessHub{}` so this Snapshot
  observes the recovery lifecycle. Merge into the hub's `:hooks` map.
  """
  @spec hook_handlers(atom()) :: map()
  def hook_handlers(hub_id) do
    %{
      Hook.recovery_state_changed() => [
        %ProcessHub.Service.HookManager{
          id: :flezha_snapshot_recovery,
          m: __MODULE__,
          f: :handle_recovery_transition,
          a: [hub_id, :_]
        }
      ]
    }
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    _ = :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  # ---------- internals ----------

  # Every `{tag, name} => value` row as `%{name => value}`; the shape both
  # keyed row families (`:recovery`, `:sensor`) are read back with.
  defp tagged_rows(tag) do
    case :ets.whereis(@table) do
      :undefined ->
        %{}

      tid ->
        tid
        |> :ets.select([{{{tag, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}])
        |> Map.new()
    end
  end

  defp lookup(key) do
    case :ets.whereis(@table) do
      :undefined ->
        nil

      tid ->
        case :ets.lookup(tid, key) do
          [{^key, value}] -> value
          [] -> nil
        end
    end
  end

  defp insert(key, value) do
    case :ets.whereis(@table) do
      :undefined ->
        :ok

      tid ->
        true = :ets.insert(tid, {key, value})
        :ok
    end
  end
end

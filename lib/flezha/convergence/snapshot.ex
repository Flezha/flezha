defmodule Flezha.Convergence.Snapshot do
  @moduledoc """
  The single published source of truth read by `/healthz`, the CLI
  status surface, and telemetry consumers.

  Owns one public ETS table holding four kinds of row:

    * `{:recovery, hub_id} => state` — the per-hub recovery lifecycle
      state, cached from ProcessHub's `recovery_state_changed` hook and
      seeded by `HubManager.start_hub/1` via `observe_recovery/2`.

    * `{:sensor, name} => {at_ms, period_ms}` — the last completed pass of
      a periodic sensor, stamped by the sensor itself (`observe_sensor/3`).
      A sensor that wedges without crashing stops stamping, which is how
      `/healthz` sees detection die. Written by the sensor rather than
      polled from it, so reading it never blocks on the wedged process.

    * `{:verdict, subject} => verdict` — one row per subject the Loop
      judged on its last tick, tagged with the tick's generation (counted
      in the `:generation` row) so a subject dropped from the next tick is
      removed. Kept apart from
      `:current` so reading one subject copies one row, whatever the fleet
      size.

    * `:current => map` — the latest reality summary published by
      `Flezha.Convergence.Loop` once per tick, without its verdicts.

  Started unconditionally by the application module so recovery-state
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
    stuck_relays: %{},
    intent_mismatch: []
  }

  # ---------- public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Latest published reality/health summary, or a safe default before the first
  tick. Its `relay_health` holds every verdict, so it costs a read of the
  whole fleet: a reader of one subject reads `record/1` instead.
  """
  @spec current() :: map()
  def current(), do: Map.put(tick(), :relay_health, verdicts())

  @doc """
  Publish the per-tick reality/health summary. Called by the Loop. It replaces
  the last one whole: its `relay_health` becomes every published verdict, and
  a map without one leaves none.
  """
  @spec publish(map()) :: :ok
  def publish(map) when is_map(map) do
    {verdicts, tick} = Map.pop(map, :relay_health, %{})
    with_table(:ok, &publish(&1, tick, verdicts))
  end

  # One insert carries the tick and all its verdicts, so no reader sees part
  # of them; the rows of subjects no longer judged go after.
  defp publish(tid, tick, verdicts) do
    generation = :ets.update_counter(tid, :generation, 1, {:generation, 0})
    rows = for {subject, verdict} <- verdicts, do: {{:verdict, subject}, generation, verdict}
    true = :ets.insert(tid, [{:current, tick} | rows])

    _ =
      :ets.select_delete(tid, [{{{:verdict, :_}, :"$1", :_}, [{:<, :"$1", generation}], [true]}])

    :ok
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
  def unmet_intent(current \\ tick()) do
    %{
      unmet_intent_ms: Map.get(current, :unmet_intent_ms),
      unmet_intent_grace_ms: Durations.get(:convergence_unmet_intent_grace_ms)
    }
  end

  @doc """
  The Loop's verdict record for `{namespace, relay_id}`, or `nil` for a
  relay it has not judged yet (before the first tick, or a durable-registry
  row with no live placement).
  """
  @spec verdict(String.t(), term()) :: Verdict.t() | nil
  def verdict(namespace, relay_id), do: record({:relay, {namespace, relay_id}})

  @doc """
  The Loop's verdict record for any subject — a runner, a plugin, a channel or
  a driver as well as a relay — or `nil` for one it has not judged. The
  one read of a published verdict, so no reader digs into the snapshot's
  shape for itself.
  """
  @spec record({Verdict.kind(), term()}) :: Verdict.t() | nil
  def record(subject), do: lookup({:verdict, subject})

  @doc """
  Per-relay health for `{namespace, relay_id}` as every user surface
  shows it: the state of the one verdict record the Loop published for it,
  worded by `Verdict.listed_state/2`, so a `:failed` or `:unbound` relay
  reads `:recovering` inside the unmet-intent grace. `:unknown` for a relay
  it has not judged. A caller that decides from the state reads `verdict/2`.
  """
  @spec relay_health(String.t(), term()) :: Verdict.state() | :recovering
  def relay_health(namespace, relay_id),
    do: namespace |> verdict(relay_id) |> listed(unmet_intent())

  @doc """
  Every judged relay of `namespace` as `%{relay_id => health}`, each worded as
  `relay_health/2` words it, read in one pass. A listing reads this once
  rather than `relay_health/2` per row.
  """
  @spec relay_health_of(String.t()) :: %{term() => Verdict.state() | :recovering}
  def relay_health_of(namespace) do
    facts = unmet_intent()

    for {relay_id, verdict} <- verdict_rows({:relay, {namespace, :"$1"}}),
        into: %{},
        do: {relay_id, listed(verdict, facts)}
  end

  defp listed(%Verdict{state: state}, facts), do: Verdict.listed_state(state, facts)
  defp listed(nil, _facts), do: :unknown

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
  def sensors(), do: tagged_rows({:sensor, :"$1"})

  @doc "Last known recovery state for `hub_id`; `:unknown` if never observed."
  @spec recovery_state(atom()) :: atom()
  def recovery_state(hub_id) when is_atom(hub_id) do
    lookup({:recovery, hub_id}) || :unknown
  end

  @doc "All observed `{hub_id => state}` recovery entries."
  @spec recovery_states() :: %{atom() => atom()}
  def recovery_states(), do: tagged_rows({:recovery, :"$1"})

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
  def reset_recovery_states(),
    do: with_table(:ok, &(:ets.match_delete(&1, {{:recovery, :_}, :_}) && :ok))

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

  defp tick(), do: lookup(:current) || @default_current

  defp verdicts(), do: verdict_rows(:"$1")

  # The verdicts whose subject matches `subject`, keyed by what it binds to
  # `:"$1"`; the generation between key and verdict is skipped.
  defp verdict_rows(subject), do: tagged_rows({:verdict, subject}, [:_])

  # Every row whose key matches `key` as `%{name => value}`, `name` being what
  # the key binds to `:"$1"`; the shape every keyed row family is read back
  # with. `skip` matches the fields a family keeps between key and value.
  defp tagged_rows(key, skip \\ []) do
    head = List.to_tuple([key | skip] ++ [:"$2"])
    with_table(%{}, &Map.new(:ets.select(&1, [{head, [], [{{:"$1", :"$2"}}]}])))
  end

  defp lookup(key) do
    with_table(nil, fn tid ->
      case :ets.lookup(tid, key) do
        [{^key, value}] -> value
        [{^key, _generation, value}] -> value
        [] -> nil
      end
    end)
  end

  defp insert(key, value), do: with_table(:ok, &(:ets.insert(&1, {key, value}) && :ok))

  # The table is gone while the Snapshot restarts; every access then answers
  # `default` rather than raising.
  defp with_table(default, fun) do
    case :ets.whereis(@table) do
      :undefined -> default
      tid -> fun.(tid)
    end
  end
end

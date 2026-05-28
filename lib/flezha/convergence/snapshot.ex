defmodule Flezha.Convergence.Snapshot do
  @moduledoc """
  The single published source of truth read by `/healthz`, the CLI
  status surface, and telemetry consumers (design.md §2, invariant I7).

  Owns one public ETS table holding two kinds of row:

    * `{:recovery, hub_id} => state` — the per-hub recovery lifecycle
      state, cached from `[:process_hub, :recovery, _]` telemetry and
      seeded by `HubManager.start_hub/1` via `observe_recovery/2`.

    * `:current => map` — the latest reality + health summary published
      by `Flezha.Convergence.Loop` once per tick.

  Started unconditionally by `Flezha.Application` so recovery-state
  reads work even when the optional `Loop` is disabled (e.g. tests).
  Reads run in the caller against the public table; the GenServer only
  owns the table and the telemetry handler lifecycle.
  """

  use GenServer

  @table __MODULE__
  @recovery_events [
    [:process_hub, :recovery, :started],
    [:process_hub, :recovery, :complete],
    [:process_hub, :recovery, :skipped],
    [:process_hub, :recovery, :timeout]
  ]
  @handler_id {__MODULE__, :recovery_events}

  @default_current %{
    last_tick_at: nil,
    stalled_relays: [],
    bound_relays: 0,
    producing_relays: 0
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

  @doc "Last known recovery state for `hub_id`; `:unknown` if never observed."
  @spec recovery_state(atom()) :: atom()
  def recovery_state(hub_id) when is_atom(hub_id) do
    lookup({:recovery, hub_id}) || :unknown
  end

  @doc "All observed `{hub_id => state}` recovery entries."
  @spec recovery_states() :: %{atom() => atom()}
  def recovery_states() do
    case :ets.whereis(@table) do
      :undefined ->
        %{}

      tid ->
        :ets.select(tid, [{{{:recovery, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}])
        |> Map.new()
    end
  end

  @doc """
  Record an observed recovery transition from a non-telemetry source —
  used by `HubManager.start_hub/1` so a hub booting straight into
  `:normal` (marker present, no replay, no telemetry) is still seen.
  """
  @spec observe_recovery(atom(), atom()) :: :ok
  def observe_recovery(hub_id, state) when is_atom(hub_id) and is_atom(state) do
    insert({:recovery, hub_id}, state)
  end

  # ---------- telemetry callback ----------

  @doc false
  def handle_recovery_event(event, _measurements, %{hub_id: hub_id}, _config)
      when is_atom(hub_id) do
    case List.last(event) do
      :started -> observe_recovery(hub_id, :recovering)
      :complete -> observe_recovery(hub_id, :normal)
      :skipped -> observe_recovery(hub_id, :skipped)
      :timeout -> observe_recovery(hub_id, :timeout)
      _ -> :ok
    end
  end

  def handle_recovery_event(_event, _measurements, _metadata, _config), do: :ok

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    _ = :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])

    # Wedged-channel watchdog strike counters (Phase C). Public so the
    # hook handler, the Loop's probe, and the eviction path all write it.
    if :ets.whereis(:flezha_channel_health) == :undefined do
      _ = :ets.new(:flezha_channel_health, [:named_table, :public, :set, write_concurrency: true])
    end

    _ =
      :telemetry.attach_many(
        @handler_id,
        @recovery_events,
        &__MODULE__.handle_recovery_event/4,
        nil
      )

    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :telemetry.detach(@handler_id)
    :ok
  end

  # ---------- internals ----------

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
      :undefined -> :ok
      tid -> true = :ets.insert(tid, {key, value}); :ok
    end
  end
end

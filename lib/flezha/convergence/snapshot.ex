defmodule Flezha.Convergence.Snapshot do
  @moduledoc """
  The single published source of truth read by `/healthz`, the CLI
  status surface, and telemetry consumers.

  Owns one public ETS table holding two kinds of row:

    * `{:recovery, hub_id} => state` — the per-hub recovery lifecycle
      state, cached from ProcessHub's `recovery_state_changed` hook and
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

  @default_current %{
    last_tick_at: nil,
    stalled_operatives: [],
    bound_operatives: 0,
    producing_operatives: 0,
    operative_health: %{}
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
  Per-operative health for `{namespace, operative_id}`, classified by the Loop on its
  last tick. `:unknown` before the first tick or for a operative the Loop has not
  observed (e.g. a durable-registry row with no live placement). One of
  `:healthy | :degraded | :failed | :unbound | :unknown`.
  """
  @spec operative_health(String.t(), term()) :: atom()
  def operative_health(namespace, operative_id) do
    current()
    |> Map.get(:operative_health, %{})
    |> Map.get({namespace, operative_id}, :unknown)
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
  Handler for ProcessHub's `recovery_state_changed` hook. Registered per-hub by
  `HubManager` (see `hook_handlers/0`). Maps the lifecycle transition to the
  cached recovery state.
  """
  def handle_recovery_transition(%{hub_id: hub_id, to: to, reason: reason})
      when is_atom(hub_id) do
    state =
      case {to, reason} do
        {:recovering, _} -> :recovering
        {:normal, :recovery_timeout} -> :timeout
        {:normal, :marker_present} -> :skipped
        {:normal, _} -> :normal
      end

    observe_recovery(hub_id, state)
  end

  def handle_recovery_transition(_data), do: :ok

  @doc """
  Hook config injected into every `%ProcessHub{}` so this Snapshot observes the
  recovery lifecycle. Merge into the hub's `:hooks` map.
  """
  @spec hook_handlers() :: map()
  def hook_handlers do
    %{
      ProcessHub.Constant.Hook.recovery_state_changed() => [
        %ProcessHub.Service.HookManager{
          id: :flezha_snapshot_recovery,
          m: __MODULE__,
          f: :handle_recovery_transition,
          a: [:_]
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

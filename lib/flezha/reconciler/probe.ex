defmodule Flezha.Reconciler.Probe do
  @moduledoc """
  Cheap liveness probes that define a relay's *actual* state for the reconciler.

  Each `probe/4` reads one relay's durable registry entry (the *desired* state)
  and snapshots the *actual* state without touching the data path:

    * relay pid alive on a live cluster node (`Flezha.Service.HubManager.live_pid/1`)
    * stale `{dead_node, pid}` entries (purge candidates)
    * the relay's Overwatch park state and bound data channel (one
      `:get_overwatch_state` call, then one `:stream_ref_status` call)
    * the assigned executor's reachability and whether *any* executor advertises
      the relay's factory
    * last-output egress age (`Flezha.Reconciler.Activity`)

  Executor reachability/availability are supplied via `ctx` (computed once per
  namespace by the loop) so the probe stays a pure read over injected facts and is
  exhaustively unit-testable. All relay/channel calls are bounded and crash-safe;
  a wedged relay degrades a dimension to `:unknown` rather than blocking the sweep.
  """

  alias Flezha.Reconciler.Activity
  alias Flezha.Service.HubManager

  @relay_call_timeout_ms 1_000

  defstruct [
    :namespace,
    :relay_id,
    :factory_id,
    :cspec,
    :node_pids,
    :relay_pid,
    :assigned_executor,
    :park_reason,
    :last_output_age_ms,
    dead_node_entries: [],
    relay_alive?: false,
    local?: false,
    parked?: false,
    # true | false | :unknown — the relay's primary data channel stream_ref
    stream_bound?: :unknown,
    executor_reachable?: false,
    executor_available?: false
  ]

  @type stream_state() :: boolean() | :unknown

  @type t() :: %__MODULE__{
          namespace: binary(),
          relay_id: binary(),
          factory_id: binary() | atom() | nil,
          cspec: map() | nil,
          node_pids: [{node(), pid()}],
          relay_pid: pid() | nil,
          assigned_executor: binary() | nil,
          park_reason: term(),
          last_output_age_ms: non_neg_integer() | nil,
          dead_node_entries: [{node(), pid()}],
          relay_alive?: boolean(),
          local?: boolean(),
          parked?: boolean(),
          stream_bound?: stream_state(),
          executor_reachable?: boolean(),
          executor_available?: boolean()
        }

  @typedoc """
  Per-namespace facts computed once by the loop:

    * `:active_executors` — MapSet of executor ids currently connected.
    * `:factory_available?` — `factory_id -> boolean()` (any active executor
      advertises it).
  """
  @type ctx() :: %{
          required(:active_executors) => MapSet.t(),
          required(:factory_available?) => (term() -> boolean())
        }

  @doc """
  Probe a single relay from its raw ProcessHub registry entry
  (`{cspec, node_pids, metadata}`).
  """
  @spec probe(binary(), binary(), tuple(), ctx()) :: t()
  def probe(namespace, relay_id, registry_entry, ctx) do
    {cspec, node_pids, metadata} = normalize_entry(registry_entry)
    relay = relay_from_cspec(cspec)
    factory_id = relay && Map.get(relay, :factory_id)
    assigned = Map.get(metadata, :tag)

    relay_pid = HubManager.live_pid(node_pids)
    relay_alive? = is_pid(relay_pid)
    local? = relay_alive? and node(relay_pid) == node()

    overwatch = if relay_alive?, do: overwatch_snapshot(relay_pid), else: nil
    executor_id = (overwatch && Map.get(overwatch, :executor_id)) || assigned

    %__MODULE__{
      namespace: namespace,
      relay_id: relay_id,
      factory_id: factory_id,
      cspec: cspec,
      node_pids: node_pids,
      relay_pid: relay_pid,
      assigned_executor: executor_id,
      relay_alive?: relay_alive?,
      local?: local?,
      dead_node_entries: dead_node_entries(node_pids),
      parked?: parked?(overwatch),
      park_reason: overwatch && Map.get(overwatch, :init_error),
      stream_bound?: stream_bound?(overwatch),
      executor_reachable?:
        executor_id != nil and MapSet.member?(ctx.active_executors, executor_id),
      executor_available?: factory_id != nil and ctx.factory_available?.(factory_id),
      last_output_age_ms: last_output_age_ms(relay_id)
    }
  end

  # ---------- helpers ----------

  defp normalize_entry({cspec, node_pids, metadata}), do: {cspec, node_pids, metadata}
  defp normalize_entry({cspec, node_pids}), do: {cspec, node_pids, %{}}

  # Registry node_pids reference nodes outside the live cluster → purge candidates.
  defp dead_node_entries(node_pids) do
    live = MapSet.new([node() | Node.list()])
    Enum.reject(node_pids, fn {n, _pid} -> MapSet.member?(live, n) end)
  end

  defp parked?(nil), do: false
  defp parked?(overwatch), do: Map.get(overwatch, :init_error) != nil

  # Resolve the relay's bound data channel from its Overwatch snapshot, then ask
  # the channel whether its NIF stream_ref is bound. `nil` channel = never bound.
  defp stream_bound?(nil), do: :unknown

  defp stream_bound?(overwatch) do
    case Map.get(overwatch, :channel) do
      nil ->
        false

      channel ->
        case safe_call(channel, :stream_ref_status) do
          {:ok, nil} -> false
          {:ok, _ref} -> true
          :error -> :unknown
        end
    end
  end

  defp overwatch_snapshot(relay_pid) do
    case safe_call(relay_pid, :get_overwatch_state) do
      {:ok, %{__struct__: _} = state} -> state
      _ -> nil
    end
  end

  defp last_output_age_ms(relay_id) do
    case Activity.last_output_ms(relay_id) do
      nil -> nil
      ts -> System.monotonic_time(:millisecond) - ts
    end
  end

  # The relay's start-args embed the relay struct as `{ns, relay}` or
  # `{ns, executor_id, relay}` (the dead-node assignment form).
  defp relay_from_cspec(%{start: {_m, _f, [arg]}}) do
    case arg do
      {_namespace, relay} when is_map(relay) -> relay
      {_namespace, _executor_id, relay} when is_map(relay) -> relay
      _ -> nil
    end
  end

  defp relay_from_cspec(_), do: nil

  defp safe_call(server, msg) do
    {:ok, GenServer.call(server, msg, @relay_call_timeout_ms)}
  catch
    :exit, _ -> :error
  end
end

defmodule Flezha.Runner.FleetManager.Cluster do
  @moduledoc """
  The fleet reads that span the cluster: every node holding active runners in
  a namespace answers its own half over `:erpc`, and an unreachable peer
  contributes nothing rather than failing the read. `Flezha.Runner.FleetManager`
  delegates its public reads here.
  """

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Parallel
  alias Flezha.Runner.FleetManager
  alias Flezha.Transport.LivenessCounters

  @doc """
  `list_factories/1` on every node holding active runners in `namespace`: one
  row per node that advertises a factory.
  """
  @spec global_factories(String.t()) :: [map()]
  def global_factories(namespace), do: across_runner_nodes(namespace, :factory_rows)

  @doc false
  # This node's rows, read over `:erpc` by `global_factories/1`.
  def factory_rows(namespace, runner \\ nil) do
    {:ok, rows} = FleetManager.list_factories(namespace, runner)
    rows
  end

  @doc """
  Runner ids across every cluster node holding active runners in `namespace`,
  resolved through the cluster directory. Serves `Flezha.list_runners/1`.
  An unreachable peer contributes nothing rather than failing the read.
  """
  @spec global_runners(String.t()) :: {:ok, [binary()]}
  def global_runners(namespace),
    do: {:ok, namespace |> across_runner_nodes(:active_runners) |> Enum.uniq()}

  @doc false
  # This node's half of `global_runners/1`, reached over `:erpc`.
  def active_runners(namespace), do: FleetManager.active_runners(namespace)

  @doc """
  Every active runner in `namespace` across the cluster, one row each: its
  `id`, the `node` holding its session, `connected_at` (unix seconds, `nil`
  for a session that predates the stamp), the runner's OS `pid` where its
  transport says (a local socket), `nil` otherwise, `silent_ms` since its last control
  frame (`nil` when none was stamped) and `late?`, true once that silence
  outlasts two keepalive periods, so at least one keepalive was missed. Each
  node answers for its own runners against its own clock. Serves
  `flz runner list`.
  """
  @spec global_runner_rows(String.t()) :: {:ok, [map()]}
  def global_runner_rows(namespace),
    do: {:ok, namespace |> across_runner_nodes(:runner_rows) |> Enum.uniq_by(& &1.id)}

  @doc false
  # This node's half of `global_runner_rows/1`, reached over `:erpc`.
  def runner_rows(namespace) do
    connected =
      GenServer.call(FleetManager, {:runner_rows, namespace}, Durations.get(:runner_lookup_ms))

    now = System.monotonic_time(:millisecond)
    late_after = 2 * Durations.get(:control_keepalive_ms)

    Enum.map(connected, fn {id, facts} ->
      silent_ms = LivenessCounters.control_silent_ms(id, now)

      %{
        id: id,
        node: node(),
        connected_at: facts[:connected_at],
        pid: facts[:pid],
        silent_ms: silent_ms,
        late?: is_integer(silent_ms) and silent_ms > late_after
      }
    end)
  end

  @doc """
  Every runner attached to `namespace` across the cluster: the active ones and
  those still in their handshake, whose hub is already up. Serves the
  vacancy check of `flz namespace delete`.
  """
  @spec global_occupants(String.t()) :: {:ok, [binary()]}
  def global_occupants(namespace),
    do: {:ok, namespace |> across_runner_nodes(:occupants) |> Enum.uniq()}

  @doc false
  # This node's half of `global_occupants/1`, reached over `:erpc`.
  def occupants(namespace),
    do: GenServer.call(FleetManager, {:occupants, namespace}, Durations.get(:runner_lookup_ms))

  @doc """
  The active session `runner_id` of `namespace`, wherever in the cluster it is
  held: its stable identity and incarnation (`nil` where the runner presented
  none), the process id of the runner on its socket where the transport can
  say, and the node. `nil` when no node holds it.
  """
  @spec find_session(String.t(), binary()) :: map() | nil
  def find_session(namespace, runner_id),
    do: namespace |> across_runner_nodes(:session_rows, [runner_id]) |> List.first()

  @doc false
  # This node's half of `find_session/2`, reached over `:erpc`.
  def session_rows(namespace, runner_id) do
    case GenServer.call(
           FleetManager,
           {:session_identity, namespace, runner_id},
           FleetManager.manager_call_timeout()
         ) do
      nil -> []
      identity -> [Map.put(identity, :node, node())]
    end
  end

  # Every node holding active runners in `namespace` answers `fun`; an
  # unreachable peer contributes nothing rather than failing the read.
  defp across_runner_nodes(namespace, fun, args \\ []) do
    [node() | Directory.runner_nodes(namespace)]
    |> Parallel.map_nodes(&:erpc.call(&1, __MODULE__, fun, [namespace | args]),
      timeout: Durations.get(:runner_lookup_ms)
    )
    |> Enum.flat_map(fn
      {_node, {:ok, rows}} -> rows
      {_node, {:exit, _reason}} -> []
    end)
  end
end

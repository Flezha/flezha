defmodule Flezha.Cluster.Orchestrator do
  @moduledoc """
  Cluster formation and membership maintenance.

  Connects to discovered peers, keeps the BEAM cluster healed (jittered reconnect
  backoff plus a periodic discovery sweep), and exposes a `cluster_ready?/0` gate
  so a namespace hub is never created on a not-yet-clustered node (which would
  diverge from its peers). The cluster is "ready" once discovery has run and
  either every discovered peer is connected OR the bounded operator
  `:cluster_formation_timeout_ms` "degrade to solo" window has elapsed — so a
  genuinely single-node deployment still boots.
  """

  alias Flezha.Cluster.DiscoveryProtocol
  alias Flezha.Cluster.K8sPodDiscovery
  alias Flezha.Cluster.ManualDiscovery
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff

  require Logger

  use GenServer
  use Flezha.Process.Timer, keys: [:degrade_to_solo, :cluster_update]

  @ets_table __MODULE__.State

  @type state() :: %{
          discovery_handler: DiscoveryProtocol.t()
        }

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc """
  Whether the cluster is formed enough to create hubs. Lock-free ETS read (no
  GenServer hop): `true` once every discovered peer is connected or the
  degrade-to-solo timeout has elapsed. `false` before the Orchestrator starts.
  """
  @spec cluster_ready?() :: boolean()
  def cluster_ready?() do
    case :ets.whereis(@ets_table) do
      :undefined -> false
      _ -> :ets.lookup(@ets_table, :ready) == [{:ready, true}]
    end
  end

  @doc """
  Block until `cluster_ready?/0` is true or `timeout_ms` elapses (default the
  configured formation timeout). Returns `:ok` when ready, `:timeout` otherwise.
  Gates on-demand hub creation during the boot window.
  """
  @spec await_cluster_ready(non_neg_integer()) :: :ok | :timeout
  def await_cluster_ready(timeout_ms \\ formation_timeout_ms()) do
    do_await_cluster_ready(System.monotonic_time(:millisecond) + timeout_ms)
  end

  defp do_await_cluster_ready(deadline) do
    cond do
      cluster_ready?() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        :timeout

      true ->
        Process.sleep(50)
        do_await_cluster_ready(deadline)
    end
  end

  @impl true
  def init(_) do
    :ets.new(@ets_table, [:set, :protected, :named_table, read_concurrency: true])
    :ets.insert(@ets_table, {:ready, false})

    # Backstop: declare the cluster ready (solo) if peers never arrive, so a
    # single-node deployment is not blocked forever on the formation gate.
    state =
      arm_timer(
        %{discovery_handler: get_discovery_handler()},
        :degrade_to_solo,
        formation_timeout_ms(),
        :degrade_to_solo
      )

    {:ok, state, {:continue, :connect_nodes}}
  end

  @impl true
  def handle_continue(:connect_nodes, state) do
    {discovery_handler, expected} = connect_nodes(state.discovery_handler)
    maybe_mark_ready(expected)

    {:noreply, state |> arm_cluster_update() |> Map.put(:discovery_handler, discovery_handler)}
  end

  @impl true
  def handle_info(:degrade_to_solo, state) do
    if not cluster_ready?() do
      Logger.info(
        "[cluster] formation timeout elapsed without all peers; degrading to solo (ready)"
      )

      mark_ready()
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    Logger.warning("[cluster] peer #{inspect(node)} went down; scheduling reconnect")
    schedule_reconnect(node, 0)

    {:noreply, state}
  end

  @impl true
  def handle_info(:update_cluster, state) do
    {discovery_handler, expected} = connect_nodes(state.discovery_handler)
    maybe_mark_ready(expected)

    {:noreply, state |> arm_cluster_update() |> Map.put(:discovery_handler, discovery_handler)}
  end

  @impl true
  def handle_info({:scheduled_reconnect, node, retries}, state) do
    max_reconnects = Config.get(:max_reconnects)

    if retries >= max_reconnects do
      # Stop active reconnect attempts and unmonitor. The periodic
      # `:update_cluster` sweep remains the durable path that re-adds the peer
      # once it is reachable again.
      Logger.warning(
        "[cluster] giving up active reconnect to #{inspect(node)} after #{retries} tries; sweep will retry"
      )

      Node.monitor(node, false)
    else
      case Node.connect(node) do
        true ->
          Logger.info("[cluster] reconnected to peer #{inspect(node)} (try #{retries})")
          Node.monitor(node, true)

        false ->
          # Retry with jittered, exponentially-growing backoff.
          schedule_reconnect(node, retries + 1)
      end
    end

    {:noreply, state}
  end

  defp connect_nodes(discovery_handler) do
    {:ok, discovery_handler, discovered} = DiscoveryProtocol.get_nodes(discovery_handler)
    # Discovery includes this pod's own address; `Node.list/0` never contains
    # self, so without this filter every sweep "connects" to self and logs it.
    to_connect = Enum.reject(discovered, &(&1 == node()))
    connected = Node.list()

    Logger.debug(
      "[cluster] discovery returned #{inspect(to_connect)}; already connected #{inspect(connected)}"
    )

    Enum.each(to_connect, fn node ->
      if !Enum.member?(connected, node) do
        case Node.connect(node) do
          true ->
            Logger.info("[cluster] connected to peer #{inspect(node)}")
            Node.monitor(node, true)

          false ->
            Logger.warning("[cluster] connect to #{inspect(node)} failed; scheduling reconnect")
            schedule_reconnect(node, 0)

          :ignored ->
            Logger.warning("[cluster] connect to #{inspect(node)} ignored (distribution down)")
        end
      end
    end)

    {discovery_handler, to_connect}
  end

  # Mark the cluster ready once every discovered peer is connected. With no
  # discovered peers (`expected` empty after dropping self) this is true on the
  # first pass — a legitimate single-node cluster.
  defp maybe_mark_ready(expected) do
    expected = Enum.reject(expected, &(&1 == node()))
    connected = Node.list()

    if not cluster_ready?() and Enum.all?(expected, &(&1 in connected)) do
      mark_ready()
    end
  end

  defp mark_ready() do
    :ets.insert(@ets_table, {:ready, true})

    Logger.info("[cluster] formed and ready; members #{inspect([node() | Node.list()])}")

    :telemetry.execute(
      [:flezha, :cluster, :ready],
      %{node_count: length(Node.list()) + 1},
      %{node: node()}
    )
  end

  # A per-node reconnect is a send-later, not a managed timer: no reference is
  # kept, nothing cancels it, and its callers are connection helpers holding no
  # process state. `Process.Timer` would add bookkeeping with nothing to book.
  defp schedule_reconnect(node, retries_count) do
    base = Config.get(:reconnect_interval)

    case base do
      0 ->
        nil

      _ ->
        Process.send_after(
          self(),
          {:scheduled_reconnect, node, retries_count},
          Backoff.arm(:cluster_reconnect, retries_count + 1, node, base: base)
        )
    end
  end

  defp arm_cluster_update(state) do
    arm_timer(state, :cluster_update, Config.get(:cluster_update_interval), :update_cluster)
  end

  defp formation_timeout_ms(), do: Durations.get(:cluster_formation_timeout_ms)

  defp get_discovery_handler() do
    result =
      case Config.get(:cluster_discovery) do
        "manual_discovery" ->
          {:ok,
           %ManualDiscovery{
             initial_nodes: Config.get(:initial_nodes)
           }}

        "k8s_pod_discovery" ->
          {:ok,
           %K8sPodDiscovery{
             service_account_path: Config.get(:k8s_service_account_path),
             cluster_domain: Config.get(:k8s_cluster_domain),
             namespace: Config.get(:k8s_namespace),
             node_base_name: Config.get(:node_base_name),
             ip_lookup_mode: Config.get(:k8s_ip_lookup_mode),
             cluster_name: Config.get(:k8s_cluster_name),
             service_name: Config.get(:k8s_service_name),
             selector: Config.get(:k8s_selector),
             master_name: Config.get(:k8s_master_name),
             master_port: Config.get(:k8s_master_port),
             host_identifier: Config.get(:k8s_host_identifier)
           }}

        handler ->
          {:error, {:unknown_discovery_handler, handler}}
      end

    case result do
      {:ok, handler} ->
        handler

      {:error, reason} ->
        raise ArgumentError, "cluster discovery refused its settings: #{inspect(reason)}"
    end
  end
end

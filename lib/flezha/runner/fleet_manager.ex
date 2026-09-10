defmodule Flezha.Runner.FleetManager do
  @moduledoc """
  The node's fleet: which runners are connected, in which namespace, and what
  factories each advertises.

  One serialized GenServer owns that state and answers every placement
  question. The state it mutates is split by what it is — `Marks` (the
  takeover fence and the retiring mark), `Roster` (the runner index),
  `Factories` (the factory table and each factory's SDK language) and
  `Session` (connect, disconnect, evict, reroute) — while the public API
  stays here.

  Invariant: a disconnect drops the runner from the active set
  unconditionally; what varies is only what becomes of its operatives.
  """

  alias Flezha.AuthZ.RunnerProof
  alias Flezha.Cluster.Directory
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Parallel
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager.Factories
  alias Flezha.Runner.FleetManager.Load
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.FleetManager.Roster
  alias Flezha.Runner.FleetManager.Session
  alias Flezha.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown

  require Logger

  use GenServer
  use Flezha.Process.Timer, keys: [:runner_cleanup]

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def list_factories(namespace) do
    GenServer.call(__MODULE__, {:list_factories, namespace}, manager_call_timeout())
  end

  @doc """
  Unified connection handler for all transports.

  Called by Transport.Server when a new SDK connects.
  `stream_ref` is a NIF StreamHandle resource.
  `namespace` is what the connect_control_channel frame CLAIMS; the namespace
  this connection acts in is what its own credential proves
  (`Flezha.AuthZ.RunnerProof`), and a frame naming another one is refused here,
  before a session is minted or a hub created.
  `init_payload` is the decoded JSON from the initial frame.
  """
  def connect_handler(stream_ref, namespace, init_payload, transport) do
    with {:ok, %{namespace: proven} = proof} <-
           RunnerProof.prove(stream_ref, namespace, init_payload, transport),
         :ok <- Session.check_not_shutting_down(proven),
         :ok <- Session.admit_runner(proven, transport) do
      connect = Session.connect_record(proof, init_payload, transport)

      case GenServer.call(
             __MODULE__,
             {:runner_connect, stream_ref, connect},
             runner_connect_timeout()
           ) do
        {:error, reason} ->
          Logger.error("Failed to connect runner: #{inspect(reason)}")
          {:error, reason}

        {:ok, {_namespace, session_token}} ->
          {:ok, session_token}
      end
    end
  end

  @doc """
  Close every runner session the credential `hash` authenticated.

  A runner credential never expires, so nothing else would ever evict its
  holder: revoking one has to reach the live sessions or the revocation is only
  a refusal of the next connect. Answers the number of sessions closed, and `0`
  when there is no fleet to reach — a revoke during boot recovery runs before
  this manager starts, which is routine and so unlogged.
  """
  @spec evict_credential(binary()) :: non_neg_integer()
  def evict_credential(hash) when is_binary(hash) do
    SafeCall.bounded(__MODULE__, {:evict_credential, hash}, manager_call_timeout(),
      on_error: 0,
      log: false
    )
  end

  @doc """
  The caller context service calls attributed to a runner connection carry:
  a `runner`-role principal bound to the namespace the connection PROVED.
  """
  defdelegate runner_caller_context(namespace), to: Session

  # Pollers pass a short timeout so a FleetManager busy inside `runner_connect/3`
  # costs them one attempt rather than their whole deadline.
  def active_runners(namespace, timeout \\ Durations.get(:runner_lookup_ms)) do
    GenServer.call(__MODULE__, {:active_runners, namespace}, timeout)
  end

  @doc """
  Atomically evict a dead/orphaned runner binding: clear every operative's
  stale stream mapping (`HubManager.hook_remove_pid/2`), stop its runner
  supervisor, drop its factory refs + operative-insertion hook, and remove it
  from `runner_namespaces`. Its operatives become misbound and rebind onto
  a live runner on the next convergence tick. Idempotent — an unknown
  id returns `:ok`. Driven by `Flezha.Convergence.Heal.execute/1` and by the
  reconnect binding-swap in `runner_connect` (to close the prior session).

  `proved_wedged: true` waives the "never evict a live runner" guard. The
  guard exists to protect a HEALTHY runner from a stale snapshot; a runner the
  data-plane classifier caught still connected and still heartbeating while
  processing nothing is not healthy, and its connection being open is the
  reason nothing else can dislodge it.
  """
  @spec evict_runner(binary(), keyword()) :: :ok
  def evict_runner(runner_id, opts \\ []) when is_binary(runner_id) do
    GenServer.call(
      __MODULE__,
      {:evict_runner, runner_id, Keyword.get(opts, :proved_wedged, false)}
    )
  end

  @doc """
  The factories `runner_id` advertises in `namespace`, as a set.

  The inverse of `runners_for_factory/3`, for the caller that holds a runner
  and asks what it can take: the convergence loop's release path, deciding
  which waiting operatives a runner arriving in the namespace is news for. An
  empty set is a runner that has advertised nothing yet, which the caller must
  not read as "serves nothing"; a fleet that could not be read at all answers
  `:unknown`, because a refused read is evidence of nothing.
  """
  @spec factories_for_runner(String.t(), binary(), timeout()) :: MapSet.t() | :unknown
  def factories_for_runner(namespace, runner_id, timeout \\ Durations.get(:runner_lookup_ms)) do
    SafeCall.bounded(__MODULE__, {:factories_for_runner, namespace, runner_id}, timeout,
      on_error: :unknown,
      log: false
    )
  end

  @doc """
  Returns active compute engines in `namespace` that advertise `factory_id`.

  Used by `OperativeManager.assign_runner/3` so we never route a
  operative to a node that doesn't own its factory — without this filter
  every input frame for a misrouted operative hits a dead registry entry
  on the foreign node and generates a `enqueue: operative X not found`
  warning on every input.
  """
  def runners_for_factory(namespace, factory_id, timeout \\ Durations.get(:runner_lookup_ms)) do
    GenServer.call(__MODULE__, {:runners_for_factory, namespace, factory_id}, timeout)
  end

  @doc """
  The runner `operative_id` is placed on in `namespace`, or `nil` when the
  candidate set is empty.

  Candidates come from the same filter `runners_for_factory/3` answers with — a
  `nil` factory widens it to every active runner — and one of them is chosen by
  bounded-load rendezvous hashing (`Load.select/3`). The choice is counted
  against the runner before this returns, which is what a recovery batch needs:
  its members are placed under one `Task.async_stream` and none of their rows is
  written until the last of them has been placed, so a count that only advanced
  when the registry did would be identical for every member and the batch would
  spread exactly as the plain hash spread it.
  """
  @spec resolve_runner(String.t(), binary() | nil, binary(), timeout()) :: binary() | nil
  def resolve_runner(
        namespace,
        factory_id,
        operative_id,
        timeout \\ Durations.get(:runner_lookup_ms)
      ) do
    GenServer.call(
      __MODULE__,
      {:resolve_runner, namespace, factory_id, operative_id},
      timeout
    )
  end

  @doc """
  Why `factory_id` has no placement candidate in `namespace`.

  `:none` when some runner can take it; `:no_such_factory` when no runner
  advertises the id at all; `{:language, established}` when runners advertise
  it in another language than the one the factory is established at — a
  factory that exists and is simply unplaceable here, which must not be
  reported as a factory that does not exist.
  """
  @spec placement_refusal(String.t(), String.t(), timeout()) ::
          :none
          | :no_such_factory
          | :ambiguous_language
          | :no_language_declared
          | {:language, byte()}
  def placement_refusal(namespace, factory_id, timeout \\ Durations.get(:runner_lookup_ms)) do
    GenServer.call(__MODULE__, {:placement_refusal, namespace, factory_id}, timeout)
  end

  @doc """
  Runner ids across every cluster node holding active runners in `namespace`,
  resolved through the cluster directory. Serves `Flezha.list_runners/1`.
  An unreachable peer contributes nothing rather than failing the read.
  """
  @spec global_runners(String.t()) :: {:ok, [binary()]}
  def global_runners(namespace) do
    runners =
      [node() | Directory.runner_nodes(namespace)]
      |> Parallel.map_nodes(&:erpc.call(&1, __MODULE__, :active_runners, [namespace]),
        timeout: Durations.get(:runner_lookup_ms)
      )
      |> Enum.flat_map(fn
        {_node, {:ok, ids}} -> ids
        {_node, {:exit, _reason}} -> []
      end)
      |> Enum.uniq()

    {:ok, runners}
  end

  def disconnect_runner(namespace, runner_id) do
    GenServer.call(__MODULE__, {:disconnect_runner, namespace, runner_id}, manager_call_timeout())
  end

  @doc """
  Clear the `stream_ref` on this runner's `ControlChannel` so the existing
  rebind machinery (see `runner-controller-connection-healing`) picks up
  the next data stream the SDK opens. Called by
  `Flezha.Operative.Overwatch` after an end-to-end ping fails, indicating the
  controller-side `ControlChannel` is alive but its underlying transport
  handle is stale.

  Returns `:ok` on success, `{:error, :no_control_channel}` if the channel
  GenServer is not registered for `runner_id`.
  """
  @spec reset_control_channel(binary()) :: :ok | {:error, term()}
  def reset_control_channel(runner_id) when is_binary(runner_id) do
    ControlChannel.reset_stream_ref(runner_id)
  end

  @doc """
  Re-issue `ConnectSocket` to `runner_id` so the SDK re-opens its data sockets;
  the `StreamAcceptor` binds each fresh stream to an unbound `StreamChannel`
  and closes the rest (see `runner-controller-connection-healing`).

  Nothing calls this. It is the only expression of a remedy the platform does
  not otherwise have: no heal primitive rebinds a data stream, so a channel
  that unbinds while its runner stays connected is never rebound.

  Re-issuing is not a safe remedy as it stands. The SDK opens a whole pair set
  and moves its output path onto it, while the acceptor binds only as many
  streams as it has unbound channels — so the runner can be left emitting on
  sockets the acceptor closed.

  Returns `:ok`, or `{:error, :no_control_unit}` when the runner's control
  unit is not registered.
  """
  @spec rebind_data_streams(binary()) :: :ok | {:error, term()}
  def rebind_data_streams(runner_id) when is_binary(runner_id) do
    control_unit = Identificator.control_unit(runner_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {:error, :no_control_unit}

      _ ->
        SafeCall.bounded(
          control_unit,
          :command_sockets_connect,
          Durations.get(:control_unit_reply_timeout_ms),
          on_error: {:error, :no_control_unit}
        )
    end
  end

  def stop_namespace(namespace) do
    GenServer.call(__MODULE__, {:stop_namespace, namespace}, manager_call_timeout())
  end

  @impl true
  def init(_) do
    # Block boot until every per-namespace hub finishes its
    # marker-gated recovery cycle. With no hubs yet (the common case at
    # init time — hubs are created lazily by `runner_connect`), this
    # returns immediately. The gate matters on a restart where the
    # supervisor brings a hub up before FleetManager
    # is asked to serve registry-touching calls.
    case HubManager.await_recovery_complete(boot_recovery_timeout()) do
      :ok ->
        :ok

      {:error, {:recovery_timeout, hubs}} ->
        Logger.warning(
          "[fleet_manager] proceeding past recovery timeout; unfinished hubs=#{inspect(hubs)}"
        )
    end

    Marks.ensure_fence_table()

    {:ok,
     arm_runner_cleanup(%{
       runner_namespaces: %{},
       namespace_factories: %{},
       stable_sessions: %{},
       session_credentials: %{},
       runner_loads: %{},
       directory_groups: MapSet.new()
     })}
  end

  defp boot_recovery_timeout() do
    Durations.get(:boot_replay_timeout_ms)
  end

  @impl true
  def handle_call({:disconnect_runner, namespace, runner_id}, _from, state) do
    case get_in(state.runner_namespaces, [namespace, runner_id]) do
      nil ->
        {:reply, {:error, "runner not found"}, state}

      _ ->
        Logger.info("Disconnecting runner #{runner_id} from namespace #{namespace}")

        # Marked BEFORE the close is issued: the mark has to be committed by
        # the time `{:flz_closed, …}` reaches the channels, or they diagnose
        # a teardown we asked for as one we did not.
        Marks.mark_runner_retiring(runner_id)
        ControlUnit.disconnect(runner_id)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:runner_disconnected, namespace, runner_id}, _from, state) do
    state = Session.handle_runner_disconnect(state, namespace, runner_id)

    {:reply, :ok, Session.sync_directory(state)}
  end

  @impl true
  def handle_call({:active_runners, namespace}, _from, state) do
    {:reply, candidates(state, namespace, nil), state}
  end

  @impl true
  def handle_call({:runners_for_factory, namespace, factory_id}, _from, state) do
    {:reply, candidates(state, namespace, factory_id), state}
  end

  @impl true
  def handle_call({:factories_for_runner, namespace, runner_id}, _from, state) do
    {:reply, Factories.runner_factories(state, namespace, runner_id), state}
  end

  # Filter, select and count in one step. Splitting it — a read here and a cast
  # to count — lets another batch member's read overtake the count, and the
  # batch is exactly where the spread matters.
  @impl true
  def handle_call({:resolve_runner, namespace, factory_id, operative_id}, _from, state) do
    case candidates(state, namespace, factory_id) do
      [] ->
        {:reply, nil, state}

      candidates ->
        {cache, entry} =
          Load.refreshed(state.runner_loads, namespace, fn -> rebuild_loads(state, namespace) end)

        {entry, runner_id} = Load.place(entry, candidates, operative_id)

        {:reply, runner_id, %{state | runner_loads: Map.put(cache, namespace, entry)}}
    end
  end

  @impl true
  def handle_call({:placement_refusal, namespace, factory_id}, _from, state) do
    {info, advertising} = Factories.advertising_runners(state, namespace, factory_id)

    # The message must read the same derivation as the decision, or the two
    # drift exactly where the record is missing and the operator is told a
    # factory is established at a language nobody ever established.
    refusal =
      cond do
        not Enum.empty?(Factories.placeable_runners(state, namespace, factory_id)) ->
          :none

        Enum.empty?(advertising) ->
          :no_such_factory

        true ->
          case Factories.established_lang(namespace, factory_id, info, advertising) do
            {:ok, lang} -> {:language, lang}
            :ambiguous -> :ambiguous_language
            :none -> :no_language_declared
          end
      end

    {:reply, refusal, state}
  end

  @impl true
  def handle_call({:stop_namespace, namespace}, _from, state) do
    {reply, runner_namespaces} =
      case Map.get(state.runner_namespaces, namespace, nil) do
        nil ->
          {:namespace_not_found, state.runner_namespaces}

        _ ->
          # The `:namespace` walk: the operatives, the runners, the plugin
          # registrations and the hub, in that order, with the namespace read
          # as quiesced for its duration.
          target = %{
            namespace: namespace,
            runner_ids: Roster.get_runner_ids(state, namespace, :active)
          }

          _ = Shutdown.run(:namespace, target, Shutdown.stop_timeout_ms())

          {:ok, Map.delete(state.runner_namespaces, namespace)}
      end

    state = Map.put(state, :runner_namespaces, runner_namespaces)
    state = Map.update!(state, :namespace_factories, &Map.delete(&1, namespace))
    state = Map.update!(state, :runner_loads, &Map.delete(&1, namespace))
    {:reply, reply, Session.sync_directory(state)}
  end

  @impl true
  def handle_call({:list_factories, namespace}, _from, state) do
    factories =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Enum.map(fn {factory_id, info} ->
        %{id: factory_id, description: info.description}
      end)
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, factories}, state}
  end

  @impl true
  def handle_call({:runner_connect, stream_ref, connect}, _from, state) do
    # runner_connect/3 mints a fresh per-connection SESSION TOKEN (the
    # Registry/StreamChannel naming + routing key) and brings up its supervision
    # tree. That tree can fail to come up for reasons that belong to this runner
    # alone — an acceptor whose socket the kernel refuses, for one — and this
    # manager serves every other runner on the node, so the connect is refused
    # rather than allowed to take the manager (and the transport server linked
    # to it) down.
    case Session.catch_runner_connect(stream_ref, connect.namespace, connect.transport) do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:ok, _namespace, session_token} ->
        Session.finish_runner_connect(state, session_token, connect)
    end
  end

  @impl true
  def handle_call({:evict_credential, hash}, _from, state) do
    sessions = for {session, held} <- state.session_credentials, held == hash, do: session

    state =
      sessions
      |> Enum.reduce(state, &Session.do_evict_runner(&2, &1))
      |> Session.sync_directory()

    {:reply, length(sessions), state}
  end

  @impl true
  def handle_call({:evict_runner, runner_id, proved_wedged}, _from, state) do
    # Safe no-op on a healthy target: never evict a runner that is
    # currently `:active` (connection open). The convergence Diff runs off
    # a snapshot that can lag a reconnect by a tick; acting on that stale
    # view is exactly the false-eviction → cascade-rebind regression this
    # change exists to remove. The authoritative live set is the gate.
    # (The reconnect binding-swap closes a prior session by id directly, not
    # through this guarded path.)
    if not proved_wedged and Roster.runner_active?(state, runner_id) do
      {:reply, :ok, state}
    else
      {:reply, :ok, state |> Session.do_evict_runner(runner_id) |> Session.sync_directory()}
    end
  end

  @impl true
  def handle_call(:runner_cleanup, _from, state) do
    {:reply, :ok, state |> runner_cleanup() |> Session.sync_directory()}
  end

  # Full-set replacement of one runner's declared factory list, cast from
  # `ControlChannel` on an SDK `UpdateFactories` frame (post-start
  # define/remove). Composes the existing removal + store paths, so redelivery
  # is idempotent and other runners' factory refs are untouched. An empty set
  # is legal: the runner stays connected with zero placeable factories.
  @impl true
  def handle_cast({:sync_factories, namespace, runner_id, factories}, state) do
    dropped = Factories.dropped_factory_ids(state, namespace, runner_id, factories)

    state =
      state
      |> Factories.remove_ce_factories(namespace, runner_id)
      |> Factories.store_factories(namespace, runner_id, factories)
      |> Factories.prune_empty_factories(namespace)

    Factories.warn_dropped_factories(state, namespace, runner_id, dropped)

    {:noreply, Session.sync_directory(state)}
  end

  @impl true
  def handle_info(:schedule_runner_cleanup, state) do
    {:noreply, state |> arm_runner_cleanup() |> runner_cleanup() |> Session.sync_directory()}
  end

  @impl true
  def handle_info(any, state) do
    Logger.info("Received unexpected message at node manager: #{inspect(any)}")

    {:noreply, state}
  end

  # The one definition of "which runners may take this work": every active
  # runner, or the subset the placement filter allows for a factory.
  defp candidates(state, namespace, nil),
    do: Roster.get_runner_ids(state, namespace, :active)

  defp candidates(state, namespace, factory_id) when is_binary(factory_id),
    do: MapSet.to_list(Factories.placeable_runners(state, namespace, factory_id))

  # Counted from the hub registry's own record of who owns each operative, over
  # the same universe a placement chooses from. A namespace whose hub is not up
  # holds no recorded binding, so every active runner counts zero and the
  # handouts alone spread the work.
  defp rebuild_loads(state, namespace) do
    Load.counts(HubManager.registry_dump(namespace), candidates(state, namespace, nil))
  end

  defp arm_runner_cleanup(state) do
    arm_timer(
      state,
      :runner_cleanup,
      get_runner_keepalive() + Durations.get(:runner_cleanup_slack_ms),
      :schedule_runner_cleanup
    )
  end

  defp get_runner_keepalive() do
    keepalive = Config.get(:runner_keepalive)

    cond do
      is_binary(keepalive) -> String.to_integer(keepalive)
      is_integer(keepalive) -> keepalive
    end
  end

  # `expired_runners/4` answers `nil` for a runner still inside its keepalive.
  defp drop_runner_ids(runners, removal_runners) do
    Enum.reduce(removal_runners, runners, fn
      nil, acc -> acc
      runner_id, acc -> Map.delete(acc, runner_id)
    end)
  end

  # Which of `namespace`'s inactive runners have outlived the keepalive. The hub
  # is reclaimed on the way past only when it holds no (suspended) operatives.
  defp expired_runners(state, namespace, now, keep_alive) do
    state
    |> Roster.get_runner_ids(namespace, :inactive)
    |> Enum.map(fn runner_id ->
      {_status, timestamp} = state.runner_namespaces[namespace][runner_id]

      if timestamp + keep_alive <= now do
        HubManager.stop_hub_if_vacant(namespace)
        runner_id
      end
    end)
  end

  defp runner_cleanup(state) do
    Marks.prune()

    now = System.monotonic_time(:millisecond)
    keep_alive = get_runner_keepalive()

    namespaces = Map.keys(state.runner_namespaces)

    removal = Enum.map(namespaces, &{&1, expired_runners(state, &1, now, keep_alive)})

    runner_namespaces =
      Enum.reduce(removal, state.runner_namespaces, fn {namespace, removal_runners}, acc ->
        Map.put(acc, namespace, drop_runner_ids(acc[namespace], removal_runners))
      end)

    state = %{state | runner_namespaces: runner_namespaces}

    # Clean factory refs for removed CEs
    Enum.reduce(removal, state, fn {namespace, removal_runners}, acc ->
      removal_runners
      |> Enum.reduce(acc, fn
        nil, acc -> acc
        ce_id, acc -> Factories.remove_ce_factories(acc, namespace, ce_id)
      end)
      |> Factories.prune_empty_factories(namespace)
    end)
  end

  # runner_connect/3 may block inside Orchestrator.await_cluster_ready/0
  # (bounded by :cluster_formation_timeout_ms, default 15s) while the cluster
  # forms after a restart. The connect call must outlast that window plus the
  # rest of the connect work, or the caller's default 5s timeout fires and
  # strands every runner that connects during formation.
  # Every call lands on this one serialized process, and a connect is allowed to
  # take `runner_connect_timeout/0` — so a caller waiting the `GenServer.call`
  # default of 5 s times out whenever it arrives while a connect is in flight.
  # That is not contention to ride out: the manager is permitted to be busy for
  # longer than the caller was willing to wait. Every caller gets the same
  # budget as the work the manager may be doing.
  defp manager_call_timeout, do: runner_connect_timeout()

  defp runner_connect_timeout, do: Durations.get(:cluster_formation_wait_ms)
end

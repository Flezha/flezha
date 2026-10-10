defmodule Flezha.Runner.FleetManager do
  @moduledoc """
  The node's fleet: which runners are connected, in which namespace, and what
  factories each advertises.

  One serialized GenServer owns that state and answers every placement
  question. The state it mutates is split by what it is — `Marks` (the
  takeover fence and the retiring mark), `Roster` (the runner index),
  `Factories` (the factory table and each factory's SDK language) and
  `Session` (the handshake, connect, disconnect, evict, reroute) — while the
  public API stays here. `Marks` also holds each live session's stable
  identity, which a relay's ownership record names.

  Invariant: a disconnect drops the runner from the active set
  unconditionally; what varies is only what becomes of its relays.
  """

  alias Flezha.AuthZ.RunnerProof
  alias Flezha.Cluster.Directory
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Parallel
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager.Announce
  alias Flezha.Runner.FleetManager.Factories
  alias Flezha.Runner.FleetManager.Load
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.FleetManager.Roster
  alias Flezha.Runner.FleetManager.Session
  alias Flezha.Runner.StreamPool
  alias Flezha.SDK.SafeCall
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
  `list_factories/1` on every node holding active runners in `namespace`: one
  row per node that advertises a factory.
  """
  @spec global_factories(String.t()) :: [map()]
  def global_factories(namespace), do: across_runner_nodes(namespace, :factory_rows)

  @doc "`factory_id`'s row of this node's `list_factories/1`, or `nil`."
  @spec factory_row(String.t(), String.t()) :: map() | nil
  def factory_row(namespace, factory_id),
    do: namespace |> factory_rows() |> Enum.find(&(&1.id == factory_id))

  @doc false
  # This node's rows, read over `:erpc` by `global_factories/1`.
  def factory_rows(namespace) do
    {:ok, rows} = list_factories(namespace)
    rows
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
  Return once the session `runner_id` is admitted. A runner can send a request
  before its handshake completes, and until then the roster holds neither the
  runner nor its factories, so a request judged against the roster waits here.
  `:ok` at once for a session not in its handshake; `{:error, :not_admitted}`
  when the handshake ends without admitting it.
  """
  @spec await_admitted(binary()) :: :ok | {:error, :not_admitted}
  def await_admitted(runner_id) when is_binary(runner_id) do
    SafeCall.bounded(
      __MODULE__,
      {:await_admitted, runner_id},
      Durations.get(:control_unit_reply_timeout_ms) + manager_call_timeout(),
      on_error: {:error, :not_admitted}
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
  Atomically evict a dead/orphaned runner binding: stop its runner supervisor
  (its stream channels, and every route they held, go with it), drop its
  factory refs, and remove it from `runner_namespaces`. Its relays become
  misbound and rebind onto
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
  which waiting relays a runner arriving in the namespace is news for. An
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

  Used by `RelayManager.assign_runner/3` so we never route a
  relay to a node that doesn't own its factory — without this filter
  every input frame for a misrouted relay hits a dead registry entry
  on the foreign node and generates a `enqueue: relay X not found`
  warning on every input.
  """
  def runners_for_factory(namespace, factory_id, timeout \\ Durations.get(:runner_lookup_ms)) do
    GenServer.call(__MODULE__, {:runners_for_factory, namespace, factory_id}, timeout)
  end

  @doc """
  The runner `relay_id` is placed on in `namespace`, or `nil` when the
  candidate set is empty.

  Candidates come from the same filter `runners_for_factory/3` answers with — a
  `nil` factory widens it to every active runner — and one of them is chosen by
  steps-load rendezvous hashing (`Load.select/3`). The choice is counted
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
        relay_id,
        timeout \\ Durations.get(:runner_lookup_ms)
      ) do
    GenServer.call(
      __MODULE__,
      {:resolve_runner, namespace, factory_id, relay_id},
      timeout
    )
  end

  @doc """
  Why `factory_id` has no placement candidate in `namespace`.

  `:none` when some runner can take it; `:no_such_factory` when no runner
  advertises the id at all; `{:language, established}` or `{:run, established}`
  when runners advertise it at another language or run than the one the
  factory is established at — a factory that exists and is simply unplaceable
  here, which must not be reported as a factory that does not exist.
  """
  @spec placement_refusal(String.t(), String.t(), timeout()) ::
          :none
          | :no_such_factory
          | :ambiguous_language
          | :no_language_declared
          | {:language, byte()}
          | :ambiguous_run
          | :no_run_declared
          | {:run, :flow | :steps}
  def placement_refusal(namespace, factory_id, timeout \\ Durations.get(:runner_lookup_ms)) do
    GenServer.call(__MODULE__, {:placement_refusal, namespace, factory_id}, timeout)
  end

  @doc """
  How `factory_id` is established to run in `namespace`, which a start takes
  rather than names: `{:ok, :flow | :steps}` when a runner here can place
  it, else `{:error, refusal}` in `placement_refusal/3`'s vocabulary.
  """
  @spec factory_run(String.t(), String.t(), timeout()) ::
          {:ok, :flow | :steps} | {:error, term()}
  def factory_run(namespace, factory_id, timeout \\ Durations.get(:runner_lookup_ms)) do
    GenServer.call(__MODULE__, {:factory_run, namespace, factory_id}, timeout)
  end

  @doc """
  Runner ids across every cluster node holding active runners in `namespace`,
  resolved through the cluster directory. Serves `Flezha.list_runners/1`.
  An unreachable peer contributes nothing rather than failing the read.
  """
  @spec global_runners(String.t()) :: {:ok, [binary()]}
  def global_runners(namespace),
    do: {:ok, namespace |> across_runner_nodes(:active_runners) |> Enum.uniq()}

  @doc """
  `{runner id, process id}` for every active runner in `namespace` across the
  cluster, with `nil` where the runner's transport cannot say. Serves
  `flz runner list`.
  """
  @spec global_runner_pids(String.t()) :: {:ok, [{binary(), non_neg_integer() | nil}]}
  def global_runner_pids(namespace),
    do: {:ok, namespace |> across_runner_nodes(:runner_pids) |> Enum.uniq_by(&elem(&1, 0))}

  @doc false
  # This node's half of `global_runner_pids/1`, reached over `:erpc`.
  def runner_pids(namespace),
    do: Enum.map(active_runners(namespace), &{&1, ControlChannel.peer_pid(&1)})

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
  # This node's half of `find_session/2`, reached over `:erpc`. The pid is read
  # from the channel outside this manager, which the channel may be calling.
  def session_rows(namespace, runner_id) do
    case GenServer.call(
           __MODULE__,
           {:session_identity, namespace, runner_id},
           manager_call_timeout()
         ) do
      nil -> []
      identity -> [Map.merge(identity, %{pid: ControlChannel.peer_pid(runner_id), node: node()})]
    end
  end

  @doc """
  Close the session, live or in its handshake, of the runner process that
  `stable_id` and `incarnation` name, as one this controller asked to leave:
  its relays are re-placed without waiting out the grace. The runner is not
  told to stop, so it reconnects, and a standing drop mark refuses it. Answers
  the number of sessions closed, `0` when there is no fleet to reach.
  """
  @spec drop_matching(binary(), binary()) :: non_neg_integer()
  def drop_matching(stable_id, incarnation) when is_binary(stable_id) do
    SafeCall.bounded(__MODULE__, {:drop_matching, stable_id, incarnation}, manager_call_timeout(),
      on_error: 0,
      log: false
    )
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

  def disconnect_runner(namespace, runner_id) do
    GenServer.call(__MODULE__, {:disconnect_runner, namespace, runner_id}, manager_call_timeout())
  end

  @doc """
  Clear the `stream_ref` on this runner's `ControlChannel` so the existing
  rebind machinery (see `runner-controller-connection-healing`) picks up
  the next data stream the SDK opens. Called by
  `Flezha.Relay.Overwatch` after an end-to-end ping fails, indicating the
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
  Rebuild `runner_id`'s data plane in its live session: every data channel lets
  its stream go, then `ConnectSocket` is re-issued. The SDK opens a whole pair
  set, and only a pool with every channel free binds all of it; one that still
  holds the old streams closes the fresh ones it has no room for, and the runner
  is left emitting on sockets nobody reads (see
  `runner-controller-connection-healing`). The runner's `ControlChannel` calls
  this when a data stream is lost.

  Answers what the runner answered: `{:ok, _}` once its pair set is up,
  `{:error, reason}` for a refusal, and `{:error, :no_control_unit}` when the
  control unit is gone or does not answer within `control_unit_reply_timeout_ms`.
  """
  @spec rebind_data_streams(binary()) :: {:ok, binary()} | {:error, term()}
  def rebind_data_streams(runner_id) when is_binary(runner_id) do
    StreamPool.call_channels(runner_id, :release_stream)

    SafeCall.bounded(
      Identificator.control_unit(runner_id),
      :command_sockets_connect,
      Durations.get(:control_unit_reply_timeout_ms),
      on_error: {:error, :no_control_unit}
    )
  end

  @doc """
  Declare again, to `namespace`'s freshly started hub, which of its factories
  this node's fleet can run. A hub's group table dies with the hub, so a
  restarted one holds nothing this controller declared before it.
  """
  @spec hub_started(String.t()) :: :ok
  def hub_started(namespace), do: GenServer.cast(__MODULE__, {:hub_started, namespace})

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

    Marks.ensure_tables()

    {:ok,
     arm_runner_cleanup(%{
       runner_namespaces: %{},
       namespace_factories: %{},
       # Each stable identity's latest session and the credential that admitted
       # it (`Session.check_identity_pin/2`).
       stable_sessions: %{},
       session_credentials: %{},
       pending_handshakes: %{},
       handshake_requests: :gen_server.reqids_new(),
       runner_loads: %{},
       directory_groups: MapSet.new(),
       # What this node last declared to each namespace hub as the groups it
       # serves, so a sync announces only what moved.
       announced_groups: %{}
     })}
  end

  defp boot_recovery_timeout() do
    Durations.get(:boot_replay_timeout_ms)
  end

  @impl true
  def handle_call({:disconnect_runner, namespace, runner_id}, _from, state) do
    case get_in(state.runner_namespaces, [namespace, runner_id]) do
      nil ->
        {:reply, {:error, {:runner_not_found, runner_id}}, state}

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
    # Read before the disconnect clears the mark: a retiring runner is one this
    # controller asked to leave, so its factories are withdrawn from the
    # namespace hub without waiting out the grace.
    retiring? = Marks.runner_retiring?(runner_id)
    state = Session.handle_runner_disconnect(state, namespace, runner_id)

    {:reply, :ok, Announce.sync(state, immediate_withdrawal: retiring?)}
  end

  @impl true
  def handle_call({:drop_matching, stable_id, incarnation}, _from, state) do
    {state, pending} =
      Session.end_handshakes(
        state,
        &(&1.stable_id == stable_id and &1.incarnation == incarnation),
        :dropped
      )

    case Map.get(state.stable_sessions, stable_id) do
      {session, _credential, ^incarnation} ->
        if Roster.runner_active?(state, session) do
          Logger.info("Dropping runner #{session} (stable identity #{stable_id})")
          # Marked before the close, as `disconnect_runner` does, so the close
          # is torn down as one this controller asked for.
          Marks.mark_runner_retiring(session)
          ControlChannel.close(session)
          {:reply, pending + 1, state}
        else
          {:reply, pending, state}
        end

      _no_session ->
        {:reply, pending, state}
    end
  end

  @impl true
  def handle_call({:session_identity, namespace, runner_id}, _from, state) do
    reply =
      case get_in(state.runner_namespaces, [namespace, runner_id]) do
        {:active, _since} ->
          Enum.find_value(
            state.stable_sessions,
            %{stable_id: nil, incarnation: nil},
            fn
              {stable_id, {^runner_id, _credential, incarnation}} ->
                %{stable_id: stable_id, incarnation: incarnation}

              _other ->
                nil
            end
          )

        _absent_or_inactive ->
          nil
      end

    {:reply, reply, state}
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
  def handle_call({:resolve_runner, namespace, factory_id, relay_id}, _from, state) do
    case candidates(state, namespace, factory_id) do
      [] ->
        {:reply, nil, state}

      candidates ->
        {cache, entry} =
          Load.refreshed(state.runner_loads, namespace, fn -> rebuild_loads(state, namespace) end)

        {entry, runner_id} = Load.place(entry, candidates, relay_id)

        {:reply, runner_id, %{state | runner_loads: Map.put(cache, namespace, entry)}}
    end
  end

  @impl true
  def handle_call({:placement_refusal, namespace, factory_id}, _from, state) do
    {:reply, placement_refusal_of(state, namespace, factory_id), state}
  end

  @impl true
  def handle_call({:factory_run, namespace, factory_id}, _from, state) do
    reply =
      case placement_refusal_of(state, namespace, factory_id) do
        :none -> {:ok, run_of(state, namespace, factory_id)}
        refusal -> {:error, refusal}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:stop_namespace, namespace}, _from, state) do
    # A session still in its handshake is the namespace's too, and would write
    # the namespace back into the roster if it were left to complete.
    {state, _} = Session.end_handshakes(state, &(&1.namespace == namespace), :namespace_stopped)

    {reply, runner_namespaces} =
      case Map.get(state.runner_namespaces, namespace, nil) do
        nil ->
          {:namespace_not_found, state.runner_namespaces}

        _ ->
          # The `:namespace` walk: the relays, the runners, the plugin
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
    {:reply, reply, Announce.sync(state)}
  end

  @impl true
  def handle_call({:list_factories, namespace}, _from, state) do
    factories =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Enum.map(fn {factory_id, info} ->
        %{
          id: factory_id,
          description: info.description,
          run: run_of(state, namespace, factory_id)
        }
        |> Map.merge(Factories.latest_api(info))
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
    with :ok <- Session.check_identity_pin(state, connect),
         :ok <- Session.check_not_dropped(connect, stream_ref),
         {:ok, namespace, session_token} <- Session.catch_runner_connect(stream_ref, connect) do
      state = Session.await_handshake(state, session_token, stream_ref, connect)
      {:reply, {:ok, {namespace, session_token}}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:await_admitted, runner_id}, from, state) do
    case Session.hold_until_admitted(state, runner_id, from) do
      {:held, state} -> {:noreply, state}
      :not_pending -> {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:evict_credential, hash}, _from, state) do
    sessions = for {session, held} <- state.session_credentials, held == hash, do: session
    state = Enum.reduce(sessions, state, &Session.do_evict_runner(&2, &1))

    # A handshake the credential admitted is not live yet, and would be the day
    # after the revocation if it were left to complete.
    {state, pending} = Session.end_handshakes(state, &(&1.credential == hash), :revoked)

    {:reply, length(sessions) + pending, Announce.sync(state)}
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
      {:reply, :ok, state |> Session.do_evict_runner(runner_id) |> Announce.sync()}
    end
  end

  @impl true
  def handle_call(:runner_cleanup, _from, state) do
    {:reply, :ok, state |> runner_cleanup() |> Announce.sync()}
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

    {:noreply, Announce.sync(state)}
  end

  @impl true
  def handle_cast({:hub_started, namespace}, state) do
    {:noreply, state |> Announce.forget(namespace) |> Announce.sync()}
  end

  @impl true
  def handle_info(:schedule_runner_cleanup, state) do
    {:noreply, state |> arm_runner_cleanup() |> runner_cleanup() |> Announce.sync()}
  end

  # The grace elapsed with no runner for the factory back. Withdrawing it is
  # what hands its relays to a controller that can still run them.
  @impl true
  def handle_info({:withdraw_group, namespace, factory_id}, state) do
    {:noreply, Announce.withdraw(state, namespace, [factory_id])}
  end

  @impl true
  def handle_info({:handshake_timeout, session_token}, state),
    do: {:noreply, Session.end_handshake(state, session_token, :timeout)}

  # A handshake answer arrives as the reply to a request `Session.await_handshake/4`
  # sent, which only the request collection can recognise.
  @impl true
  def handle_info(any, state) do
    case :gen_server.check_response(any, state.handshake_requests, true) do
      {response, {session_token, command}, requests} ->
        state = %{state | handshake_requests: requests}
        {:noreply, Session.handshake_answered(state, session_token, command, response)}

      _not_an_answer ->
        Logger.info("Received unexpected message at node manager: #{inspect(any)}")
        {:noreply, state}
    end
  end

  # The message must read the same derivation as the decision, or the two
  # drift exactly where the record is missing and the user is told a
  # factory is established at a value nobody ever established.
  defp placement_refusal_of(state, namespace, factory_id) do
    case Factories.placement(state, namespace, factory_id) do
      {:ok, runners} ->
        if Enum.empty?(runners), do: :no_such_factory, else: :none

      {:refused, attribute, verdict} ->
        refusal(attribute, verdict)
    end
  end

  defp refusal(:sdk_lang, {:ok, lang}), do: {:language, lang}
  defp refusal(:sdk_lang, :ambiguous), do: :ambiguous_language
  defp refusal(:sdk_lang, :none), do: :no_language_declared
  defp refusal(:run, {:ok, run}), do: {:run, run}
  defp refusal(:run, :ambiguous), do: :ambiguous_run
  defp refusal(:run, :none), do: :no_run_declared
  defp refusal(:shape, {:ok, shape}), do: {:run, shape}
  defp refusal(:shape, :ambiguous), do: :ambiguous_run

  # The run a listed factory is established at, `nil` while nothing decides it.
  defp run_of(state, namespace, factory_id) do
    {info, advertising} = Factories.advertising_runners(state, namespace, factory_id)

    case Factories.established(namespace, factory_id, :run, info, advertising) do
      {:ok, run} -> run
      _undecided -> nil
    end
  end

  # The one definition of "which runners may take this work": every active
  # runner, or the subset the placement filter allows for a factory.
  defp candidates(state, namespace, nil),
    do: Roster.get_runner_ids(state, namespace, :active)

  defp candidates(state, namespace, factory_id) when is_binary(factory_id),
    do: MapSet.to_list(Factories.placeable_runners(state, namespace, factory_id))

  # Counted from the hub registry's own record of who owns each relay, over
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
  # is reclaimed on the way past only when it holds no (suspended) relays.
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

defmodule Flezha.Service.RelayManager do
  @moduledoc """
  Manages the relay startings and stoppings.
  """

  alias Flezha.Cluster.Directory
  alias Flezha.Cluster.Reads
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Relay.Builder, as: RelayBuilder
  alias Flezha.Relay.Calls.Inbound
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Ingest.Verdict
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Plugin.Host
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager.Admission
  alias Flezha.Service.RelayManager.Init
  alias Flezha.Service.RelayManager.Placement
  alias Flezha.Service.RelayManager.Spawn
  alias Flezha.Service.RelayManager.Teardown
  alias Flezha.Shutdown
  alias Flezha.State.DurabilityTier
  alias Flezha.State.InputLog
  alias Flezha.State.Completion
  alias Flezha.State.Paths

  defdelegate assign_runner(arg1, arg2, arg3), to: Placement
  defdelegate await_runner(arg1, arg2, arg3, arg4, arg5 \\ fn _ -> true end), to: Placement
  @doc false
  defdelegate pick_runner(arg1, arg2), to: Placement
  defdelegate redeclare(arg1, arg2), to: Init
  defdelegate stamp_window_end(arg1, arg2, arg3), to: Init
  defdelegate window_end(arg1, arg2), to: Init
  defdelegate runner_registered?(arg1), to: Placement

  # How many ids a start without one draws before it is refused.
  @id_draws 3

  # The hub's rows of relays: a plugin entry's row is keyed `{:plugin, name}`.
  def relay_registry(namespace) do
    with {:ok, registry} <- HubManager.get_registry(namespace),
         do: Map.filter(registry, fn {id, _row} -> is_binary(id) end)
  end

  def get_relay_ids(namespace),
    do: {:ok, namespace |> HubManager.get_all_relays() |> Enum.map(&elem(&1, 0))}

  @doc """
  Resolve `relay_id`'s pid from ANY controller node. A node that is not a
  member of the namespace's hub asks one hub-member node (resolved through the
  cluster directory, forwarded at most once); every pid-interaction verb then
  rides BEAM distribution on the returned pid, so the interaction still
  executes inside the relay's own node.
  """
  def get_relay_pid(namespace, relay_id, opts \\ []),
    do: on_hub_member(namespace, relay_id, {HubManager, :get_pid, [namespace, relay_id]}, opts)

  @doc """
  Run `module.function(args)` where `namespace`'s hub is: here when this node
  is a hub member, or when the call was already forwarded once (`routed:
  true`, the loop guard); else on one hub-member node the directory names,
  chosen by `key`. With no hub anywhere it runs here, which keeps the answer a
  down hub gives. A member that does not answer is `{:error,
  {:routed_call_failed, node, kind, reason}}`.
  """
  @spec on_hub_member(binary(), term(), {module(), atom(), list()}, keyword()) :: term()
  def on_hub_member(namespace, key, {module, function, args} = call, opts \\ []) do
    if Keyword.get(opts, :routed, false) or HubManager.hub_alive?(namespace) do
      apply(module, function, args)
    else
      case List.delete(Directory.hub_nodes(namespace), node()) do
        [] ->
          apply(module, function, args)

        nodes ->
          target = Placement.pick_runner(nodes, key)

          Reads.on(target, __MODULE__, :on_hub_member, [namespace, key, call, [routed: true]])
      end
    end
  end

  @doc """
  Run `module.function(args)` on the controller holding `instance_id`'s state:
  the one hosting its Overwatch while it runs, else the one whose state root
  holds its class. When no controller holds it, it runs here if every
  controller answered, and is refused `state_unreachable`, naming the
  controllers that did not, otherwise.
  """
  @spec on_state_node(String.t(), module(), atom(), list()) :: term()
  def on_state_node(instance_id, module, function, args) do
    case state_node(instance_id) do
      {:ok, holder} when holder == node() ->
        apply(module, function, args)

      {:ok, holder} ->
        Reads.on(holder, module, function, args,
          on_error: fn _failed -> {:error, {:state_unreachable, instance_id, [holder]}} end
        )

      {:error, []} ->
        apply(module, function, args)

      {:error, unreachable} ->
        {:error, {:state_unreachable, instance_id, unreachable}}
    end
  end

  defp state_node(instance_id) do
    with {:ok, namespace} <- namespace_of(instance_id),
         {:ok, pid} <- get_relay_pid(namespace, instance_id) do
      {:ok, node(pid)}
    else
      _not_running ->
        with {:ok, holder, true} <-
               Reads.find(Reads.controllers(), Paths, :holds_class?, [instance_id]),
             do: {:ok, holder}
    end
  end

  # Resolve the pid, then run `fun` on it — every error passes through.
  defp with_relay_pid(namespace, relay_id, fun) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, relay_pid} -> fun.(relay_pid)
      err -> err
    end
  end

  @doc """
  The relay's Overwatch state. `:facts` answers it with the relay's own data —
  init values, snapshot, held outputs and seed — as `nil`, since that data can
  run to any size.
  """
  def get_overwatch_state(namespace, relay_id, view \\ :full) do
    request = if view == :facts, do: :get_overwatch_facts, else: :get_overwatch_state

    # `SafeCall.bounded` returns `{:ok, state} | {:error, reason}` — the same
    # success shape as before, but a wedged relay now yields a logged
    # `{:error, :timeout}` (with the target's mailbox/current-function) instead
    # of an unhandled exit.
    with_relay_pid(namespace, relay_id, &call_overwatch(&1, request))
  end

  @doc "A bounded call to the Overwatch `pid`: `{:ok, reply}` or `{:error, reason}`."
  def call_overwatch(pid, request),
    do: SafeCall.bounded(pid, request, overwatch_call_timeout_ms())

  @doc """
  Starts a new relay to the runner.

  Location-transparent: a node that cannot satisfy the start itself (no
  namespace hub / no runner advertising the factory) resolves the capable
  nodes through the cluster directory and executes the same call on one of
  them, forwarded at most once (`routed: true`). The executing node
  re-validates everything, so a stale directory answer costs one failed
  forward, never a wrong start.

  This is the single start-authority seam: every in-cluster starter — the
  CLI/manager, a schedule's slot, a tasklet's `spawn_steps`, and the
  orphan reconcile through the same durable registry — resolves the id claim
  here. A start that loses the claim answers `:duplicate` and leaves the
  winner strictly alone; it is never reported as a failed start, because the
  failure path stops the child it names.

  A start whose builder carries no id is given one here, before it is placed or
  forwarded, so every node it touches acts on the same id. A generated id that
  meets a start meaning the id is taken is drawn again (`with_generated_id/3`);
  a caller's own id never is. `generated: true` marks an id a caller drew
  through `with_generated_id/3` itself, so it gets the checks only a generated
  id gets. `:mint` replaces the generator, for a test that forces a clash.
  `origin:` names who or what started the run (default `%{kind: "reconcile"}`),
  `announce: false` starts it unannounced, and `record?:` (default `announce`)
  says whether the relay history records the start.
  """
  @spec start_relay(String.t(), RelayBuilder.t(), keyword()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def start_relay(namespace, relay, opts \\ [])

  def start_relay(namespace, %{id: nil} = relay, opts) do
    {draw_opts, opts} = Keyword.split(opts, [:mint])
    opts = Keyword.put(opts, :generated, true)
    start = &start_relay(namespace, %{relay | id: &1}, opts)

    with_generated_id(relay.factory_id, start, draw_opts)
  end

  def start_relay(namespace, relay, opts) do
    if Keyword.get(opts, :routed, false) or locally_startable?(namespace, relay.factory_id) do
      do_start_relay(namespace, relay, opts)
    else
      route_start(namespace, relay, opts)
    end
  end

  @doc """
  Run `attempt` with an id generated for `factory_id`, drawing again while its
  verdict means the id was taken, at most `#{@id_draws}` draws, then refuse.
  Every start that generates an id goes through here. `reserve:` holds back
  bytes for a suffix the caller appends; `mint:` replaces the generator.
  """
  @spec with_generated_id(String.t(), (String.t() -> verdict), keyword()) ::
          verdict | {:error, {:no_free_id, String.t(), pos_integer()}}
        when verdict: term()
  def with_generated_id(factory_id, attempt, opts \\ []) do
    reserve = Keyword.get(opts, :reserve, 0)
    mint = Keyword.get(opts, :mint, &Identificator.mint(:relay, &1, reserve: reserve))

    draw_id(factory_id, mint, attempt, @id_draws)
  end

  defp draw_id(factory_id, _mint, _attempt, 0),
    do: {:error, {:no_free_id, factory_id, @id_draws}}

  defp draw_id(factory_id, mint, attempt, draws) do
    verdict = attempt.(mint.(factory_id))

    if id_taken?(verdict),
      do: draw_id(factory_id, mint, attempt, draws - 1),
      else: verdict
  end

  # The verdicts that mean the id, not the start, was refused: a live or stale
  # claim, another namespace's claim on its state, or a group or leftover
  # state the id already names.
  defp id_taken?(:duplicate), do: true
  defp id_taken?({:error, :id_taken}), do: true
  defp id_taken?({:error, {:namespace_conflict, _class, _owner}}), do: true
  defp id_taken?({:error, {:id_in_use, _id}}), do: true
  defp id_taken?(_verdict), do: false

  defp locally_startable?(namespace, factory_id) do
    HubManager.hub_alive?(namespace) and local_runners?(namespace, factory_id)
  end

  defp local_runners?(namespace, factory_id) when is_binary(factory_id),
    do: FleetManager.runners_for_factory(namespace, factory_id) != []

  defp local_runners?(namespace, _factory_id),
    do: FleetManager.active_runners(namespace) != []

  defp capable_nodes(namespace, factory_id) when is_binary(factory_id),
    do: List.delete(Directory.factory_nodes(namespace, factory_id), node())

  defp capable_nodes(namespace, _factory_id),
    do: List.delete(Directory.runner_nodes(namespace), node())

  defp route_start(namespace, relay, opts) do
    case capable_nodes(namespace, relay.factory_id) do
      [] ->
        {:error, Admission.cluster_wide_refusal(namespace, relay.factory_id)}

      nodes ->
        target = Placement.pick_runner(nodes, relay.id)

        Reads.on(target, __MODULE__, :start_relay, [
          namespace,
          relay,
          Keyword.put(opts, :routed, true)
        ])
    end
  end

  defp do_start_relay(namespace, relay, opts) do
    generated? = Keyword.get(opts, :generated, false)

    with :ok <- Admission.check_not_shutting_down(namespace),
         :ok <- Admission.check_generated_id_free(namespace, relay.id, generated?),
         :ok <- Admission.validate_relay(namespace, relay),
         :ok <- Admission.refuse_group_base_claim(namespace, relay),
         {:ok, relay} <- Admission.resolve_factory(namespace, relay),
         {:ok, answer} <- start_walk(namespace, stamped(relay, opts), opts) do
      answer
    else
      {:error, reason} ->
        if Init.claim_loss?(reason),
          do: Init.claim_lost(&exist?/2, namespace, relay.id),
          else: {:error, reason}
    end
  end

  # Who or what started the run, and when it first started, written once into
  # the declared spec, so a restart, a failover or a move carries them on.
  defp stamped(relay, opts) do
    stamp =
      relay.opts
      |> Keyword.put_new(:origin, Keyword.get(opts, :origin, %{kind: "reconcile"}))
      |> Keyword.put_new(:first_started_at, System.os_time(:millisecond))

    %{relay | opts: stamp}
  end

  # The `:start` steps take what the start holds, the declared start last; the
  # quota take is then kept or given back by what the start answered. A start
  # that is announced is recorded in the relay history unless `record?:` says.
  defp start_walk(namespace, relay, opts) do
    announce? = Keyword.get(opts, :announce, true)

    target = %{
      namespace: namespace,
      relay: relay,
      announce?: announce?,
      record?: Keyword.get(opts, :record?, announce?)
    }

    {_, report} = Shutdown.run(:start, target, Durations.get(:hub_start_child_ms))

    walked =
      case report do
        %{errors: [{_step, reason} | _]} -> {:error, reason}
        %{results: %{declared_start: answer}} -> {:ok, answer}
        %{skipped: skipped} -> {:error, {:start_failed, relay.id, {:not_run, skipped}}}
      end

    Admission.settle_start(namespace, match?({:ok, {:ok, _}}, walked))
    walked
  end

  @doc """
  Stops `relay_id` in the way its run expects.

  A `:steps` relay is stopped **cooperatively**, awake or asleep: a
  `StepsStop` trips the SDK's cancellation token and the relay winds
  down to its own `RelayCompleted` (status "cancelled"), which drives the
  single teardown via `ControlChannel.handle_relay_completed/2`. A sleeping
  one is first woken onto a runner, and the stop follows the bind. A flow relay,
  and a steps one no runner can take or whose runner cannot be reached, take the
  hard `stop_relay/2` teardown. An unreadable run (not found / wedged)
  also falls back to the hard stop so `flz relay stop` always attempts
  removal.

  Returns `{:ok, :cooperative}` when a cooperative stop was signalled, `:ok` after
  a hard teardown, or `{:error, reason}`. `by:` names who asked
  (`%{principal, source}`), recorded on the instance before the stop acts.
  """
  @spec request_stop(String.t(), String.t(), keyword()) ::
          :ok | {:ok, :cooperative} | {:error, term()}
  def request_stop(namespace, relay_id, opts \\ []) do
    # Whole-call forward, unlike the pid-riding read verbs: the stop path's
    # side effects (ControlUnit lookup, plugin disable, state-dir delete,
    # quota release) are node-local and must run on the hosting node.
    by = Keyword.get(opts, :by)
    on_hub_member(namespace, relay_id, {Teardown, :request_stop, [namespace, relay_id, by]}, opts)
  end

  @doc """
  Hard teardown: the `:relay` walk of the shutdown plan — flush the runner's
  channels, stop the child through the hub, delete its on-disk state, release
  its quota — under the one relay-stop budget. Drives `RemRelay`
  (terminal) — the converged teardown for both an explicit stop and a
  `RelayCompleted`. `preserve_state: true` keeps the files for a rescale to
  move and `:sealed` also seals a steps relay's journal at its latest committed
  iteration; either closes the writers on the node the relay ran on.
  `cause:` (`:stop`, `:complete`, `:rescale`, `:rescale_leave` or `:failed_start`)
  decides which steps act, and `terminal:` is how a completed run said it ended. `overwatch:` is the Overwatch a completion was admitted against: a
  task can complete before the hub writes the row of the start that ran it, and
  the hub queues a stop behind a start it sent and has not yet registered. The
  fallback covers a completion handled on the node that started the relay.
  """
  @spec stop_relay(String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def stop_relay(namespace, relay_id, opts \\ []) do
    on_hub_member(namespace, relay_id, {Teardown, :stop_relay, [namespace, relay_id, opts]}, opts)
  end

  @doc """
  Stop every relay in `namespace` through the hub and reclaim their state
  directories — the `:namespace` walk's first step, callable on its own for a
  namespace whose runners stay.
  """
  def stop_all_relays(namespace), do: Teardown.stop_all(namespace, Shutdown.stop_timeout_ms())

  @doc """
  Reclaim a stopped relay's state directory on this node: its writers
  closed, its tree removed unless a completion record keeps it for the
  retention window.
  """
  @spec delete_state_dir(String.t()) :: :ok
  defdelegate delete_state_dir(relay_id), to: Teardown, as: :delete_relay_state_dir

  @doc "Close every writer of `relay_class` on this node, flushed; its files stay."
  @spec close_writers(String.t()) :: :ok
  defdelegate close_writers(relay_class), to: Flezha.State.Supervisor

  @doc "The completion record a finished steps instance of `relay_class` left."
  defdelegate completion(relay_class), to: Completion, as: :read

  @doc "Whether a finished steps instance of `relay_class` left a completion record."
  defdelegate completed?(relay_class), to: Completion, as: :exists?

  @doc """
  Keep a `ctx.effect` result of `relay_id` in its instance's effect log,
  synced, and shipped first at `durable: remote`: `:kept`, `{:first, payload}`
  when the identity already held another, or `{:error, reason}`. The input
  log is keyed by the relay's class, the last of its state keys.
  """
  @spec keep_effect(String.t(), non_neg_integer(), String.t(), binary(), binary()) ::
          :kept | {:first, binary()} | {:error, term()}
  def keep_effect(relay_id, offset, identity, cell, payload) do
    relay_class = relay_id |> ClassRegistry.state_keys() |> List.last()

    case InputLog.keep_effect(relay_class, offset, identity, cell, payload) do
      {:error, _reason} = refused -> refused
      kept -> with :ok <- ship_effects(relay_id, relay_class), do: kept
    end
  end

  defp ship_effects(relay_id, relay_class) do
    if DurabilityTier.sync_class?(relay_id),
      do: DurabilityTier.commit_sync({:effects_log, relay_class}),
      else: :ok
  end

  @doc """
  Run the target's single pre-delivery ingest path through
  `Flezha.Plugin.Host.inject/5` in `:verdict` mode and answer what happened, so
  a forwarding sink can tell a terminal outcome from one worth retrying. An
  unresolvable target answers the lookup's own reason, and one that exits
  `{:error, {:unreachable, reason}}`: both retryable, since the relay may not
  be started yet.
  """
  @spec send_io_confirming(String.t(), String.t(), binary(), map(), timeout()) :: Verdict.t()
  def send_io_confirming(
        relay_id,
        namespace,
        message,
        metadata,
        timeout \\ Durations.get(:overwatch_call_timeout_ms)
      ) do
    with {:ok, relay_pid} <- get_relay_pid(namespace, relay_id),
         do: Host.inject(relay_pid, message, metadata, :verdict, timeout)
  end

  @doc "The input metadata naming the entry channel `channel`; `nil` or `\"\"` names none."
  @spec channel_metadata(String.t() | nil) :: map()
  def channel_metadata(channel) when channel in [nil, ""], do: %{}
  def channel_metadata(channel), do: %{"channel" => channel}

  @doc """
  The relay an input to `relay_id` in `namespace` reaches: the relay
  itself, or, when `relay_id` names a partition group, the member whose
  key-group range owns `key`. A group named with no key, or with a key no
  member owns, is refused naming its members.
  """
  @spec resolve_member(String.t(), String.t(), String.t() | nil) ::
          {:ok, String.t()}
          | {:error, {:unkeyed, String.t(), [String.t()]}}
          | {:error, {:relay_not_found, String.t(), String.t()}}
  def resolve_member(namespace, relay_id, key) do
    with false <- exist?(namespace, relay_id),
         [_ | _] = members <- PartitionGroup.members(namespace, relay_id) do
      case key && Enum.find(members, &Partition.owns?(&1.partition.range, key)) do
        nil -> {:error, {:unkeyed, relay_id, Enum.map(members, & &1.id)}}
        member -> {:ok, member.id}
      end
    else
      true -> {:ok, relay_id}
      [] -> {:error, {:relay_not_found, namespace, relay_id}}
    end
  end

  @doc """
  Call `relay_id` (a partition group's member by `key`) with one input and
  wait for its answer: the first output it caused (on the channel
  `metadata` names under `flz.call_answer_channel`, when it names one), or a
  coded error saying why none came by `deadline_ms`. `source` names the
  caller's surface to the relay's calls listeners.
  """
  @spec call_relay(
          String.t(),
          String.t(),
          binary(),
          map(),
          pos_integer(),
          String.t() | nil,
          String.t()
        ) :: {:ok, binary(), map()} | {:error, term()}
  def call_relay(namespace, relay_id, message, metadata, deadline_ms, key, source) do
    with {:ok, member} <- resolve_member(namespace, relay_id, key),
         {:ok, pid} <- get_relay_pid(namespace, member),
         do: Inbound.call(pid, message, metadata, deadline_ms, member, source)
  end

  @doc """
  `send_io_confirming/5` for a source holding many inputs at once, with up to
  `window` requests in flight (`Flezha.Plugin.Host.ingest_window/4`): the
  relay runs its single ingest path once per input, in input order, and only
  this caller's round trips overlap, which is what lets one source drive a
  pipeline at line rate instead of at one round trip per record. Answers a
  verdict per input, in input order, `{:error, {:unreachable, :timeout}}` for
  each one unanswered `timeout` after the call began; an unresolvable target
  answers `{:error, :not_found}` for all of them. `window` defaults to
  `plugin_ingest_window`.
  """
  @spec send_io_confirming_many(
          String.t(),
          String.t(),
          [{binary(), map()}],
          timeout(),
          pos_integer()
        ) :: [Verdict.t()]
  def send_io_confirming_many(
        relay_id,
        namespace,
        payloads,
        timeout \\ Durations.get(:overwatch_call_timeout_ms),
        window \\ Config.get(:plugin_ingest_window)
      )

  def send_io_confirming_many(relay_id, namespace, payloads, timeout, window) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, relay_pid} -> Host.ingest_window(relay_pid, payloads, timeout, window)
      _err -> Enum.map(payloads, fn _ -> {:error, :not_found} end)
    end
  end

  @doc """
  Start a steps instance of `factory_id` under `instance_id`, or under a
  generated id when it is `nil`, answering `:duplicate` when a given id is
  already claimed.

  Shared by every in-cluster spawner — a schedule's slot firing and a
  tasklet's `spawn_or_deliver/6`. The duplicate verdict itself is minted by
  `start_relay/3`, the one start-authority seam.
  """
  @spec start_steps(String.t(), String.t() | nil, String.t(), map(), keyword(), keyword()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def start_steps(namespace, instance_id, factory_id, init_values, opts, start_opts) do
    builder = %RelayBuilder{id: instance_id, factory_id: factory_id, run: :steps, opts: opts}
    start_relay(namespace, %{builder | init_values: init_values}, start_opts)
  end

  @doc """
  A start verdict as a published door answers it: the id the relay runs
  under, and whether this start began it or found a live instance holding
  `requested_id`. The manager keeps answering `:duplicate`, which the spawn
  race and partition convergence match on.
  """
  @spec start_answer({:ok, map()} | :duplicate | {:error, term()}, String.t() | nil) ::
          {:ok, %{id: String.t(), outcome: :started | :already_running}} | {:error, term()}
  def start_answer({:ok, %{relay_id: id}}, _requested_id),
    do: {:ok, %{id: id, outcome: :started}}

  def start_answer(:duplicate, requested_id),
    do: {:ok, %{id: requested_id, outcome: :already_running}}

  def start_answer({:error, _reason} = refusal, _requested_id), do: refusal

  @doc """
  The audit target of a `relay_start` act: the id its answer names, else
  the id its last argument asked for (a spec, its named fields, a bare id, or
  a wire verb's options), which for a start refused before an id was
  generated is none.
  """
  @spec start_target([term()], term()) :: %{relay: String.t() | nil}
  def start_target(_args, {:ok, %{id: id}}), do: %{relay: id}
  def start_target(args, _answer), do: %{relay: requested_id(List.last(args))}

  defp requested_id(%{id: id}), do: id
  defp requested_id(fields) when is_list(fields), do: Keyword.get(fields, :id)
  defp requested_id(id) when is_binary(id) or is_nil(id), do: id

  @doc """
  Spawn-or-deliver `instance_id` in `namespace`, the controller half of a
  tasklet's `ctx.spawn_steps`, for the identity `caller_context` names,
  answering the outcome and the id the request resolved to.

  A free id spawns a steps instance of `factory_id` carrying `payload` as its
  first input (`:seed_input`, ingested inside the instance's bind continue, so
  nothing can be ingested ahead of it). A taken id either receives `payload` as
  one more input (`if_running: :deliver`) or refuses (`if_running: :error`).
  Concurrent requests for one id race-resolve at the registry: whoever loses the
  uniqueness check converts to the same delivery a later request would get, so
  exactly one instance exists and every payload lands exactly once.

  An empty `instance_id` asks for a generated one: `if_running` has no meaning
  there, and every request spawns its own instance. A refusal answers the id the
  request carried.

  `identity` names where a tasklet's request came from (`""` for none). A
  generated id is derived from it, and the payload is journaled with it as its
  `flz.msg_id`, so a re-run of the invocation that asks again resolves to the
  relay the first request did, running or completed, and delivers nothing
  twice.

  Each act is judged on this node as the act it is: a start against the
  `relay_start` row, which records it, and a delivery against `spawn_deliver`,
  the `relay.send` right with no record, because a delivery happens once per
  event. A refusal of either is audited. A start no local runner can take is
  forwarded by `start_relay/3` inside that judgment, so no peer acts on a
  request nobody judged. `relay.already_running`, `relay.factory_unknown`
  and `relay.factory_not_steps` (a factory its runners announced as a
  flow relay) perform no act and are answered without one.

  `opts` (`t:Flezha.Service.RelayManager.Spawn.opts/0`) is what a relay's
  call adds: the kinds it may start, `if_absent: :not_found`, the metadata the
  seed and the delivery carry, and the call the callee's `:call` entry point
  takes its token from.
  """
  defdelegate spawn_or_deliver(
                namespace,
                instance_id,
                factory_id,
                payload,
                if_running,
                caller_context,
                identity \\ "",
                opts \\ []
              ),
              to: Spawn

  @doc """
  How `factory_id` is established to run, asked of this node when a runner
  here advertises it and of a node the directory names otherwise, so the
  answer is the cluster's.
  """
  def factory_run(namespace, factory_id),
    do: on_factory_node(namespace, factory_id, :factory_run, {:error, :no_such_factory})

  @doc """
  `factory_id`'s factory-table row or `nil`, its API as `host`'s runner (a relay's,
  `HubManager.relay_spec/2`) announced it, else asked as `factory_run/2` is.
  """
  def factory_row(namespace, factory_id, host),
    do: on_factory_node(namespace, factory_id, :factory_row, nil, host)

  @doc "`factory_id`'s announced graph or `nil`, asked as `factory_row/3` is."
  def factory_graph(namespace, factory_id, host),
    do: on_factory_node(namespace, factory_id, :factory_graph, nil, host)

  # A relay runs on its runner's node, which holds that runner's advertisement.
  defp on_factory_node(namespace, factory_id, fun, none, host \\ %{runner: nil}) do
    args = [namespace, factory_id]

    cond do
      read =
          host.runner && host.node &&
            Reads.on(host.node, FleetManager, fun, args ++ [host.runner],
              on_error: fn _ -> nil end
            ) ->
        read

      FleetManager.runners_for_factory(namespace, factory_id) != [] ->
        apply(FleetManager, fun, args)

      target = List.first(capable_nodes(namespace, factory_id)) ->
        Reads.on(target, FleetManager, fun, args)

      true ->
        none
    end
  end

  def create_snapshot(namespace, relay_id) do
    with_relay_pid(namespace, relay_id, fn pid ->
      SafeCall.bounded(pid, :create_snapshot, Durations.get(:overwatch_call_timeout_ms),
        on_error: &{:error, {:overwatch_unavailable, &1}}
      )
    end)
  end

  def exist?(namespace, relay_id) do
    case get_relay_pid(namespace, relay_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  @doc """
  The running namespace holding `relay_id`, or `:error` when no running
  namespace does. `:except` skips one namespace, which is how a caller asks
  "does this id resolve somewhere OTHER than here" (the cross-namespace
  refusals) rather than "where does it resolve" (the CLI's read verbs).
  """
  @spec namespace_of(binary(), keyword()) :: {:ok, binary()} | :error
  def namespace_of(relay_id, opts \\ []) when is_binary(relay_id) do
    except = opts |> Keyword.get(:except) |> normalize_namespace()

    candidate_namespaces()
    |> Enum.reject(&(&1 == except))
    |> first_namespace(&exist?(&1, relay_id))
  end

  defp normalize_namespace(nil), do: nil
  defp normalize_namespace(namespace), do: to_string(namespace)

  @doc """
  The namespace a relay class belongs to, for a caller that must scope an
  authorization decision to it.

  The running registries answer first; a class that is not running is resolved
  from its state root, so a verb against a stopped relay still authorizes
  in the right namespace rather than in `_system`.
  """
  @spec namespace_of_class(String.t()) :: String.t()
  def namespace_of_class(relay_class) when is_binary(relay_class) do
    case namespace_of(relay_class) do
      {:ok, namespace} -> namespace
      :error -> Paths.namespace_of_class(relay_class)
    end
  end

  @doc """
  The namespace whose registry holds members of the partition group
  `group_id`, or `:error`.

  A group id names no relay, so `namespace_of/1` cannot find it; this
  answers the same question for the one that names N.
  """
  @spec namespace_of_group(String.t()) :: {:ok, String.t()} | :error
  def namespace_of_group(group_id) when is_binary(group_id) do
    first_namespace(candidate_namespaces(), &(PartitionGroup.members(&1, group_id) != []))
  end

  # Every namespace this cluster might hold the answer in: the ones running
  # here plus the ones only a peer node hosts. `exist?/2` and the group read
  # both probe through the routed lookup, so either answer is cluster-wide.
  defp candidate_namespaces do
    HubManager.get_running_namespaces()
    |> Enum.map(&to_string/1)
    |> Enum.concat(Directory.namespaces())
    |> Enum.uniq()
  end

  defp first_namespace(namespaces, holds?) do
    case Enum.find(namespaces, holds?) do
      nil -> :error
      namespace -> {:ok, namespace}
    end
  end

  @doc """
  Whole-Relay read: every Tasklet's Cell state as one JSON blob. The
  seq-less shape the migration/handover and introspection callers predate the
  selector with.
  """
  def get_cell_state(namespace, relay_id) do
    case get_cell_state(namespace, relay_id, %{}) do
      {:ok, json, _change_seq} -> {:ok, json}
      err -> err
    end
  end

  @doc """
  Live read narrowed by `selector` (`%{tasklet: name}` or
  `%{tasklet: name, cell_key: key}`; `%{}` covers every Tasklet). Answers
  `{:ok, json, change_seq}` — the seq the state was serialized at, which may be
  ahead of what is durable — or `{:error, reason}`, including the
  `{:not_resident, reason, relay_id}` refusals for a Cell that is not held in memory.
  """
  @spec get_cell_state(binary(), binary(), map()) ::
          {:ok, binary(), non_neg_integer() | nil} | {:error, term()}
  def get_cell_state(namespace, relay_id, selector) when is_map(selector) do
    ask_overwatch(namespace, relay_id, {:get_cell_state, selector})
  end

  @doc """
  The encoded `TaskletActivityResponse` of the runner hosting `relay_id`, with
  that runner's id; `read` may carry `cells_of` and `limit`. A runner that does
  not answer, whose control unit is gone, or that answers without knowing the
  read, is `{:activity_no_answer, relay_id, runner_id}`; its own refusal passes
  through.
  """
  @spec get_tasklet_activity(binary(), binary(), map()) ::
          {:ok, binary(), binary()} | {:error, term()}
  def get_tasklet_activity(namespace, relay_id, read) do
    case ask_overwatch(namespace, relay_id, {:get_tasklet_activity, read}) do
      {:error, _} = refused -> refused
      {{:ok, raw}, runner} when raw != "" -> {:ok, raw, runner}
      {{:error, %Flezha.Error{} = refusal}, _runner} -> {:error, refusal}
      {_unanswered, runner} -> {:error, {:activity_no_answer, relay_id, runner}}
    end
  end

  # The relay Overwatch's own reply, or `{:error, reason}` when it is not
  # found or does not answer in time.
  @doc false
  def ask_overwatch(namespace, relay_id, request) do
    with_relay_pid(namespace, relay_id, fn pid ->
      case call_overwatch(pid, request) do
        {:ok, reply} -> reply
        {:error, _} = unreachable -> unreachable
      end
    end)
  end

  defp overwatch_call_timeout_ms,
    do: Durations.get(:overwatch_call_timeout_ms)
end

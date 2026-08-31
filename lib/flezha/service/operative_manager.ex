defmodule Flezha.Service.OperativeManager do
  @moduledoc """
  Manages the operative startings and stoppings.
  """

  require Logger

  alias Flezha.AuthZ.Quotas
  alias Flezha.Cluster.Directory
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.Convergence.SafeCall
  alias Flezha.Operative.Builder, as: OperativeBuilder
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Validator
  alias Flezha.Shutdown.Coordinator, as: ShutdownCoordinator
  alias Flezha.State.Completion
  alias Flezha.State.Paths

  @id_in_use Validator.id_in_use()

  def operative_registry(namespace) do
    case HubManager.get_registry(namespace) do
      {:ok, registry} ->
        registry

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_operative_ids(namespace) do
    operatives =
      namespace
      |> HubManager.get_all_operatives()
      |> Enum.map(&elem(&1, 0))

    {:ok, operatives}
  end

  @doc """
  Resolve `operative_id`'s pid from ANY controller node. A node that is not a
  member of the namespace's hub asks one hub-member node (resolved through the
  cluster directory, forwarded at most once); every pid-interaction verb then
  rides BEAM distribution on the returned pid, so the interaction still
  executes inside the operative's own node.
  """
  def get_operative_pid(namespace, operative_id, opts \\ []) do
    on_hub_member(namespace, operative_id, opts, :get_operative_pid, fn ->
      case HubManager.get_pid(namespace, operative_id) do
        {:ok, pid} -> {:ok, pid}
        {:error, "no pid found"} -> {:error, "operative not found"}
        err -> err
      end
    end)
  end

  # Run `local` when this node is a hub member (or the call was already
  # forwarded — the loop guard), else forward `fun` once to one deterministic
  # hub-member node. Local with no hub anywhere preserves today's refusal.
  defp on_hub_member(namespace, operative_id, opts, fun, local) do
    if Keyword.get(opts, :routed, false) or HubManager.hub_alive?(namespace) do
      local.()
    else
      case List.delete(Directory.hub_nodes(namespace), node()) do
        [] ->
          local.()

        nodes ->
          target = pick_runner(nodes, operative_id)
          route_call(target, fun, [namespace, operative_id, [routed: true]], &{:error, &1})
      end
    end
  end

  # Resolve the pid, then run `fun` on it — every error passes through.
  defp with_operative_pid(namespace, operative_id, fun) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} -> fun.(operative_pid)
      err -> err
    end
  end

  # One workflow forward; an unreachable or crashed peer maps through `on_error`
  # instead of raising into the caller. Must exceed the remote start's own
  # internal budget (ProcessHub's 15s wait plus the :no_response retry ladder).
  @route_call_timeout_ms 30_000
  defp route_call(target, fun, args, on_error) do
    :erpc.call(target, __MODULE__, fun, args, @route_call_timeout_ms)
  catch
    kind, reason -> on_error.({:routed_call_failed, target, kind, reason})
  end

  def get_overwatch_state(namespace, operative_id) do
    # `SafeCall.bounded` returns `{:ok, state} | {:error, reason}` — the same
    # success shape as before, but a wedged operative now yields a logged
    # `{:error, :timeout}` (with the target's mailbox/current-function) instead
    # of an unhandled exit.
    with_operative_pid(namespace, operative_id, fn pid ->
      SafeCall.bounded(pid, :get_overwatch_state, overwatch_call_timeout_ms())
    end)
  end

  @doc """
  Starts a new operative to the runner.

  Location-transparent: a node that cannot satisfy the start itself (no
  namespace hub / no runner advertising the factory) resolves the capable
  nodes through the cluster directory and executes the same call on one of
  them, forwarded at most once (`routed: true`). The executing node
  re-validates everything, so a stale directory answer costs one failed
  forward, never a wrong start.

  This is the single start-authority seam: every in-cluster starter — the
  CLI/manager, a schedule's slot, a procedure's `spawn_workflow`, and the
  orphan reconcile through the same durable registry — resolves the id claim
  here. A start that loses the claim answers `:duplicate` and leaves the
  winner strictly alone; it is never reported as a failed start, because the
  failure path stops the child it names.
  """
  @spec start_operative(String.t(), OperativeBuilder.t(), keyword()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def start_operative(namespace, operative, opts \\ []) do
    if Keyword.get(opts, :routed, false) or locally_startable?(namespace, operative.factory_id) do
      do_start_operative(namespace, operative)
    else
      route_start(namespace, operative)
    end
  end

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

  defp route_start(namespace, operative) do
    case capable_nodes(namespace, operative.factory_id) do
      [] ->
        {:error,
         %{
           operative_id: operative.id,
           node: node(),
           reason: cluster_wide_refusal(namespace, operative.factory_id)
         }}

      nodes ->
        target = pick_runner(nodes, operative.id)

        route_call(
          target,
          :start_operative,
          [namespace, operative, [routed: true]],
          &{:error, %{operative_id: operative.id, node: target, reason: &1}}
        )
    end
  end

  defp cluster_wide_refusal(namespace, factory_id) when is_binary(factory_id) do
    factory_refusal(
      namespace,
      factory_id,
      "no runners for factory #{factory_id} in namespace #{namespace} on any cluster node"
    )
  end

  defp cluster_wide_refusal(namespace, _factory_id),
    do: "no runners in namespace #{namespace} on any cluster node"

  # A factory that exists in another language is not a factory nobody has:
  # reporting it as missing sends the operator looking for a runner that is
  # already connected. Both refusal sites read this, so the two cannot drift.
  defp factory_refusal(namespace, factory_id, absent) do
    case FleetManager.placement_refusal(namespace, factory_id) do
      {:language, established} ->
        "factory #{factory_id} in namespace #{namespace} is established at sdk_lang " <>
          "#{inspect(established)}; every runner advertising it is of another SDK and " <>
          "cannot read its state — start a runner of that language, or clear the " <>
          "factory's durable state so it re-establishes"

      :no_language_declared ->
        "factory #{factory_id} in namespace #{namespace} is advertised without an sdk_lang " <>
          "declaration; a runner that does not declare its language is never a placement " <>
          "candidate, because durable state is per-SDK — declare sdk_lang in the runner's " <>
          "factory advertisement"

      :ambiguous_language ->
        "factory #{factory_id} in namespace #{namespace} is advertised at more than one " <>
          "sdk_lang with none established; the controller cannot tell which language owns " <>
          "its state — run one language's runners, or clear the factory's durable state"

      _ ->
        absent
    end
  end

  defp do_start_operative(namespace, operative) do
    with :ok <- check_not_shutting_down(),
         :ok <- OperativeBuilder.validate_operative(namespace, operative),
         :ok <- refuse_group_base_claim(namespace, operative),
         :ok <- validate_factory(namespace, operative.factory_id),
         :ok <- check_operative_quota(namespace),
         :ok <- check_entity_quota(namespace, operative),
         :ok <- check_disk_quota(namespace),
         :ok <- Paths.claim_namespace(operative.id, namespace) do
      case init_operative(namespace, operative) do
        {:ok, _} = ok ->
          _ = Quotas.record_allocation(namespace, :max_operatives, 1)
          seg_count = entity_count(operative)

          if seg_count > 0 do
            _ =
              Quotas.record_allocation(
                namespace,
                :max_entities_per_operative,
                seg_count
              )
          end

          ok

        other ->
          other
      end
    else
      {:error, :shutting_down} ->
        {:error, :shutting_down}

      {:error, {:quota_exceeded, _, _} = qerr} ->
        {:error, qerr}

      {:error, @id_in_use} ->
        claim_lost(namespace, operative.id)

      {:error, reason} when is_binary(reason) ->
        {:error, %{operative_id: operative.id, node: node(), reason: reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A group's base id is never claimed — only member ids are — so without this
  # a plain start of the base mints a singleton beside the group. Member starts
  # carry partition identity in their opts and skip the registry walk.
  defp refuse_group_base_claim(namespace, operative) do
    case Flezha.Partition.identity(operative.opts) do
      nil -> refuse_live_group(namespace, operative.id)
      _member_identity -> :ok
    end
  end

  defp refuse_live_group(namespace, operative_id) do
    case PartitionGroup.members(namespace, operative_id) do
      [] ->
        :ok

      members ->
        {:error,
         "#{operative_id} names a partition group of #{length(members)} live member(s); " <>
           "re-run with --partitions #{hd(members).partition.count} to converge it, or stop " <>
           "the group first"}
    end
  end

  # Losing the id claim is not a failed start: another path already runs this
  # operative. Conceding is the WHOLE remedy — the winner is never stopped,
  # restarted, or rebound, which is exactly what the generic failure path
  # (`stop_child_quietly/2`) would have done to it.
  #
  # A claim refused with no live winner behind it is a different thing — a
  # stale registry row — and stays an error so it is not silently accepted.
  defp claim_lost(namespace, operative_id) do
    if exist?(namespace, operative_id) do
      Logger.debug(
        "Operative #{operative_id} start conceded: id already claimed by a live instance"
      )

      :duplicate
    else
      {:error, %{operative_id: operative_id, node: node(), reason: @id_in_use}}
    end
  end

  defp check_operative_quota(namespace) do
    check_quota(namespace, :max_operatives, 1)
  end

  # A namespace over its disk quota refuses NEW allocation — running
  # operatives and their in-flight durable writes are never interrupted
  # (the Watchdog raises the alarm for those).
  defp check_disk_quota(namespace) do
    if Flezha.AuthZ.enabled?() do
      _ = Quotas.refresh_disk_bytes(namespace)
      check_quota(namespace, :max_state_bytes, 0)
    else
      :ok
    end
  end

  defp check_entity_quota(namespace, operative) do
    case entity_count(operative) do
      0 -> :ok
      requested -> check_quota(namespace, :max_entities_per_operative, requested)
    end
  end

  # An unregistered namespace streams: quotas gate registered tenants, and
  # refusing every legacy/implicit namespace here would brick starts the
  # moment enforcement flips on.
  defp check_quota(namespace, kind, requested) do
    case Quotas.check_allocation(namespace, kind, requested) do
      :ok -> :ok
      {:error, :namespace_unknown} -> :ok
      {:error, _} = err -> err
    end
  end

  defp check_not_shutting_down() do
    if ShutdownCoordinator.shutting_down?(), do: {:error, :shutting_down}, else: :ok
  end

  defp validate_factory(namespace, factory_id) when is_binary(factory_id) do
    case FleetManager.runners_for_factory(namespace, factory_id) do
      [_ | _] ->
        :ok

      [] ->
        {:error,
         factory_refusal(namespace, factory_id, "Factory with id #{factory_id} does not exist")}
    end
  end

  defp validate_factory(_namespace, _factory_id), do: :ok

  @doc """
  Stops `operative_id` the way its kind expects.

  A bound `:workflow` operative is stopped **cooperatively**: a `WorkflowStop`
  trips the SDK's cancellation token and the operative winds down to its own
  `WorkflowCompleted` (status "cancelled"), which drives the single teardown via
  `ControlChannel.handle_workflow_completed/2`. Everything else (`:stream`, or
  a workflow operative not bound to a runner) takes the hard `stop_operative/2`
  teardown. An unreadable kind (not found / wedged) also falls back to the hard
  stop so `flz operative stop` always attempts removal.

  Returns `{:ok, :cooperative}` when a cooperative stop was signalled, `:ok` after
  a hard teardown, or `{:error, reason}`.
  """
  @spec request_stop(String.t(), String.t(), keyword()) ::
          :ok | {:ok, :cooperative} | {:error, term()}
  def request_stop(namespace, operative_id, opts \\ []) do
    # Whole-call forward, unlike the pid-riding read verbs: the stop path's
    # side effects (ControlUnit lookup, plugin disable, state-dir delete,
    # quota release) are node-local and must run on the hosting node.
    on_hub_member(namespace, operative_id, opts, :request_stop, fn ->
      do_request_stop(namespace, operative_id)
    end)
  end

  defp do_request_stop(namespace, operative_id) do
    case get_overwatch_state(namespace, operative_id) do
      {:ok, %{builder: %{kind: :workflow}, runner_id: runner_id}}
      when is_binary(runner_id) ->
        case ControlUnit.workflow_stop(runner_id, operative_id) do
          :ok -> {:ok, :cooperative}
          # No control unit to cooperate with — fall back to the hard teardown.
          {:error, _} -> stop_operative(namespace, operative_id)
        end

      _ ->
        stop_operative(namespace, operative_id)
    end
  end

  @doc """
  Hard teardown: stop the operative, disable its plugins, delete its on-disk
  state, and release its quota. Drives `RemOperative` (terminal) — the converged
  teardown for both an explicit stop and a `WorkflowCompleted`.
  """
  @spec stop_operative(String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def stop_operative(namespace, operative_id, opts \\ []) do
    on_hub_member(namespace, operative_id, opts, :stop_operative, fn ->
      do_stop_operative(namespace, operative_id, Keyword.get(opts, :preserve_state, false))
    end)
  end

  defp do_stop_operative(namespace, operative_id, preserve_state?) do
    with_operative_pid(namespace, operative_id, fn _operative_pid ->
      # Read the entity count before the stop tears the operative down — the
      # operative is gone afterwards and the count is needed to release the
      # `:max_entities_per_operative` quota symmetrically with start.
      seg_count = operative_entity_count(namespace, operative_id)

      case stop_via_hub(namespace, operative_id) do
        :ok ->
          _ = PluginManager.disable_operative_plugins(namespace, operative_id)
          # A stop that is a move — a rescale taking the group down to
          # redistribute its Entities — keeps the files: they are the state
          # about to be handed to another member, not garbage.
          unless preserve_state?, do: delete_operative_state_dir(operative_id)
          _ = Quotas.record_deallocation(namespace, :max_operatives, 1)

          if seg_count > 0 do
            _ =
              Quotas.record_deallocation(
                namespace,
                :max_entities_per_operative,
                seg_count
              )
          end

          :ok

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  defp stop_via_hub(namespace, operative_id) do
    case hub_stop_children(namespace, [operative_id], 15_000) do
      :ok ->
        :ok

      {:error, errors} when is_list(errors) ->
        case List.keyfind(errors, operative_id, 0) do
          {_cid, reason} -> {:error, reason}
          nil -> {:error, :stop_failed}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The ONE seam flezha stops children through: the hub's precommit removes
  # each declared-set entry BEFORE anything terminates (remove-before-terminate),
  # so a stop is remembered as list absence and the reconcile can never
  # resurrect it. The Overwatch's teardown — telling the runner to drop the
  # operative, plugin `:on_terminate`, table teardown — runs in its
  # `terminate/2` under the supervisor's shutdown. Best-effort callers discard
  # the verdict; a dead or mid-restart hub maps to an error, never a raise.
  defp hub_stop_children(namespace, operative_ids, timeout) do
    result =
      try do
        Identificator.hub(namespace)
        |> ProcessHub.stop_children(operative_ids, awaitable: true, timeout: timeout)
        |> ProcessHub.Future.await()
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      %ProcessHub.StopResult{status: :ok} -> :ok
      %ProcessHub.StopResult{errors: errors} -> {:error, errors || []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp operative_entity_count(namespace, operative_id) do
    case get_overwatch_state(namespace, operative_id) do
      {:ok, %{builder: builder}} -> entity_count(builder)
      _ -> 0
    end
  end

  defp entity_count(operative) do
    case Map.get(operative, :entities) do
      ps when is_list(ps) -> length(ps)
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  def stop_all_operatives(namespace) do
    operatives = HubManager.get_all_operatives(namespace)
    operative_ids = Enum.map(operatives, fn {id, _pid} -> id end)

    Enum.each(operative_ids, fn operative_id ->
      _ = PluginManager.disable_operative_plugins(namespace, operative_id)
    end)

    if operative_ids != [] do
      _ = hub_stop_children(namespace, operative_ids, 15_000)

      Enum.each(operative_ids, &delete_operative_state_dir/1)
    end

    :ok
  end

  # On explicit stop, delete the on-disk state directories owned by the
  # operative. The operative_id is used as the operative_class identifier — the SDK
  # convention is that each operative's entity frames carry the operative_id
  # as operative_class. State files written under `app_data/state/<operative_id>/`
  # are removed; any procedure directory containing a `.tmp` or `.rewriting`
  # marker is skipped (with WARN) because a write is in flight and
  # truncating it would corrupt the rename ceremony.
  #
  # `child_unregistered_hook` deliberately does NOT call this — HotSwap
  # source-side and redistribution-away preserve files so the
  # destination (or next host) can pull them via WAL-follows-operative.
  defp delete_operative_state_dir(operative_id) do
    # Retained or deleted, the writer processes go with the operative.
    Flezha.State.Supervisor.close_writers(operative_id)

    if Completion.exists?(operative_id) do
      # Finished workflow operative: retain its state + completion record for the
      # retention window so the outcome stays inspectable; `Flezha.State.Retention`
      # reclaims it once the window elapses.
      :ok
    else
      do_delete_operative_state_dir(operative_id)
    end
  end

  defp do_delete_operative_state_dir(operative_id) do
    class_dir = Paths.class_dir(operative_id)

    case File.ls(class_dir) do
      {:ok, procedure_names} ->
        Enum.each(procedure_names, fn procedure ->
          procedure_dir = Path.join(class_dir, procedure)
          maybe_delete_procedure(procedure_dir)
        end)

        _ = File.rmdir(class_dir)
        :ok

      {:error, :enoent} ->
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp maybe_delete_procedure(procedure_dir) do
    case File.ls(procedure_dir) do
      {:ok, names} ->
        if Enum.any?(names, fn n ->
             String.ends_with?(n, ".tmp") or String.ends_with?(n, ".rewriting")
           end) do
          Logger.warning(
            "[operative_manager] skipping state dir #{procedure_dir}: write in flight " <>
              "(.tmp / .rewriting present)"
          )
        else
          _ = File.rm_rf(procedure_dir)
        end

      {:error, _} ->
        :ok
    end
  end

  def send_io(operative_id, namespace, message, metadata) do
    with_operative_pid(namespace, operative_id, fn pid ->
      GenServer.cast(pid, {:io_in, message, metadata})
    end)
  end

  @typedoc """
  What became of a payload handed to `send_io_confirming/5`. The three `:ok`
  shapes are terminal — the receiver has decided the payload needs no further
  delivery — and every error is retryable by the sender.
  """
  @type ingest_verdict ::
          :ok
          | {:ok, :absorbed}
          | {:ok, :suspended}
          | {:error, :not_ready | :not_found | :refused}

  @doc """
  Confirming counterpart to `send_io/4`: runs the target's single pre-delivery
  ingest path and answers what happened, so a forwarding sink can tell a
  terminal outcome from one worth retrying. An unresolvable target is
  `{:error, :not_found}` — also retryable, since the operative may not be
  started yet.
  """
  @spec send_io_confirming(String.t(), String.t(), binary(), map(), timeout()) :: ingest_verdict()
  def send_io_confirming(operative_id, namespace, message, metadata, timeout \\ 5_000) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} ->
        GenServer.call(operative_pid, {:ingest, message, metadata}, timeout)

      _err ->
        {:error, :not_found}
    end
  end

  @doc """
  `send_io_confirming/5` for a source holding many inputs at once, with up to
  `window` requests in flight. Every input is the same `{:ingest, ...}` call —
  the operative runs its single ingest path once per input, in input order —
  only this caller's round trips overlap, which is what lets one source drive
  a pipeline at line rate instead of at one round trip per record. Answers a
  verdict per input, in input order; an unresolvable target answers
  `{:error, :not_found}` for all of them.
  """
  @spec send_io_confirming_many(
          String.t(),
          String.t(),
          [{binary(), map()}],
          timeout(),
          pos_integer()
        ) :: [ingest_verdict()]
  def send_io_confirming_many(operative_id, namespace, payloads, timeout \\ 5_000, window \\ 64)

  def send_io_confirming_many(_operative_id, _namespace, [], _timeout, _window), do: []

  def send_io_confirming_many(operative_id, namespace, payloads, timeout, window) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} ->
        pipeline(
          operative_pid,
          Enum.with_index(payloads),
          timeout,
          window,
          :gen_server.reqids_new(),
          %{}
        )

      _err ->
        Enum.map(payloads, fn _ -> {:error, :not_found} end)
    end
  end

  # Fill the window, then await one reply per free slot. Replies come back in
  # request order (one server, one caller); `verdicts` is keyed by input index
  # only so the order the collection hands them out in cannot matter.
  defp pipeline(pid, pending, timeout, window, reqids, verdicts) do
    in_flight = :gen_server.reqids_size(reqids)

    cond do
      pending != [] and in_flight < window ->
        [{{message, metadata}, index} | rest] = pending
        reqids = :gen_server.send_request(pid, {:ingest, message, metadata}, index, reqids)
        pipeline(pid, rest, timeout, window, reqids, verdicts)

      in_flight == 0 ->
        verdicts |> Enum.sort() |> Enum.map(&elem(&1, 1))

      true ->
        case :gen_server.wait_response(reqids, timeout, true) do
          {{:reply, verdict}, index, reqids} ->
            pipeline(pid, pending, timeout, window, reqids, Map.put(verdicts, index, verdict))

          {{:error, {reason, _server}}, _index, _reqids} ->
            exit({reason, {__MODULE__, :send_io_confirming_many, [pid, timeout]}})

          :timeout ->
            exit({:timeout, {__MODULE__, :send_io_confirming_many, [pid, timeout]}})
        end
    end
  end

  @doc """
  Start a workflow instance of `factory_id` under `instance_id`, answering
  `:duplicate` when that id is already claimed.

  Shared by every in-cluster spawner — a schedule's slot firing and a
  procedure's `spawn_or_deliver/5`. The duplicate verdict itself is minted by
  `start_operative/3`, the one start-authority seam.
  """
  @spec start_workflow(String.t(), String.t(), String.t(), map(), Keyword.t()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def start_workflow(namespace, instance_id, factory_id, init_values \\ %{}, opts \\ []) do
    start_operative(namespace, %OperativeBuilder{
      id: instance_id,
      factory_id: factory_id,
      kind: :workflow,
      init_values: init_values,
      opts: opts
    })
  end

  @typedoc """
  How a procedure-requested spawn resolved. `:spawned` and `:delivered` are
  the two success shapes; the rest report why nothing was spawned.
  """
  @type spawn_outcome ::
          :spawned | :delivered | :already_running | :factory_unknown | {:refused, String.t()}

  @doc """
  Spawn-or-deliver `instance_id` in `namespace` — the controller half of an
  procedure's `ctx.spawn_workflow`.

  A free id spawns a workflow instance of `factory_id` carrying `payload` as its
  first input (`:seed_input`, ingested inside the instance's bind continue, so
  nothing can be ingested ahead of it). A taken id either receives `payload` as
  one more input (`if_running: :deliver`) or refuses (`if_running: :error`).
  Concurrent requests for one id race-resolve at the registry: whoever loses the
  uniqueness check converts to the same delivery a later request would get, so
  exactly one instance exists and every payload lands exactly once.
  """
  @spec spawn_or_deliver(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          :deliver | :error,
          keyword()
        ) ::
          spawn_outcome()
  def spawn_or_deliver(namespace, instance_id, factory_id, payload, if_running, opts \\ []) do
    cond do
      exist?(namespace, instance_id) ->
        resolve_running(namespace, instance_id, payload, if_running)

      FleetManager.runners_for_factory(namespace, factory_id) != [] ->
        spawn_fresh(namespace, instance_id, factory_id, payload, if_running)

      # The loop guard: a forwarded request finding no local runner answers the
      # honest refusal instead of forwarding again.
      Keyword.get(opts, :routed, false) ->
        :factory_unknown

      true ->
        route_spawn_or_deliver(namespace, instance_id, factory_id, payload, if_running)
    end
  end

  # No local runner registers the factory — resolve cluster-wide and run the
  # WHOLE call where the namespace hub is alive, so `exist?/2` and delivery
  # execute against live truth. `:factory_unknown` is now an honest
  # cluster-wide verdict.
  defp route_spawn_or_deliver(namespace, instance_id, factory_id, payload, if_running) do
    case capable_nodes(namespace, factory_id) do
      [] ->
        :factory_unknown

      nodes ->
        target = pick_runner(nodes, instance_id)

        route_call(
          target,
          :spawn_or_deliver,
          [namespace, instance_id, factory_id, payload, if_running, [routed: true]],
          &{:refused, inspect(&1)}
        )
    end
  end

  defp spawn_fresh(namespace, instance_id, factory_id, payload, if_running) do
    case start_workflow(namespace, instance_id, factory_id, %{}, seed_input: payload) do
      {:ok, _} ->
        :spawned

      # Lost the id race: the winner spawned it, so this request becomes the
      # delivery a request arriving a moment later would have made.
      :duplicate ->
        resolve_running(namespace, instance_id, payload, if_running)

      {:error, reason} ->
        {:refused, inspect(reason)}
    end
  end

  defp resolve_running(_namespace, _instance_id, _payload, :error), do: :already_running

  defp resolve_running(namespace, instance_id, payload, :deliver) do
    case send_io_confirming(instance_id, namespace, payload, %{}) do
      {:error, reason} -> {:refused, inspect(reason)}
      _delivered_or_absorbed -> :delivered
    end
  end

  def create_snapshot(namespace, operative_id) do
    with_operative_pid(namespace, operative_id, fn pid ->
      GenServer.call(pid, :create_snapshot)
    end)
  end

  @no_response_retry_attempts 8
  @no_response_retry_initial_ms 150

  defp init_operative(namespace, operative, attempt \\ 0) do
    child_spec = %{
      id: operative.id,
      start: {
        Flezha.Operative.Overwatch,
        :start_link,
        [{namespace, operative}]
      },
      restart: :transient
    }

    result =
      Identificator.hub(namespace)
      |> ProcessHub.start_child(child_spec,
        awaitable: true,
        # We will get the errors from the start result.
        disable_logging: true,
        timeout: 15_000,
        # A partition member's identity is on its row from the first
        # registration, so `PartitionGroup.members/2` sees it before it binds.
        metadata: partition_metadata(operative),
        # Declared intent: the start records the child in the hub's declared
        # set BEFORE anything runs, so a whole-cluster cold start restores it
        # and a deliberate stop (hub stop path) removes it for good. Off in
        # lockstep with auto-recovery — a hub without the reconcile refuses
        # durable starts.
        durable: HubManager.durable_starts?()
      )
      |> ProcessHub.Future.await()

    # `:no_response` surfaces when ProcessHub's `with_partition_check`
    # short-circuits `execute/2` without sending a response — i.e. the
    # local hub's `dist_sup` was momentarily missing or dead, so
    # `Service.State.is_partitioned?/1` returned `true`. The dist_sup
    # restarts on its own; the only thing the caller can do is wait it
    # out. Retry up to @no_response_retry_attempts times with
    # exponential backoff (150, 300, 600, 1200, 2400, ... ms, capped at
    # 5s) so a transient partition during controller/cluster startup, a
    # snapshot/restore taking down `dist_sup`, or parallel-suite test
    # churn doesn't surface as a hard failure. Stable steady-state
    # `:no_response` (8 attempts, ~14s of wait) still propagates.
    cond do
      no_response?(result) and attempt < @no_response_retry_attempts ->
        backoff = min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000)
        Process.sleep(backoff)
        init_operative(namespace, operative, attempt + 1)

      true ->
        finalize_init_operative(namespace, operative, result)
    end
  end

  defp partition_metadata(operative) do
    case Flezha.Partition.identity(operative.opts) do
      nil -> %{}
      partition -> %{partition: partition}
    end
  end

  defp no_response?(%ProcessHub.StartResult{errors: errors}) when is_list(errors) do
    Enum.any?(errors, fn
      {_cid, :no_response} -> true
      {_cid, {:no_response, _}} -> true
      {_cid, {:error, :no_response}} -> true
      _ -> false
    end)
  end

  defp no_response?(_result), do: false

  defp finalize_init_operative(namespace, operative, result) do
    case result do
      %ProcessHub.StartResult{} ->
        our_started =
          Enum.find(result.started || [], fn {cid, _} -> cid == operative.id end)

        case our_started do
          {_cid, [{operative_node, pid}]} ->
            # ProcessHub reports `started` as soon as `start_link/1`
            # returns; the operative's `handle_continue(:bind_runner, …)`
            # surfaces a plugin-chain failure via the `init_error` field
            # on Overwatch state — `:registry_runner_set` returns
            # `{:error, _}` instead of crashing the GenServer. The
            # `:exit` catch covers older failure modes (timeout, kill).
            try do
              case GenServer.call(pid, :registry_runner_set) do
                {:ok, runner_id} ->
                  {:ok,
                   %{
                     operative_id: operative.id,
                     runner_id: runner_id,
                     node: operative_node,
                     pid: pid
                   }}

                {:error, reason} ->
                  _ = stop_child_quietly(namespace, operative.id)
                  {:error, %{operative_id: operative.id, node: node(), reason: reason}}
              end
            catch
              # Started, then exited cleanly before we could read its binding —
              # a run-once workflow operative that already completed, or a concurrent stop. A
              # clean exit is a successful start, not a failure; no live binding
              # remains, so `runner_id` is nil.
              :exit, {reason, _} when reason in [:normal, :noproc] ->
                {:ok,
                 %{operative_id: operative.id, runner_id: nil, node: operative_node, pid: pid}}

              :exit, {reason, _} ->
                Logger.warning(
                  "Operative #{operative.id} appeared in `started` but died during " <>
                    "post-init continue (reason=#{inspect(reason)})"
                )

                _ = stop_child_quietly(namespace, operative.id)
                {:error, %{operative_id: operative.id, node: node(), reason: reason}}
            end

          _ ->
            reason =
              case Enum.find(result.errors || [], fn {cid, _} -> cid == operative.id end) do
                {_cid, {:error, reason}} -> reason
                {_cid, {reason, _child_spec}} -> reason
                {_cid, reason} -> reason
                _ -> :unknown
              end

            if claim_loss?(reason) do
              claim_lost(namespace, operative.id)
            else
              Logger.warning(
                "Operative #{operative.id} failed to start. " <>
                  "Started: #{inspect(result.started)}, Errors: #{inspect(result.errors)}, " <>
                  "Status: #{inspect(result.status)}"
              )

              _ = stop_child_quietly(namespace, operative.id)
              {:error, %{operative_id: operative.id, node: node(), reason: reason}}
            end
        end

      {:error, reason} ->
        if claim_loss?(reason),
          do: claim_lost(namespace, operative.id),
          else: {:error, %{operative_id: operative.id, node: node(), reason: reason}}
    end
  end

  # The registry refused the id: a live instance already holds it. Taking the
  # failure path on that verdict would call `stop_child_quietly/2` on the id —
  # killing the winner — which is precisely the start-authority violation the
  # claim exists to prevent.
  defp claim_loss?(:already_started), do: true
  defp claim_loss?({:already_started, _ids}), do: true
  defp claim_loss?(@id_in_use), do: true
  defp claim_loss?(_reason), do: false

  defp stop_child_quietly(namespace, operative_id) do
    _ = hub_stop_children(namespace, [operative_id], 5_000)
    :ok
  end

  def exist?(namespace, operative_id) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  @doc """
  The running namespace holding `operative_id`, or `:error` when no running
  namespace does. `:except` skips one namespace, which is how a caller asks
  "does this id resolve somewhere OTHER than here" (the cross-namespace
  refusals) rather than "where does it resolve" (the CLI's read verbs).
  """
  @spec namespace_of(binary(), keyword()) :: {:ok, binary()} | :error
  def namespace_of(operative_id, opts \\ []) when is_binary(operative_id) do
    except = opts |> Keyword.get(:except) |> normalize_namespace()

    candidate_namespaces()
    |> Enum.reject(&(&1 == except))
    |> first_namespace(&exist?(&1, operative_id))
  end

  defp normalize_namespace(nil), do: nil
  defp normalize_namespace(namespace), do: to_string(namespace)

  @doc """
  The namespace whose registry holds members of the partition group
  `group_id`, or `:error`.

  A group id names no operative, so `namespace_of/1` cannot find it; this
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
  Whole-Operative read: every Procedure's Entity state as one JSON blob. The
  seq-less shape the migration/handover and introspection callers predate the
  selector with.
  """
  def get_entity_state(namespace, operative_id) do
    case get_entity_state(namespace, operative_id, %{}) do
      {:ok, json, _change_seq} -> {:ok, json}
      err -> err
    end
  end

  @doc """
  Live read narrowed by `selector` (`%{procedure: name}` or
  `%{procedure: name, entity_key: key}`; `%{}` covers every Procedure). Answers
  `{:ok, json, change_seq}` — the seq the state was serialized at, which may be
  ahead of what is durable — or `{:error, reason}`, including the
  `{:not_resident, reason}` refusals for an Entity that is not held in memory.
  """
  @spec get_entity_state(binary(), binary(), map()) ::
          {:ok, binary(), non_neg_integer() | nil} | {:error, term()}
  def get_entity_state(namespace, operative_id, selector) when is_map(selector) do
    with_operative_pid(namespace, operative_id, fn pid ->
      # Unwrap one level: the operative's own reply is already
      # `{:ok, json, seq} | {:error, reason}`, so collapse `SafeCall`'s outer
      # `{:ok, _}`; a bounded timeout/noproc stays `{:error, reason}`.
      case SafeCall.bounded(pid, {:get_entity_state, selector}, overwatch_call_timeout_ms()) do
        {:ok, reply} -> reply
        {:error, _} = err -> err
      end
    end)
  end

  defp overwatch_call_timeout_ms,
    do: Application.get_env(:flezha, :overwatch_call_timeout_ms, 5_000)

  @runner_poll_interval_ms 50
  @runner_lookup_timeout_ms 5_000
  # Per-attempt budget for the polling path. Must stay well under any caller's
  # deadline, or one blocked lookup consumes it and the loop never iterates.
  @runner_poll_call_timeout_ms 250
  # Floor for the clamped attempt: below this a healthy FleetManager could not
  # answer, and we would blame it for our own exhausted budget.
  @runner_min_call_timeout_ms 25

  @doc """
  Pick an active compute engine for `operative_id` in `namespace`.

  Placement is deterministic — rendezvous (HRW) hashing of `operative_id` over
  the candidate runners — so every controller and every tick computes the
  same target with no coordination, load spreads evenly, and only ~1/N
  operatives move when the runner set changes. A newly connected runner
  never reshuffles existing operatives: placement is computed only at start /
  forced rebind.

  When `factory_id` is given the pool is restricted to runners advertising
  that factory. Without this filter a mixed-factory namespace (e.g.
  market-operative owning `crypto_trades` + market-app owning
  `market-init-state`) would pick the wrong node, routing every input frame
  to a process that doesn't exist on the foreign node (the Java SDK surfaces
  this as `DefaultOperativeDispatcher.enqueue: operative 'X' not found`). Returns
  `nil` when no candidate exists.
  """
  @spec assign_runner(String.t(), binary() | nil, binary()) :: binary() | nil
  def assign_runner(namespace, factory_id, operative_id) do
    case resolve_runner(namespace, factory_id, operative_id, @runner_lookup_timeout_ms) do
      {:ok, runner_id} -> runner_id
      {:error, :unavailable} -> nil
    end
  end

  @doc """
  True when `runner_id`'s control unit is registered — the cheap half of runner
  readiness, shared by everything that must not hand out a phantom runner id.
  """
  @spec runner_registered?(binary()) :: boolean()
  def runner_registered?(runner_id) when is_binary(runner_id),
    do: match?([{_pid, _}], Registry.lookup(Identificator, "#{runner_id}_cu"))

  def runner_registered?(_), do: false

  @doc """
  Poll for a runner `ready_fun` accepts until `timeout_ms` elapses.

  Returns `{:ok, runner_id}`, `{:error, :timeout}` when the fleet simply has no
  candidate, or `{:error, :unavailable}` when `FleetManager` never answered.
  Callers reach for this whenever the answer may arrive late: a runner becomes
  placeable only after `FleetManager` finishes its `runner_connect/3` handler,
  which is the same handler that starts the hub and its recovery replay.

  Every attempt is capped at the smaller of #{@runner_poll_call_timeout_ms} ms
  and what is left of `timeout_ms`, floored at #{@runner_min_call_timeout_ms} ms,
  so a blocked `FleetManager` can overrun the deadline by at most that floor no
  matter how many callers share one budget.
  """
  @spec await_runner(
          String.t(),
          binary() | nil,
          binary(),
          non_neg_integer(),
          (binary() -> boolean())
        ) :: {:ok, binary()} | {:error, :timeout | :unavailable}
  def await_runner(namespace, factory_id, operative_id, timeout_ms, ready_fun \\ fn _ -> true end)

  # An exhausted budget must cost nothing: blocking here is what walks a large
  # batch past `cross_node_request_timeout`. `:timeout`, not `:unavailable` —
  # the fleet was never asked.
  def await_runner(_namespace, _factory_id, _operative_id, timeout_ms, _ready_fun)
      when timeout_ms <= 0,
      do: {:error, :timeout}

  def await_runner(namespace, factory_id, operative_id, timeout_ms, ready_fun) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_runner(namespace, factory_id, operative_id, deadline, ready_fun)
  end

  # Every branch hands `poll_again` the reason its own attempt produced, so the
  # returned error always describes the last attempt.
  defp do_await_runner(namespace, factory_id, operative_id, deadline, ready_fun) do
    case resolve_runner(namespace, factory_id, operative_id, call_timeout(deadline)) do
      {:ok, runner_id} when is_binary(runner_id) ->
        if ready_fun.(runner_id),
          do: {:ok, runner_id},
          else: poll_again(namespace, factory_id, operative_id, deadline, ready_fun, :timeout)

      {:ok, nil} ->
        poll_again(namespace, factory_id, operative_id, deadline, ready_fun, :timeout)

      {:error, :unavailable} ->
        poll_again(namespace, factory_id, operative_id, deadline, ready_fun, :unavailable)
    end
  end

  defp poll_again(namespace, factory_id, operative_id, deadline, ready_fun, last_reason) do
    if System.monotonic_time(:millisecond) + @runner_poll_interval_ms >= deadline do
      {:error, last_reason}
    else
      Process.sleep(@runner_poll_interval_ms)
      do_await_runner(namespace, factory_id, operative_id, deadline, ready_fun)
    end
  end

  defp call_timeout(deadline) do
    (deadline - System.monotonic_time(:millisecond))
    |> min(@runner_poll_call_timeout_ms)
    |> max(@runner_min_call_timeout_ms)
  end

  defp resolve_runner(namespace, factory_id, operative_id, call_timeout_ms) do
    case candidate_runners(namespace, factory_id, call_timeout_ms) do
      :unavailable -> {:error, :unavailable}
      runner_ids -> {:ok, pick_runner(runner_ids, operative_id)}
    end
  end

  # `:unavailable`, never `[]`: collapsing an unanswered read into "no runners"
  # blames the fleet for a control-plane stall (see `Convergence.Probe`).
  defp candidate_runners(namespace, factory_id, call_timeout_ms) do
    lookup_runners(namespace, factory_id, call_timeout_ms)
  catch
    :exit, reason ->
      Logger.warning(
        "[OperativeManager] FleetManager runner lookup did not answer for namespace " <>
          "#{namespace} factory #{inspect(factory_id)}: #{inspect(reason)}"
      )

      :unavailable
  end

  defp lookup_runners(namespace, nil, timeout),
    do: FleetManager.active_runners(namespace, timeout)

  defp lookup_runners(namespace, factory_id, timeout) when is_binary(factory_id),
    do: FleetManager.runners_for_factory(namespace, factory_id, timeout)

  # The candidate set is the authority: a runner is in it only while its
  # transport connection is open. HRW then picks the runner with the
  # highest hash of `{operative_id, runner}` — deterministic and stable under
  # membership change.
  @doc false
  def pick_runner([], _operative_id), do: nil

  def pick_runner(runner_ids, operative_id),
    do: Enum.max_by(runner_ids, fn ce -> :erlang.phash2({operative_id, ce}) end)
end

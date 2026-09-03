defmodule Flezha.Service.OperativeManager do
  @moduledoc """
  Manages the operative startings and stoppings.
  """

  alias Flezha.AuthZ.Quotas
  alias Flezha.Cluster.Directory
  alias Flezha.Operative.Builder, as: OperativeBuilder
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager.Admission
  alias Flezha.Service.OperativeManager.Init
  alias Flezha.Service.OperativeManager.Placement
  alias Flezha.Service.OperativeManager.Teardown
  alias Flezha.Service.PluginManager
  alias Flezha.State.Paths

  require Logger

  defdelegate assign_runner(arg1, arg2, arg3), to: Placement
  defdelegate await_runner(arg1, arg2, arg3, arg4, arg5 \\ fn _ -> true end), to: Placement
  defdelegate pick_runner(arg1, arg2), to: Placement
  defdelegate runner_registered?(arg1), to: Placement

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
          target = Placement.pick_runner(nodes, operative_id)
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
           reason: Admission.cluster_wide_refusal(namespace, operative.factory_id)
         }}

      nodes ->
        target = Placement.pick_runner(nodes, operative.id)

        route_call(
          target,
          :start_operative,
          [namespace, operative, [routed: true]],
          &{:error, %{operative_id: operative.id, node: target, reason: &1}}
        )
    end
  end

  defp do_start_operative(namespace, operative) do
    with :ok <- Admission.check_not_shutting_down(),
         :ok <- Admission.validate_operative(namespace, operative),
         :ok <- Admission.refuse_group_base_claim(namespace, operative),
         :ok <- Admission.validate_factory(namespace, operative.factory_id),
         :ok <- Admission.check_operative_quota(namespace),
         :ok <- Admission.check_entity_quota(namespace, operative),
         :ok <- Admission.check_disk_quota(namespace),
         :ok <- Paths.claim_namespace(operative.id, namespace) do
      case Init.init_operative(&exist?/2, namespace, operative) do
        {:ok, _} = ok ->
          _ = Quotas.record_allocation(namespace, :max_operatives, 1)
          seg_count = Admission.entity_count(operative)

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

      {:error, reason} when is_binary(reason) ->
        if Init.claim_loss?(reason) do
          Init.claim_lost(&exist?/2, namespace, operative.id)
        else
          {:error, %{operative_id: operative.id, node: node(), reason: reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

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
          unless preserve_state?, do: Teardown.delete_operative_state_dir(operative_id)
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
    case Teardown.hub_stop_children(namespace, [operative_id], 15_000) do
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

  defp operative_entity_count(namespace, operative_id) do
    case get_overwatch_state(namespace, operative_id) do
      {:ok, %{builder: builder}} -> Admission.entity_count(builder)
      _ -> 0
    end
  end

  @doc """
  Stop every operative in `namespace` and hand its state directory to
  `Teardown`.

  The walk is the whole point: an operative whose plugins cannot be disabled —
  a wedged Overwatch, a manager too busy to answer — is named in the log and
  stepped over, never a reason to abandon the operatives behind it.
  """
  def stop_all_operatives(namespace) do
    operatives = HubManager.get_all_operatives(namespace)
    operative_ids = Enum.map(operatives, fn {id, _pid} -> id end)

    undisabled =
      Enum.reject(operative_ids, fn operative_id ->
        PluginManager.disable_operative_plugins(namespace, operative_id) == :ok
      end)

    if undisabled != [] do
      Logger.warning(
        "[operative_manager] teardown of #{namespace} could not disable plugins for " <>
          "#{length(undisabled)} of #{length(operative_ids)} operative(s): " <>
          "#{inspect(undisabled)}; stopping them anyway"
      )
    end

    if operative_ids != [] do
      _ = Teardown.hub_stop_children(namespace, operative_ids, 15_000)

      Enum.each(operative_ids, &Teardown.delete_operative_state_dir/1)
    end

    :ok
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
        target = Placement.pick_runner(nodes, instance_id)

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
end

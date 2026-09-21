defmodule Flezha.Service.OperativeManager do
  @moduledoc """
  Manages the operative startings and stoppings.
  """

  alias Flezha.AuthZ.{Quotas, Verb}
  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Operative.Builder, as: OperativeBuilder
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager.Admission
  alias Flezha.Service.OperativeManager.Init
  alias Flezha.Service.OperativeManager.Placement
  alias Flezha.Service.OperativeManager.Teardown
  alias Flezha.Shutdown
  alias Flezha.State.Paths

  defdelegate assign_runner(arg1, arg2, arg3), to: Placement
  defdelegate await_runner(arg1, arg2, arg3, arg4, arg5 \\ fn _ -> true end), to: Placement
  defdelegate pick_runner(arg1, arg2), to: Placement
  defdelegate redeclare_plugins(arg1, arg2), to: Init
  defdelegate runner_registered?(arg1), to: Placement

  # How many ids a start without one draws before it is refused.
  @id_draws 3

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
      HubManager.get_pid(namespace, operative_id)
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

  defp route_call(target, fun, args, on_error),
    do: route_call(target, __MODULE__, fun, args, on_error)

  defp route_call(target, module, fun, args, on_error) do
    :erpc.call(target, module, fun, args, Durations.get(:route_call_timeout_ms))
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
  CLI/manager, a schedule's slot, a tasklet's `spawn_workflow`, and the
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
  """
  @spec start_operative(String.t(), OperativeBuilder.t(), keyword()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def start_operative(namespace, operative, opts \\ [])

  def start_operative(namespace, %{id: nil} = operative, opts) do
    {draw_opts, opts} = Keyword.split(opts, [:mint])
    opts = Keyword.put(opts, :generated, true)
    start = &start_operative(namespace, %{operative | id: &1}, opts)

    with_generated_id(operative.factory_id, start, draw_opts)
  end

  def start_operative(namespace, operative, opts) do
    if Keyword.get(opts, :routed, false) or locally_startable?(namespace, operative.factory_id) do
      do_start_operative(namespace, operative, Keyword.get(opts, :generated, false))
    else
      route_start(namespace, operative, opts)
    end
  end

  @doc """
  Run `attempt` with an id generated for `factory_id`, drawing again while its
  verdict means the id was taken, at most `#{@id_draws}` draws, then refuse.
  Every start that generates an id goes through here. `reserve:` holds back
  bytes for a suffix the caller appends; `mint:` replaces the generator.
  """
  @spec with_generated_id(String.t(), (String.t() -> verdict), keyword()) ::
          verdict | {:error, String.t()}
        when verdict: term()
  def with_generated_id(factory_id, attempt, opts \\ []) do
    reserve = Keyword.get(opts, :reserve, 0)
    mint = Keyword.get(opts, :mint, &Identificator.mint(:operative, &1, reserve: reserve))

    draw_id(factory_id, mint, attempt, @id_draws)
  end

  defp draw_id(factory_id, _mint, _attempt, 0),
    do: {:error, "no free operative id was found for factory #{factory_id} in #{@id_draws} draws"}

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
  defp id_taken?({:error, %{reason: reason}}) when is_binary(reason), do: Init.claim_loss?(reason)
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

  defp route_start(namespace, operative, opts) do
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
          [namespace, operative, [routed: true, generated: Keyword.get(opts, :generated, false)]],
          &{:error, %{operative_id: operative.id, node: target, reason: &1}}
        )
    end
  end

  defp do_start_operative(namespace, operative, generated?) do
    with :ok <- Admission.check_not_shutting_down(namespace),
         :ok <- Admission.check_generated_id_free(namespace, operative.id, generated?),
         :ok <- Admission.validate_operative(namespace, operative),
         :ok <- Admission.refuse_group_base_claim(namespace, operative),
         :ok <- Admission.validate_factory(namespace, operative.factory_id),
         :ok <- Admission.check_operative_quota(namespace),
         :ok <- Admission.check_cell_quota(namespace, operative),
         :ok <- Admission.check_disk_quota(namespace),
         :ok <- Paths.claim_namespace(operative.id, namespace) do
      case Init.init_operative(&exist?/2, namespace, operative) do
        {:ok, _} = ok ->
          _ = Quotas.record_allocation(namespace, :max_operatives, 1)
          seg_count = Admission.cell_count(operative)

          if seg_count > 0 do
            _ =
              Quotas.record_allocation(
                namespace,
                :max_cells_per_operative,
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
  Hard teardown: the `:operative` walk of the shutdown plan — flush the runner's
  channels, stop the child through the hub, delete its on-disk state, release
  its quota — under the one operative-stop budget. Drives `RemOperative`
  (terminal) — the converged teardown for both an explicit stop and a
  `WorkflowCompleted`.
  """
  @spec stop_operative(String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def stop_operative(namespace, operative_id, opts \\ []) do
    on_hub_member(namespace, operative_id, opts, :stop_operative, fn ->
      do_stop_operative(namespace, operative_id, Keyword.get(opts, :preserve_state, false))
    end)
  end

  defp do_stop_operative(namespace, operative_id, preserve_state?) do
    with_operative_pid(namespace, operative_id, fn _operative_pid ->
      # Read the binding and the cell count before the stop tears the
      # operative down — the operative is gone afterwards, the flush needs its
      # runner and the quota step needs the count to release
      # `:max_cells_per_operative` symmetrically with the start.
      {runner_id, cell_count} = stop_facts(namespace, operative_id)

      target = %{
        namespace: namespace,
        operative_id: operative_id,
        runner_id: runner_id,
        preserve_state?: preserve_state?,
        cell_count: cell_count
      }

      case Shutdown.run(:operative, target, Shutdown.stop_timeout_ms()) do
        {:ok, _report} -> :ok
        {:partial, %{errors: [{_step, reason} | _]}} -> {:error, reason}
        {:partial, %{skipped: skipped}} -> {:error, {:timeout, skipped}}
      end
    end)
  end

  defp stop_facts(namespace, operative_id) do
    case get_overwatch_state(namespace, operative_id) do
      {:ok, %{builder: builder} = ow} ->
        {normalize_runner(Map.get(ow, :runner_id)), Admission.cell_count(builder)}

      _ ->
        {nil, 0}
    end
  end

  defp normalize_runner(runner_id) when is_binary(runner_id), do: runner_id
  defp normalize_runner(_), do: nil

  @doc """
  Stop every operative in `namespace` through the hub and reclaim their state
  directories — the `:namespace` walk's first step, callable on its own for a
  namespace whose runners stay.
  """
  def stop_all_operatives(namespace), do: Teardown.stop_all(namespace, Shutdown.stop_timeout_ms())

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
  Start a workflow instance of `factory_id` under `instance_id`, or under a
  generated id when it is `nil`, answering `:duplicate` when a given id is
  already claimed.

  Shared by every in-cluster spawner — a schedule's slot firing and a
  tasklet's `spawn_or_deliver/6`. The duplicate verdict itself is minted by
  `start_operative/3`, the one start-authority seam.
  """
  @spec start_workflow(String.t(), String.t() | nil, String.t(), map(), Keyword.t()) ::
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

  @doc """
  A start verdict as a published door answers it: the id the operative runs
  under, and whether this start began it or found a live instance holding
  `requested_id`. The manager keeps answering `:duplicate`, which the spawn
  race and partition convergence match on.
  """
  @spec start_answer({:ok, map()} | :duplicate | {:error, term()}, String.t() | nil) ::
          {:ok, %{id: String.t(), outcome: :started | :already_running}} | {:error, term()}
  def start_answer({:ok, %{operative_id: id}}, _requested_id),
    do: {:ok, %{id: id, outcome: :started}}

  def start_answer(:duplicate, requested_id),
    do: {:ok, %{id: requested_id, outcome: :already_running}}

  def start_answer({:error, _reason} = refusal, _requested_id), do: refusal

  @doc """
  The audit target of an `operative_start` act: the id its answer names, else
  the id its last argument asked for (a spec, its named fields, a bare id, or
  a wire verb's options), which for a start refused before an id was
  generated is none.
  """
  @spec start_target([term()], term()) :: %{operative: String.t() | nil}
  def start_target(_args, {:ok, %{id: id}}), do: %{operative: id}
  def start_target(args, _answer), do: %{operative: requested_id(List.last(args))}

  defp requested_id(%{id: id}), do: id
  defp requested_id(fields) when is_list(fields), do: Keyword.get(fields, :id)
  defp requested_id(id) when is_binary(id) or is_nil(id), do: id

  @typedoc """
  How a tasklet-requested spawn resolved. `:spawned` and `:delivered` are
  the two success shapes; the rest report why nothing was spawned. It is
  answered beside the id the request resolved to.
  """
  @type spawn_outcome ::
          :spawned | :delivered | :already_running | :factory_unknown | {:refused, String.t()}

  @doc """
  Spawn-or-deliver `instance_id` in `namespace`, the controller half of a
  tasklet's `ctx.spawn_workflow`, for the identity `caller_context` names,
  answering the outcome and the id the request resolved to.

  A free id spawns a workflow instance of `factory_id` carrying `payload` as its
  first input (`:seed_input`, ingested inside the instance's bind continue, so
  nothing can be ingested ahead of it). A taken id either receives `payload` as
  one more input (`if_running: :deliver`) or refuses (`if_running: :error`).
  Concurrent requests for one id race-resolve at the registry: whoever loses the
  uniqueness check converts to the same delivery a later request would get, so
  exactly one instance exists and every payload lands exactly once.

  An empty `instance_id` asks for a generated one: it names no running instance,
  so nothing is looked up, `if_running` has no meaning, and every such request
  spawns its own instance. A refusal answers the id the request carried.

  Each act is judged on this node as the act it is: a start against the
  `operative_start` row, which records it, and a delivery against `spawn_deliver`,
  the `operative.send` right with no record, because a delivery happens once per
  event. A refusal of either is audited. A start no local runner can take is
  forwarded by `start_operative/3` inside that judgment, so no peer acts on a
  request nobody judged. `:already_running` and `:factory_unknown` perform no
  act and are answered without one.
  """
  @spec spawn_or_deliver(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          :deliver | :error,
          Flezha.AuthZ.caller_context()
        ) ::
          {spawn_outcome(), String.t()}
  def spawn_or_deliver(namespace, instance_id, factory_id, payload, if_running, caller_context) do
    cond do
      instance_id != "" and exist?(namespace, instance_id) ->
        resolve_running(namespace, instance_id, payload, if_running, caller_context)

      FleetManager.runners_for_factory(namespace, factory_id) == [] and
          capable_nodes(namespace, factory_id) == [] ->
        {:factory_unknown, instance_id}

      true ->
        spawn_fresh(namespace, instance_id, factory_id, payload, if_running, caller_context)
    end
  end

  defp spawn_fresh(namespace, instance_id, factory_id, payload, if_running, caller_context) do
    requested = if instance_id == "", do: nil, else: instance_id

    start = fn _call ->
      namespace
      |> start_workflow(requested, factory_id, %{}, seed_input: payload)
      |> start_answer(requested)
    end

    case Verb.run(:operative_start, caller_context, [namespace, requested], start,
           target: &start_target/2
         ) do
      {:ok, %{id: id, outcome: :started}} ->
        {:spawned, id}

      # Lost the id race: the winner spawned it, so this request becomes the
      # delivery a request arriving a moment later would have made. A generated
      # id never gets here, because a claim it loses draws another.
      {:ok, %{id: id, outcome: :already_running}} ->
        resolve_running(namespace, id, payload, if_running, caller_context)

      {:error, reason} ->
        {refused_start(reason, namespace, factory_id), instance_id}
    end
  end

  # A start refused on a node where no runner advertises the factory, this one or
  # a peer the directory still named, is a factory nobody registers. The map
  # names the node the start ran on; a policy or quota refusal carries none.
  defp refused_start(%{node: refusing, reason: text} = reason, namespace, factory_id)
       when is_binary(text) do
    placement =
      route_call(refusing, FleetManager, :placement_refusal, [namespace, factory_id], & &1)

    if placement == :no_such_factory, do: :factory_unknown, else: {:refused, inspect(reason)}
  end

  defp refused_start(reason, _namespace, _factory_id), do: {:refused, inspect(reason)}

  defp resolve_running(_namespace, instance_id, _payload, :error, _caller_context),
    do: {:already_running, instance_id}

  defp resolve_running(namespace, instance_id, payload, :deliver, caller_context) do
    deliver = fn _call -> send_io_confirming(instance_id, namespace, payload, %{}) end

    case Verb.run(:spawn_deliver, caller_context, [namespace, instance_id], deliver) do
      {:error, reason} -> {{:refused, inspect(reason)}, instance_id}
      _delivered_or_absorbed -> {:delivered, instance_id}
    end
  end

  def create_snapshot(namespace, operative_id) do
    with_operative_pid(namespace, operative_id, fn pid ->
      SafeCall.bounded(pid, :create_snapshot, Durations.get(:overwatch_call_timeout_ms),
        on_error: &{:error, {:overwatch_unavailable, &1}}
      )
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
  The namespace an operative class belongs to, for a caller that must scope an
  authorization decision to it.

  The running registries answer first; a class that is not running is resolved
  from its state root, so a verb against a stopped operative still authorizes
  in the right namespace rather than in `_system`.
  """
  @spec namespace_of_class(String.t()) :: String.t()
  def namespace_of_class(operative_class) when is_binary(operative_class) do
    case namespace_of(operative_class) do
      {:ok, namespace} -> namespace
      :error -> Paths.namespace_of_class(operative_class)
    end
  end

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
  Whole-Operative read: every Tasklet's Cell state as one JSON blob. The
  seq-less shape the migration/handover and introspection callers predate the
  selector with.
  """
  def get_cell_state(namespace, operative_id) do
    case get_cell_state(namespace, operative_id, %{}) do
      {:ok, json, _change_seq} -> {:ok, json}
      err -> err
    end
  end

  @doc """
  Live read narrowed by `selector` (`%{tasklet: name}` or
  `%{tasklet: name, cell_key: key}`; `%{}` covers every Tasklet). Answers
  `{:ok, json, change_seq}` — the seq the state was serialized at, which may be
  ahead of what is durable — or `{:error, reason}`, including the
  `{:not_resident, reason}` refusals for a Cell that is not held in memory.
  """
  @spec get_cell_state(binary(), binary(), map()) ::
          {:ok, binary(), non_neg_integer() | nil} | {:error, term()}
  def get_cell_state(namespace, operative_id, selector) when is_map(selector) do
    with_operative_pid(namespace, operative_id, fn pid ->
      # Unwrap one level: the operative's own reply is already
      # `{:ok, json, seq} | {:error, reason}`, so collapse `SafeCall`'s outer
      # `{:ok, _}`; a bounded timeout/noproc stays `{:error, reason}`.
      case SafeCall.bounded(pid, {:get_cell_state, selector}, overwatch_call_timeout_ms()) do
        {:ok, reply} -> reply
        {:error, _} = err -> err
      end
    end)
  end

  defp overwatch_call_timeout_ms,
    do: Durations.get(:overwatch_call_timeout_ms)
end

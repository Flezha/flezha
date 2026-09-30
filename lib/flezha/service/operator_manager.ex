defmodule Flezha.Service.OperatorManager do
  @moduledoc """
  Manages the operator startings and stoppings.
  """

  alias Flezha.AuthZ.Quotas
  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Operator.Builder, as: OperatorBuilder
  alias Flezha.Operator.Calls
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager.Admission
  alias Flezha.Service.OperatorManager.Init
  alias Flezha.Service.OperatorManager.Placement
  alias Flezha.Service.OperatorManager.Spawn
  alias Flezha.Service.OperatorManager.Teardown
  alias Flezha.Shutdown
  alias Flezha.State.Completion
  alias Flezha.State.Paths

  defdelegate assign_runner(arg1, arg2, arg3), to: Placement
  defdelegate await_runner(arg1, arg2, arg3, arg4, arg5 \\ fn _ -> true end), to: Placement
  defdelegate pick_runner(arg1, arg2), to: Placement
  defdelegate redeclare_plugins(arg1, arg2), to: Init
  defdelegate runner_registered?(arg1), to: Placement

  # How many ids a start without one draws before it is refused.
  @id_draws 3

  def operator_registry(namespace) do
    case HubManager.get_registry(namespace) do
      {:ok, registry} ->
        registry

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_operator_ids(namespace) do
    operators =
      namespace
      |> HubManager.get_all_operators()
      |> Enum.map(&elem(&1, 0))

    {:ok, operators}
  end

  @doc """
  Resolve `operator_id`'s pid from ANY controller node. A node that is not a
  member of the namespace's hub asks one hub-member node (resolved through the
  cluster directory, forwarded at most once); every pid-interaction verb then
  rides BEAM distribution on the returned pid, so the interaction still
  executes inside the operator's own node.
  """
  def get_operator_pid(namespace, operator_id, opts \\ []) do
    on_hub_member(namespace, operator_id, opts, :get_operator_pid, fn ->
      HubManager.get_pid(namespace, operator_id)
    end)
  end

  # Run `local` when this node is a hub member (or the call was already
  # forwarded — the loop guard), else forward `fun` once to one deterministic
  # hub-member node. Local with no hub anywhere preserves today's refusal.
  defp on_hub_member(namespace, operator_id, opts, fun, local) do
    if Keyword.get(opts, :routed, false) or HubManager.hub_alive?(namespace) do
      local.()
    else
      case List.delete(Directory.hub_nodes(namespace), node()) do
        [] ->
          local.()

        nodes ->
          target = Placement.pick_runner(nodes, operator_id)
          route_call(target, fun, [namespace, operator_id, [routed: true]], &{:error, &1})
      end
    end
  end

  # Resolve the pid, then run `fun` on it — every error passes through.
  defp with_operator_pid(namespace, operator_id, fun) do
    case get_operator_pid(namespace, operator_id) do
      {:ok, operator_pid} -> fun.(operator_pid)
      err -> err
    end
  end

  # One bounded operator forward; an unreachable or crashed peer maps through `on_error`
  # instead of raising into the caller. Must exceed the remote start's own
  # internal budget (ProcessHub's 15s wait plus the :no_response retry ladder).

  defp route_call(target, fun, args, on_error),
    do: route_call(target, __MODULE__, fun, args, on_error)

  defp route_call(target, module, fun, args, on_error) do
    :erpc.call(target, module, fun, args, Durations.get(:route_call_timeout_ms))
  catch
    kind, reason -> on_error.({:routed_call_failed, target, kind, reason})
  end

  def get_overwatch_state(namespace, operator_id) do
    # `SafeCall.bounded` returns `{:ok, state} | {:error, reason}` — the same
    # success shape as before, but a wedged operator now yields a logged
    # `{:error, :timeout}` (with the target's mailbox/current-function) instead
    # of an unhandled exit.
    with_operator_pid(namespace, operator_id, fn pid ->
      SafeCall.bounded(pid, :get_overwatch_state, overwatch_call_timeout_ms())
    end)
  end

  @doc """
  Starts a new operator to the runner.

  Location-transparent: a node that cannot satisfy the start itself (no
  namespace hub / no runner advertising the factory) resolves the capable
  nodes through the cluster directory and executes the same call on one of
  them, forwarded at most once (`routed: true`). The executing node
  re-validates everything, so a stale directory answer costs one failed
  forward, never a wrong start.

  This is the single start-authority seam: every in-cluster starter — the
  CLI/manager, a schedule's slot, a tasklet's `spawn_bounded`, and the
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
  @spec start_operator(String.t(), OperatorBuilder.t(), keyword()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def start_operator(namespace, operator, opts \\ [])

  def start_operator(namespace, %{id: nil} = operator, opts) do
    {draw_opts, opts} = Keyword.split(opts, [:mint])
    opts = Keyword.put(opts, :generated, true)
    start = &start_operator(namespace, %{operator | id: &1}, opts)

    with_generated_id(operator.factory_id, start, draw_opts)
  end

  def start_operator(namespace, operator, opts) do
    if Keyword.get(opts, :routed, false) or locally_startable?(namespace, operator.factory_id) do
      do_start_operator(namespace, operator, Keyword.get(opts, :generated, false))
    else
      route_start(namespace, operator, opts)
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
    mint = Keyword.get(opts, :mint, &Identificator.mint(:operator, &1, reserve: reserve))

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

  defp route_start(namespace, operator, opts) do
    case capable_nodes(namespace, operator.factory_id) do
      [] ->
        {:error, Admission.cluster_wide_refusal(namespace, operator.factory_id)}

      nodes ->
        target = Placement.pick_runner(nodes, operator.id)

        route_call(
          target,
          :start_operator,
          [namespace, operator, [routed: true, generated: Keyword.get(opts, :generated, false)]],
          &{:error, &1}
        )
    end
  end

  defp do_start_operator(namespace, operator, generated?) do
    with :ok <- Admission.check_not_shutting_down(namespace),
         :ok <- Admission.check_generated_id_free(namespace, operator.id, generated?),
         :ok <- Admission.validate_operator(namespace, operator),
         :ok <- Admission.refuse_group_base_claim(namespace, operator),
         {:ok, operator} <- Admission.resolve_factory(namespace, operator),
         :ok <- Admission.check_operator_quota(namespace),
         :ok <- Admission.check_cell_quota(namespace, operator),
         :ok <- Admission.check_disk_quota(namespace),
         :ok <- Paths.claim_namespace(operator.id, namespace) do
      case Init.init_operator(&exist?/2, namespace, operator) do
        {:ok, _} = ok ->
          _ = Quotas.record_allocation(namespace, :max_operators, 1)
          seg_count = Admission.cell_count(operator)

          if seg_count > 0 do
            _ =
              Quotas.record_allocation(
                namespace,
                :max_cells_per_operator,
                seg_count
              )
          end

          ok

        other ->
          other
      end
    else
      {:error, reason} ->
        if Init.claim_loss?(reason),
          do: Init.claim_lost(&exist?/2, namespace, operator.id),
          else: {:error, reason}
    end
  end

  @doc """
  Stops `operator_id` the way its kind expects.

  A `:bounded` operator is stopped **cooperatively**, awake or asleep: a
  `BoundedStop` trips the SDK's cancellation token and the operator winds
  down to its own `BoundedCompleted` (status "cancelled"), which drives the
  single teardown via `ControlChannel.handle_bounded_completed/2`. A sleeping
  one is first woken onto a runner, and the stop follows the bind. An unbounded operator,
  and a bounded one no runner can take or whose runner cannot be reached, take the
  hard `stop_operator/2` teardown. An unreadable kind (not found / wedged)
  also falls back to the hard stop so `flz operator stop` always attempts
  removal.

  Returns `{:ok, :cooperative}` when a cooperative stop was signalled, `:ok` after
  a hard teardown, or `{:error, reason}`.
  """
  @spec request_stop(String.t(), String.t(), keyword()) ::
          :ok | {:ok, :cooperative} | {:error, term()}
  def request_stop(namespace, operator_id, opts \\ []) do
    # Whole-call forward, unlike the pid-riding read verbs: the stop path's
    # side effects (ControlUnit lookup, plugin disable, state-dir delete,
    # quota release) are node-local and must run on the hosting node.
    on_hub_member(namespace, operator_id, opts, :request_stop, fn ->
      do_request_stop(namespace, operator_id)
    end)
  end

  defp do_request_stop(namespace, operator_id, wake? \\ true) do
    case get_overwatch_state(namespace, operator_id) do
      {:ok, %{builder: %{kind: :bounded}, suspended: true}} when wake? ->
        case wake_to_stop(namespace, operator_id) do
          :ok -> {:ok, :cooperative}
          # Woken meanwhile: stopped as an awake one, once.
          {:error, :awake} -> do_request_stop(namespace, operator_id, false)
          {:error, _no_runner} -> stop_operator(namespace, operator_id)
        end

      {:ok, %{builder: %{kind: :bounded}, runner_id: runner_id}}
      when is_binary(runner_id) ->
        case ControlUnit.bounded_stop(runner_id, operator_id) do
          :ok -> {:ok, :cooperative}
          # No control unit to cooperate with — fall back to the hard teardown.
          {:error, _} -> stop_operator(namespace, operator_id)
        end

      _ ->
        stop_operator(namespace, operator_id)
    end
  end

  defp wake_to_stop(namespace, operator_id) do
    with_operator_pid(namespace, operator_id, fn pid ->
      case SafeCall.bounded(pid, {:wake, then: :stop}, overwatch_call_timeout_ms()) do
        {:ok, reply} -> reply
        {:error, _} = unreachable -> unreachable
      end
    end)
  end

  @doc """
  Hard teardown: the `:operator` walk of the shutdown plan — flush the runner's
  channels, stop the child through the hub, delete its on-disk state, release
  its quota — under the one operator-stop budget. Drives `RemOperator`
  (terminal) — the converged teardown for both an explicit stop and a
  `BoundedCompleted`. `preserve_state: true` keeps the files for a rescale to
  move and `:sealed` also seals a bounded operator's journal at its latest committed
  iteration; either closes the writers on the node the operator ran on.
  """
  @spec stop_operator(String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def stop_operator(namespace, operator_id, opts \\ []) do
    on_hub_member(namespace, operator_id, opts, :stop_operator, fn ->
      do_stop_operator(namespace, operator_id, Keyword.get(opts, :preserve_state, false))
    end)
  end

  defp do_stop_operator(namespace, operator_id, preserve_state?) do
    with_operator_pid(namespace, operator_id, fn operator_pid ->
      # Read the binding and the cell count before the stop tears the
      # operator down — the operator is gone afterwards, the flush needs its
      # runner and the quota step needs the count to release
      # `:max_cells_per_operator` symmetrically with the start.
      {runner_id, cell_count} = stop_facts(namespace, operator_id)

      target = %{
        namespace: namespace,
        operator_id: operator_id,
        node: node(operator_pid),
        runner_id: runner_id,
        preserve_state?: preserve_state?,
        cell_count: cell_count
      }

      case Shutdown.run(:operator, target, Shutdown.stop_timeout_ms()) do
        {:ok, _report} ->
          :ok

        {:partial, %{errors: [{step, reason} | _]}} ->
          {:error, {:stop_failed, operator_id, step, reason}}

        {:partial, %{skipped: skipped}} ->
          {:error, {:stop_timeout, operator_id, skipped}}
      end
    end)
  end

  defp stop_facts(namespace, operator_id) do
    case get_overwatch_state(namespace, operator_id) do
      {:ok, %{builder: builder} = ow} ->
        {normalize_runner(Map.get(ow, :runner_id)), Admission.cell_count(builder)}

      _ ->
        {nil, 0}
    end
  end

  defp normalize_runner(runner_id) when is_binary(runner_id), do: runner_id
  defp normalize_runner(_), do: nil

  @doc """
  Stop every operator in `namespace` through the hub and reclaim their state
  directories — the `:namespace` walk's first step, callable on its own for a
  namespace whose runners stay.
  """
  def stop_all_operators(namespace), do: Teardown.stop_all(namespace, Shutdown.stop_timeout_ms())

  @doc """
  Reclaim a stopped operator's state directory on this node: its writers
  closed, its tree removed unless a completion record keeps it for the
  retention window.
  """
  @spec delete_state_dir(String.t()) :: :ok
  defdelegate delete_state_dir(operator_id), to: Teardown, as: :delete_operator_state_dir

  @doc "The completion record a finished bounded instance of `operator_class` left."
  defdelegate completion(operator_class), to: Completion, as: :read

  @doc "Whether a finished bounded instance of `operator_class` left a completion record."
  defdelegate completed?(operator_class), to: Completion, as: :exists?

  def send_io(operator_id, namespace, message, metadata) do
    with_operator_pid(namespace, operator_id, fn pid ->
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
          | {:error, {:not_accepting_input | :input_refused, String.t()} | term()}

  @doc """
  Confirming counterpart to `send_io/4`: runs the target's single pre-delivery
  ingest path and answers what happened, so a forwarding sink can tell a
  terminal outcome from one worth retrying. An unresolvable target answers the
  lookup's own reason — also retryable, since the operator may not be started
  yet.
  """
  @spec send_io_confirming(String.t(), String.t(), binary(), map(), timeout()) :: ingest_verdict()
  def send_io_confirming(operator_id, namespace, message, metadata, timeout \\ 5_000) do
    with {:ok, operator_pid} <- get_operator_pid(namespace, operator_id) do
      operator_pid
      |> GenServer.call({:ingest, message, metadata}, timeout)
      |> named_verdict(operator_id)
    end
  end

  @doc "The input metadata naming the entry channel `channel`; `nil` or `\"\"` names none."
  @spec channel_metadata(String.t() | nil) :: map()
  def channel_metadata(channel) when channel in [nil, ""], do: %{}
  def channel_metadata(channel), do: %{"channel" => channel}

  @doc """
  The operator an input to `operator_id` in `namespace` reaches: the operator
  itself, or, when `operator_id` names a partition group, the member whose
  key-group range owns `key`. A group named with no key, or with a key no
  member owns, is refused naming its members.
  """
  @spec resolve_member(String.t(), String.t(), String.t() | nil) ::
          {:ok, String.t()}
          | {:error, {:unkeyed, String.t(), [String.t()]}}
          | {:error, {:operator_not_found, String.t(), String.t()}}
  def resolve_member(namespace, operator_id, key) do
    with false <- exist?(namespace, operator_id),
         [_ | _] = members <- PartitionGroup.members(namespace, operator_id) do
      case key && Enum.find(members, &Partition.owns?(&1.partition.range, key)) do
        nil -> {:error, {:unkeyed, operator_id, Enum.map(members, & &1.id)}}
        member -> {:ok, member.id}
      end
    else
      true -> {:ok, operator_id}
      [] -> {:error, {:operator_not_found, namespace, operator_id}}
    end
  end

  @doc """
  Call `operator_id` (a partition group's member by `key`) with one input and
  wait for its answer: the one output a `response()` end produced from it, or
  a coded error saying why none came by `deadline_ms`.
  """
  @spec call_operator(String.t(), String.t(), binary(), map(), pos_integer(), String.t() | nil) ::
          {:ok, binary(), map()} | {:error, term()}
  def call_operator(namespace, operator_id, message, metadata, deadline_ms, key) do
    with {:ok, member} <- resolve_member(namespace, operator_id, key),
         {:ok, pid} <- get_operator_pid(namespace, member),
         do: Calls.call(pid, message, metadata, deadline_ms, member)
  end

  # The ingest path answers for itself; a caller is told which operator.
  defp named_verdict({:error, :not_ready}, operator_id),
    do: {:error, {:not_accepting_input, operator_id}}

  defp named_verdict({:error, :refused}, operator_id), do: {:error, {:input_refused, operator_id}}
  defp named_verdict(verdict, _operator_id), do: verdict

  @doc """
  `send_io_confirming/5` for a source holding many inputs at once, with up to
  `window` requests in flight. Every input is the same `{:ingest, ...}` call —
  the operator runs its single ingest path once per input, in input order —
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
  def send_io_confirming_many(operator_id, namespace, payloads, timeout \\ 5_000, window \\ 64)

  def send_io_confirming_many(_operator_id, _namespace, [], _timeout, _window), do: []

  def send_io_confirming_many(operator_id, namespace, payloads, timeout, window) do
    case get_operator_pid(namespace, operator_id) do
      {:ok, operator_pid} ->
        pipeline(
          operator_pid,
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
  Start a bounded instance of `factory_id` under `instance_id`, or under a
  generated id when it is `nil`, answering `:duplicate` when a given id is
  already claimed.

  Shared by every in-cluster spawner — a schedule's slot firing and a
  tasklet's `spawn_or_deliver/6`. The duplicate verdict itself is minted by
  `start_operator/3`, the one start-authority seam.
  """
  @spec start_bounded(String.t(), String.t() | nil, String.t(), map(), Keyword.t()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def start_bounded(namespace, instance_id, factory_id, init_values \\ %{}, opts \\ []) do
    start_operator(namespace, %OperatorBuilder{
      id: instance_id,
      factory_id: factory_id,
      kind: :bounded,
      init_values: init_values,
      opts: opts
    })
  end

  @doc """
  A start verdict as a published door answers it: the id the operator runs
  under, and whether this start began it or found a live instance holding
  `requested_id`. The manager keeps answering `:duplicate`, which the spawn
  race and partition convergence match on.
  """
  @spec start_answer({:ok, map()} | :duplicate | {:error, term()}, String.t() | nil) ::
          {:ok, %{id: String.t(), outcome: :started | :already_running}} | {:error, term()}
  def start_answer({:ok, %{operator_id: id}}, _requested_id),
    do: {:ok, %{id: id, outcome: :started}}

  def start_answer(:duplicate, requested_id),
    do: {:ok, %{id: requested_id, outcome: :already_running}}

  def start_answer({:error, _reason} = refusal, _requested_id), do: refusal

  @doc """
  The audit target of an `operator_start` act: the id its answer names, else
  the id its last argument asked for (a spec, its named fields, a bare id, or
  a wire verb's options), which for a start refused before an id was
  generated is none.
  """
  @spec start_target([term()], term()) :: %{operator: String.t() | nil}
  def start_target(_args, {:ok, %{id: id}}), do: %{operator: id}
  def start_target(args, _answer), do: %{operator: requested_id(List.last(args))}

  defp requested_id(%{id: id}), do: id
  defp requested_id(fields) when is_list(fields), do: Keyword.get(fields, :id)
  defp requested_id(id) when is_binary(id) or is_nil(id), do: id

  @doc """
  Spawn-or-deliver `instance_id` in `namespace`, the controller half of a
  tasklet's `ctx.spawn_bounded`, for the identity `caller_context` names,
  answering the outcome and the id the request resolved to.

  A free id spawns a bounded instance of `factory_id` carrying `payload` as its
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
  operator the first request did, running or completed, and delivers nothing
  twice.

  Each act is judged on this node as the act it is: a start against the
  `operator_start` row, which records it, and a delivery against `spawn_deliver`,
  the `operator.send` right with no record, because a delivery happens once per
  event. A refusal of either is audited. A start no local runner can take is
  forwarded by `start_operator/3` inside that judgment, so no peer acts on a
  request nobody judged. `operator.already_running`, `operator.factory_unknown`
  and `operator.factory_not_bounded` (a factory its runners announced as an
  unbounded operator) perform no act and are answered without one.

  `opts` (`Flezha.Service.OperatorManager.Spawn.opts/0`) is what an operator's
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
  The kind `factory_id` is established at, asked of this node when a runner
  here advertises it and of a node the directory names otherwise, so the
  answer is the cluster's.
  """
  def factory_kind(namespace, factory_id) do
    if FleetManager.runners_for_factory(namespace, factory_id) != [] do
      FleetManager.factory_kind(namespace, factory_id)
    else
      case capable_nodes(namespace, factory_id) do
        [] ->
          {:error, :no_such_factory}

        [target | _] ->
          route_call(target, FleetManager, :factory_kind, [namespace, factory_id], &{:error, &1})
      end
    end
  end

  def create_snapshot(namespace, operator_id) do
    with_operator_pid(namespace, operator_id, fn pid ->
      SafeCall.bounded(pid, :create_snapshot, Durations.get(:overwatch_call_timeout_ms),
        on_error: &{:error, {:overwatch_unavailable, &1}}
      )
    end)
  end

  def exist?(namespace, operator_id) do
    case get_operator_pid(namespace, operator_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  @doc """
  The running namespace holding `operator_id`, or `:error` when no running
  namespace does. `:except` skips one namespace, which is how a caller asks
  "does this id resolve somewhere OTHER than here" (the cross-namespace
  refusals) rather than "where does it resolve" (the CLI's read verbs).
  """
  @spec namespace_of(binary(), keyword()) :: {:ok, binary()} | :error
  def namespace_of(operator_id, opts \\ []) when is_binary(operator_id) do
    except = opts |> Keyword.get(:except) |> normalize_namespace()

    candidate_namespaces()
    |> Enum.reject(&(&1 == except))
    |> first_namespace(&exist?(&1, operator_id))
  end

  defp normalize_namespace(nil), do: nil
  defp normalize_namespace(namespace), do: to_string(namespace)

  @doc """
  The namespace an operator class belongs to, for a caller that must scope an
  authorization decision to it.

  The running registries answer first; a class that is not running is resolved
  from its state root, so a verb against a stopped operator still authorizes
  in the right namespace rather than in `_system`.
  """
  @spec namespace_of_class(String.t()) :: String.t()
  def namespace_of_class(operator_class) when is_binary(operator_class) do
    case namespace_of(operator_class) do
      {:ok, namespace} -> namespace
      :error -> Paths.namespace_of_class(operator_class)
    end
  end

  @doc """
  The namespace whose registry holds members of the partition group
  `group_id`, or `:error`.

  A group id names no operator, so `namespace_of/1` cannot find it; this
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
  Whole-Operator read: every Tasklet's Cell state as one JSON blob. The
  seq-less shape the migration/handover and introspection callers predate the
  selector with.
  """
  def get_cell_state(namespace, operator_id) do
    case get_cell_state(namespace, operator_id, %{}) do
      {:ok, json, _change_seq} -> {:ok, json}
      err -> err
    end
  end

  @doc """
  Live read narrowed by `selector` (`%{tasklet: name}` or
  `%{tasklet: name, cell_key: key}`; `%{}` covers every Tasklet). Answers
  `{:ok, json, change_seq}` — the seq the state was serialized at, which may be
  ahead of what is durable — or `{:error, reason}`, including the
  `{:not_resident, reason, operator_id}` refusals for a Cell that is not held in memory.
  """
  @spec get_cell_state(binary(), binary(), map()) ::
          {:ok, binary(), non_neg_integer() | nil} | {:error, term()}
  def get_cell_state(namespace, operator_id, selector) when is_map(selector) do
    with_operator_pid(namespace, operator_id, fn pid ->
      # Unwrap one level: the operator's own reply is already
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

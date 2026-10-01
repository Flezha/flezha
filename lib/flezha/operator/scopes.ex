defmodule Flezha.Operator.Scopes do
  @moduledoc """
  The stepped scopes of one outer instance, as its `Overwatch` keeps them.

  A graph node that embeds a steps class runs one scope of it per edge key
  inside the outer instance, on the outer instance's runner. A scope is
  identified by `(instance, inner, key)`, and its local id
  (`Flezha.State.ScopePaths.scope_id/3`) keys its task journal, its input log and
  its Cells, all under the outer instance's directory. It has no Overwatch,
  placement or CLI presence of its own: this module binds it the way a
  top-level steps instance is bound, on the outer instance's runner.

  Inside a durable outer instance every message for a scope arrives here first
  (`ScopeInput`): it is appended to the scope's input log, answered, and
  forwarded to the scope, or, when the scope is asleep or absent, the scope is
  bound first: an `AddOperator` carrying the scope, its Cells' restores, then
  its owed input tail. A scope that sleeps (`IterPark` with its scope) is
  evicted and woken by its key's next message or its wake deadline; a scope
  that ends is evicted, and a `once` scope stays finished, refusing later
  messages into the dead-letter queue. A rebind of the outer instance leaves
  every scope asleep and binds again only the ones that owe work: an open pass
  or an undelivered input.

  Every function runs in the outer instance's Overwatch process and takes its
  state; a bind runs in a task linked to it.
  """

  use Flezha.Process.Timer, keys: [:scope_wake]

  require Logger

  alias Flezha.AuthZ.Quotas
  alias Flezha.Dlq.{Entry, Store}
  alias Flezha.Operator.{ClassRegistry, Lifecycle, Lookup, Migration, Overwatch, Replay}
  alias Flezha.Operator.Scopes.Exits
  alias Flezha.Runner.ControlChannel.Requests
  alias Flezha.Runner.ControlUnit
  alias Flezha.State.{InputLog, Paths, ScopePaths}
  alias Flezha.State.Supervisor, as: StateSupervisor

  @typedoc "A scope of this instance: the embedding node and the edge key."
  @type scope :: {binary(), binary() | nil}

  @typedoc """
  Who an input's answer goes to: the control channel and the reference, or
  nobody for a message re-entered after its scope ended.
  """
  @type reply :: {pid(), binary()} | nil

  @doc "The wire `Scope` of `scope`."
  @spec wire(scope()) :: struct()
  def wire({inner, key}), do: %RunnerCommands.Scope{inner: inner, key: key}

  @doc "The local id of `scope` of the instance `state` runs."
  @spec id(Overwatch.t(), scope()) :: binary()
  def id(%Overwatch{id: id}, {inner, key}), do: ScopePaths.scope_id(id, inner, key)

  @doc """
  Where a frame for the instance `id` is addressed: a stepped scope's to its
  instance, with what its id carries after the instance's in `flz.scope`,
  since a frame names an id no longer than an operator id may be. Only this
  function sets `flz.scope`: `Flezha.Operator.Ingest` drops one an outside
  input carries.
  """
  @spec addressed(binary(), {term(), map()}) :: {binary(), {term(), map()}}
  def addressed(id, {message, metadata} = payload) do
    case :binary.match(id, "/") do
      :nomatch ->
        {id, payload}

      {at, _} ->
        scope = binary_part(id, at, byte_size(id) - at)
        {binary_part(id, 0, at), {message, Map.put(metadata, "flz.scope", scope)}}
    end
  end

  @doc """
  A message for `scope`, journaled before anything else: the answer goes back
  to the runner once the append is durable, and the message on to the scope.
  One whose `flz.msg_id` the log still holds is answered and goes nowhere: a
  replay of the outer input that sent it. A finished `once` scope, or a new
  one past the namespace's operator quota, refuses it into the dead-letter
  queue.
  """
  @spec input(Overwatch.t(), scope(), {binary(), map()}, reply()) :: Overwatch.t()
  def input(%Overwatch{} = state, scope, {payload, metadata}, reply) do
    case status(state, scope) |> admitted(state) do
      {:refused, why} ->
        dead_letter(state, scope, metadata["channel"], payload, why)
        Requests.answer_scope_input(reply, :refused)
        state

      _open ->
        reqids =
          InputLog.append_async(
            id(state, scope),
            payload,
            metadata,
            reqids(state),
            {:scope_input, scope, reply}
          )

        put(state, :reqids, reqids)
    end
  end

  @doc """
  A logged scope's exit values `exit`, which its journal entry `key` made
  final, into this instance's own input log (`Flezha.Operator.Scopes.Exits`);
  `reply_to` is told the verdict once they are durable.
  """
  @spec take_exit(Overwatch.t(), scope(), Exits.key(), Exits.exit(), term()) ::
          {term() | :pending, Overwatch.t()}
  def take_exit(%Overwatch{} = state, scope, key, exit, reply_to),
    do: Exits.take(state, token(state, scope), key, exit, reply_to)

  @doc """
  The input log's answer to a scope's append, or `:ignore` for a message that
  is not one.
  """
  @spec journal_reply(Overwatch.t(), term()) :: {:ok, Overwatch.t()} | :ignore
  def journal_reply(%Overwatch{} = state, msg) do
    case :gen_server.check_response(msg, reqids(state), true) do
      {{:reply, reply}, {:scope_input, scope, to}, reqids} ->
        state = put(state, :reqids, reqids)
        {:ok, journaled(state, scope, to, reply)}

      {{:error, {reason, _log}}, {:scope_input, _scope, to}, reqids} ->
        Requests.answer_scope_input(to, {:input_log_unavailable, reason})
        {:ok, put(state, :reqids, reqids)}

      _not_ours ->
        :ignore
    end
  end

  defp journaled(state, scope, to, {:ok, offset}) do
    Requests.answer_scope_input(to, nil)

    case status(state, scope) do
      :awake -> forward(state, scope, offset)
      :binding -> state
      _asleep_or_absent -> bind(state, scope)
    end
  end

  defp journaled(state, _scope, to, {:duplicate, _offset}) do
    Requests.answer_scope_input(to, nil)
    state
  end

  defp journaled(state, _scope, to, refused) do
    Requests.answer_scope_input(to, refused)
    state
  end

  defp forward(state, scope, offset) do
    case InputLog.read_range(id(state, scope), offset, offset) do
      [{^offset, message, metadata}] ->
        Overwatch.deliver(as_scope(state, scope), {message, stamp(metadata, offset)})

      _gone ->
        :ok
    end

    state
  end

  defp stamp(metadata, offset), do: Map.put(metadata, "flz.offset", Integer.to_string(offset))

  @doc """
  Bind `scope` on the outer instance's runner, in a task: its journal and
  Cells restored. The task reports with `{:scope_bound, scope, result}`, and
  the bound scope is then given the input it is owed.
  """
  @spec bind(Overwatch.t(), scope()) :: Overwatch.t()
  def bind(%Overwatch{runner_id: nil} = state, _scope), do: state

  def bind(%Overwatch{} = state, {inner, _key} = scope) do
    case Map.fetch(factories(state), inner) do
      {:ok, factory_id} ->
        owner = owner_pid(state)
        evicting = scopes(state) |> Map.get(:evicting, %{}) |> Map.get(scope)

        Task.start(fn ->
          after_eviction(evicting)
          send(owner, {:scope_bound, scope, bind_now(state, scope, factory_id)})
        end)

        set(state, scope, :binding)

      :error ->
        Logger.warning("[Overwatch] #{state.id} has no node #{inspect(inner)} embedding a class")
        state
    end
  end

  defp bind_now(state, scope, factory_id) do
    scope_id = id(state, scope)
    scoped = as_scope(state, scope)

    with :ok <- Paths.claim_namespace(scope_id, state.namespace),
         :ok <- ClassRegistry.put(scope_id, :steps),
         :ok <- Replay.seed_steps_outboxes(scoped),
         {:ok, raw} <-
           ControlUnit.send_control_await(
             state.runner_id,
             {:add_operator, add_operator(state, scope, factory_id)}
           ),
         declarations = ClassRegistry.decode_declarations(raw),
         :ok <- ClassRegistry.put_declarations(scope_id, declarations),
         :ok <- Replay.restore_class(scope_id, state.runner_id),
         do: {:ok, declarations.each}
  end

  defp add_operator(state, scope, factory_id) do
    struct!(
      %RunnerCommands.AddOperator{
        operator_id: state.id,
        factory_id: factory_id,
        run: "steps",
        scope: wire(scope),
        dead_letter_turns: true
      },
      Lifecycle.resume_state(id(state, scope))
    )
  end

  @doc """
  A bind's report. A bound scope takes the input that arrived while it was
  binding; a refused one is asleep again, its input still in its log.
  """
  @spec bound(Overwatch.t(), scope(), {:ok, atom()} | term()) :: Overwatch.t()
  def bound(%Overwatch{} = state, {inner, _key} = scope, {:ok, each}) do
    remember(state, scope, %{wake: nil})
    Replay.reforward_undelivered_tail(as_scope(state, scope))
    state = Exits.relog(state, token(state, scope), id(state, scope))

    state
    |> put(:each, Map.put(Map.get(scopes(state), :each, %{}), inner, each))
    |> set(scope, :awake)
  end

  def bound(%Overwatch{} = state, scope, refused) do
    Logger.warning(
      "[Overwatch] #{state.id} could not bind scope #{inspect(scope)}: #{inspect(refused)}"
    )

    set(state, scope, :asleep)
  end

  @doc """
  `scope` asked to sleep at a committed boundary: it is evicted from the
  runner, and woken by its key's next message or by `wake_deadline_ms`.
  """
  @spec parked(Overwatch.t(), scope(), non_neg_integer() | nil) :: Overwatch.t()
  def parked(%Overwatch{} = state, scope, wake_deadline_ms) do
    remember(state, scope, %{wake: wake_deadline_ms})

    state
    |> evict(scope)
    |> sleep(scope, wake_deadline_ms)
    |> rehydrate_if_owed(scope)
  end

  defp sleep(state, scope, nil), do: set(state, scope, :asleep)

  defp sleep(state, scope, wake_deadline_ms) do
    delay = max(wake_deadline_ms - System.system_time(:millisecond), 0)

    state
    |> arm_timer({:scope_wake, scope}, delay, {:scope_wake, scope})
    |> set(scope, :asleep)
  end

  # A message forwarded while the scope was parking never reached its run:
  # it is owed, so the scope is bound again at once rather than at its key's
  # next message.
  defp rehydrate_if_owed(state, scope) do
    if Replay.owed_count(id(state, scope)) > 0, do: bind(state, scope), else: state
  end

  @doc """
  Rewind `scope`'s journal alone to `to_iter`, as a top-level steps instance's
  rewind does, and bind it again: off the runner first, so the bind restores
  what the marker left.
  """
  @spec rewind(Overwatch.t(), scope(), non_neg_integer()) ::
          {:ok, Overwatch.t()} | {:error, term()}
  def rewind(%Overwatch{} = state, scope, to_iter) do
    scoped = as_scope(state, scope)

    with :ok <- Migration.rewind_journal(scoped, to_iter) do
      if state.runner_id,
        do: ControlUnit.send_control_await(state.runner_id, rem_operator(state, scope))

      {:ok, bind(state, scope)}
    end
  end

  @doc "A sleeping scope's wake deadline came due."
  @spec wake(Overwatch.t(), scope()) :: Overwatch.t()
  def wake(%Overwatch{} = state, scope) do
    state = timer_fired(state, {:scope_wake, scope})

    case status(state, scope) do
      :asleep -> bind(state, scope)
      _awake_or_gone -> state
    end
  end

  @doc """
  `scope` ran to its end and is evicted; `took` is the message the pass that
  ended it abnormally took. A `once` scope stays finished; any other leaves
  nothing behind, so its key's next message starts a new one, and a message
  that reached it after its end, `took` aside, starts that one now.
  """
  @spec completed(Overwatch.t(), scope(), non_neg_integer() | nil) :: Overwatch.t()
  def completed(%Overwatch{} = state, {inner, _key} = scope, took) do
    state = evict(state, scope)

    case scopes(state) |> Map.get(:each, %{}) |> Map.get(inner) do
      :once ->
        remember(state, scope, %{finished: :once})
        set(state, scope, {:finished, :once})

      _ ->
        later = arrived_after_end(state, scope, took)
        forget(id(state, scope))

        Enum.reduce(later, drop(state, scope), fn {_offset, message, metadata}, state ->
          input(state, scope, {message, metadata}, nil)
        end)
    end
  end

  defp forget(scope_id) do
    StateSupervisor.close_writers(scope_id)
    InputLog.forget(scope_id)
    Paths.delete_class_dir(scope_id)
  end

  # What the scope's log holds past what its run took: a clean end released
  # all it took; an abnormal one names the message its last pass took, and a
  # pass a timer or the loop's own handoff started took none.
  defp arrived_after_end(state, scope, took) do
    case InputLog.undelivered(id(state, scope)) do
      owed when is_list(owed) -> Enum.reject(owed, fn {offset, _, _} -> offset == took end)
      {:error, _refused} -> []
    end
  end

  @doc """
  The outer instance was bound again: none of its scopes is on the runner. A
  scope with an open pass, or with input it was never given, is bound again
  at once; every other one sleeps until its key's next message.
  """
  @spec rebound(Overwatch.t()) :: Overwatch.t()
  def rebound(%Overwatch{} = state) do
    state
    |> on_disk()
    |> Enum.reduce(release(state), fn scope, state ->
      case recalled(state, scope) do
        %{finished: :once} ->
          set(state, scope, {:finished, :once})

        fact ->
          if owes_work?(state, scope),
            do: bind(state, scope),
            else: sleep(state, scope, fact[:wake])
      end
    end)
  end

  # What outlives the instance's process about a scope off the runner, in one
  # record beside its journal that moves with it: the incarnation its exit
  # values are named by, a sleeping scope's wake deadline, a finished `once`
  # scope's end. A record that cannot be written or read is logged, and the
  # scope goes on without it.
  defp remember(state, scope, changes) do
    held = recalled(state, scope)
    fact = Map.merge(held, changes)
    write = fn fd -> :file.write(fd, :erlang.term_to_binary(fact)) end

    with true <- fact != held,
         {:error, reason} <- Paths.atomic_install(fact_path(state, scope), write, :scope_fact),
         do: fact_refused(state, scope, "could not be kept", reason)

    :ok
  end

  defp recalled(state, scope) do
    with {:ok, bin} <- File.read(fact_path(state, scope)),
         %{} = fact <- :erlang.binary_to_term(bin, [:safe]) do
      fact
    else
      {:error, :enoent} -> %{}
      refused -> fact_refused(state, scope, "is unreadable", refused)
    end
  rescue
    ArgumentError -> fact_refused(state, scope, "is unreadable", :not_a_term)
  end

  defp fact_refused(state, scope, what, reason) do
    Logger.warning(
      "[Overwatch] #{state.id}: the record of scope #{inspect(scope)} #{what}: " <>
        "#{inspect(reason)}; it goes on without it"
    )

    %{}
  end

  defp fact_path(state, scope), do: Paths.scope_fact_path(Paths.journal_id(id(state, scope)))

  # The scope's incarnation: minted at its first bind, gone with its state.
  defp token(state, scope) do
    case recalled(state, scope) do
      %{token: token} ->
        token

      _none ->
        token = Base.url_encode64(:crypto.strong_rand_bytes(9), padding: false)
        remember(state, scope, %{token: token})
        token
    end
  end

  defp owes_work?(state, scope), do: Replay.owes_work?(id(state, scope))

  # Every scope of an embedding node this instance holds state for.
  # A class that embeds nothing holds no scope, and its directory is not read.
  defp on_disk(state) do
    case factories(state) do
      embedding when embedding == %{} ->
        []

      embedding ->
        Enum.filter(ScopePaths.scopes_of(state.id), fn {inner, _key} ->
          Map.has_key?(embedding, inner)
        end)
    end
  end

  @doc """
  The scopes of each embedding node by what they are doing: awake, asleep,
  and in a pass (an awake one whose journal names a stage of an open
  iteration). Any state an operator's info reads may be given: one with no
  scopes counts none.
  """
  @spec counts(Overwatch.t() | map()) :: %{
          binary() => %{awake: integer(), sleeping: integer(), in_pass: integer()}
        }
  def counts(state) do
    base = Map.new(Map.keys(factories(state)), &{&1, %{awake: 0, sleeping: 0, in_pass: 0}})

    Enum.reduce(Map.get(scopes(state), :status, %{}), base, fn {{inner, _} = scope, status},
                                                               acc ->
      Map.update(
        acc,
        inner,
        %{awake: 0, sleeping: 0, in_pass: 0},
        &count(&1, state, scope, status)
      )
    end)
  end

  defp count(row, state, scope, status) when status in [:awake, :binding] do
    in_pass = if owes_work?(state, scope), do: 1, else: 0
    %{row | awake: row.awake + 1, in_pass: row.in_pass + in_pass}
  end

  defp count(row, _state, _scope, :asleep), do: %{row | sleeping: row.sleeping + 1}
  defp count(row, _state, _scope, _finished), do: row

  # The scope as the instance-keyed machinery reads an instance: its id in the
  # instance's place, a durable steps run, nothing to correct.
  defp as_scope(%Overwatch{builder: builder} = state, scope) do
    %Overwatch{
      state
      | id: id(state, scope),
        builder: %{builder | opts: Keyword.delete(builder.opts, :operator_class)},
        durable_steps: true,
        correction_window_ms: nil
    }
  end

  defp evict(%Overwatch{runner_id: nil} = state, _scope), do: state

  # The eviction runs in a task; a bind of the same scope waits for it, so its
  # `RemOperator` never lands after the bind's `AddOperator`.
  defp evict(%Overwatch{} = state, scope) do
    command = rem_operator(state, scope)
    runner_id = state.runner_id
    {:ok, task} = Task.start(fn -> ControlUnit.send_control_await(runner_id, command) end)

    evicting =
      for {other, pid} <- Map.get(scopes(state), :evicting, %{}),
          Process.alive?(pid),
          into: %{},
          do: {other, pid}

    put(state, :evicting, Map.put(evicting, scope, task))
  end

  defp after_eviction(nil), do: :ok

  defp after_eviction(task) do
    ref = Process.monitor(task)

    receive do
      {:DOWN, ^ref, :process, _task, _reason} -> :ok
    end
  end

  defp rem_operator(state, scope),
    do: {:rem_operator, %RunnerCommands.RemOperator{operator_id: state.id, scope: wire(scope)}}

  # A finished `once` scope takes nothing more; a new scope counts against the
  # namespace's operator quota.
  defp admitted({:finished, :once}, _state),
    do: {:refused, "ran once and finished; it takes no further message"}

  defp admitted(:absent, state) do
    case Quotas.check_allocation(state.namespace, :max_operators) do
      :ok -> :absent
      {:error, refusal} -> {:refused, "would exceed the namespace's quota: #{inspect(refusal)}"}
    end
  end

  defp admitted(status, _state), do: status

  defp dead_letter(state, {inner, key} = scope, channel, payload, why) do
    fields = %{
      operator_class: id(state, scope),
      tasklet_id: inner,
      cell_key: key || "",
      channel: channel,
      error: "scope #{inspect(id(state, scope))} #{why}",
      attempts: 0,
      payload: payload
    }

    with {:ok, entry} <- Entry.new(fields), do: Store.append_async(entry, fn _ref -> :ok end)
  end

  # The live Overwatch, which a bind running in a bind task reports to.
  defp owner_pid(state) do
    case Lookup.overwatch(Overwatch.operator_class(state)) do
      {:ok, pid} -> pid
      {:error, _reason} -> self()
    end
  end

  defp factories(state), do: Map.get(Map.get(state, :settings) || %{}, :scopes, %{})
  defp scopes(state), do: Map.get(state, :scopes) || %{}
  defp reqids(state), do: Map.get(scopes(state), :reqids, :gen_server.reqids_new())

  defp put(%Overwatch{} = state, key, value),
    do: %Overwatch{state | scopes: Map.put(scopes(state), key, value)}

  defp status(state, scope), do: scopes(state) |> Map.get(:status, %{}) |> Map.get(scope, :absent)

  # A scope counts against the namespace's operator quota from the moment the
  # outer instance knows it until it is forgotten or the instance stops.
  defp set(state, scope, status) do
    statuses = Map.get(scopes(state), :status, %{})

    unless Map.has_key?(statuses, scope),
      do: Quotas.record_allocation(state.namespace, :max_operators)

    put(state, :status, Map.put(statuses, scope, status))
  end

  defp drop(state, scope) do
    statuses = Map.get(scopes(state), :status, %{})

    if Map.has_key?(statuses, scope),
      do: Quotas.record_deallocation(state.namespace, :max_operators)

    put(state, :status, Map.delete(statuses, scope))
  end

  @doc """
  Every scope this instance knows stops counting against the namespace's
  operator quota: at a rebind, and when its Overwatch exits (`Migration`'s
  `:scope_quota` step).
  """
  @spec release(Overwatch.t()) :: Overwatch.t()
  def release(%Overwatch{} = state) do
    known = map_size(Map.get(scopes(state), :status, %{}))
    if known > 0, do: Quotas.record_deallocation(state.namespace, :max_operators, known)
    put(state, :status, %{})
  end
end

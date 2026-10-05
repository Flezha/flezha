defmodule Flezha.Service.RelayManager.Spawn do
  @moduledoc """
  Spawn-or-deliver: the controller half of a tasklet's `ctx.spawn_steps`
  and of a relay's call, and the one path either takes to start or reach
  an instance by id.

  Concurrent requests for one id race-resolve at the registry: whoever loses
  the uniqueness check converts to the same delivery a later request would
  get, so exactly one instance exists and every payload lands exactly once.
  """

  alias Flezha.AuthZ.Verb
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Relay.Builder, as: RelayBuilder
  alias Flezha.Service.RelayManager
  alias Flezha.State.Completion
  alias Flezha.State.InputLog

  @typedoc """
  How a request resolved: `:spawned` or `:delivered`, or the refusal that
  stopped it, carrying the code a `flz relay start` would get. A call's
  delivery the callee could not take yet is `{:retry, reason}`.
  """
  @type outcome :: :spawned | :delivered | {:refused, Flezha.Error.t()} | {:retry, term()}

  @typedoc """
  What a call adds to a request: the runs it may start (`ctx.spawn_steps`
  starts steps only), whether an absent id is started or refused
  `relay.not_found`, the metadata the seed and the delivery carry, and the
  relay call itself, which the callee's `:call` entry point reads its token
  from and which never rides metadata.
  """
  @type opts :: [
          runs: [:steps | :flow],
          if_absent: :spawn | :not_found,
          metadata: map(),
          call: map() | nil
        ]

  @doc """
  Spawn-or-deliver `instance_id` (see `Flezha.Service.RelayManager.spawn_or_deliver/8`).

  With a `:call` option the delivery goes through the instance's `:call` entry
  point, which answers its verdict, and the seed carries the call so the new
  instance's first input answers its caller.
  """
  @spec spawn_or_deliver(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          :deliver | :error,
          Flezha.AuthZ.caller_context(),
          String.t(),
          opts()
        ) :: {outcome(), String.t()}
  def spawn_or_deliver(
        namespace,
        instance_id,
        factory_id,
        payload,
        if_running,
        caller_context,
        identity \\ "",
        opts \\ []
      ) do
    case earlier_answer(namespace, instance_id, factory_id, identity) do
      {:answered, answer} ->
        answer

      {:ask, id, repeat} ->
        request = %{
          payload: payload,
          if_running: repeat || if_running,
          identity: identity,
          runs: Keyword.get(opts, :runs, [:steps]),
          if_absent: Keyword.get(opts, :if_absent, :spawn),
          metadata: Keyword.get(opts, :metadata, %{}),
          call: Keyword.get(opts, :call)
        }

        spawn_or_resolve(namespace, id, factory_id, request, caller_context)
    end
  end

  @doc """
  Deliver a call's request to the instance or partition group `target` names
  (a group's member by `key`), judged as `caller_context`'s `spawn_deliver`,
  through its `:call` entry point.
  """
  @spec deliver_to(
          String.t(),
          String.t(),
          String.t() | nil,
          binary(),
          map(),
          map(),
          Flezha.AuthZ.caller_context()
        ) :: {outcome(), String.t()}
  def deliver_to(namespace, target, key, payload, metadata, call, caller_context) do
    case RelayManager.resolve_member(namespace, target, key) do
      {:ok, member} ->
        request = %{payload: payload, identity: "", metadata: metadata, call: call}
        deliver(namespace, member, request, caller_context)

      {:error, reason} ->
        {refused(reason), target}
    end
  end

  # What a run of the same invocation already got, for a tasklet's request: a
  # generated id is the identity's, so a repeat finds its own instance, and a
  # named instance keeps the identity with the payload it took. Otherwise the
  # id to ask for, and `:repeat` when a generated id's start may find it taken.
  defp earlier_answer(_namespace, instance_id, _factory_id, ""), do: {:ask, instance_id, nil}

  defp earlier_answer(namespace, "", factory_id, identity) do
    id = Identificator.mint(:relay, factory_id, seed: identity)

    if RelayManager.exist?(namespace, id) or Completion.exists?(id),
      do: {:answered, {:spawned, id}},
      else: {:ask, id, :repeat}
  end

  defp earlier_answer(_namespace, instance_id, _factory_id, identity) do
    case InputLog.msg_offset(instance_id, identity) do
      0 -> {:answered, {:spawned, instance_id}}
      offset when is_integer(offset) -> {:answered, {:delivered, instance_id}}
      nil -> {:ask, instance_id, nil}
    end
  end

  defp spawn_or_resolve(namespace, instance_id, factory_id, request, caller_context) do
    cond do
      instance_id != "" and RelayManager.exist?(namespace, instance_id) ->
        resolve_running(namespace, instance_id, request, caller_context)

      request.if_absent == :not_found ->
        {refused({:relay_not_found, namespace, instance_id}), instance_id}

      true ->
        spawn_absent(namespace, instance_id, factory_id, request, caller_context)
    end
  end

  # A generated id always starts a steps instance: a flow one nobody can name
  # would be one nothing outside could stop.
  defp spawn_absent(namespace, instance_id, factory_id, request, caller_context) do
    case RelayManager.factory_run(namespace, factory_id) do
      {:error, :no_such_factory} ->
        {refused({:factory_unknown, namespace, factory_id}), instance_id}

      {:ok, :flow} when instance_id != "" ->
        if :flow in request.runs,
          do: spawn_fresh(namespace, instance_id, factory_id, :flow, request, caller_context),
          else: {not_steps(namespace, factory_id), instance_id}

      {:ok, :flow} ->
        {not_steps(namespace, factory_id), instance_id}

      _steps_or_refused ->
        spawn_fresh(namespace, instance_id, factory_id, :steps, request, caller_context)
    end
  end

  defp not_steps(namespace, factory_id),
    do: refused({:factory_run_mismatch, :steps, namespace, factory_id, :flow})

  defp spawn_fresh(namespace, instance_id, factory_id, run, request, caller_context) do
    requested = if instance_id == "", do: nil, else: instance_id
    {:ok, {message, metadata}} = RelayBuilder.seed_input(request.payload, nil)
    seed = {message, stamped(Map.merge(metadata, request.metadata), request)}

    opts =
      [seed_input: seed] ++
        if(request.call, do: [seed_call: %{request.call | created: true}], else: [])

    start = fn _call ->
      namespace
      |> RelayManager.start_relay(%RelayBuilder{
        id: requested,
        factory_id: factory_id,
        run: run,
        init_values: %{},
        opts: opts
      })
      |> RelayManager.start_answer(requested)
    end

    case Verb.run(:relay_start, caller_context, [namespace, requested], start,
           target: &RelayManager.start_target/2
         ) do
      {:ok, %{id: id, outcome: :started}} ->
        {:spawned, id}

      # Lost the id race: the winner spawned it, so this request becomes the
      # delivery a request arriving a moment later would have made. A drawn
      # id never gets here, because a claim it loses draws another.
      {:ok, %{id: id, outcome: :already_running}} ->
        resolve_running(namespace, id, request, caller_context)

      {:error, error} ->
        {{:refused, error}, instance_id}
    end
  end

  defp refused(reason), do: {:refused, Flezha.Error.from_reason(reason, :"wire.spawn")}

  # `:repeat` is a generated id whose identity already started it.
  defp resolve_running(_namespace, instance_id, %{if_running: :repeat}, _caller_context),
    do: {:spawned, instance_id}

  defp resolve_running(_namespace, instance_id, %{if_running: :error}, _caller_context),
    do: {refused({:id_in_use, instance_id}), instance_id}

  defp resolve_running(namespace, instance_id, %{if_running: :deliver} = request, caller_context),
    do: deliver(namespace, instance_id, request, caller_context)

  defp deliver(namespace, instance_id, request, caller_context) do
    deliver = fn _call -> deliver_payload(namespace, instance_id, request) end

    case Verb.run(:spawn_deliver, caller_context, [namespace, instance_id], deliver) do
      {:retry, reason} -> {{:retry, reason}, instance_id}
      {:error, error} -> {{:refused, error}, instance_id}
      _delivered_or_absorbed -> {:delivered, instance_id}
    end
  end

  # A tasklet's payload is sent as any input is; a call's goes through the
  # callee's `:call` entry point, which answers whether it took it. A callee
  # that cannot take it yet is `{:retry, reason}`, which the authorization
  # seam passes through rather than converting into a refusal.
  defp deliver_payload(namespace, instance_id, %{call: nil} = request) do
    RelayManager.send_io_confirming(
      instance_id,
      namespace,
      request.payload,
      stamped(request.metadata, request)
    )
  end

  defp deliver_payload(namespace, instance_id, request) do
    with {:ok, pid} <- RelayManager.get_relay_pid(namespace, instance_id),
         verdict <-
           GenServer.call(
             pid,
             {:call, request.payload, request.metadata, %{request.call | created: false}},
             Durations.get(:overwatch_call_timeout_ms)
           ) do
      case verdict do
        {:error, %Flezha.Error{}} -> verdict
        {:error, reason} -> {:retry, reason}
        taken -> taken
      end
    else
      {:error, reason} -> {:retry, reason}
    end
  catch
    :exit, reason -> {:retry, {:unreachable, reason}}
  end

  # A tasklet's payload carries its identity as its message id, which is what
  # lets the instance's input log absorb a repeat.
  defp stamped(metadata, %{identity: ""}), do: metadata
  defp stamped(metadata, %{identity: identity}), do: Map.put(metadata, "flz.msg_id", identity)
end

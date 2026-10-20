defmodule Flezha.Service.Calls.Router do
  @moduledoc """
  The process registered as `Flezha.Process.CallDelivery`: it keys each
  request by its caller and target and each answer by its callee and caller,
  hands it to that key's delivery worker, and stops a worker that reports
  itself idle and still holds nothing.

  A request is delivered through `RelayManager.spawn_or_deliver/8` for a
  factory and through the target's `:call` entry point for an instance or a
  group, judged as the caller namespace's runner identity. The callee's
  verdict is reported to the caller: taken (the callee's id and whether the
  call started it), or refused, which ends the call as an error `Response`. A
  callee that cannot take it yet is retried until the call's deadline, when
  the request leaves the queue and the caller's own deadline answers it.

  An answer is delivered to the caller's Overwatch as `{:call_answer, id,
  outcome}`, retried until the caller takes it or the call's deadline plus
  `call_retention_margin_ms` has passed, when it is dropped as `caller_gone`
  and its outbox hold answered; a caller its namespace no longer declares is
  gone at once. The worker is the answer's one retry owner: an answer worker
  that dies other than by an orderly stop is reported to the callee's live
  Cell writers, which dispatch its caller's held answers again, and a router
  that starts reports to every live Cell writer, since its workers went with
  the router before it.
  """

  use GenServer

  alias Flezha.Config.Durations
  alias Flezha.Relay.Calls.{Inbound, Outbound}
  alias Flezha.Plugin.Provider.RelayForward.ForwardWorker
  alias Flezha.Process.CallDelivery
  alias Flezha.Process.Watch
  alias Flezha.Runner.FleetManager.Session
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Spawn
  alias Flezha.State.{Outbox, WalWriter, WriterRegistry}

  @doc "Start the router; `:supervisor` names the supervisor its workers run under."
  def start_link(opts) do
    supervisor = Keyword.fetch!(opts, :supervisor)
    GenServer.start_link(__MODULE__, supervisor, name: CallDelivery.name())
  end

  @impl true
  # `workers` finds a key's worker; `watch` names each worker's key.
  def init(supervisor) do
    redrive_every_answer()
    {:ok, %{workers: %{}, watch: Watch.new(), supervisor: supervisor}}
  end

  # The controller starts the router before its state side: at boot no
  # registry, and no Cell writer, exists yet.
  defp redrive_every_answer() do
    if Process.whereis(WriterRegistry) do
      for {_cell, writer} <- WriterRegistry.cell_writers(),
          do: WalWriter.redrive_answers(writer, :all)
    end
  end

  @impl true
  def handle_cast({:request, namespace, caller, request}, state) do
    key = {:request, namespace, caller, route(request)}
    {:noreply, hand(state, key, {request, %{}}, fn -> request_worker(namespace, caller) end)}
  end

  def handle_cast({:answer, namespace, callee, call, outcome, metadata}, state) do
    key = {:answer, namespace, callee, call.caller}
    item = {{call, outcome}, metadata}
    {:noreply, hand(state, key, item, fn -> answer_worker(namespace, callee, call.caller) end)}
  end

  @impl true
  # A worker handed a payload after it reported idle is busy again and stays.
  def handle_info({:worker_idle, pid}, state) do
    with :ok <- ForwardWorker.stop_if_idle(pid),
         {[key], watch} <- Watch.forget(state.watch, pid) do
      {:noreply, %{state | watch: watch, workers: Map.delete(state.workers, key)}}
    else
      _busy_or_unknown -> {:noreply, state}
    end
  catch
    :exit, _gone -> {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Watch.down(state.watch, ref) do
      {[key], watch} ->
        lost(key, reason)
        {:noreply, %{state | watch: watch, workers: Map.delete(state.workers, key)}}

      {_unknown, watch} ->
        {:noreply, %{state | watch: watch}}
    end
  end

  # The answers a crashed worker held go again from the callee's outbox; a
  # request it held ends at its call's deadline.
  defp lost(_key, reason) when reason in [:normal, :shutdown], do: :ok
  defp lost(_key, {:shutdown, _reason}), do: :ok

  defp lost({:answer, _namespace, callee, caller}, _reason),
    do: Enum.each(WriterRegistry.live_writers(callee), &WalWriter.redrive_answers(&1, caller))

  defp lost({:request, _namespace, _caller, _route}, _reason), do: :ok

  # Requests to one target keep one order whatever their key, except a
  # factory's, where the key names the instance.
  defp route(%{target: "factory:" <> _ = target, key: key}) when is_binary(key), do: {target, key}
  defp route(%{target: target}), do: target

  defp hand(state, key, item, opts) do
    {pid, state} = worker(state, key, opts)
    :ok = ForwardWorker.absorb(pid, item)
    state
  end

  defp worker(state, key, opts) do
    case Map.fetch(state.workers, key) do
      {:ok, pid} ->
        {pid, state}

      :error ->
        spec =
          Supervisor.child_spec({ForwardWorker, Map.put(opts.(), :owner, self())},
            restart: :temporary
          )

        {:ok, pid} = DynamicSupervisor.start_child(state.supervisor, spec)

        {pid,
         %{
           state
           | workers: Map.put(state.workers, key, pid),
             watch: Watch.watch(state.watch, pid, key)
         }}
    end
  end

  defp worker_opts(namespace, source, target) do
    %{
      plugin_id: "call",
      source_namespace: namespace,
      source_relay_id: source,
      target_relay_id: target,
      # Bounded upstream: requests by the caller's outbox window and their
      # deadlines, answers by the callee's `call_pending_max`.
      queue_max: :infinity,
      retry_backoff_ms: 50,
      retry_max_backoff_ms: 2_000,
      idle_ms: Durations.get(:call_worker_idle_ms)
    }
  end

  defp request_worker(namespace, caller) do
    namespace
    |> worker_opts(caller, "call")
    |> Map.merge(%{
      confirm: false,
      deliver: fn request, _metadata -> deliver_request(namespace, caller, request) end,
      expires_at: fn request, _metadata -> request.deadline_at end
    })
  end

  defp answer_worker(namespace, callee, caller) do
    namespace
    |> worker_opts(callee, caller)
    |> Map.merge(%{
      confirm: false,
      deliver: fn {call, outcome}, metadata ->
        namespace |> deliver_answer(caller, call.call, outcome) |> answered(metadata)
      end,
      expires_at: fn {call, _outcome}, _metadata -> Inbound.retained_until(call.deadline_at) end,
      on_expire: fn {_call, _outcome}, metadata ->
        answered(caller_gone(namespace, callee, caller), metadata)
      end
    })
  end

  # The answer's hold is a delivery hold, answered once the caller took the
  # answer or is gone; a verdict the worker retries answers nothing.
  defp answered({:error, _reason} = verdict, _metadata), do: verdict

  defp answered(verdict, metadata) do
    Outbox.confirm_delivery(metadata)
    verdict
  end

  defp deliver_request(namespace, caller, request) do
    context = Session.runner_caller_context(namespace)

    case reach(namespace, caller, request, context) do
      {outcome, callee} when outcome in [:spawned, :delivered] ->
        report_taken(namespace, caller, request.id, callee, outcome == :spawned)

      {{:refused, error}, callee} ->
        called = if callee == "", do: request, else: %{request | callee: callee}
        deliver_answer(namespace, caller, request.id, Outbound.own_outcome(called, error))

      {{:retry, reason}, _callee} ->
        {:error, reason}
    end
  end

  defp reach(namespace, caller, %{target: "id:" <> target} = request, context) do
    call = call_of(caller, request, nil)

    Spawn.deliver_to(
      namespace,
      target,
      request.key,
      request.request,
      metadata(request),
      call,
      context
    )
  end

  defp reach(namespace, caller, %{target: "factory:" <> factory} = request, context) do
    RelayManager.spawn_or_deliver(
      namespace,
      request.key || "",
      factory,
      request.request,
      :deliver,
      context,
      request.id,
      runs: if(request.key, do: [:steps, :flow], else: [:steps]),
      if_absent: if(request.create, do: :spawn, else: :not_found),
      metadata: metadata(request),
      call: call_of(caller, request, factory)
    )
  end

  defp call_of(caller, request, factory),
    do: %{
      caller: caller,
      call: request.id,
      deadline_at: request.deadline_at,
      answer_channel: Map.get(request, :answer_channel),
      created: false,
      factory: factory
    }

  defp metadata(%{channel: channel}), do: RelayManager.channel_metadata(channel)

  defp report_taken(namespace, caller, id, callee, created) do
    with {:ok, pid} <- RelayManager.get_relay_pid(namespace, caller),
         do: GenServer.cast(pid, {:call_accepted, id, callee, created})

    :ok
  end

  defp deliver_answer(namespace, caller, id, outcome) do
    case RelayManager.get_relay_pid(namespace, caller) do
      {:ok, pid} ->
        case RelayManager.call_overwatch(pid, {:call_answer, id, outcome}) do
          {:ok, verdict} -> verdict
          {:error, reason} -> {:error, {:unreachable, reason}}
        end

      {:error, {:relay_not_found, _namespace, _id}} = absent ->
        if gone?(namespace, caller),
          do: caller_gone(namespace, outcome.target, caller),
          else: absent

      unreachable ->
        unreachable
    end
  end

  # Only a declared set read whole and lacking the caller means it is gone: a
  # caller still declared is asleep, moving or restarting, and a hub that
  # cannot be read decides nothing.
  defp gone?(namespace, caller) do
    case HubManager.declared_relays(namespace, :unreadable) do
      :unreadable -> false
      declared -> not Enum.any?(declared, &(&1.id == caller))
    end
  end

  defp caller_gone(namespace, callee, caller) do
    Inbound.answer_dropped(%{namespace: namespace, relay_id: callee}, "caller_gone", %{
      caller: caller
    })

    {:ok, :absorbed}
  end
end

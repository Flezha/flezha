defmodule Flezha.Plugin.Provider.RelayForward.ForwardWorker do
  @moduledoc """
  Per-`{plugin_id, target_relay_id}` delivery worker: buffers payloads the
  source relay handed over and delivers each into the target's durable
  input stream, retrying until the target confirms. That confirming reply also
  answers the outbox hold a delivery-guaranteed payload carries.

  Delivery runs in a short-lived monitored process, never in this worker's own
  `handle_call`, which is what makes a cycle of relays forwarding to each
  other safe: no Overwatch ever waits on another.

  Order is preserved — one payload in flight, a retryable verdict re-arms the
  same head. Nothing is dropped; enqueue callers park past `:queue_max` and the
  backpressure propagates into the source Overwatch.

  A relay's call delivery (`Flezha.Service.Calls`) runs the same worker
  with four options of its own: `:deliver`, the function that delivers one
  payload in place of an ingest into the target; `:expires_at`, a payload's
  wall-clock millisecond past which it leaves the queue undelivered, handed to
  `:on_expire`; `confirm: false` for a payload whose outbox hold is answered
  elsewhere; and `:idle_ms`, after which a worker with an empty queue tells
  `:owner` it is idle.
  """

  alias Flezha.Config.Backoff
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.RelayManager
  alias Flezha.State.Outbox

  # `:transient` so disabling the plugin stops the worker for good: the host
  # kills the owned pid with `:shutdown`, which a permanent child would simply
  # be restarted from.
  use GenServer, restart: :transient
  use Flezha.Process.Timer, keys: [:retry, :idle]

  require Logger

  defstruct [
    :source_namespace,
    :target_relay_id,
    :queue_max,
    :retry_backoff_ms,
    :retry_max_backoff_ms,
    :call_timeout_ms,
    # Telemetry metadata, constant for the worker's lifetime — built once here
    # rather than per payload.
    :labels,
    :inflight,
    :deliver,
    :expires_at,
    :on_expire,
    :idle_ms,
    :owner,
    confirm: true,
    timers: %{},
    buffer: :queue.new(),
    buffer_len: 0,
    waiters: [],
    attempts: 0
  ]

  @doc "Started by the extension table at activation, one per target member."
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts),
    do: GenServer.start_link(__MODULE__, opts, Enum.to_list(Map.take(opts, [:name])))

  @doc """
  Enqueue one payload for delivery. Replies `:ok` immediately while the buffer
  is under `:queue_max`; past it the caller parks until a delivery frees space —
  delay, never loss.
  """
  @spec enqueue(GenServer.server(), binary(), map()) :: :ok
  def enqueue(server, message, metadata) when is_map(metadata) do
    GenServer.call(server, {:enqueue, message, metadata}, :infinity)
  end

  @doc """
  Take one payload another worker is handing over, without ever parking the
  caller.

  The hand-over moves payloads between two workers of the SAME plugin, so total
  buffered volume is unchanged and the `:queue_max` back-pressure this would
  otherwise trip is measuring the wrong thing — worse, the caller is the
  Overwatch re-running `init_provider/1`, which must not block. The queue may
  sit briefly above `:queue_max`; the next delivery brings it back under.
  """
  @spec absorb(GenServer.server(), {binary(), map()}) :: :ok
  def absorb(server, {message, metadata}) when is_map(metadata) do
    GenServer.call(server, {:absorb, message, metadata}, 5_000)
  end

  @doc """
  Hand back everything this worker still owes and stop it.

  Called when the worker's target is gone — a partition group rescaled
  out from under it — so the payloads can be re-routed to their new owner
  instead of dying with the worker. The in-flight head is included: it is peeked
  rather than popped, and its delivery is against a relay that is gone, so
  a duplicate is the safe direction and the at-least-once contract absorbs it.
  """
  @spec drain(GenServer.server()) :: [{binary(), map()}]
  def drain(server), do: GenServer.call(server, :drain, 5_000)

  @doc """
  Stop if nothing is queued or in flight, else `:busy`: the owner's answer to
  an idle report, which a payload handed after it may have made stale.
  """
  @spec stop_if_idle(GenServer.server()) :: :ok | :busy
  def stop_if_idle(server), do: GenServer.call(server, :stop_if_idle, 5_000)

  @doc "Synchronous counters read (plugin health/stats contract)."
  @spec stats(GenServer.server()) :: map()
  def stats(server), do: GenServer.call(server, :stats, 5_000)

  # Every tunable is read as a declared field: the provider's option rows are
  # the one default table, and a fallback here would be a second copy of it.
  @impl true
  def init(opts) do
    state = %__MODULE__{
      source_namespace: opts.source_namespace,
      target_relay_id: opts.target_relay_id,
      queue_max: opts.queue_max,
      retry_backoff_ms: opts.retry_backoff_ms,
      retry_max_backoff_ms: opts.retry_max_backoff_ms,
      call_timeout_ms: opts.call_timeout_ms,
      labels: %{
        plugin_id: opts.plugin_id,
        namespace: opts.source_namespace,
        source_relay_id: opts.source_relay_id,
        target_relay_id: opts.target_relay_id
      },
      deliver: Map.get(opts, :deliver),
      expires_at: Map.get(opts, :expires_at),
      on_expire: Map.get(opts, :on_expire),
      idle_ms: Map.get(opts, :idle_ms),
      owner: Map.get(opts, :owner),
      confirm: Map.get(opts, :confirm, true)
    }

    {:ok, arm_idle(state)}
  end

  @impl true
  def handle_call({:enqueue, message, metadata}, from, state) do
    state = %{
      state
      | buffer: :queue.in({message, metadata}, state.buffer),
        buffer_len: state.buffer_len + 1
    }

    emit_queue_depth(state.buffer_len, state.labels)
    state = deliver_next(state)

    if ProviderBehaviour.saturated?(state) do
      emit_saturated(state.buffer_len, state.labels)
    end

    ProviderBehaviour.park_or_reply(state, from)
  end

  def handle_call({:absorb, message, metadata}, _from, state) do
    state = %{
      state
      | buffer: :queue.in({message, metadata}, state.buffer),
        buffer_len: state.buffer_len + 1
    }

    emit_queue_depth(state.buffer_len, state.labels)
    {:reply, :ok, deliver_next(state)}
  end

  def handle_call(:drain, _from, state) do
    # Parked enqueue callers are released first: the payloads they were waiting
    # behind are leaving with this reply, and a caller left parked against a
    # stopping worker would wait out its own `:infinity` call.
    Enum.each(state.waiters, &GenServer.reply(&1, :ok))
    {:stop, :normal, :queue.to_list(state.buffer), %{state | waiters: []}}
  end

  def handle_call(:stop_if_idle, _from, %{buffer_len: 0, inflight: nil} = state),
    do: {:stop, :normal, :ok, state}

  def handle_call(:stop_if_idle, _from, state), do: {:reply, :busy, state}

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       buffer_len: state.buffer_len,
       attempts: state.attempts,
       inflight: state.inflight != nil,
       target_relay_id: state.target_relay_id
     }, state}
  end

  @impl true
  def handle_info({:delivery_result, tag, verdict}, %{inflight: {tag, mref}} = state) do
    Process.demonitor(mref, [:flush])
    {:noreply, settle(%{state | inflight: nil}, verdict)}
  end

  # The delivery process died without reporting — indistinguishable from an
  # unreachable target, so it is retryable.
  def handle_info({:DOWN, mref, :process, _pid, reason}, %{inflight: {_tag, mref}} = state) do
    {:noreply, settle(%{state | inflight: nil}, {:error, {:delivery_crashed, reason}})}
  end

  def handle_info(:retry_delivery, state) do
    {:noreply, deliver_next(timer_fired(state, :retry))}
  end

  def handle_info(:idle, state) do
    state = timer_fired(state, :idle)
    if state.buffer_len == 0 and state.owner, do: send(state.owner, {:worker_idle, self()})
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- delivery ----------

  defp deliver_next(%{inflight: inflight} = state) when inflight != nil, do: state
  defp deliver_next(state) when :erlang.map_get(:retry, state.timers) != nil, do: state
  defp deliver_next(%{buffer_len: 0} = state), do: arm_idle(state)

  defp deliver_next(state) do
    # Peek, not pop: the payload stays owned by the queue until the target
    # confirms, so a retryable verdict re-arms this same head.
    {:value, {message, metadata}} = :queue.peek(state.buffer)

    if expired?(state, message, metadata) do
      state |> drop_head() |> deliver_next()
    else
      spawn_delivery(cancel_timer(state, :idle), message, metadata)
    end
  end

  defp spawn_delivery(state, message, metadata) do
    parent = self()
    deliver = state.deliver || send_io(state)
    tag = make_ref()

    pid =
      spawn(fn ->
        verdict =
          try do
            deliver.(message, metadata)
          catch
            :exit, reason -> {:error, {:unreachable, reason}}
          end

        send(parent, {:delivery_result, tag, verdict})
      end)

    %{state | inflight: {tag, Process.monitor(pid)}}
  end

  defp send_io(%{target_relay_id: target, source_namespace: namespace} = state) do
    timeout = state.call_timeout_ms
    &RelayManager.send_io_confirming(target, namespace, &1, &2, timeout)
  end

  defp expired?(%{expires_at: nil}, _message, _metadata), do: false

  defp expired?(state, message, metadata) do
    case state.expires_at.(message, metadata) do
      at when is_integer(at) -> System.system_time(:millisecond) >= at
      nil -> false
    end
  end

  # A payload past its deadline leaves undelivered; its hold is answered so
  # the source's cursor moves on.
  defp drop_head(state) do
    {{message, metadata}, state} = pop_head(state)
    if state.on_expire, do: state.on_expire.(message, metadata)
    state
  end

  # The head leaves the queue for good, its outbox hold answered: delivered,
  # or given up at its deadline.
  defp pop_head(state) do
    {{:value, {_message, metadata} = head}, rest} = :queue.out(state.buffer)
    if state.confirm, do: Outbox.confirm(metadata)

    state = %{
      state
      | buffer: rest,
        buffer_len: state.buffer_len - 1,
        attempts: Backoff.reset(:forward_deliver, state.target_relay_id)
    }

    {head, ProviderBehaviour.release_waiters(state)}
  end

  defp arm_idle(%{idle_ms: ms, buffer_len: 0, inflight: nil} = state) when is_integer(ms),
    do: arm_timer(state, :idle, ms, :idle)

  defp arm_idle(state), do: state

  # Terminal: the receiver has decided the payload needs no further delivery.
  # The target's confirming ingest is the delivery confirmation: it answers
  # the outbox hold this payload was handed over with.
  defp settle(state, verdict) when verdict in [:ok, {:ok, :absorbed}, {:ok, :suspended}] do
    {_delivered, state} = pop_head(state)
    emit_delivered(verdict_tag(verdict), state.labels)
    deliver_next(state)
  end

  defp settle(state, {:error, reason}) do
    attempts = state.attempts + 1

    backoff =
      Backoff.arm(:forward_deliver, attempts, state.target_relay_id,
        base: state.retry_backoff_ms,
        cap: state.retry_max_backoff_ms
      )

    emit_retry(attempts, backoff, reason, state.labels)

    if attempts == 1 do
      Logger.warning(
        "[relay-forward] #{state.labels.source_relay_id} -> " <>
          "#{state.target_relay_id} delivery deferred: #{inspect(reason)} " <>
          "(retrying in #{backoff} ms)"
      )
    end

    arm_timer(%{state | attempts: attempts}, :retry, backoff, :retry_delivery)
  end

  defp verdict_tag(:ok), do: :delivered
  defp verdict_tag({:ok, tag}), do: tag

  # This family's telemetry vocabulary. Every event carries the source
  # relay, the target relay and the plugin id, so a saturating cycle
  # between two relays is visible to a user without parsing log lines.
  # The prefix is derived from the provider's id; nothing here names a root.
  @provider "relay_forward"

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)

  # One payload reached the target; `outcome` is the verdict's terminal tag.
  defp emit_delivered(outcome, labels) do
    emit(:shipped, %{count: 1}, Map.put(labels, :outcome, outcome))
  end

  # A delivery attempt failed and the payload is retained for retry.
  defp emit_retry(attempt, backoff_ms, reason, labels) do
    emit(
      :retried,
      %{attempt: attempt, backoff_ms: backoff_ms},
      Map.put(labels, :reason, reason)
    )
  end

  # Current queue depth, emitted on every enqueue.
  defp emit_queue_depth(depth, labels) do
    emit(:queue_depth, %{depth: depth}, labels)
  end

  # The queue reached `:queue_max` and the enqueueing caller is parked.
  defp emit_saturated(depth, labels) do
    emit(:overflowed, %{depth: depth}, labels)
  end
end

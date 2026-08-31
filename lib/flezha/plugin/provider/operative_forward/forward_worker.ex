defmodule Flezha.Plugin.Provider.OperativeForward.ForwardWorker do
  @moduledoc """
  Per-`{plugin_id, target_operative_id}` delivery worker: buffers payloads the
  source operative handed over and delivers each into the target's durable
  input stream, retrying until the target confirms. That confirming reply also
  answers the outbox hold a delivery-guaranteed payload carries.

  Delivery runs in a short-lived monitored process, never in this worker's own
  `handle_call`, which is what makes a cycle of operatives forwarding to each
  other safe: no Overwatch ever waits on another.

  Order is preserved — one payload in flight, a retryable verdict re-arms the
  same head. Nothing is dropped; enqueue callers park past `:queue_max` and the
  backpressure propagates into the source Overwatch.
  """

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Outbox

  # `:transient` so disabling the plugin stops the worker for good: the host
  # kills the owned pid with `:shutdown`, which a permanent child would simply
  # be restarted from.
  use GenServer, restart: :transient

  require Logger

  defstruct [
    :source_namespace,
    :target_operative_id,
    :queue_max,
    :retry_backoff_ms,
    :retry_max_backoff_ms,
    :call_timeout_ms,
    # Telemetry metadata, constant for the worker's lifetime — built once here
    # rather than per payload.
    :labels,
    :inflight,
    :retry_timer,
    buffer: :queue.new(),
    buffer_len: 0,
    waiters: [],
    attempts: 0
  ]

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

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
  rather than popped, and its delivery is against an operative that is gone, so
  a duplicate is the safe direction and the at-least-once contract absorbs it.
  """
  @spec drain(GenServer.server()) :: [{binary(), map()}]
  def drain(server), do: GenServer.call(server, :drain, 5_000)

  @doc "Synchronous counters read (plugin health/stats contract)."
  @spec stats(GenServer.server()) :: map()
  def stats(server), do: GenServer.call(server, :stats, 5_000)

  # Every tunable is `fetch!`ed: `OperativeForward.init_provider/1` is the only
  # constructor and it applies the defaults, so the provider struct stays the
  # single default table instead of a second copy drifting here.
  @impl true
  def init(opts) do
    state = %__MODULE__{
      source_namespace: Keyword.fetch!(opts, :source_namespace),
      target_operative_id: Keyword.fetch!(opts, :target_operative_id),
      queue_max: Keyword.fetch!(opts, :queue_max),
      retry_backoff_ms: Keyword.fetch!(opts, :retry_backoff_ms),
      retry_max_backoff_ms: Keyword.fetch!(opts, :retry_max_backoff_ms),
      call_timeout_ms: Keyword.fetch!(opts, :call_timeout_ms),
      labels: %{
        plugin_id: Keyword.fetch!(opts, :plugin_id),
        namespace: Keyword.fetch!(opts, :source_namespace),
        source_operative_id: Keyword.fetch!(opts, :source_operative_id),
        target_operative_id: Keyword.fetch!(opts, :target_operative_id)
      }
    }

    {:ok, state}
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

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       buffer_len: state.buffer_len,
       attempts: state.attempts,
       inflight: state.inflight != nil,
       target_operative_id: state.target_operative_id
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
    {:noreply, deliver_next(%{state | retry_timer: nil})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- delivery ----------

  defp deliver_next(%{inflight: inflight} = state) when inflight != nil, do: state
  defp deliver_next(%{retry_timer: timer} = state) when timer != nil, do: state
  defp deliver_next(%{buffer_len: 0} = state), do: state

  defp deliver_next(state) do
    # Peek, not pop: the payload stays owned by the queue until the target
    # confirms, so a retryable verdict re-arms this same head.
    {:value, {message, metadata}} = :queue.peek(state.buffer)

    parent = self()
    target = state.target_operative_id
    namespace = state.source_namespace
    timeout = state.call_timeout_ms
    tag = make_ref()

    pid =
      spawn(fn ->
        verdict =
          try do
            OperativeManager.send_io_confirming(target, namespace, message, metadata, timeout)
          catch
            :exit, reason -> {:error, {:unreachable, reason}}
          end

        send(parent, {:delivery_result, tag, verdict})
      end)

    %{state | inflight: {tag, Process.monitor(pid)}}
  end

  # Terminal: the receiver has decided the payload needs no further delivery.
  defp settle(state, verdict) when verdict in [:ok, {:ok, :absorbed}, {:ok, :suspended}] do
    {delivered, rest} = :queue.out(state.buffer)

    # The target's confirming ingest is the delivery confirmation: it answers
    # the outbox hold this payload was handed over with.
    with {:value, {_message, metadata}} <- delivered,
         do: Outbox.confirm(metadata)

    state = %{
      state
      | buffer: rest,
        buffer_len: state.buffer_len - 1,
        attempts: 0
    }

    emit_delivered(verdict_tag(verdict), state.labels)

    state
    |> ProviderBehaviour.release_waiters()
    |> deliver_next()
  end

  defp settle(state, {:error, reason}) do
    attempts = state.attempts + 1

    backoff =
      ProviderBehaviour.retry_backoff_ms(
        state.retry_backoff_ms,
        state.retry_max_backoff_ms,
        attempts
      )

    emit_retry(attempts, backoff, reason, state.labels)

    if attempts == 1 do
      Logger.warning(
        "[operative-forward] #{state.labels.source_operative_id} -> " <>
          "#{state.target_operative_id} delivery deferred: #{inspect(reason)} " <>
          "(retrying in #{backoff} ms)"
      )
    end

    %{
      state
      | attempts: attempts,
        retry_timer: Process.send_after(self(), :retry_delivery, backoff)
    }
  end

  defp verdict_tag(:ok), do: :delivered
  defp verdict_tag({:ok, tag}), do: tag

  # This family's telemetry vocabulary. Every event carries the source
  # operative, the target operative and the plugin id, so a saturating cycle
  # between two operatives is visible to an operator without parsing log lines.
  @prefix [:flezha, :plugin, :operative_forward]

  # One payload reached the target; `outcome` is the verdict's terminal tag.
  defp emit_delivered(outcome, labels) do
    :telemetry.execute(@prefix ++ [:delivered], %{count: 1}, Map.put(labels, :outcome, outcome))
  end

  # A delivery attempt failed and the payload is retained for retry.
  defp emit_retry(attempt, backoff_ms, reason, labels) do
    :telemetry.execute(
      @prefix ++ [:retry],
      %{attempt: attempt, backoff_ms: backoff_ms},
      Map.put(labels, :reason, reason)
    )
  end

  # Current queue depth, emitted on every enqueue.
  defp emit_queue_depth(depth, labels) do
    :telemetry.execute(@prefix ++ [:queue_depth], %{depth: depth}, labels)
  end

  # The queue reached `:queue_max` and the enqueueing caller is parked.
  defp emit_saturated(depth, labels) do
    :telemetry.execute(@prefix ++ [:saturated], %{depth: depth}, labels)
  end
end

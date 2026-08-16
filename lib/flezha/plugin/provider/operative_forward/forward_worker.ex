defmodule Flezha.Plugin.Provider.OperativeForward.ForwardWorker do
  @moduledoc """
  Per-`{plugin_id, target_operative_id}` delivery worker for the
  operative-forward plugin: buffers payloads handed over by the source
  operative's `:on_output` / `:on_input` callback and delivers each one into the
  target operative's durable input stream, retrying until the target confirms.

  Delivery runs in a short-lived monitored process, never in the worker's own
  `handle_call`, so the worker always services an enqueue promptly. That is what
  makes a cycle of operatives forwarding to each other safe: no Overwatch ever
  waits on another Overwatch, however long a peer takes to answer.

  Order is preserved — one payload is in flight at a time and a retryable
  verdict re-arms the same head of the queue. An unreachable target delays the
  queue (retry with capped exponential backoff); payloads are never dropped, and
  enqueue callers park once the buffer passes `:queue_max`, propagating
  backpressure into the source Overwatch.
  """

  alias Flezha.Plugin.Provider.OperativeForward.{ForwardSup, Telemetry}
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.OperativeManager

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

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: {:via, Registry, {ForwardSup.registry_name(), ForwardSup.key(opts)}}
    )
  end

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

  Called when the worker's target no longer exists — a partition group rescaled
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

    Telemetry.queue_depth(state.buffer_len, state.labels)
    state = deliver_next(state)

    if ProviderBehaviour.saturated?(state) do
      Telemetry.saturated(state.buffer_len, state.labels)
    end

    ProviderBehaviour.park_or_reply(state, from)
  end

  def handle_call({:absorb, message, metadata}, _from, state) do
    state = %{
      state
      | buffer: :queue.in({message, metadata}, state.buffer),
        buffer_len: state.buffer_len + 1
    }

    Telemetry.queue_depth(state.buffer_len, state.labels)
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
    {_delivered, rest} = :queue.out(state.buffer)

    state = %{
      state
      | buffer: rest,
        buffer_len: state.buffer_len - 1,
        attempts: 0
    }

    Telemetry.delivered(verdict_tag(verdict), state.labels)

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

    Telemetry.retry(attempts, backoff, reason, state.labels)

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
end

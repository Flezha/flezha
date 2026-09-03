defmodule Flezha.Runner.StreamChannel.Gate do
  @moduledoc """
  The channel's send gate and its one-shot bind-waiter set.

  A frame offered while the transport's write buffer is over its high mark is
  held, not discarded: `send_held/3` parks the caller until the driver lowers
  the gate or the hold budget runs out, and only then reports the frame
  undeliverable.

  The bind waiters are one-shot by construction — a waiter registered before
  the stream is bound is woken exactly once, by the turn that publishes the
  ref, so a bind landing between the decision and the record cannot be missed.
  """

  alias Flezha.Transport.LivenessCounters

  @send_gate_hold_ms 5_000

  # The send, and the hold the send gate asks for. `send_frame/4` answers
  # `{:error, :send_buffer_full}` from one relaxed atomic load, on a crossing
  # this caller already pays — the answer is a congestion signal, not a loss,
  # so the frame is offered again once the buffer has drained. Any other error
  # is a driver that is gone: the frame IS lost, and it is reported through the
  # same counter and event an unbound stream uses rather than a second one.
  def send_held(channel, stream_ref, liveness_ref, operative_id, message, metadata) do
    case Flezha.Transport.Stream.send_frame(stream_ref, operative_id, message, metadata) do
      :ok ->
        :ok

      {:error, :send_buffer_full} ->
        LivenessCounters.bump_channel(liveness_ref, :send_gate_held)

        case await_send_gate(channel, send_gate_hold_ms()) do
          :open -> send_held(channel, stream_ref, liveness_ref, operative_id, message, metadata)
          :closed -> undeliverable({nil, liveness_ref}, operative_id)
          :timeout -> {:error, :send_buffer_full}
        end

      {:error, _driver_gone} ->
        undeliverable({nil, liveness_ref}, operative_id)
    end
  end

  # One statement of "this channel could not carry the frame", for the three
  # ways that happens: no binding, a stream that closed under a held sender,
  # and a driver thread that is gone.
  def undeliverable(published, operative_id) do
    if published, do: LivenessCounters.bump_channel(elem(published, 1), :nil_drops)
    :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{operative_id: operative_id})
    {:error, :no_stream}
  end

  # How long a held sender waits for the driver to report the buffer drained.
  # It is a lost-wake fallback, not a pacing dial: the wake arrives on the
  # crossing, and this only bounds a wake that never came.
  def send_gate_hold_ms,
    do: Application.get_env(:flezha, :send_gate_hold_ms, @send_gate_hold_ms)

  # Register interest and wait, in the calling process. Deciding and recording
  # happen in the channel's own turn, so a drain landing between the two cannot
  # be missed; the wait is a selective receive, so everything else the caller
  # is holding stays queued.
  def await_send_gate(channel, timeout_ms) do
    case GenServer.whereis(channel) && GenServer.call(channel, {:await_send_gate, self()}) do
      :held ->
        receive do
          {:send_gate, ^channel, result} -> result
        after
          timeout_ms -> :timeout
        end

      _open_or_no_channel ->
        :open
    end
  end

  @doc """
  Ask this channel to tell `pid` about its next stream bind, as
  `{:stream_bound, name}` under the channel's registered name.

  Answers `:bound` when a stream is already up — the caller has nothing to wait
  for — `:unbound` once the interest is recorded, and `:no_channel` when no such
  channel is running. Deciding and recording happen in the channel process, so a
  bind landing between the two cannot be missed.

  One-shot: every bind notifies the recorded pids and forgets them, so a caller
  that still cares after a later close asks again.
  """
  @spec notify_on_bind(GenServer.name() | nil, pid()) :: :bound | :unbound | :no_channel
  def notify_on_bind(nil, _pid), do: :no_channel

  def notify_on_bind(channel, pid) when is_pid(pid) do
    case GenServer.whereis(channel) do
      nil -> :no_channel
      _ -> GenServer.call(channel, {:notify_on_bind, pid})
    end
  end

  @doc """
  Block the CALLING process until `channel` has a bound stream, at most
  `timeout_ms`.

  Used by the operative bind so nothing downstream of it — the durable restore,
  the journal re-forward, the first ingest queued behind it — is issued onto a
  stream that cannot carry it. The wait is a selective receive, so every other
  message the caller is holding stays queued, which is what makes the caller's
  mailbox the queue an unbound stream needs.

  `:ok` also answers a channel that is not running: an operative whose runner has
  no channel pool has nothing to wait for and its delivery failure is reported by
  `io_in/3` as before.
  """
  @spec await_bind(GenServer.name() | nil, non_neg_integer()) ::
          :ok | {:error, :stream_unbound}

  def await_bind(channel, timeout_ms) do
    case notify_on_bind(channel, self()) do
      :unbound ->
        receive do
          {:stream_bound, ^channel} -> :ok
        after
          timeout_ms -> {:error, :stream_unbound}
        end

      _bound_or_absent ->
        :ok
    end
  end

  # One waiter set per event, so the bind hold and the send-gate hold are one
  # mechanism rather than two that can drift.
  def record_waiter(state, event, pid),
    do: %{state | waiters: Map.update!(state.waiters, event, &MapSet.put(&1, pid))}

  def wake_waiters(state, event, message) do
    Enum.each(state.waiters[event], &send(&1, message))
    %{state | waiters: Map.put(state.waiters, event, MapSet.new())}
  end
end

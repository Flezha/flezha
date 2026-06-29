defmodule Flezha.Executor.StreamChannel do
  alias Flezha.State.Wire, as: StateWire
  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  def start_link({executor_id, name, type}) do
    GenServer.start_link(__MODULE__, {executor_id, type, name}, name: name)
  end

  @io_liveness_interval_ms 10_000
  @io_liveness_threshold_ms 30_000

  @impl true
  def init({executor_id, type, name}) do
    # The executor heartbeats every SECONDARY stream every ~10s; a bound
    # secondary whose last frame (heartbeat or data) is older than the
    # threshold is stalled — a silent data-plane wedge that raises no
    # `{:flz_closed}` write-error. Primary channels are send-only (the
    # executor never writes them), so the check would flag them forever.
    if type == :secondary, do: schedule_io_liveness_check()

    {
      :ok,
      %{
        executor_id: executor_id,
        type: type,
        stream_ref: nil,
        operative_mapping: %{},
        name: name,
        last_seen: System.monotonic_time(:millisecond),
        liveness_ref: LivenessCounters.register_channel(executor_id, name)
      }
    }
  end

  defp schedule_io_liveness_check do
    interval =
      Application.get_env(:flezha, :io_stream_liveness_interval_ms, @io_liveness_interval_ms)

    if interval > 0, do: Process.send_after(self(), :io_liveness_check, interval)
  end

  defp mark_seen(state) do
    LivenessCounters.bump_channel(state[:liveness_ref], :flz_data_received)
    %{state | last_seen: System.monotonic_time(:millisecond)}
  end

  # Count an outbound io_in: one frame received and one send_frame issued, with
  # the send timestamp. A stall shows whether the channel is still being fed
  # (io_in_received) and still calling send_frame (send_frame_calls).
  defp bump_send(state) do
    ref = state[:liveness_ref]
    LivenessCounters.bump_channel(ref, :io_in_received)
    LivenessCounters.bump_channel(ref, :send_frame_calls)
    LivenessCounters.set_channel(ref, :last_send_ms, System.monotonic_time(:millisecond))
  end

  # Count an io_in that hit an unbound stream: fed, but dropped (not sent).
  defp bump_drop(state) do
    ref = state[:liveness_ref]
    LivenessCounters.bump_channel(ref, :io_in_received)
    LivenessCounters.bump_channel(ref, :nil_drops)
  end

  @impl true
  def handle_cast(
        {:io_in, operative_id, {_message, _metadata}},
        %{stream_ref: nil} = state
      ) do
    # Unbound channel: emit `[:flezha, :stream, :drop]` so the seam
    # is observable. The convergence Loop subscribes to `:output_stall`
    # for rebind decisions; drop telemetry remains a useful operator
    # signal independent of that.
    bump_drop(state)
    :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{operative_id: operative_id})
    {:noreply, state}
  end

  @impl true
  def handle_cast(
        {:io_in, operative_id, {message, metadata}},
        %{stream_ref: stream_ref} = state
      ) do
    bump_send(state)
    Native.send_frame(stream_ref, operative_id, message, metadata)
    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:io_in, _operative_id, {_message, _metadata}},
        _from,
        %{stream_ref: nil} = state
      ) do
    bump_drop(state)
    {:reply, {:error, :no_stream}, state}
  end

  @impl true
  def handle_call(
        {:io_in, operative_id, {message, metadata}},
        _from,
        %{stream_ref: stream_ref} = state
      ) do
    bump_send(state)
    Native.send_frame(stream_ref, operative_id, message, metadata)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:operative_pid_update, operative_id, updated_pids}, _from, state) do
    operative_mapping = Map.put(state[:operative_mapping], operative_id, updated_pids)

    {:reply, :ok, Map.put(state, :operative_mapping, operative_mapping)}
  end

  @impl true
  def handle_call({:operative_pid_remove, operative_id}, _from, state) do
    operative_mapping = Map.delete(state[:operative_mapping], operative_id)

    {:reply, :ok, Map.put(state, :operative_mapping, operative_mapping)}
  end

  @impl true
  def handle_call({:operative_bulk_insert, operatives}, _from, state) do
    operative_mapping =
      Enum.reduce(operatives, state.operative_mapping, fn {operative_id, updated_pids}, acc ->
        Map.put(acc, operative_id, updated_pids)
      end)

    {:reply, :ok, Map.put(state, :operative_mapping, operative_mapping)}
  end

  # Probe used by acceptors to find rebindable channels. Returns the current
  # stream_ref (or nil if unbound). Snapshot only — the acceptor must then
  # call {:bind, ref} to atomically claim a nil-ref channel.
  @impl true
  def handle_call(:stream_ref_status, _from, state) do
    {:reply, state.stream_ref, state}
  end

  # Atomic bind. Succeeds only if stream_ref is currently nil. The acceptor
  # uses :already_bound as a signal to try the next candidate, avoiding a
  # snapshot-vs-bind race that could otherwise overwrite a live ref.
  @impl true
  def handle_call({:bind, new_ref}, _from, %{stream_ref: nil} = state) do
    {:reply, :ok, %{state | stream_ref: new_ref, last_seen: System.monotonic_time(:millisecond)}}
  end

  @impl true
  def handle_call({:bind, _new_ref}, _from, state) do
    {:reply, :already_bound, state}
  end

  # Unified NIF transport message — complete parsed frame.
  #
  # Two reserved payload-ids carry persistence-bound data (see
  # `Flezha.State.Wire`). All other payload-ids are operative outputs
  # routed via the per-operative mapping.
  @impl true
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "segment_state_change" do
    handle_segment_state_change(state.executor_id, message)
    {:noreply, mark_seen(state)}
  end

  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "segment_state_snapshot" do
    handle_segment_state_snapshot(state.executor_id, message)
    {:noreply, mark_seen(state)}
  end

  # Dynamic-mode hard-delete: the executor evicted a persistence-enabled
  # Segment. Drop its WAL/snapshot/outbox files so a later recreate
  # cold-starts. Fire-and-forget — no response frame is written back.
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "delete_segment_state" do
    handle_delete_segment_state(message)
    {:noreply, mark_seen(state)}
  end

  # Data-channel heartbeat: the executor emits this every ~10s while connected,
  # so an idle-but-healthy operative keeps the channel live without producing
  # output. Metadata carries the executor's dispatch-progress self-report; the
  # stall classifier compares it across ticks to catch an alive-but-wedged
  # dispatch pool (a state every heartbeat-level signal is blind to).
  def handle_info({:flz_data, _stream_id, "data_heartbeat", _message, metadata}, state) do
    ref = state[:liveness_ref]
    LivenessCounters.bump_channel(ref, :hb_count)

    case Integer.parse(Map.get(metadata, "dispatches", "")) do
      {dispatches, _} -> LivenessCounters.set_channel(ref, :hb_dispatches, dispatches)
      :error -> :ok
    end

    {:noreply, mark_seen(state)}
  end

  def handle_info({:flz_data, _stream_id, operative_id, message, metadata}, state) do
    case Map.get(state.operative_mapping, operative_id, []) do
      [] ->
        :ok

      [pid] ->
        GenServer.cast(pid, {:io_out, message, metadata})

      [first | _] = pids ->
        # Defensive: the HubManager `hook_insert_pid/2` filter should
        # guarantee a single-pid list for any operative, but if a
        # regression slips that through we must NOT fan out frames to
        # every pid — that is the EVIDENCE1 §2 data-plane drain that
        # produced 22 frames/s on unchosen executors. Route to the
        # first pid only and surface the misuse via telemetry.
        Logger.error(
          "[stream_channel] fan-out warning for operative=#{inspect(operative_id)}: " <>
            "expected one pid, got #{length(pids)}: #{inspect(pids)}"
        )

        :telemetry.execute(
          [:flezha, :stream, :fanout_warning],
          %{count: 1, pid_count: length(pids)},
          %{operative_id: operative_id, executor_id: state.executor_id}
        )

        GenServer.cast(first, {:io_out, message, metadata})
    end

    {:noreply, mark_seen(state)}
  end

  # Periodic liveness check: a bound SECONDARY stream whose last frame
  # (heartbeat or data) is older than the threshold has missed several
  # heartbeats and is stalled. Emit `output_stall` for each mapped operative (the
  # signal the convergence Loop rebinds on) — but keep `stream_ref`, since the
  # rebind re-establishes it. Primary streams are send-only and never receive
  # heartbeats, so the check is meaningless there (it flagged a false
  # output_stall every 10s for 21h straight on 2026-06-09/10).
  @impl true
  def handle_info(:io_liveness_check, %{type: type} = state) when type != :secondary do
    {:noreply, state}
  end

  def handle_info(:io_liveness_check, state) do
    schedule_io_liveness_check()

    threshold =
      Application.get_env(:flezha, :io_stream_liveness_threshold_ms, @io_liveness_threshold_ms)

    stale? = System.monotonic_time(:millisecond) - state.last_seen >= threshold

    if not is_nil(state.stream_ref) and stale? do
      Logger.warning(
        "[stream_channel] IO stream silent past liveness threshold, flagging output_stall: " <>
          "executor=#{inspect(state.executor_id)} operatives=#{inspect(Map.keys(state.operative_mapping))}"
      )

      for {operative_id, _} <- state.operative_mapping do
        :telemetry.execute(
          [:flezha, :state, :output_stall],
          %{count: 1},
          %{operative_instance: operative_id}
        )
      end
    end

    {:noreply, state}
  end

  # NIF transport: stream closed. Synchronous rebindability contract — once
  # this clause runs, state.stream_ref is nil and no outbound send may use
  # the dead ref. The acceptor will pick this channel up via :stream_ref_status
  # on the next incoming data stream for the same executor_id.
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    Logger.warning("Connection closed for IO socket.")

    # Emit output_stall immediately for every operative in the mapping so the
    # convergence Loop triggers a rebind tick rather than waiting for the
    # Watchdog's 60s grace period to detect the dead socket.
    for {operative_id, _} <- state.operative_mapping do
      :telemetry.execute(
        [:flezha, :state, :output_stall],
        %{count: 1},
        %{operative_instance: operative_id}
      )
    end

    {:noreply, %{state | stream_ref: nil}}
  end

  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  # Persistence frames are handed to the per-segment WalWriter via a
  # non-blocking cast: the writer appends, drives seq-gap resync, and writes
  # snapshots (an fsync) in its own process, so disk/recovery latency never
  # stalls this channel — which also carries the operative's output frames.
  defp handle_segment_state_change(executor_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{segment_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        segment_id = {rc, sh, pk}

        case Flezha.State.Supervisor.find_or_start(segment_id) do
          {:ok, pid} ->
            Flezha.State.WalWriter.append_async(pid, seq, header, body, executor_id)

          {:error, reason} ->
            Logger.error(
              "[stream] segment_state_change WalWriter start failed " <>
                "segment=#{inspect(segment_id)}: #{inspect(reason)}"
            )
        end

      {:error, reason} ->
        Logger.warning("[stream] segment_state_change decode failed: #{inspect(reason)}")
    end
  end

  # Writing a snapshot fsyncs and truncates the per-segment WAL (a call back
  # into its writer), so run it in a separate process — off this channel's loop
  # and outside the writer (which would self-call).
  defp handle_segment_state_snapshot(_executor_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{segment_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        segment_id = {rc, sh, pk}
        spawn(fn -> write_snapshot(segment_id, seq, header, body) end)

      {:error, reason} ->
        Logger.warning("[stream] segment_state_snapshot decode failed: #{inspect(reason)}")
    end
  end

  # Run the file teardown off this channel's loop: it stops the writer and
  # unlinks files, which we never want to gate the output-carrying channel on.
  defp handle_delete_segment_state(frame) do
    case StateWire.decode_segment_id(frame) do
      {:ok, {rc, sh, pk}} ->
        spawn(fn -> Flezha.State.Supervisor.delete_segment({rc, sh, pk}) end)

      {:error, reason} ->
        Logger.warning("[stream] delete_segment_state decode failed: #{inspect(reason)}")
    end
  end

  defp write_snapshot(segment_id, seq, header, body) do
    Flezha.State.Watchdog.compaction_completed(segment_id)

    case Flezha.State.Snapshot.write_snapshot(segment_id, seq, header, body) do
      {:ok, _path} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "[stream] segment_state_snapshot write failed " <>
            "segment=#{inspect(segment_id)}: #{inspect(reason)}"
        )
    end
  end
end

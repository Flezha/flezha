defmodule Flezha.Executor.StreamChannel do
  alias Flezha.State.Wire, as: StateWire
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  def start_link({executor_id, name, type}) do
    GenServer.start_link(__MODULE__, {executor_id, type, name}, name: name)
  end

  @impl true
  def init({executor_id, _type, name}) do
    # Channel starts unbound. Acceptors atomically attach a NIF stream
    # via the {:bind, ref} call once a connection arrives.
    {
      :ok,
      %{
        executor_id: executor_id,
        stream_ref: nil,
        relay_mapping: %{},
        name: name
      }
    }
  end

  @impl true
  def handle_cast(
        {:io_in, relay_id, {_message, _metadata}},
        %{stream_ref: nil} = state
      ) do
    # Unbound channel: emit `[:flezha, :stream, :drop]` so the seam
    # is observable. The Convergence Health watcher subscribes to
    # `:output_stall` for rebind decisions; drop telemetry remains a
    # useful operator signal independent of that.
    :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{relay_id: relay_id})
    {:noreply, state}
  end

  @impl true
  def handle_cast(
        {:io_in, relay_id, {message, metadata}},
        %{stream_ref: stream_ref} = state
      ) do
    Native.send_frame(stream_ref, relay_id, message, metadata)
    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:io_in, _relay_id, {_message, _metadata}},
        _from,
        %{stream_ref: nil} = state
      ) do
    {:reply, {:error, :no_stream}, state}
  end

  @impl true
  def handle_call(
        {:io_in, relay_id, {message, metadata}},
        _from,
        %{stream_ref: stream_ref} = state
      ) do
    Native.send_frame(stream_ref, relay_id, message, metadata)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:relay_pid_update, relay_id, updated_pids}, _from, state) do
    relay_mapping = Map.put(state[:relay_mapping], relay_id, updated_pids)

    {:reply, :ok, Map.put(state, :relay_mapping, relay_mapping)}
  end

  @impl true
  def handle_call({:relay_bulk_insert, relays}, _from, state) do
    relay_mapping =
      Enum.reduce(relays, state.relay_mapping, fn {relay_id, updated_pids}, acc ->
        Map.put(acc, relay_id, updated_pids)
      end)

    {:reply, :ok, Map.put(state, :relay_mapping, relay_mapping)}
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
    {:reply, :ok, %{state | stream_ref: new_ref}}
  end

  @impl true
  def handle_call({:bind, _new_ref}, _from, state) do
    {:reply, :already_bound, state}
  end

  # Unified NIF transport message — complete parsed frame.
  #
  # Two reserved payload-ids carry persistence-bound data (see
  # `Flezha.State.Wire`). All other payload-ids are relay outputs
  # routed via the per-relay mapping.
  @impl true
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "partition_state_change" do
    handle_partition_state_change(state.executor_id, message)
    {:noreply, state}
  end

  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "partition_state_snapshot" do
    handle_partition_state_snapshot(state.executor_id, message)
    {:noreply, state}
  end

  def handle_info({:flz_data, _stream_id, relay_id, message, metadata}, state) do
    case Map.get(state.relay_mapping, relay_id, []) do
      [] ->
        :ok

      [pid] ->
        GenServer.cast(pid, {:io_out, message, metadata})

      [first | _] = pids ->
        # Defensive: the HubManager `hook_insert_pid/2` filter should
        # guarantee a single-pid list for any relay, but if a
        # regression slips that through we must NOT fan out frames to
        # every pid — that is the EVIDENCE1 §2 data-plane drain that
        # produced 22 frames/s on unchosen executors. Route to the
        # first pid only and surface the misuse via telemetry.
        Logger.error(
          "[stream_channel] fan-out warning for relay=#{inspect(relay_id)}: " <>
            "expected one pid, got #{length(pids)}: #{inspect(pids)}"
        )

        :telemetry.execute(
          [:flezha, :stream, :fanout_warning],
          %{count: 1, pid_count: length(pids)},
          %{relay_id: relay_id, executor_id: state.executor_id}
        )

        GenServer.cast(first, {:io_out, message, metadata})
    end

    {:noreply, state}
  end

  # NIF transport: stream closed. Synchronous rebindability contract — once
  # this clause runs, state.stream_ref is nil and no outbound send may use
  # the dead ref. The acceptor will pick this channel up via :stream_ref_status
  # on the next incoming data stream for the same executor_id.
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    Logger.warning("Connection closed for IO socket.")

    {:noreply, %{state | stream_ref: nil}}
  end

  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_partition_state_change(executor_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{partition_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        partition_id = {rc, sh, pk}

        case Flezha.State.Supervisor.find_or_start(partition_id) do
          {:ok, pid} ->
            # `:ok`/`:duplicate` are terminal; a gap means the SDK is ahead
            # of us, so resync rather than wedge the Partition forever. The
            # `executor_id` lets the WalWriter realign its epoch when a *new*
            # executor (a restart/reroute) resumes from a diverged seq base.
            case Flezha.State.WalWriter.append(pid, seq, header, body, executor_id) do
              {:error, {:gap, expected: expected, got: got}} ->
                recover_seq_gap(executor_id, partition_id, expected, got)

              _ ->
                :ok
            end

          {:error, reason} ->
            Logger.error(
              "[stream] partition_state_change WalWriter start failed " <>
                "partition=#{inspect(partition_id)}: #{inspect(reason)}"
            )
        end

      {:error, reason} ->
        Logger.warning("[stream] partition_state_change decode failed: #{inspect(reason)}")
    end
  end

  defp recover_seq_gap(executor_id, {rc, sh, pk} = partition_id, expected, got) do
    :telemetry.execute(
      [:flezha, :state, :seq_gap],
      %{expected: expected, got: got},
      %{relay: rc, shard: sh, key: pk}
    )

    _ =
      Flezha.State.Recovery.resync(partition_id, executor_id: executor_id, reason: :seq_gap)

    :ok
  end

  defp handle_partition_state_snapshot(_executor_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{partition_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        partition_id = {rc, sh, pk}
        Flezha.State.Watchdog.compaction_completed(partition_id)

        case Flezha.State.Snapshot.write_snapshot(partition_id, seq, header, body) do
          {:ok, _path} ->
            :ok

          {:error, reason} ->
            Logger.error(
              "[stream] partition_state_snapshot write failed " <>
                "partition=#{inspect(partition_id)}: #{inspect(reason)}"
            )
        end

      {:error, reason} ->
        Logger.warning("[stream] partition_state_snapshot decode failed: #{inspect(reason)}")
    end
  end
end

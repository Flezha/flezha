defmodule Flezha.ComputeEngine.StreamChannel do
  alias Flezha.State.Wire, as: StateWire
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  def start_link({compute_engine_id, name, type}) do
    GenServer.start_link(__MODULE__, {compute_engine_id, type, name}, name: name)
  end

  @impl true
  def init({compute_engine_id, _type, name}) do
    # Channel starts unbound. Acceptors atomically attach a NIF stream
    # via the {:bind, ref} call once a connection arrives.
    {
      :ok,
      %{
        compute_engine_id: compute_engine_id,
        stream_ref: nil,
        processor_mapping: %{},
        name: name
      }
    }
  end

  @impl true
  def handle_cast(
        {:io_in, processor_id, {_message, _metadata}},
        %{stream_ref: nil} = state
      ) do
    Logger.debug(
      "[StreamChannel #{inspect(state.name)}] dropping outbound cast for " <>
        "#{processor_id}: stream_ref is nil (awaiting rebind)"
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast(
        {:io_in, processor_id, {message, metadata}},
        %{stream_ref: stream_ref} = state
      ) do
    Native.send_frame(stream_ref, processor_id, message, metadata)
    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:io_in, _processor_id, {_message, _metadata}},
        _from,
        %{stream_ref: nil} = state
      ) do
    {:reply, {:error, :no_stream}, state}
  end

  @impl true
  def handle_call(
        {:io_in, processor_id, {message, metadata}},
        _from,
        %{stream_ref: stream_ref} = state
      ) do
    Native.send_frame(stream_ref, processor_id, message, metadata)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:processor_pid_update, processor_id, updated_pids}, _from, state) do
    processor_mapping = Map.put(state[:processor_mapping], processor_id, updated_pids)

    {:reply, :ok, Map.put(state, :processor_mapping, processor_mapping)}
  end

  @impl true
  def handle_call({:processor_bulk_insert, processors}, _from, state) do
    processor_mapping =
      Enum.reduce(processors, state.processor_mapping, fn {processor_id, updated_pids}, acc ->
        Map.put(acc, processor_id, updated_pids)
      end)

    {:reply, :ok, Map.put(state, :processor_mapping, processor_mapping)}
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
  # `Flezha.State.Wire`). All other payload-ids are processor outputs
  # routed via the per-processor mapping.
  @impl true
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "partition_state_change" do
    handle_partition_state_change(state.compute_engine_id, message)
    {:noreply, state}
  end

  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "partition_state_snapshot" do
    handle_partition_state_snapshot(state.compute_engine_id, message)
    {:noreply, state}
  end

  def handle_info({:flz_data, _stream_id, processor_id, message, metadata}, state) do
    processors = Map.get(state.processor_mapping, processor_id, [])

    Enum.each(processors, fn pid ->
      GenServer.cast(pid, {:io_out, message, metadata})
    end)

    {:noreply, state}
  end

  # NIF transport: stream closed. Synchronous rebindability contract — once
  # this clause runs, state.stream_ref is nil and no outbound send may use
  # the dead ref. The acceptor will pick this channel up via :stream_ref_status
  # on the next incoming data stream for the same compute_engine_id.
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    Logger.warning("Connection closed for IO socket.")

    {:noreply, %{state | stream_ref: nil}}
  end

  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_partition_state_change(compute_engine_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{partition_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        partition_id = {compute_engine_id, rc, sh, pk}

        case Flezha.State.Supervisor.find_or_start(partition_id) do
          {:ok, pid} ->
            # `:duplicate` is silently dropped (dedup on agent outbox).
            _ = Flezha.State.WalWriter.append(pid, seq, header, body)
            :ok

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

  defp handle_partition_state_snapshot(compute_engine_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{partition_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        partition_id = {compute_engine_id, rc, sh, pk}

        case Flezha.State.Snapshot.write_snapshot(partition_id, seq, header, body) do
          {:ok, _path} -> :ok
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

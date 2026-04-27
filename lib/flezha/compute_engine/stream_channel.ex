defmodule Flezha.ComputeEngine.StreamChannel do
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  def start_link({compute_engine_id, name, type}) do
    GenServer.start_link(__MODULE__, {compute_engine_id, type, name}, name: name)
  end

  @impl true
  def init({_compute_engine_id, _type, name}) do
    # Channel starts unbound. Acceptors atomically attach a NIF stream
    # via the {:bind, ref} call once a connection arrives.
    {
      :ok,
      %{
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

  # Unified NIF transport message — complete parsed frame
  @impl true
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
  @impl true
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    Logger.warning("Connection closed for IO socket.")

    {:noreply, %{state | stream_ref: nil}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end
end

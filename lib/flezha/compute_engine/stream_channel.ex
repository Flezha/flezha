defmodule Flezha.ComputeEngine.StreamChannel do
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  def start_link({compute_engine_id, name, type}) do
    GenServer.start_link(__MODULE__, {compute_engine_id, type, name}, name: name)
  end

  @impl true
  def init({_compute_engine_id, _type, name}) do
    # In NIF mode, streams are attached later via {:attach_stream, stream_ref}.
    # No per-stream socket listeners or QUIC stream acceptors needed.
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
        {:io_in, processor_id, {message, metadata}},
        %{stream_ref: stream_ref} = state
      ) do
    send_channel_payload(stream_ref, processor_id, message, metadata)

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:io_in, processor_id, {message, metadata}},
        _from,
        %{stream_ref: stream_ref} = state
      ) do
    send_channel_payload(stream_ref, processor_id, message, metadata)

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

  # Attach a NIF stream ref (replaces both UDS accept and QUIC stream_attached)
  @impl true
  def handle_call({:attach_stream, stream_ref}, _from, state) do
    {:reply, :ok, %{state | stream_ref: stream_ref}}
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

  # NIF transport: stream closed
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

  defp send_channel_payload(nil, _processor_id, _message, _metadata), do: :ok

  defp send_channel_payload(stream_ref, processor_id, message, metadata) do
    Native.send_frame(stream_ref, processor_id, message, metadata)
  end
end

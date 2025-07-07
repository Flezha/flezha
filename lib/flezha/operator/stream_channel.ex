defmodule Flezha.Operator.StreamChannel do
  alias Flezha.Service.Payloader
  alias Flezha.Service.Identificator

  require Logger

  use GenServer

  def start_link({operator_id, name, type}) do
    GenServer.start_link(__MODULE__, {operator_id, type, name}, name: name)
  end

  @impl true
  def init({operator_id, type, name}) do
    lsock = GenServer.call(Identificator.stream_listener(operator_id, type), :get_socket)

    {
      :ok,
      %{
        lsock: lsock,
        sock: nil,
        pipeline_mapping: %{},
        name: name,
        buffer: {:empty, <<>>}
      },
      {:continue, :accept_connection}
    }
  end

  @impl true
  def handle_continue(:accept_connection, state) do
    socket = accept_connection(state.lsock)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_cast(
        {:io_in, pipeline_id, {type, message}, handling_type},
        %{sock: sock} = state
      ) do
    send_channel_payload(sock, pipeline_id, type, handling_type, message)

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:io_in, pipeline_id, {type, message}, handling_type},
        _from,
        %{sock: sock} = state
      ) do
    send_channel_payload(sock, pipeline_id, type, handling_type, message)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:pipeline_pid_update, pipeline_id, updated_pids}, _from, state) do
    pipeline_mapping = Map.put(state[:pipeline_mapping], pipeline_id, updated_pids)

    {:reply, :ok, Map.put(state, :pipeline_mapping, pipeline_mapping)}
  end

  @impl true
  def handle_call({:pipeline_bulk_insert, pipelines}, _from, state) do
    pipeline_mapping =
      Enum.reduce(pipelines, state.pipeline_mapping, fn {pipeline_id, updated_pids}, acc ->
        Map.put(acc, pipeline_id, updated_pids)
      end)

    {:reply, :ok, Map.put(state, :pipeline_mapping, pipeline_mapping)}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning("TCP connection closed for IO socket.")

    socket = accept_connection(state.lsock)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_info({:tcp, _port, frame}, %{buffer: buffer} = state) do
    pipeline_mappings = state.pipeline_mapping

    {payloads, buffer} = Payloader.parse_frame(buffer, frame, [])

    Enum.each(payloads, fn {pipeline_id, virtual_channel, message} ->
      pipelines = Map.get(pipeline_mappings, pipeline_id, [])

      # TODO: Are we handling binary or text data?
      Enum.each(pipelines, fn pid ->
        GenServer.cast(pid, {:io_out, {:text, message}, virtual_channel})
      end)
    end)

    state = Map.put(state, :buffer, buffer)

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp send_channel_payload(sock, pipeline_id, type, handling_type, message) do
    :gen_tcp.send(
      sock,
      Payloader.build_from_bin(
        pipeline_id,
        # TODO: Hardcoded virtual channel.
        1,
        input_type(type) <> handling_type(handling_type) <> message
      )
    )
  end

  defp accept_connection(lsock) do
    {:ok, socket} = :gen_tcp.accept(lsock)

    :inet.setopts(socket, [{:active, true}])
    :gen_tcp.controlling_process(socket, self())

    socket
  end

  defp input_type(type) do
    # Binary is 1, Text is 2, wrapped in a 8-bit integer
    case type do
      :binary -> <<1::size(8)>>
      :text -> <<2::size(8)>>
    end
  end

  defp handling_type(handling_type) do
    # Stream is 1, Task is 2, wrapped in a 8-bit integer
    case handling_type do
      :stream -> <<1::size(8)>>
      :task -> <<2::size(8)>>
    end
  end
end

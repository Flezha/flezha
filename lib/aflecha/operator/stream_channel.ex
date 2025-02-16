defmodule Aflecha.Operator.StreamChannel do
  alias Aflecha.Service.Payloader
  alias Aflecha.Service.Identificator

  require Logger

  use GenServer

  def start_link({operator_id, name, type}) do
    GenServer.start_link(__MODULE__, {operator_id, type}, name: name)
  end

  @impl true
  def init({operator_id, type}) do
    lsock = GenServer.call(Identificator.stream_listener(operator_id, type), :get_socket)

    {
      :ok,
      %{
        lsock: lsock,
        sock: nil,
        worker_mapping: %{}
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
        {:io_in, worker_id, payload, data_type, handling_type},
        %{sock: sock} = state
      ) do
    :gen_tcp.send(
      sock,
      Payloader.build_from_bin(
        worker_id,
        input_type(data_type) <> handling_type(handling_type) <> payload
      )
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:worker_pid_update, worker_id, updated_pids}, state) do
    worker_mapping = Map.put(state[:worker_mapping], worker_id, updated_pids)

    {:noreply, Map.put(state, :worker_mapping, worker_mapping)}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning("TCP connection closed for IO socket.")

    socket = accept_connection(state.lsock)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_info({:tcp, _port, payload}, state) do
    compute_mappings = state.worker_mapping

    Payloader.parse_payload(payload)
    |> Enum.each(fn {worker_id, message} ->
      workers = Map.get(compute_mappings, worker_id)

      # TODO: Are we handling binary or text data?
      Enum.each(workers, fn pid -> GenServer.cast(pid, {:io_out, :text, message}) end)
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp accept_connection(lsock) do
    {:ok, socket} = :gen_tcp.accept(lsock)

    :inet.setopts(socket, [{:active, true}])
    :gen_tcp.controlling_process(socket, self())

    socket
  end

  defp input_type(data_type) do
    # Binary is 1, Text is 2, wrapped in a 8-bit integer
    case data_type do
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

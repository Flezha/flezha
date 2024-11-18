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
        computation_mapping: %{}
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
  def handle_cast({:io_in, type, computation_id, message}, %{sock: sock} = state) do
    :gen_tcp.send(
      sock,
      Payloader.build_from_bin(
        computation_id,
        input_type(type) <> message
      )
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:computation_pid_update, computation_id, updated_pids}, state) do
    computation_mapping = Map.put(state[:computation_mapping], computation_id, updated_pids)

    {:noreply, Map.put(state, :computation_mapping, computation_mapping)}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning("TCP connection closed for IO socket.")

    socket = accept_connection(state.lsock)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_info({:tcp, _port, payload}, state) do
    compute_mappings = state.computation_mapping

    Payloader.parse_payload(payload)
    |> Enum.each(fn {computation_id, message} ->
      computations = Map.get(compute_mappings, computation_id)

      Enum.each(computations, fn pid -> GenServer.cast(pid, {:io_out, :text, message}) end)
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

  defp input_type(type) do
    # Binary is 1, Text is 2, wrapped in a 8-bit integer
    case type do
      :binary -> <<1::size(8)>>
      :text -> <<2::size(8)>>
    end
  end
end

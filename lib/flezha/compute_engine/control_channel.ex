defmodule Flezha.ComputeEngine.ControlChannel do
  alias Flezha.Socket.Support, as: SocketSupport
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.ComputeEngine.FleetManager

  require Logger

  use GenServer

  def start_link({namespace, compute_engine_id, sock}) do
    GenServer.start_link(__MODULE__, {namespace, compute_engine_id, sock},
      name: Identificator.control_channel(compute_engine_id)
    )
  end

  def send_control(compute_engine_id, command) do
    control_channel = Identificator.control_channel(compute_engine_id)

    case GenServer.whereis(control_channel) do
      nil ->
        {:error, :no_control_channel}

      _ ->
        ref = gen_ref()

        req =
          struct(
            ComputeEngineCommands.CommandRequest,
            reference_id: ref,
            command: command
          )

        {:ok, iodata, _size} = Protox.encode(req)

        payload =
          Payloader.build_from_bin(
            "control_command",
            IO.iodata_to_binary(iodata)
          )

        GenServer.call(control_channel, {:send_control, payload})

        {:ok, ref}
    end
  end

  def send_control_async(compute_engine_id, command) do
    control_channel = Identificator.control_channel(compute_engine_id)

    case GenServer.whereis(control_channel) do
      nil ->
        :ok

      _ ->
        ref = gen_ref()

        req =
          struct(
            ComputeEngineCommands.CommandRequest,
            reference_id: ref,
            command: command
          )

        {:ok, iodata, _size} = Protox.encode(req)

        payload =
          Payloader.build_from_bin(
            "control_command",
            IO.iodata_to_binary(iodata)
          )

        GenServer.cast(control_channel, {:send_control_async, payload})
        :ok
    end
  end

  @impl true
  def init({namespace, compute_engine_id, sock}) do
    SocketSupport.set_active(sock)

    {:ok,
     %{
       namespace: namespace,
       compute_engine_id: compute_engine_id,
       sock: sock,
       control_unit: Identificator.control_unit(compute_engine_id),
       buffer: {:empty, <<>>}
     }}
  end

  @impl true
  def handle_call({:send_control, payload}, _from, state) do
    res =
      case Map.get(state, :sock, nil) do
        nil ->
          {:error, :no_connection_available}

        socket ->
          :gen_tcp.send(socket, payload)
      end

    {:reply, res, state}
  end

  @impl true
  def handle_call({:handle_frame, frame}, _from, state) do
    {:reply, :ok, handle_payload(state, frame)}
  end

  @impl true
  def handle_cast({:send_control_async, payload}, state) do
    case Map.get(state, :sock, nil) do
      nil -> :ok
      socket -> :gen_tcp.send(socket, payload)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning(
      "TCP connection closed for control socket for #{state.compute_engine_id} on #{state.namespace}"
    )

    GenServer.call(FleetManager, {
      :compute_engine_disconnected,
      state.namespace,
      state.compute_engine_id
    })

    {:noreply, state}
  end

  @impl true
  def handle_info({:tcp, _port, frame}, state) do
    {:noreply, handle_payload(state, frame)}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at control channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_payload(%{buffer: buffer} = state, frame) do
    {payloads, buffer} = Payloader.parse_frame(buffer, frame, [])

    Enum.each(payloads, fn {_payload_id, message, _metadata} ->
      # Send the payload the the control unit.
      GenServer.cast(
        state.control_unit,
        {:handle_payload, message}
      )
    end)

    Map.put(state, :buffer, buffer)
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end
end

defmodule Flezha.ComputeEngine.ControlChannel do
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.ComputeEngine.FleetManager
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  def start_link({namespace, compute_engine_id, stream_ref}) do
    GenServer.start_link(__MODULE__, {namespace, compute_engine_id, stream_ref},
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
  def init({namespace, compute_engine_id, stream_ref}) do
    {:ok,
     %{
       namespace: namespace,
       compute_engine_id: compute_engine_id,
       stream_ref: stream_ref,
       control_unit: Identificator.control_unit(compute_engine_id)
     }}
  end

  @impl true
  def handle_call({:send_control, payload}, _from, state) do
    res =
      case Map.get(state, :stream_ref, nil) do
        nil ->
          {:error, :no_connection_available}

        stream_ref ->
          Native.send_data(stream_ref, payload)
      end

    {:reply, res, state}
  end

  @impl true
  def handle_call({:handle_frame, _frame}, _from, state) do
    # No longer needed — framing is done in Rust NIF
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:send_control_async, payload}, state) do
    case Map.get(state, :stream_ref, nil) do
      nil -> :ok
      stream_ref -> Native.send_data(stream_ref, payload)
    end

    {:noreply, state}
  end

  # Unified NIF transport message — complete parsed frame
  @impl true
  def handle_info({:flz_data, _stream_id, _payload_id, message, _metadata}, state) do
    GenServer.cast(state.control_unit, {:handle_payload, message})

    {:noreply, state}
  end

  # NIF transport: connection closed
  @impl true
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    handle_connection_closed(state)
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at control channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_connection_closed(state) do
    Logger.warning(
      "Connection closed for control socket for #{state.compute_engine_id} on #{state.namespace}"
    )

    GenServer.call(FleetManager, {
      :compute_engine_disconnected,
      state.namespace,
      state.compute_engine_id
    })

    {:noreply, state}
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end
end

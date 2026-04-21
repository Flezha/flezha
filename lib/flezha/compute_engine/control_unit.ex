defmodule Flezha.ComputeEngine.ControlUnit do
  alias Flezha.Service.Identificator
  alias Flezha.ComputeEngine.ControlChannel
  alias Flezha.Service.ConfigManager

  use GenServer

  require Logger

  @spec start_link({binary(), binary()}) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link({namespace, compute_engine_id}) do
    GenServer.start_link(
      __MODULE__,
      {namespace, compute_engine_id},
      name: Identificator.control_unit(compute_engine_id)
    )
  end

  @doc """
  Sends a `ProcessorMessage` to the SDK-side processor via the control
  channel and returns the decoded `ProcessorMessageResponse`.

  Mirrors the `ProcessorState` request/reply pattern: the compute engine's
  reply (a protobuf-encoded `ProcessorMessageResponse`) is delivered by
  `handle_payload/2` as the `raw_data` field of the outer `CommandResponse`.
  An empty `error` string is mapped to `{:ok, payload}`; a non-empty error
  becomes `{:error, error}`. Transport-layer or encoding failures surface as
  `{:error, reason}` unchanged.
  """
  @spec send_processor_message(binary(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def send_processor_message(compute_engine_id, processor_id, payload)
      when is_binary(compute_engine_id) and is_binary(processor_id) and is_binary(payload) do
    command = %ComputeEngineCommands.ProcessorMessage{
      processor_id: processor_id,
      payload: payload
    }

    case GenServer.call(
           Identificator.control_unit(compute_engine_id),
           {:send_control, {:processor_message, command}}
         ) do
      {:ok, raw_data} -> decode_processor_message_response(raw_data)
      {:error, _} = err -> err
    end
  catch
    :exit, reason -> {:error, {:control_unit_exit, reason}}
  end

  defp decode_processor_message_response(raw_data) do
    case ComputeEngineCommands.ProcessorMessageResponse.decode(raw_data) do
      {:ok, %ComputeEngineCommands.ProcessorMessageResponse{error: "", payload: payload}} ->
        {:ok, payload}

      {:ok, %ComputeEngineCommands.ProcessorMessageResponse{error: err}} ->
        {:error, err}

      {:error, reason} ->
        {:error, {:decode_error, reason}}
    end
  end

  @impl true
  def init({namespace, compute_engine_id}) do
    {
      :ok,
      %{
        namespace: namespace,
        compute_engine_id: compute_engine_id,
        response_refs: %{},
        env: Application.get_env(:flezha, :env)
      }
    }
  end

  @impl true
  def handle_call({:get_processor_state, processor_id}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.compute_engine_id,
        {
          :get_processor_state,
          %ComputeEngineCommands.ProcessorState{processor_id: processor_id}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:send_control, command}, from, state) do
    res = ControlChannel.send_control(state.compute_engine_id, command)

    case res do
      {:ok, ref} ->
        {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:command_assign_compute_engine_id, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.compute_engine_id,
        {
          :assign_compute_engine_id,
          %ComputeEngineCommands.AssignComputeEngineId{compute_engine_id: state.compute_engine_id}
        }
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:command_sockets_connect, from, state) do
    sockets_count = ConfigManager.concurrent_channel_count()

    {:ok, ref} =
      ControlChannel.send_control(
        state.compute_engine_id,
        {
          :connect_socket,
          %ComputeEngineCommands.ConnectSocket{sockets_count: sockets_count}
        }
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:start_processor, processor}, from, state) do
    # TODO: Not sure if we should convert to map from the beginning.
    init_values =
      case processor.init_values !== %{} do
        true ->
          {:ok, json_iv} = Jason.encode(processor.init_values)
          json_iv

        false ->
          ""
      end

    command = %ComputeEngineCommands.AddProcessor{
      processor_id: processor.id,
      factory_id: processor.factory_id,
      init_values: init_values
    }

    {:ok, ref} = ControlChannel.send_control(state.compute_engine_id, {:add_processor, command})

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:stop_processor, processor_id}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.compute_engine_id,
        {
          :rem_processor,
          %ComputeEngineCommands.RemProcessor{processor_id: processor_id}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:disconnect_compute_engine, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.compute_engine_id,
        {
          :compute_engine_disconnect,
          %ComputeEngineCommands.ComputeEngineDisconnect{}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_cast({:stop_processor_async, processor_id}, state) do
    ControlChannel.send_control_async(
      state.compute_engine_id,
      {
        :rem_processor,
        %ComputeEngineCommands.RemProcessor{processor_id: processor_id}
      }
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:handle_payload, payload}, state) do
    {:noreply, handle_payload(state, payload)}
  end

  defp handle_payload(state, payload) do
    {:ok, response} = ComputeEngineCommands.CommandResponse.decode(payload)
    ref = Map.get(response, :reference_id, nil)

    reply =
      case response.status do
        "success" ->
          {:ok, response.raw_data}

        "error" ->
          reason = Enum.join(response.errors, " | ")

          if state[:env] !== :test do
            Logger.error(reason)
          end

          {:error, reason}
      end

    new_refs = handle_refs(state[:response_refs], ref, reply)

    Map.put(state, :response_refs, new_refs)
  end

  defp handle_refs(refs, ref, reply) do
    case Map.get(refs, ref, nil) do
      nil ->
        refs

      from ->
        GenServer.reply(from, reply)
        Map.delete(refs, ref)
    end
  end
end

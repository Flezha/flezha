defmodule Flezha.Executor.ControlUnit do
  alias Flezha.Service.Identificator
  alias Flezha.Executor.ControlChannel
  alias Flezha.Service.ConfigManager

  use GenServer

  require Logger

  @spec start_link({binary(), binary()}) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link({namespace, executor_id}) do
    GenServer.start_link(
      __MODULE__,
      {namespace, executor_id},
      name: Identificator.control_unit(executor_id)
    )
  end

  @doc """
  Sends a `RelayMessage` to the SDK-side relay via the control
  channel and returns the decoded `RelayMessageResponse`.

  Mirrors the `PartitionState` request/reply pattern: the compute engine's
  reply (a protobuf-encoded `RelayMessageResponse`) is delivered by
  `handle_payload/2` as the `raw_data` field of the outer `CommandResponse`.
  An empty `error` string is mapped to `{:ok, payload}`; a non-empty error
  becomes `{:error, error}`. Transport-layer or encoding failures surface as
  `{:error, reason}` unchanged.
  """
  @spec send_relay_message(binary(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def send_relay_message(executor_id, relay_id, payload)
      when is_binary(executor_id) and is_binary(relay_id) and is_binary(payload) do
    command = %ExecutorCommands.RelayMessage{
      relay_id: relay_id,
      payload: payload
    }

    case GenServer.call(
           Identificator.control_unit(executor_id),
           {:send_control, {:relay_message, command}}
         ) do
      {:ok, raw_data} -> decode_relay_message_response(raw_data)
      {:error, _} = err -> err
    end
  catch
    :exit, reason -> {:error, {:control_unit_exit, reason}}
  end

  defp decode_relay_message_response(raw_data) do
    case ExecutorCommands.RelayMessageResponse.decode(raw_data) do
      {:ok, %ExecutorCommands.RelayMessageResponse{error: "", payload: payload}} ->
        {:ok, payload}

      {:ok, %ExecutorCommands.RelayMessageResponse{error: err}} ->
        {:error, err}

      {:error, reason} ->
        {:error, {:decode_error, reason}}
    end
  end

  @impl true
  def init({namespace, executor_id}) do
    {
      :ok,
      %{
        namespace: namespace,
        executor_id: executor_id,
        response_refs: %{},
        env: Application.get_env(:flezha, :env)
      }
    }
  end

  @impl true
  def handle_call({:get_partition_state, relay_id}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.executor_id,
        {
          :get_partition_state,
          %ExecutorCommands.PartitionState{relay_id: relay_id}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:send_control, command}, from, state) do
    res = ControlChannel.send_control(state.executor_id, command)

    case res do
      {:ok, ref} ->
        {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:command_assign_executor_id, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.executor_id,
        {
          :assign_executor_id,
          %ExecutorCommands.AssignExecutorId{executor_id: state.executor_id}
        }
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:command_sockets_connect, from, state) do
    sockets_count = ConfigManager.concurrent_channel_count()

    {:ok, ref} =
      ControlChannel.send_control(
        state.executor_id,
        {
          :connect_socket,
          %ExecutorCommands.ConnectSocket{sockets_count: sockets_count}
        }
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:start_relay, relay}, from, state) do
    # TODO: Not sure if we should convert to map from the beginning.
    init_values =
      case relay.init_values !== %{} do
        true ->
          {:ok, json_iv} = Jason.encode(relay.init_values)
          json_iv

        false ->
          ""
      end

    command = %ExecutorCommands.AddRelay{
      relay_id: relay.id,
      factory_id: relay.factory_id,
      init_values: init_values
    }

    {:ok, ref} = ControlChannel.send_control(state.executor_id, {:add_relay, command})

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:stop_relay, relay_id}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.executor_id,
        {
          :rem_relay,
          %ExecutorCommands.RemRelay{relay_id: relay_id}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:disconnect_executor, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.executor_id,
        {
          :executor_disconnect,
          %ExecutorCommands.ExecutorDisconnect{}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_cast({:stop_relay_async, relay_id}, state) do
    ControlChannel.send_control_async(
      state.executor_id,
      {
        :rem_relay,
        %ExecutorCommands.RemRelay{relay_id: relay_id}
      }
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:handle_payload, payload}, state) do
    {:noreply, handle_payload(state, payload)}
  end

  defp handle_payload(state, payload) do
    {:ok, response} = ExecutorCommands.CommandResponse.decode(payload)
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

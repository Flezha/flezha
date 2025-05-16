defmodule Aflecha.Operator.ControlUnit do
  alias Aflecha.Service.Identificator
  alias Aflecha.Operator.ControlChannel
  alias Aflecha.Service.ConcurrencyMapper

  use GenServer

  require Logger

  @spec start_link({binary(), binary()}) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link({group_id, operator_id}) do
    GenServer.start_link(
      __MODULE__,
      {group_id, operator_id},
      name: Identificator.control_unit(operator_id)
    )
  end

  @impl true
  def init({group_id, operator_id}) do
    {
      :ok,
      %{
        group_id: group_id,
        operator_id: operator_id,
        response_refs: %{},
        env: Application.get_env(:aflecha, :env)
      }
    }
  end

  @impl true
  def handle_call({:get_pipeline_state, pipeline_id}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        {
          :get_pipeline_state,
          %OperatorCommands.PipelineState{pipeline_id: pipeline_id}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:send_control, command}, from, state) do
    res = ControlChannel.send_control(state.operator_id, command)

    case res do
      {:ok, ref} ->
        {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:command_assign_operator_id, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        {
          :assign_operator_id,
          %OperatorCommands.AssignOperatorId{operator_id: state.operator_id}
        }
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:command_sockets_connect, from, state) do
    sockets_count = ConcurrencyMapper.concurrent_entity_count()

    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        {
          :connect_socket,
          %OperatorCommands.ConnectSocket{sockets_count: sockets_count}
        }
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:start_pipeline, pipeline}, from, state) do
    # TODO: Not sure if we should convert to map from the beginning.
    init_values =
      case pipeline.init_values !== %{} do
        true ->
          {:ok, json_iv} = Jason.encode(pipeline.init_values)
          json_iv

        false ->
          ""
      end

    command = %OperatorCommands.AddPipeline{
      pipeline_id: pipeline.id,
      factory_id: pipeline.factory_id,
      init_values: init_values
    }

    {:ok, ref} = ControlChannel.send_control(state.operator_id, {:add_pipeline, command})

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:stop_pipeline, pipeline_id}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        {
          :rem_pipeline,
          %OperatorCommands.RemPipeline{pipeline_id: pipeline_id}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:disconnect_operator, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        {
          :operator_disconnect,
          %OperatorCommands.OperatorDisconnect{}
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_cast({:handle_payload, payload}, state) do
    {:noreply, handle_payload(state, payload)}
  end

  defp handle_payload(state, payload) do
    {:ok, response} = OperatorCommands.CommandResponse.decode(payload)
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

defmodule Aflecha.Operator.ControlUnit do
  alias Aflecha.Service.Identificator
  alias Aflecha.Operator.ControlChannel
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Pipeline.Builder, as: PipelineBuilder

  use GenServer

  require Logger

  @payload_id_connect_control_channel "connect_control_channel"
  @payload_id_assign_operator_id "assign_operator_id"
  # TODO: perhaps add: @payload_id_factory_register "register_factory"
  @payload_id_connect_sockets "connect_sockets"
  @payload_id_pipeline_start "start_pipeline"
  @payload_id_pipeline_stop "stop_pipeline"
  @payload_id_serialize_pipe "migration_serialization"
  @payload_id_deserialize_pipe "migration_deserialization"
  @payload_id_pipeline_state "pipeline_state"
  @payload_id_operator_disconnect "operator_disconnect"

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
        response_refs: %{}
      }
    }
  end

  @impl true
  def handle_call({:get_pipeline_state, pipeline_id}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        @payload_id_pipeline_state,
        %{"pipeline_id" => pipeline_id}
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:send_control, command_id, data}, from, state) do
    res =
      ControlChannel.send_control(
        state.operator_id,
        command_id,
        data
      )

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
        @payload_id_assign_operator_id,
        %{"operator_id" => state.operator_id}
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:command_sockets_connect, from, state) do
    sockets_count = ConcurrencyMapper.concurrent_entity_count()

    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        @payload_id_connect_sockets,
        %{"sockets_count" => sockets_count}
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  @spec handle_call({:start_pipeline, PipelineBuilder.t()}, {pid(), term()}, map()) ::
          {:noreply, any()}
  def handle_call({:start_pipeline, pipeline}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        @payload_id_pipeline_start,
        %{
          "pipeline_id" => pipeline.id,
          "factory_id" => pipeline.factory_id,
          "init_values" => pipeline.init_values
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:stop_pipeline, pipeline_id}, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        @payload_id_pipeline_stop,
        %{"pipeline_id" => pipeline_id}
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:disconnect_operator, from, state) do
    {:ok, ref} =
      ControlChannel.send_control(
        state.operator_id,
        @payload_id_operator_disconnect,
        %{}
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_cast({:handle_payload, payload_id, message}, state) do
    {:noreply, handle_payload(state, {payload_id, message})}
  end

  defp handle_payload(state, {payload_id, message}) do
    {:ok, json_message} = Jason.decode(message)
    ref = Map.get(json_message, "reference_id", nil)

    cond do
      Enum.member?(payload_id_list(), payload_id) ->
        reply =
          case json_message["status"] do
            "success" ->
              {:ok, json_message["info"]}

            "error" ->
              reason = Enum.join(json_message["errors"], " | ")

              if Application.get_env(:aflecha, :env) !== :test do
                Logger.error(reason)
              end

              {:error, reason}
          end

        new_refs = handle_refs(state[:response_refs], ref, reply)
        Map.put(state, :response_refs, new_refs)

      true ->
        Logger.error("Unknown message id: #{payload_id}")
        state
    end
  end

  defp payload_id_list() do
    # TODO: refactor.
    [
      @payload_id_connect_control_channel,
      @payload_id_assign_operator_id,
      @payload_id_pipeline_start,
      @payload_id_pipeline_stop,
      @payload_id_pipeline_state,
      @payload_id_serialize_pipe,
      @payload_id_deserialize_pipe,
      @payload_id_connect_sockets,
      @payload_id_operator_disconnect
    ]
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

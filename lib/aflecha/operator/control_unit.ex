defmodule Aflecha.Operator.ControlUnit do
  alias Aflecha.Service.Identificator
  alias Aflecha.Service.Payloader
  alias Aflecha.Operator.ControlChannel
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Service.EventManager
  alias Aflecha.Pipeline.Builder, as: PipelineBuilder

  use GenServer

  require Logger

  @payload_id_connect_control_channel "connect_control_channel"
  @payload_id_assign_operator_id "assign_operator_id"
  @payload_id_factory_register "register_factory"
  @payload_id_connect_sockets "connect_sockets"
  @payload_id_pipeline_add "add_pipeline"
  @payload_id_pipeline_rem "rem_pipeline"
  @payload_id_serialize_pipe "serialize_pipeline"
  @payload_id_deserialize_pipe "deserialize_pipeline"
  @payload_id_comp_state "pipeline_state"

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
  def handle_call({:handle_initial_payloads, payloads}, _from, state) do
    handle_payload(state, payloads)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get_pipeline_state, pipeline_id}, from, state) do
    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.operator_id),
        @payload_id_comp_state,
        %{"pipeline_id" => pipeline_id}
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:send_control, command_id, data}, from, state) do
    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.operator_id),
        command_id,
        data
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:command_assign_operator_id, from, state) do
    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.operator_id),
        @payload_id_assign_operator_id,
        %{"operator_id" => state.operator_id}
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:command_sockets_connect, from, state) do
    sockets_count = ConcurrencyMapper.concurrent_entity_count()

    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.operator_id),
        @payload_id_connect_sockets,
        %{"sockets_count" => sockets_count}
      )

    {:reply, :ok, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  @spec handle_call({:start_pipeline, PipelineBuilder.t()}, {pid(), term()}, map()) ::
          {:noreply, any()}
  def handle_call({:start_pipeline, pipeline}, from, state) do
    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.operator_id),
        @payload_id_pipeline_add,
        %{
          "pipeline_id" => pipeline.id,
          "factory_id" => pipeline.factory_id,
          "variables" => pipeline.variables
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:stop_pipeline, pipeline_id}, from, state) do
    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.operator_id),
        @payload_id_pipeline_rem,
        %{"pipeline_id" => pipeline_id}
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_cast({:handle_payload, payload}, state) do
    payloads = Payloader.parse_payload(payload)

    {:noreply, handle_payload(state, payloads)}
  end

  defp handle_payload(state, []), do: state

  defp handle_payload(state, [{payload_id, message} | payloads]) do
    {:ok, json_message} = Jason.decode(message)
    ref = Map.get(json_message, "reference_id", nil)

    reply =
      case payload_id do
        @payload_id_connect_control_channel ->
          :ok

        @payload_id_assign_operator_id ->
          :ok

        @payload_id_pipeline_add ->
          dbg()
          :ok

        @payload_id_pipeline_rem ->
          :ok

        @payload_id_comp_state ->
          json_message["data"]

        @payload_id_serialize_pipe ->
          json_message["data"]

        @payload_id_deserialize_pipe ->
          :ok

        @payload_id_connect_sockets ->
          :ok

        _ ->
          {:error, "Unknown message id: #{payload_id}"}
      end

    case reply do
      {:error, reason} ->
        Logger.error(reason)
        handle_payload(state, payloads)

      _ ->
        new_refs = handle_refs(state[:response_refs], ref, reply)
        handle_payload(Map.put(state, :response_refs, new_refs), payloads)
    end
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

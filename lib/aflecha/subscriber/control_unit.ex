defmodule Aflecha.Subscriber.ControlUnit do
  alias Aflecha.Service.Identificator
  alias Aflecha.Service.Payloader
  alias Aflecha.Subscriber.ControlChannel
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Service.EventManager

  use GenServer

  require Logger

  @payload_id_connect_control_channel "connect_control_channel"
  @payload_id_factory_register "register_factory"
  @payload_id_connect_sockets "connect_sockets"
  @payload_id_computation_add "add_computation"
  @payload_id_computation_rem "rem_computation"
  @payload_id_serialize_comp "serialize_computation"
  @payload_id_deserialize_comp "deserialize_computation"

  @spec start_link(binary()) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link(group_id) do
    GenServer.start_link(__MODULE__, group_id, name: Identificator.control_unit(group_id))
  end

  @impl true
  def init(group_id) do
    {:ok, %{group_id: group_id, response_refs: %{}}}
  end

  @impl true
  def handle_call({:handle_initial_payloads, payloads}, _from, state) do
    handle_payload(state, payloads)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:send_control, command_id, data}, from, state) do
    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.group_id),
        command_id,
        data
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call(:command_sockets_connect, from, state) do
    sockets_count = ConcurrencyMapper.concurrent_entity_count()

    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.group_id),
        @payload_id_connect_sockets,
        %{"sockets_count" => sockets_count}
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:add_computation, computation}, from, state) do
    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.group_id),
        @payload_id_computation_add,
        %{
          "computation_id" => computation.id,
          "factory_id" => computation.factory_id,
          "variables" => computation.variables
        }
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:rem_computation, computation_id}, from, state) do
    ref =
      ControlChannel.send_control(
        Identificator.control_channel(state.group_id),
        @payload_id_computation_rem,
        %{"computation_id" => computation_id}
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

        @payload_id_computation_add ->
          :ok

        @payload_id_computation_rem ->
          :ok

        @payload_id_serialize_comp ->
          json_message["data"]

        @payload_id_deserialize_comp ->
          :ok

        @payload_id_connect_sockets ->
          :ok

        @payload_id_factory_register ->
          handle_factory_registration(message)

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

  defp handle_factory_registration(message) do
    {:ok, data} = Jason.decode(message)

    EventManager.dispath(:factory_registered, data["factory_id"])

    :ok
  end
end

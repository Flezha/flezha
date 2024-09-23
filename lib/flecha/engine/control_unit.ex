defmodule Flecha.Engine.ControlUnit do
  alias Flecha.Utility.Identifier
  alias Flecha.Service.Payloader
  alias Flecha.Engine.ControlChannel
  alias Flecha.Service.ConcurrencyMapper
  alias Flecha.Service.EventManager
  alias Flecha.Warden

  use GenServer

  require Logger

  @payload_id_connect_control_channel "0"
  @payload_id_blueprint_register "39"
  @payload_id_connect_sockets "40"
  @payload_id_computation_new "41"
  @payload_id_serialize_comp "42"
  @payload_id_deserialize_comp "43"

  @spec start_link(binary()) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link(engine_id) do
    GenServer.start_link(__MODULE__, engine_id, name: Identifier.control_unit(engine_id))
  end

  @impl true
  def init(engine_id) do
    Warden.update(:engines, fn engines ->
      Map.put(engines, engine_id, [])
    end)

    {:ok, %{engine_id: engine_id, response_refs: %{}}}
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
        Identifier.control_channel(state.engine_id),
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
        Identifier.control_channel(state.engine_id),
        @payload_id_connect_sockets,
        %{"sockets_count" => sockets_count}
      )

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:add_computation, computation}, from, state) do
    ref =
      ControlChannel.send_control(
        Identifier.control_channel(state.engine_id),
        @payload_id_computation_new,
        %{
          "computation_id" => computation.id,
          "blueprint_id" => computation.blueprint_id,
          "variables" => computation.variables
        }
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

        @payload_id_computation_new ->
          :ok

        @payload_id_serialize_comp ->
          json_message["data"]

        @payload_id_deserialize_comp ->
          :ok

        @payload_id_connect_sockets ->
          :ok

        @payload_id_blueprint_register ->
          handle_blueprint_registration(state, message)

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

  defp handle_blueprint_registration(state, message) do
    {:ok, data} = Jason.decode(message)

    Warden.update(:engines, fn engines ->
      Map.update(engines, state[:engine_id], [], fn blueprints ->
        [data["blueprint_id"] | blueprints]
      end)
    end)

    EventManager.dispath(:blueprint_registered, data["blueprint_id"])

    :ok
  end
end

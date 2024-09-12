defmodule Flecha.Engine.ControlUnit do
  alias Flecha.Utility.Identifier
  alias Flecha.Service.Payloader
  alias Flecha.Engine.ControlChannel
  alias Flecha.Service.ConcurrencyMapper
  alias Flecha.Warden

  use GenServer

  require Logger

  @payload_id_reference "30"
  @payload_id_template_register "39"
  @payload_id_connect_sockets "40"
  @payload_id_computation_new "41"

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
          "template_id" => computation.template_id,
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
    state =
      case payload_id do
        @payload_id_reference ->
          handle_reference_response(state, message)

        @payload_id_template_register ->
          handle_template_registration(state, message)

        _ ->
          Logger.error("Unknown message id: #{payload_id}")
          state
      end

    handle_payload(state, payloads)
  end

  defp handle_template_registration(state, message) do
    {:ok, data} = Jason.decode(message)
    # TODO: maybe add schema opton? {:ok, schema} = Jason.decode(data["schema"])

    Warden.update(:engines, fn engines ->
      Map.update(engines, state[:engine_id], [], fn templates ->
        [data["template_id"] | templates]
      end)
    end)

    state
  end

  defp handle_reference_response(state, message) do
    case Map.get(state[:response_refs], message) do
      nil ->
        state[:response_refs]

      from ->
        GenServer.reply(from, :ok)
    end

    Map.put(state, :response_refs, Map.delete(state[:response_refs], message))
  end
end

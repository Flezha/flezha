defmodule Flecha.EngineManager do
  alias Flecha.Service.Payloader
  alias Flecha.CommandChannel

  require Logger

  use GenServer

  @payload_id_reference "30"
  @payload_id_template_register "39"
  @payload_id_socket_open "40"
  @payload_id_computation_new "41"

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  # TODO: we should be actually deal with only one port not many..

  @impl true
  def init(_) do
    ports = initialize_ports()

    {:ok, %{ports: ports, response_refs: %{}}}
  end

  @impl true
  def handle_call({:open_socket, socket_id}, from, state) do
    ref = send_command(@payload_id_socket_open, %{"socket_id" => socket_id})

    {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  end

  @impl true
  def handle_call({:add_computation, computation}, from, state) do
    ref =
      send_command(
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

  @impl true
  def handle_info({_port, {:data, port_msg}}, state) do
    Logger.warning("Received data from port: #{inspect(port_msg)}")

    {:noreply, state}
  end

  def handle_payload(state, []), do: state

  def handle_payload(state, [{payload_id, message} | payloads]) do
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
    {:ok, schema} = Jason.decode(data["schema"])

    # TODO: should be coming from the configuration.
    engine_name = "default"

    computation_engines =
      Application.get_env(:flecha_core, :computation_engines, %{})
      |> put_in([engine_name, :computation_templates, data["template_id"]], schema)

    Application.put_env(:flecha_core, :computation_engines, computation_engines)

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

  defp initialize_ports() do
    computation_engines = Application.fetch_env!(:flecha_core, :computation_engines)

    Enum.map(computation_engines, fn {name, engine} ->
      port =
        Port.open(
          {:spawn, engine[:command]},
          [:binary]
        )

      Port.monitor(port)

      {name, port}
    end)
  end

  defp send_command(payload_id, data) do
    ref = gen_ref()

    command =
      Jason.encode!(%{
        "data" => data,
        "reference_id" => ref
      })

    payload = Payloader.build_payload(payload_id, command)

    GenServer.call(CommandChannel, {:send_command, payload})

    ref
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end
end

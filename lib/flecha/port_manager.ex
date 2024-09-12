defmodule Flecha.PortManager do
  require Logger

  use GenServer

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    ports = initialize_ports()

    {:ok, %{ports: ports, response_refs: %{}}}
  end

  @impl true
  def handle_info({_port, {:data, port_msg}}, state) do
    Logger.info("Received data from port: #{inspect(port_msg)}")

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :port, _port, :normal}, state) do
    # TODO: Try restarting the port process but do not try indefinetly.

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.info("Received unexpected message: #{inspect(msg)}")

    {:noreply, state}
  end

  defp initialize_ports() do
    external_processes = Application.fetch_env!(:flecha_core, :external_processes)

    Enum.map(external_processes, fn command ->
      port =
        Port.open(
          {:spawn, command},
          [:binary]
        )

      Port.monitor(port)

      port
    end)
  end

  # TODO: move to somewhere else this logic.
  # @payload_id_reference "30"
  # @payload_id_template_register "39"
  # @payload_id_socket_open "40"
  # @payload_id_computation_new "41"

  # @impl true
  # def handle_call({:add_computation, computation}, from, state) do
  #   ref =
  #     send_command(
  #       @payload_id_computation_new,
  #       %{
  #         "computation_id" => computation.id,
  #         "template_id" => computation.template_id,
  #         "variables" => computation.variables
  #       }
  #     )

  #   {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  # end

  # @impl true
  # def handle_cast({:handle_payload, payload}, state) do
  #   payloads = Payloader.parse_payload(payload)

  #   {:noreply, handle_payload(state, payloads)}
  # end

  # defp handle_payload(state, []), do: state

  # defp handle_payload(state, [{payload_id, message} | payloads]) do
  #   state =
  #     case payload_id do
  #       @payload_id_reference ->
  #         handle_reference_response(state, message)

  #       @payload_id_template_register ->
  #         handle_template_registration(state, message)

  #       _ ->
  #         Logger.error("Unknown message id: #{payload_id}")
  #         state
  #     end

  #   handle_payload(state, payloads)
  # end

  # defp handle_template_registration(state, message) do
  #   {:ok, data} = Jason.decode(message)
  #   {:ok, schema} = Jason.decode(data["schema"])

  #   # TODO: should be coming from the configuration.
  #   engine_name = "default"

  #   computation_engines =
  #     Application.get_env(:flecha_core, :computation_engines, %{})
  #     |> put_in([engine_name, :computation_templates, data["template_id"]], schema)

  #   Application.put_env(:flecha_core, :computation_engines, computation_engines)

  #   state
  # end

  # defp handle_reference_response(state, message) do
  #   case Map.get(state[:response_refs], message) do
  #     nil ->
  #       state[:response_refs]

  #     from ->
  #       GenServer.reply(from, :ok)
  #   end

  #   Map.put(state, :response_refs, Map.delete(state[:response_refs], message))
  # end

  # defp send_command(payload_id, data) do
  #   ref = gen_ref()

  #   command =
  #     Jason.encode!(%{
  #       "data" => data,
  #       "reference_id" => ref
  #     })

  #   payload = Payloader.build_payload(payload_id, command)

  #   GenServer.call(CommandChannel, {:send_control, payload})

  #   ref
  # end

  # defp gen_ref() do
  #   make_ref()
  #   |> :erlang.ref_to_list()
  #   |> List.to_string()
  # end
end

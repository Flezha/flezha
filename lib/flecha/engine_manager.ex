defmodule Flecha.EngineManager do
  alias Flecha.Service.Payloader

  require Logger

  use GenServer

  @payload_id_reference "30"
  @payload_id_template_register "39"
  @payload_id_socket_open "40"
  @payload_id_computation_new "41"

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    ports = initialize_ports()

    {:ok, %{ports: ports, response_refs: %{}}}
  end

  @impl true
  def handle_call({:open_socket, socket_id}, from, state) do
    refs =
      command(
        state[:ports],
        @payload_id_socket_open,
        %{"socket_id" => socket_id}
      )
      |> Enum.map(&{&1, from})
      |> Map.new()

    {:noreply, Map.put(state, :response_refs, Map.merge(state.response_refs, refs))}
  end

  @impl true
  def handle_call({:add_computation, computation}, from, state) do
    refs =
      command(
        state[:ports],
        @payload_id_computation_new,
        %{
          "computation_id" => computation.id,
          "template_id" => computation.template_id,
          "variables" => computation.variables
        }
      )
      |> Enum.map(&{&1, from})
      |> Map.new()

    {:noreply, Map.put(state, :response_refs, Map.merge(state.response_refs, refs))}
  end

  @impl true
  def handle_info({port, {:data, payload}}, state) do
    payloads = Payloader.parse_payload(payload)

    {:noreply, handle_payload(state, port, payloads)}
  end

  def handle_payload(state, _port, []), do: state

  def handle_payload(state, port, [{payload_id, message} | payloads]) do
    state =
      case payload_id do
        @payload_id_reference ->
          handle_reference_response(state, message)

        @payload_id_template_register ->
          handle_template_registration(state, message, port)

        _ ->
          Logger.error("Unknown message id: #{payload_id}")
          state
      end

    handle_payload(state, port, payloads)
  end

  defp handle_template_registration(state, message, port) do
    {:ok, data} = Jason.decode(message)
    {:ok, schema} = Jason.decode(data["schema"])

    engine_name = Enum.find(state.ports, fn {_name, p} -> p === port end) |> elem(0)

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

  defp command(ports, payload_id, data) do
    Enum.map(ports, fn {_name, port} ->
      ref = gen_ref()

      command =
        Jason.encode!(%{
          "data" => data,
          "reference_id" => ref
        })

      payloader = Payloader.build_payload(payload_id, command)

      Port.command(port, payloader)

      ref
    end)
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end
end

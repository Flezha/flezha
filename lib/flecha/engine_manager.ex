defmodule Flecha.EngineManager do
  require Logger

  use GenServer

  @code_template_register "39"
  @code_socket_open "40"
  @code_computation_new "41"

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
        @code_socket_open,
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
        @code_computation_new,
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
  def handle_info({port, {:data, "CODE:" <> @code_template_register <> json_data}}, state) do
    IO.inspect(json_data, label: "Engine manager received")

    {:ok, data} = Jason.decode(json_data)
    {:ok, schema} = Jason.decode(data["schema"])

    engine_name = Enum.find(state.ports, fn {_name, p} -> p === port end) |> elem(0)

    computation_engines =
      Application.get_env(:flecha_core, :computation_engines, %{})
      |> put_in([engine_name, :computation_templates, data["template_id"]], schema)

    Application.put_env(:flecha_core, :computation_engines, computation_engines)

    {:noreply, state}
  end

  @impl true
  def handle_info({_port, {:data, "COMMAND_REF:" <> ref_id}}, state) do
    case Map.get(state[:response_refs], ref_id) do
      nil ->
        state[:response_refs]

      from ->
        GenServer.reply(from, :ok)
    end

    {:noreply, Map.put(state, :response_refs, Map.delete(state[:response_refs], ref_id))}
  end

  @impl true
  def handle_info({_port, {:data, msg}}, state) do
    IO.inspect(msg, label: "Engine manager received")

    {:noreply, state}
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

  defp command(ports, code, data) do
    Enum.map(ports, fn {_name, port} ->
      ref = gen_ref()

      command =
        Jason.encode!(%{
          "code" => String.to_integer(code),
          "data" => data,
          "reference_id" => ref
        })

      Port.command(port, command <> "\n")

      ref
    end)
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end
end

defmodule Flecha.EngineManager do
  require Logger

  use GenServer

  @code_query_processor 39

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    ports = initialize_ports()

    command(ports, @code_query_processor, %{})

    {:ok, %{ports: ports, response_refs: %{}}}
  end

  @impl true
  def handle_info({_port, {:data, msg}}, state) do
    IO.inspect(msg, label: "Engine manager received")

    {:noreply, state}
  end

  defp initialize_ports() do
    computation_engines = Application.fetch_env!(:flecha_core, :computation_engines)

    Enum.map(computation_engines, fn {_, command} ->
      port =
        Port.open(
          {:spawn, command},
          [:binary]
        )

      Port.monitor(port)

      port
    end)
  end

  defp command(ports, code, data) do
    Enum.map(ports, fn port ->
      ref = gen_ref()

      command =
        Jason.encode!(%{
          "code" => code,
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

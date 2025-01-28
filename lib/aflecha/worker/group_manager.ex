defmodule Aflecha.Worker.GroupManager do
  alias Aflecha.Worker.WorkerSupervisor
  alias Aflecha.Worker.FleetSupervisor
  alias Aflecha.Service.Payloader
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator
  alias Aflecha.HubManager

  require Logger

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def connect_handler(sock) do
    GenServer.call(__MODULE__, {:worker_connect, sock})
  end

  @impl true
  def init(_) do
    expect_factories()

    {:ok, %{}}
  end

  @impl true
  def handle_call({:worker_connect, sock}, _from, state) do
    case worker_connect(sock) do
      {:ok, group_id, worker_id} ->
        group_workers = Map.get(state, group_id, []) ++ [worker_id]
        state = Map.put(state, group_id, group_workers)

        if !HubManager.hub_alive?(group_id) do
          HubManager.start_hub(group_id)
        end

        {:reply, {:ok, worker_id}, state}

      {:error, :no_group} ->
        {:reply, {:error, :no_group}, state}
    end
  end

  defp worker_connect(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, payload} ->
        [{"connect_control_channel", group_id} | rest] = Payloader.parse_payload(payload)
        worker_id = Identificator.unique_id(group_id)
        control_unit = Identificator.control_unit(group_id, worker_id)

        FleetSupervisor.start_operator(group_id, worker_id, sock)

        SocketSupport.pass_ownership(
          sock,
          WorkerSupervisor.find_control_channel(group_id)
        )

        GenServer.call(control_unit, {:handle_initial_payloads, rest})
        GenServer.call(control_unit, :command_sockets_connect)

        {:ok, group_id, worker_id}

      {:error, reason} ->
        :gen_tcp.close(sock)
        Logger.error("Failed to receive group id: #{inspect(reason)}")

        {:error, :no_group}
    end
  end

  defp expect_factories do
    Aflecha.Service.EventManager.add_handler(:factory_registered)

    count = Application.get_env(:aflecha, :factories_expected_count)

    if is_integer(count) && count > 0 do
      for _x <- 1..count do
        receive do
          {:factory_registered, _factory_id} ->
            :ok
        after
          3000 -> raise "Registered factories not equal to expected count"
        end
      end
    end
  end
end

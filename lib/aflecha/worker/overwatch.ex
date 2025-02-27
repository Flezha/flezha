defmodule Aflecha.Worker.Overwatch do
  alias Aflecha.IO.IOSupervisor
  alias Aflecha.Plugin.ManagedProcess, as: ManagerProcessPlugin
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Worker.MigratorService
  alias Aflecha.Operator.StreamSup
  alias Aflecha.Service.Identificator

  use GenServer

  defstruct [:id, :group_id, :channel, :supervisors, :plugin_subscribers, variables: %{}]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init({group_id, operator_id, worker}) do
    GenServer.call(
      Identificator.control_unit(operator_id),
      {:start_worker, worker}
    )

    {:ok,
     %__MODULE__{
       id: worker.id,
       group_id: group_id,
       variables: worker.variables,
       channel: attach_channel(worker.id, group_id),
       plugin_subscribers: %{
         on_input: [],
         on_output: [],
         interval: []
       }
     }}
  end

  @impl true
  def handle_call(
        {:add_plugin_subscriber, event, %ManagerProcessPlugin{} = settings},
        _from,
        state
      ) do
    {:ok, conn} =
      apply(
        settings.plugin_mod,
        settings.plugin_func,
        settings.plugin_args
      )

    subscribers =
      state
      |> Map.get(:plugin_subscribers)
      |> Map.get(event)

    state =
      put_in(
        state,
        [Access.key(:plugin_subscribers), event],
        [conn | subscribers]
      )

    {:reply, {:ok, conn}, state}
  end

  # @impl true
  # def handle_call({:add_io, direction, io_struct, opts}, _from, state) do
  #   {module, func, args} = Aflecha.IO.Provider.mfa(io_struct)
  #   new_args = [self() | args] ++ [opts]
  #   io_id = Enum.random(1..10000) |> Integer.to_string()

  #   response =
  #     Map.get(state.supervisors, direction)
  #     |> IOSupervisor.start_child(%{
  #       id: io_id,
  #       start: {module, func, new_args}
  #     })

  #   case response do
  #     {:ok, pid} ->
  #       pids = Map.get(state.io, direction, [])
  #       io_handlers = Map.put(state.io, direction, [{module, pid} | pids])

  #       {:reply, {:ok, io_id}, %__MODULE__{state | io: io_handlers}}

  #     {:error, reason} ->
  #       {:reply, {:error, reason}, state}
  #   end
  # end

  # @impl true
  # def handle_call({:send_io_payload, io_id, direction, payload}, _from, state)
  #     when is_struct(payload) do
  #   io_data = Map.get(state.io, direction)

  #   case io_data do
  #     nil ->
  #       {:reply, {:error, "IO handler not found"}, state}

  #     {io_module, _initial_io_pid} ->
  #       sup = Map.get(state.supervisors, direction)
  #       {:ok, io_pid} = IOSupervisor.find_pid(sup, io_id)
  #       res = apply(io_module, :send_payload, [io_pid, payload])

  #       {:reply, res, state}
  #   end
  # end

  # @impl true
  # def handle_call({:send_io_payload, _payload}, _from, state) do
  #   {:reply, {:error, "Invalid payload"}, state}
  # end

  # @impl true
  # def handle_cast({:io_in, payload, data_type, handling_type}, state) do
  #   GenServer.cast(state.channel, {:io_in, state.id, payload, data_type, handling_type})

  #   {:noreply, state}
  # end

  # @impl true
  # def handle_cast({:io_out, payload, data_type}, %{io: %{output: out_pids}} = state) do
  #   Enum.each(out_pids, fn {_module, out_pid} ->
  #     GenServer.cast(out_pid, {:io_out, payload, data_type})
  #   end)

  #   {:noreply, state}
  # end

  @impl true
  def handle_info({:process_hub, :handover_start, startup_resp, child_id, from}, state) do
    case startup_resp do
      {:ok, pid} ->
        serialized_data = MigratorService.serialize_worker(state.group_id, state.id)

        # Send the state to the remote process.
        Process.send(pid, {:process_hub, :handover, {state, serialized_data}}, [])

        # Signal the handler process that the state handover has been handled.
        Process.send(from, {:process_hub, :retention_handled, child_id}, [])

      _ ->
        nil
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:process_hub, :handover, {handover_state, serialized_data}}, state) do
    MigratorService.deserialize_worker(state.group_id, state.id, serialized_data)

    {:noreply, handover_state}
  end

  def attach_channel(worker_id, group_id) do
    # Get numeric representation of the worker_id string.
    worker_int = String.to_charlist(worker_id) |> Enum.sum()

    # Pick the channel index based on the channel_name_int.
    count = ConcurrencyMapper.concurrent_entity_count()
    channel_index = rem(worker_int, count)

    StreamSup.stream_ids(group_id, :primary) |> Enum.at(channel_index)
  end
end

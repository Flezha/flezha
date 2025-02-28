defmodule Aflecha.Worker.Overwatch do
  alias Aflecha.IO.IOSupervisor
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Worker.MigratorService
  alias Aflecha.Operator.StreamSup
  alias Aflecha.Service.Identificator
  alias Aflecha.HubManager
  alias Aflecha.Plugin.WorkerSubscriber

  use GenServer

  defstruct [:id, :group_id, :channel, :supervisors, :subscribers, variables: %{}]

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
       subscribers: %{
         on_input: [],
         on_output: [],
         interval: []
       }
     }}
  end

  def attach_subscriber(group_id, worker_id, event, settings) do
    # TODO: Move worker ID extraction to separate service.
    {:ok, worker_pid} = HubManager.get_pid(group_id, worker_id)

    GenServer.call(worker_pid, {:add_subscriber, event, settings})
  end

  @impl true
  def handle_call({:add_subscriber, event, settings}, _from, state) do
    subscriber = WorkerSubscriber.subscribe(settings)
    subscribers = Map.get(state.subscribers, event, [])

    {:reply, :ok,
     %__MODULE__{
       state
       | subscribers:
           Map.put(
             state.subscribers,
             event,
             [subscriber | subscribers]
           )
     }}
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

  @impl true
  def handle_cast(
        {:io_out, payload, data_type},
        %__MODULE__{subscribers: %{on_output: subs}} = state
      ) do
    dbg()

    Enum.each(subs, fn handler ->
      handler.(payload, data_type)
    end)

    {:noreply, state}
  end

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

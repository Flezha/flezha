defmodule Aflecha.Computation.WatchDog do
  # TODO: rename this module.
  alias Aflecha.IO.IOSupervisor
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Computation.Migrator
  alias Aflecha.Operator.StreamSup

  use GenServer

  defstruct [:id, :operator_id, :channel, :supervisors, :io, variables: %{}]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init({operator_id, computation}) do
    {:ok,
     %__MODULE__{
       id: computation.id,
       operator_id: operator_id,
       variables: computation.variables,
       channel: attach_channel(computation.id, operator_id),
       io: %{
         input: [],
         output: []
       },
       supervisors: %{
         input: start_sup(),
         output: start_sup()
       }
     }}
  end

  @impl true
  def handle_call({:add_io, direction, io_struct, opts}, _from, state) do
    {module, func, args} = Aflecha.IO.Provider.mfa(io_struct)
    new_args = [self() | args] ++ [opts]
    io_id = Enum.random(1..10000) |> Integer.to_string()

    response =
      Map.get(state.supervisors, direction)
      |> IOSupervisor.start_child(%{
        id: io_id,
        start: {module, func, new_args}
      })

    case response do
      {:ok, pid} ->
        pids = Map.get(state.io, direction, [])
        io_handlers = Map.put(state.io, direction, [{module, pid} | pids])

        {:reply, {:ok, io_id}, %__MODULE__{state | io: io_handlers}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:send_io_payload, io_id, direction, payload}, _from, state)
      when is_struct(payload) do
    io_data = Map.get(state.io, direction)

    case io_data do
      nil ->
        {:reply, {:error, "IO handler not found"}, state}

      {io_module, _initial_io_pid} ->
        sup = Map.get(state.supervisors, direction)
        {:ok, io_pid} = IOSupervisor.find_pid(sup, io_id)
        res = apply(io_module, :send_payload, [io_pid, payload])

        {:reply, res, state}
    end
  end

  @impl true
  def handle_call({:send_io_payload, _payload}, _from, state) do
    {:reply, {:error, "Invalid payload"}, state}
  end

  @impl true
  def handle_cast({:io_in, payload, data_type, handling_type}, state) do
    GenServer.cast(state.channel, {:io_in, state.id, payload, data_type, handling_type})

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, payload, data_type}, %{io: %{output: out_pids}} = state) do
    Enum.each(out_pids, fn {_module, out_pid} ->
      GenServer.cast(out_pid, {:io_out, payload, data_type})
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:process_hub, :handover_start, startup_resp, child_id, from}, state) do
    case startup_resp do
      {:ok, pid} ->
        serialized_data = Migrator.serialize_computation(state.operator_id, state.id)

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
    Migrator.deserialize_computation(state.operator_id, state.id, serialized_data)

    {:noreply, handover_state}
  end

  defp start_sup() do
    case IOSupervisor.start_link() do
      {:ok, pid} -> pid
      {:error, _} -> raise "Failed to start IO Supervisor"
    end
  end

  def attach_channel(computation_id, operator_id) do
    # Get numeric representation of the computation_id string.
    computation_int = String.to_charlist(computation_id) |> Enum.sum()

    # Pick the channel index based on the channel_name_int.
    count = ConcurrencyMapper.concurrent_entity_count()
    channel_index = rem(computation_int, count)

    StreamSup.stream_ids(operator_id, :primary) |> Enum.at(channel_index)
  end
end

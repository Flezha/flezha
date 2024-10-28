defmodule Aflecha.Computation.WatchDog do
  alias Aflecha.IO.IOSupervisor
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Computation.Migrator
  alias Aflecha.Engine.StreamSupervisor

  use GenServer

  defstruct [:id, :channels, :supervisors, :io, engines: [], variables: %{}]

  def start_link(settings) do
    GenServer.start_link(__MODULE__, settings)
  end

  @impl true
  def init(settings) do
    {:ok,
     %__MODULE__{
       id: settings.id,
       variables: settings.variables,
       channels: attach_channels(settings.id, settings.engines),
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
  def handle_call({:add_io, io_id, direction, settings}, _from, state) do
    {module, func, args} = Aflecha.IO.Base.mfa(settings)
    new_args = [self() | args]

    response =
      IOSupervisor.start_child(Map.get(state.supervisors, direction), %{
        id: io_id,
        start: {module, func, new_args}
      })

    case response do
      {:ok, pid} ->
        pids = Map.get(state.io, direction, [])
        io_handlers = Map.put(state.io, direction, [{module, pid} | pids])

        {:reply, {:ok, "IO created"}, %__MODULE__{state | io: io_handlers}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:io_internal_payload, io_id, direction, payload}, _from, state)
      when is_struct(payload) do
    io_data = Map.get(state.io, direction)

    case io_data do
      nil ->
        {:reply, {:error, "IO handler not found"}, state}

      {io_module, _initial_io_pid} ->
        sup = Map.get(state.supervisors, direction)
        {:ok, io_pid} = IOSupervisor.find_pid(sup, io_id)
        res = apply(io_module, :handle_internal_payload, [io_pid, payload])

        {:reply, res, state}
    end
  end

  @impl true
  def handle_call({:io_internal_payload, _payload}, _from, state) do
    {:reply, {:error, "Invalid payload"}, state}
  end

  @impl true
  def handle_cast({:io_in, _type, message}, state) do
    Enum.each(state.channels, &GenServer.cast(&1, {:io_in, :binary, state.id, message}))

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, type, message}, %{io: %{output: out_pids}} = state) do
    Enum.each(out_pids, fn {_module, out_pid} ->
      GenServer.cast(out_pid, {:io_out, type, message})
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:process_hub, :handover_start, startup_resp, child_id, from}, state) do
    case startup_resp do
      {:ok, pid} ->
        serialized_data = Migrator.serialize_computation(state.id)

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
    Migrator.deserialize_computation(state.id, serialized_data)

    {:noreply, handover_state}
  end

  defp start_sup() do
    case IOSupervisor.start_link() do
      {:ok, pid} -> pid
      {:error, _} -> raise "Failed to start IO Supervisor"
    end
  end

  def attach_channels(computation_id, engines) do
    # Get numeric representation of the computation_id string.
    computation_int = String.to_charlist(computation_id) |> Enum.sum()

    # Pick the channel index based on the channel_name_int.
    count = ConcurrencyMapper.concurrent_entity_count()
    channel_index = rem(computation_int, count)

    # Pick the processor engine that can handle the selected engines.
    pick_channels(engines, channel_index)
  end

  defp pick_channels(engines, channel_index) do
    Enum.map(engines, fn {engine_id, _} ->
      # Select one of the stream channels from that engine.
      StreamSupervisor.stream_ids(engine_id, :primary)
      |> Enum.at(channel_index)
    end)
  end
end

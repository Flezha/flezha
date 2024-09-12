defmodule Flecha.Computation do
  alias Flecha.IOSupervisor
  alias Flecha.Service.ConcurrencyMapper
  alias Flecha.Warden

  use GenServer

  defstruct [:id, :template_id, :channel, :supervisors, :io, variables: %{}]

  def start_link(settings) do
    GenServer.start_link(__MODULE__, settings)
  end

  @impl true
  def init(settings) do
    {:ok,
     %__MODULE__{
       id: settings.id,
       variables: settings.variables,
       channel: attach_channel(settings.id, settings.template_id),
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
    {module, func, args} = Flecha.IO.Base.mfa(settings)
    new_args = [self() | args]

    response =
      IOSupervisor.start_child(Map.get(state.supervisors, direction), %{
        id: io_id,
        start: {module, func, new_args}
      })

    case response do
      {:ok, pid} ->
        pids = Map.get(state.io, direction, [])
        io_handlers = Map.put(state.io, direction, [pid | pids])

        {:reply, {:ok, "IO created"}, %__MODULE__{state | io: io_handlers}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # TODO: later add for websocket.
  # @impl true
  # def handle_call({:io_internal_payload, io_id, direction, payload}, _from, state)
  #     when is_struct(payload) do
  #   mfa = Map.get(state.io, direction)
  #   sup = Map.get(state.supervisors, direction)

  #   {:ok, io_pid} = IOSupervisor.find_pid(sup, io_id)

  #   case mfa do
  #     nil ->
  #       {:reply, {:error, "IO handler not found"}, state}

  #     {m, _f, _a} ->
  #       res = apply(m, :send_internal_payload, [io_pid, payload])

  #       {:reply, res, state}
  #   end
  # end
  # @impl true
  # def handle_call({:io_internal_payload, _payload}, _from, state) do
  #   {:reply, {:error, "Invalid payload"}, state}
  # end

  @impl true
  def handle_cast({:io_in, _type, message}, state) do
    GenServer.cast(state.channel, {:io_in, :binary, state.id, message})

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, type, message}, %{io: %{output: out_pids}} = state) do
    Enum.each(out_pids, fn out_pid ->
      GenServer.cast(out_pid, {:io_out, type, message})
    end)

    {:noreply, state}
  end

  defp start_sup() do
    case IOSupervisor.start_link() do
      {:ok, pid} -> pid
      {:error, _} -> raise "Failed to start IO Supervisor"
    end
  end

  def attach_channel(computation_id, template_id) do
    # Get numeric representation of the computation_id string.
    computation_id_string = String.to_charlist(computation_id) |> Enum.sum()

    # Pick the processor engine that can handle the template_id.
    processor_id = available_processor_id(template_id)

    # Pick the channel index based on the channel_name_int.
    count = ConcurrencyMapper.concurrent_entity_count()
    channel_index = rem(computation_id_string, count) + 1

    ("io_" <> processor_id <> "_" <> Integer.to_string(channel_index)) |> String.to_atom()
  end

  # TODO: We're only attaching one processor but we may accidently have more than one
  # processor engines with the same template_id
  defp available_processor_id(template) do
    engines = Warden.get(:engines)

    item =
      Enum.find(engines, fn {_engine_id, templates} ->
        Enum.any?(templates, fn template_id ->
          template == template_id
        end)
      end)

    case item do
      nil -> raise "computation handler not found"
      {processor_id, _} -> processor_id
    end
  end
end

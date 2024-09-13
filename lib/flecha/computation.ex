defmodule Flecha.Computation do
  alias Flecha.IOSupervisor
  alias Flecha.Service.ConcurrencyMapper
  alias Flecha.Warden
  alias Flecha.Utility.Identifier

  use GenServer

  defstruct [:id, :template_id, :channels, :supervisors, :io, variables: %{}]

  def start_link(settings) do
    GenServer.start_link(__MODULE__, settings)
  end

  @impl true
  def init(settings) do
    {:ok,
     %__MODULE__{
       id: settings.id,
       variables: settings.variables,
       channels: attach_channels(settings.id, settings.template_id),
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
    Enum.each(state.channels, &GenServer.cast(&1, {:io_in, :binary, state.id, message}))

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

  def attach_channels(computation_id, template_id) do
    # Get numeric representation of the computation_id string.
    computation_id_string = String.to_charlist(computation_id) |> Enum.sum()

    # Pick the channel index based on the channel_name_int.
    count = ConcurrencyMapper.concurrent_entity_count()
    channel_index = rem(computation_id_string, count) + 1

    # Pick the processor engine that can handle the template_id.
    filter_engines(template_id) |> pick_channel(channel_index)
  end

  defp filter_engines(template_id) do
    engines =
      Warden.get(:engines)
      |> Enum.filter(fn {_engine_id, templates} ->
        Enum.any?(templates, &(&1 === template_id))
      end)

    case engines do
      [] -> raise "computation engines not found for template #{inspect(template_id)}"
      engines -> engines
    end
  end

  defp pick_channel(engines, channel_index) do
    Enum.map(engines, fn {engine_id, _} ->
      # Select one of the stream channels from that engine.
      Supervisor.which_children(Identifier.stream_sup(engine_id))
      |> Enum.at(channel_index)
      |> elem(1)
    end)
  end
end

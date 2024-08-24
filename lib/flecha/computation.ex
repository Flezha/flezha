defmodule Flecha.Computation do
  alias Flecha.IOSupervisor

  use GenServer

  defstruct [:computation_id, :computation_name, :channel, :supervisors, :io]

  def start_link(settings) do
    GenServer.start_link(__MODULE__, settings)
  end

  @impl true
  def init(settings) do
    {:ok,
     %__MODULE__{
       computation_id: settings[:computation_id],
       channel: attach_channel(settings[:computation_id], settings[:computation_name]),
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

  # @impl true
  # def handle_call({:add_io, io_id, direction, settings}, _from, state) do
  #   {module, func, args} = Flecha.IO.Base.mfa(settings)
  #   new_args = [self() | args]

  #   response =
  #     IOSupervisor.start_child(Map.get(state.supervisors, direction), %{
  #       id: io_id,
  #       start: {module, func, new_args}
  #     })

  #   case response do
  #     {:ok, pid} ->
  #       pids = Map.get(state.io, direction, [])
  #       io_handlers = Map.put(state.io, direction, [pid | pids])

  #       {:reply, {:ok, "IO created"}, %__MODULE__{state | io: io_handlers}}

  #     {:error, reason} ->
  #       {:reply, {:error, reason}, state}
  #   end
  # end

  @impl true
  def handle_call({:io_internal_payload, io_id, direction, payload}, _from, state)
      when is_struct(payload) do
    mfa = Map.get(state.io, direction)
    sup = Map.get(state.supervisors, direction)

    {:ok, io_pid} = IOSupervisor.find_pid(sup, io_id)

    case mfa do
      nil ->
        {:reply, {:error, "IO handler not found"}, state}

      {m, _f, _a} ->
        res = apply(m, :send_internal_payload, [io_pid, payload])

        {:reply, res, state}
    end
  end

  @impl true
  def handle_call({:io_internal_payload, _payload}, _from, state) do
    {:reply, {:error, "Invalid payload"}, state}
  end

  @impl true
  def handle_cast({:io_in, _type, message}, state) do
    index = 0
    message = "#{state.computation_id}\0#{index}\0#{message}"
    message_length = byte_size(message)
    message = <<message_length::size(32), message::binary>>

    GenServer.cast(state.channel, {:io_in, :binary, message})

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

  def attach_channel(computation_id, computation_name) do
    # Get numeric representation of the channel_id string.
    channel_name_int = String.to_charlist(computation_id) |> Enum.sum()
    processor_id = available_processor_id(computation_name)

    core_count = System.schedulers_online() * 2
    channel_index = rem(channel_name_int, core_count) + 1

    (Integer.to_string(channel_index) <> "_" <> processor_id) |> String.to_atom()
  end

  defp available_processor_id(computation_name) do
    processors = Application.get_env(:flecha_core, :computation_engines)

    item =
      Enum.find(processors, fn {_processor_id, processor_data} ->
        computations = processor_data["computations"]

        Enum.any?(computations, fn %{"name" => cn} ->
          computation_name == cn
        end)
      end)

    case item do
      nil -> raise "Computation handler not found"
      {processor_id, _} -> processor_id
    end
  end
end

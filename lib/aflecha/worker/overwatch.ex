defmodule Aflecha.Worker.Overwatch do
  alias Aflecha.IO.IOSupervisor
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Worker.MigratorService
  alias Aflecha.Operator.StreamSup
  alias Aflecha.Service.Identificator
  alias Aflecha.HubManager
  alias Aflecha.Plugin.WorkerBinding

  use GenServer

  defstruct [:id, :group_id, :channel, :supervisors, :bindings, variables: %{}]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  # TODO: binding on migration we should rerun all of them.
  # TODO: add ability to remove bindings.
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
       channel: attach_channel(worker.id, operator_id),
       bindings: %{
         on_input: [],
         on_output: [],
         interval: [],
         proactive: []
       }
     }}
  end

  @impl true
  def handle_call({:add_binding, binding, mode, opts}, _from, state) do
    case WorkerBinding.bind(binding) do
      {:ok, binding} ->
        bindings = [binding | Map.get(state.bindings, mode, [])]

        case mode do
          :interval -> add_interval_binding(binding, opts)
          _ -> nil
        end

        {
          :reply,
          :ok,
          %__MODULE__{state | bindings: Map.put(state.bindings, mode, bindings)}
        }

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_cast({:io_in, payload}, state) do
    # TODO: were currently hardcoding the handling_type to :stream (:task is also a option)
    # The worker itself should be able to define the handling type.
    # Meaning we should store the handling type in the overwatch state.

    # TODO: we are hardcoding it right now, should be dynamic or removed after all.
    data_type = :text

    payload = exec_binding(state.bindings.on_output, payload)

    GenServer.cast(state.channel, {:io_in, state.id, payload, data_type, :stream})

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, payload}, state) do
    exec_binding(state.bindings.on_output, payload)

    {:noreply, state}
  end

  @impl true
  def handle_info({:binding_interval_exec, binding, opts}, state) do
    exec_binding(binding, opts)
    add_interval_binding(binding, opts)

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

    # TODO: we should rerun all bindings here to create new connections.

    {:noreply, handover_state}
  end

  def attach_channel(worker_id, operator_id) do
    # Get numeric representation of the worker_id string.
    worker_int = String.to_charlist(worker_id) |> Enum.sum()

    # Pick the channel index based on the channel_name_int.
    count = ConcurrencyMapper.concurrent_entity_count()
    channel_index = rem(worker_int, count)

    StreamSup.stream_ids(operator_id, :primary) |> Enum.at(channel_index)
  end

  defp exec_binding(bindings, payload) do
    Enum.reduce(bindings, payload, fn binding, p ->
      case WorkerBinding.exec(binding, p) do
        {:ok, p} -> p
        {:error, _} -> p
      end
    end)
  end

  defp add_interval_binding(binding, opts) do
    interval = Keyword.get(opts, :interval, nil)

    if is_integer(interval) do
      Process.send_after(self(), {:binding_interval_exec, binding, opts}, interval)
    end
  end
end

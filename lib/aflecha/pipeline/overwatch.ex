defmodule Aflecha.Pipeline.Overwatch do
  alias Aflecha.Service.HubManager
  alias Aflecha.Service.ConcurrencyMapper
  alias Aflecha.Operator.StreamSup
  alias Aflecha.Service.Identificator
  alias Aflecha.Service.Payloader
  alias Aflecha.Plugin.PipelineBinding

  require Logger

  use GenServer

  @snapshop_interval 60_000

  defstruct [
    :id,
    :group_id,
    :operator_id,
    :factory_id,
    :channel,
    :bindings,
    snapshot_interval: @snapshop_interval,
    snapshot: nil,
    init_values: %{}
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  # TODO: binding on migration we should rerun all of them.
  # TODO: add ability to remove bindings.
  @impl true
  def init({group_id, operator_id, pipeline}) do
    snapshot_interval = Keyword.get(pipeline.opts, :snapshot_interval, @snapshop_interval)

    # TODO: Perhaps move to handle continue.
    schedule_snapshot(snapshot_interval)

    state = %__MODULE__{
      id: pipeline.id,
      group_id: group_id,
      factory_id: pipeline.factory_id,
      init_values: pipeline.init_values,
      operator_id: operator_id,
      channel: attach_channel(pipeline.id, operator_id),
      snapshot_interval: snapshot_interval,
      snapshot: nil,
      bindings: %{
        on_input: [],
        on_output: [],
        interval: [],
        proactive: []
      }
    }

    case start_pipeline_sdk(operator_id, pipeline) do
      {:ok, _} ->
        {:ok, state, {:continue, :additional_init}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def handle_continue(:additional_init, state) do
    {:noreply, state}
  end

  @impl true
  def handle_call(:registry_operator_set, _from, state) do
    HubManager.pipeline_set_operator(state.group_id, state.id, state.operator_id)

    {:reply, {:ok, state.operator_id}, state}
  end

  @impl true
  def handle_call(:stop_pipeline, from, state) do
    GenServer.call(
      Identificator.control_unit(state.operator_id),
      {:stop_pipeline, state.id}
    )

    GenServer.reply(from, :ok)

    {:stop, :normal, state}
  end

  @impl true
  def handle_call({:add_binding, binding}, _from, state) do
    case add_binding(state.bindings, binding) do
      {:ok, bindings} ->
        {:reply, :ok, %__MODULE__{state | bindings: bindings}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:get_operator, _from, %__MODULE__{operator_id: operator} = state) do
    {:reply, operator, state}
  end

  @impl true
  def handle_call(:get_overwatch_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:get_pipeline_state, _from, state) do
    command = %OperatorCommands.PipelineState{pipeline_id: state.id}

    # TODO: any rpc command may timeout causing the pipeline overwatch to crash.
    # We should handle this in the control unit.
    result = send_control(state.operator_id, {:pipeline_state, command})

    {:reply, result, state}
  end

  @impl true
  def handle_call({:operator_migrate, new_operator_id}, _from, state) do
    state = %__MODULE__{
      state
      | operator_id: new_operator_id,
        channel: attach_channel(state.id, new_operator_id)
    }

    init_values =
      cond do
        state.snapshot !== nil -> Payloader.json_to_map(state.snapshot)
        true -> state.init_values
      end

    pipeline = %Aflecha.Pipeline.Builder{
      id: state.id,
      factory_id: state.factory_id,
      init_values: init_values,
      opts: []
    }

    start_pipeline_sdk(new_operator_id, pipeline)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, :pong, state}
  end

  @impl true
  def handle_call({:io_in, payload}, _from, state) do
    payload = exec_binding(state.bindings.on_input, payload)

    GenServer.call(state.channel, {:io_in, state.id, payload, :stream})

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:create_snapshot, _from, state) do
    snapshot = create_snapshot(state.id, state.operator_id)

    {:reply, {:ok, snapshot}, %__MODULE__{state | snapshot: snapshot}}
  end

  @impl true
  def handle_cast({:io_in, payload}, state) do
    # TODO: were currently hardcoding the handling_type to :stream (:task is also a option)
    # The pipeline itself should be able to define the handling type.
    # Meaning we should store the handling type in the overwatch state.
    payload = exec_binding(state.bindings.on_input, payload)

    GenServer.cast(state.channel, {:io_in, state.id, payload, :stream})

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, payload}, state) do
    exec_binding(state.bindings.on_output, payload)

    {:noreply, state}
  end

  @impl true
  def handle_info({:binding_interval_exec, binding}, state) do
    exec_binding(binding, nil)
    add_interval_binding(binding)

    {:noreply, state}
  end

  @impl true
  def handle_info({:process_hub, :send_handover_state, receiver, child_id, opts}, state) do
    serialize_data =
      case serialize_pipeline_sdk(state.operator_id, state.id) do
        nil ->
          state.snapshot

        data ->
          data
      end

    # Send the state to the remote process.
    if is_pid(receiver) do
      Process.send(receiver, {:process_hub, :handover, child_id, {state, serialize_data}}, [])
    end

    # Signal the handler process that the state handover has been handled.
    if is_pid(opts[:retention_receiver]) do
      Process.send(opts[:retention_receiver], {:process_hub, :retention_handled, child_id}, [])
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:process_hub, :handover, _pipeline_id, {handover_state, pipe_serialized}},
        state
      ) do
    command = %OperatorCommands.MigrationDeserialization{
      pipeline_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: pipe_serialized
    }

    result = send_control(state.operator_id, {:migration_deserialization, command})

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize pipeline: #{inspect(reason)}")
      _else -> :ok
    end

    new_bindings = sync_bindings(state.bindings, handover_state.bindings)

    new_state = %__MODULE__{
      state
      | bindings: new_bindings,
        init_values: handover_state.init_values
    }

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:schedule_snapshot, state) do
    snapshot = create_snapshot(state.id, state.operator_id)

    if state.snapshot_interval > 0 do
      schedule_snapshot(state.snapshot_interval)
    end

    {:noreply, %__MODULE__{state | snapshot: snapshot}}
  end

  defp create_snapshot(pipeline_id, operator_id) do
    command = %OperatorCommands.PipelineState{pipeline_id: pipeline_id}

    case send_control(operator_id, {:pipeline_state, command}) do
      {:ok, snapshot} ->
        :binary.bin_to_list(snapshot)

      {:error, _reason} ->
        nil
    end
  end

  defp send_control(operator_id, command) do
    control_unit = Identificator.control_unit(operator_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {
          :error,
          "control unit not available for operator #{operator_id} with payload"
        }

      _ ->
        GenServer.call(
          Identificator.control_unit(operator_id),
          {:send_control, command}
        )
    end
  end

  defp start_pipeline_sdk(operator_id, pipeline) do
    GenServer.call(
      Identificator.control_unit(operator_id),
      {:start_pipeline, pipeline}
    )
  end

  defp schedule_snapshot(snapshot_interval) when is_integer(snapshot_interval) do
    Process.send_after(self(), :schedule_snapshot, snapshot_interval)
  end

  defp schedule_snapshot(_snapshot_interval), do: nil

  defp attach_channel(pipeline_id, operator_id) do
    # Get numeric representation of the pipeline_id string.
    pipeline_int = String.to_charlist(pipeline_id) |> Enum.sum()

    # Pick the channel index based on the channel_name_int.
    count = ConcurrencyMapper.concurrent_entity_count()
    channel_index = rem(pipeline_int, count)

    StreamSup.stream_ids(operator_id, :primary) |> Enum.at(channel_index)
  end

  defp sync_bindings(default_bindings, handover_bindings) do
    migr_bindings =
      Enum.map(handover_bindings, fn {_trigger, bindings} ->
        bindings
      end)
      |> List.flatten()

    Enum.reduce(migr_bindings, default_bindings, fn binding, acc ->
      case add_binding(acc, binding) do
        {:ok, new_bindings} ->
          new_bindings

        {:error, reason} ->
          Logger.error("Failed to synchronize binding: #{inspect(reason)}")

          acc
      end
    end)
  end

  defp add_binding(bindings, binding) do
    case PipelineBinding.bind(binding) do
      {:ok, binding} ->
        binding_group = [binding | Map.get(bindings, binding.trigger, [])]

        case binding.trigger do
          :interval -> add_interval_binding(binding)
          _ -> nil
        end

        {:ok, Map.put(bindings, binding.trigger, binding_group)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp serialize_pipeline_sdk(operator_id, pipeline_id) do
    command = %OperatorCommands.MigrationSerialization{pipeline_id: pipeline_id}

    result = send_control(operator_id, {:migration_serialization, command})

    case result do
      {:ok, serialized_data} ->
        # TODO: 1
        {:ok, data} = Jason.decode(serialized_data)
        data["serialized_state"]

      {:error, reason} ->
        # TODO: we should actually check if overwatch status is :ok
        if Application.get_env(:aflecha, :env) !== :test do
          Logger.error("Failed to serialize pipeline: #{inspect(reason)}")
        end

        nil
    end
  end

  defp exec_binding(bindings, payload) do
    Enum.reduce(bindings, payload, fn binding, p ->
      case PipelineBinding.exec(binding, p) do
        {:ok, p} -> p
        {:error, _} -> p
      end
    end)
  end

  defp add_interval_binding(binding) do
    interval = Map.get(binding.interval, :interval, nil)

    if is_integer(interval) do
      Process.send_after(self(), {:binding_interval_exec, binding}, interval)
    end
  end
end

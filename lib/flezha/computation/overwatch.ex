defmodule Flezha.Computation.Overwatch do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Operator.StreamSup
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Plugin.ComputationBinding

  require Logger

  use GenServer
  use ProcessHub.Strategy.Migration.HotSwap

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
    init_values: %{},
    # 1 - binary, 2 - text, 3 - custom
    default_input_type: "2"
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  # TODO: binding on migration we should rerun all of them.
  # TODO: add ability to remove bindings.
  @impl true
  def init({group_id, operator_id, computation}) do
    snapshot_interval = Keyword.get(computation.opts, :snapshot_interval, @snapshop_interval)

    # TODO: Perhaps move to handle continue.
    schedule_snapshot(snapshot_interval)

    state = %__MODULE__{
      id: computation.id,
      group_id: group_id,
      factory_id: computation.factory_id,
      init_values: computation.init_values,
      operator_id: operator_id,
      channel: attach_channel(computation.id, operator_id),
      snapshot_interval: snapshot_interval,
      snapshot: nil,
      default_input_type: Keyword.get(computation.opts, :default_input_type, "2"),
      bindings: %{
        on_input: [],
        on_output: [],
        interval: [],
        proactive: []
      }
    }

    case start_computation_sdk(operator_id, computation) do
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
    HubManager.computation_set_operator(state.group_id, state.id, state.operator_id)

    {:reply, {:ok, state.operator_id}, state}
  end

  @impl true
  def handle_call(:stop_computation, from, state) do
    GenServer.call(
      Identificator.control_unit(state.operator_id),
      {:stop_computation, state.id}
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
  def handle_call({:remove_binding, binding_id}, _from, state) do
    case remove_binding(state.bindings, binding_id) do
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
  def handle_call(:get_computation_state, _from, state) do
    command = %OperatorCommands.ComputationState{computation_id: state.id}

    # TODO: any rpc command may timeout causing the computation overwatch to crash.
    # We should handle this in the control unit.
    result = send_control(state.operator_id, {:computation_state, command})

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

    computation = %Flezha.Computation.Builder{
      id: state.id,
      factory_id: state.factory_id,
      init_values: init_values,
      opts: []
    }

    start_computation_sdk(new_operator_id, computation)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, :pong, state}
  end

  @impl true
  def handle_call({:io_in, message, metadata}, _from, state) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = exec_bindings(state.bindings.on_input, {message, metadata})

    case payload do
      nil -> nil
      _ -> GenServer.call(state.channel, {:io_in, state.id, payload})
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:create_snapshot, _from, state) do
    snapshot = create_snapshot(state.id, state.operator_id)

    {:reply, {:ok, snapshot}, %__MODULE__{state | snapshot: snapshot}}
  end

  @impl true
  def handle_cast({:io_in, message, metadata}, state) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = exec_bindings(state.bindings.on_input, {message, metadata})

    case payload do
      nil -> nil
      _ -> GenServer.cast(state.channel, {:io_in, state.id, payload})
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, message, metadata}, state) do
    exec_bindings(state.bindings.on_output, {message, metadata})

    {:noreply, state}
  end

  @impl true
  def handle_info({:binding_interval_exec, binding}, state) do
    ComputationBinding.exec(binding, {"", %{}})
    add_interval_binding(binding)

    {:noreply, state}
  end

  @impl true
  def handle_info(:schedule_snapshot, state) do
    snapshot = create_snapshot(state.id, state.operator_id)

    if state.snapshot_interval > 0 do
      schedule_snapshot(state.snapshot_interval)
    end

    {:noreply, %__MODULE__{state | snapshot: snapshot}}
  end

  defp create_snapshot(computation_id, operator_id) do
    command = %OperatorCommands.ComputationState{computation_id: computation_id}

    case send_control(operator_id, {:computation_state, command}) do
      {:ok, snapshot} ->
        snapshot

      {:error, _reason} ->
        nil
    end
  end

  @impl true
  def prepare_handover_state(state) do
    serialize_data =
      case serialize_computation_sdk(state.operator_id, state.id) do
        nil ->
          state.snapshot

        data ->
          data
      end

    {state, serialize_data}
  end

  @impl true
  def alter_handover_state(state, {handover_state, pipe_serialized}) do
    command = %OperatorCommands.MigrationDeserialization{
      computation_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: pipe_serialized
    }

    result = send_control(state.operator_id, {:migration_deserialization, command})

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize computation: #{inspect(reason)}")
      _else -> :ok
    end

    new_bindings = sync_bindings(state.bindings, handover_state.bindings)

    %__MODULE__{
      state
      | bindings: new_bindings,
        init_values: handover_state.init_values
    }
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

  defp start_computation_sdk(operator_id, computation) do
    GenServer.call(
      Identificator.control_unit(operator_id),
      {:start_computation, computation}
    )
  end

  defp schedule_snapshot(snapshot_interval) when is_integer(snapshot_interval) do
    Process.send_after(self(), :schedule_snapshot, snapshot_interval)
  end

  defp schedule_snapshot(_snapshot_interval), do: nil

  defp attach_channel(computation_id, operator_id) do
    # Get numeric representation of the computation_id string.
    computation_int = String.to_charlist(computation_id) |> Enum.sum()

    # Pick the channel index based on the channel_name_int.
    count = ConfigManager.concurrent_channel_count()
    channel_index = rem(computation_int, count)

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
    case ComputationBinding.bind(binding) do
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

  defp serialize_computation_sdk(operator_id, computation_id) do
    command = %OperatorCommands.MigrationSerialization{computation_id: computation_id}

    result = send_control(operator_id, {:migration_serialization, command})

    case result do
      {:ok, serialized_data} ->
        serialized_data

      {:error, reason} ->
        # TODO: we should actually check if overwatch status is :ok
        if Application.get_env(:flezha, :env) !== :test do
          Logger.error("Failed to serialize computation: #{inspect(reason)}")
        end

        nil
    end
  end

  defp exec_bindings(bindings, {message, metadata}) do
    plugin_group = Map.get(metadata, "plugin_group", "0")

    Enum.reduce_while(bindings, {message, metadata}, fn
      binding, payload when plugin_group === binding.plugin_group ->
        case ComputationBinding.exec(binding, payload) do
          {:ok, payload} -> {:cont, payload}
          {:skip, :normal} -> {:halt, nil}
          {:error, _} -> {:cont, payload}
        end

      _, payload ->
        {:cont, payload}
    end)
  end

  defp add_interval_binding(binding) do
    interval = Map.get(binding.interval, :interval, nil)

    if is_integer(interval) do
      Process.send_after(self(), {:binding_interval_exec, binding}, interval)
    end
  end

  defp remove_binding(bindings, binding_id) do
    # Remove binding with matching id from all trigger categories
    updated_bindings =
      Enum.map(bindings, fn {trigger, bindings_list} ->
        filtered_bindings =
          Enum.reject(bindings_list, fn binding ->
            Map.get(binding, :id) == binding_id
          end)

        {trigger, filtered_bindings}
      end)
      |> Map.new()

    {:ok, updated_bindings}
  end
end

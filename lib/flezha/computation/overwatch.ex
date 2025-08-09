defmodule Flezha.Computation.Overwatch do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Operator.StreamSup
  alias Flezha.Computation.IOBinding
  alias Flezha.Plugin.Driver.IOBindingPlugin

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
  def handle_call({:enable_plugin, plugin_id, plugin}, _from, state) do
    case add_binding(state.bindings, plugin_id, plugin, []) do
      {:ok, bindings} ->
        {:reply, :ok, %__MODULE__{state | bindings: bindings}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:disable_plugin, plugin_id}, _from, state) do
    {:ok, bindings} = remove_binding(state.bindings, plugin_id, [])

    {:reply, :ok, %__MODULE__{state | bindings: bindings}}
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
  def handle_info({:binding_interval_exec, %IOBindingPlugin{provider: provider}} = plugin, state) do
    IOBinding.exec(provider, {"", %{}})
    add_interval_binding(plugin)

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    # Find the binding by the pid.
    {plugin_id, plugin} =
      Enum.find(state.bindings, fn {_trigger, bindings} ->
        Enum.any?(bindings, fn {_id, %IOBindingPlugin{pid: binding_pid}} -> binding_pid == pid end)
      end)

    case is_struct(plugin) do
      true ->
        case reason do
          :normal ->
            Logger.info("Binding with pid #{inspect(pid)} terminated normally.")

            # Remove the binding from the state.
            {:ok, new_bindings} = remove_binding(state.bindings, plugin_id, unregister: true)

            {:noreply, %__MODULE__{state | bindings: new_bindings}}

          _ ->
            Logger.error(
              "Binding with pid #{inspect(pid)} terminated with reason: #{inspect(reason)}"
            )

            # Try to restart the binding.
            {:ok, new_bindings} = remove_binding(state.bindings, plugin_id, unregister: true)

            new_bindings =
              case add_binding(new_bindings, plugin_id, plugin, register: true) do
                {:ok, restarted_bindings} ->
                  restarted_bindings

                {:error, reason} ->
                  Logger.error("Failed to restart binding: #{inspect(reason)}")
                  new_bindings
              end

            {:noreply, %__MODULE__{state | bindings: new_bindings}}
        end

      false ->
        Logger.error("Binding not found for pid #{inspect(pid)} with reason: #{inspect(reason)}")

        {:noreply, state}
    end
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

    Enum.reduce(migr_bindings, default_bindings, fn {plugin_id, plugin}, acc ->
      case add_binding(acc, plugin_id, plugin, register: true) do
        {:ok, new_bindings} ->
          new_bindings

        {:error, reason} ->
          Logger.error("Failed to synchronize binding: #{inspect(reason)}")

          acc
      end
    end)
  end

  defp add_binding(bindings, plugin_id, plugin, opts) do
    case IOBinding.bind(plugin.provider) do
      {:ok, {plugin_provider, pid}} ->
        plugin = %IOBindingPlugin{plugin | provider: plugin_provider, pid: pid}

        # Add binding to the group and sort by weight (ascending - smaller weight executes first)
        binding_group =
          [{plugin_id, plugin} | Map.get(bindings, plugin.trigger, [])]
          |> Enum.sort_by(fn {_id, %IOBindingPlugin{weight: weight}} -> weight end)

        case plugin.trigger do
          :interval -> add_interval_binding(plugin)
          _ -> nil
        end

        case is_pid(pid) do
          true ->
            Process.monitor(pid)

          false ->
            nil
        end

        case Keyword.get(opts, :register) do
          true ->
            PluginManager.register_plugin(plugin_id, {
              plugin.__struct__,
              {plugin.group, plugin.computation_id}
            })

          _ ->
            nil
        end

        {:ok, Map.put(bindings, plugin.trigger, binding_group)}

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
      %IOBindingPlugin{provider: p, plugin_group: pg}, payload when plugin_group === pg ->
        case IOBinding.exec(p, payload) do
          {:ok, payload} -> {:cont, payload}
          {:skip, :normal} -> {:halt, nil}
          {:error, _} -> {:cont, payload}
        end

      _, payload ->
        {:cont, payload}
    end)
  end

  defp add_interval_binding(plugin) do
    interval = Map.get(plugin.interval, :interval, nil)

    if is_integer(interval) do
      Process.send_after(self(), {:binding_interval_exec, plugin}, interval)
    end
  end

  defp remove_binding(bindings, plugin_id, opts) do
    # Remove binding with matching id from all trigger categories
    updated_bindings =
      Enum.map(bindings, fn {trigger, bindings_list} ->
        filtered_bindings =
          Enum.reject(bindings_list, fn {binding_id, _plugin} ->
            plugin_id == binding_id
          end)

        {trigger, filtered_bindings}
      end)
      |> Map.new()

    case Keyword.get(opts, :unregister) do
      true ->
        PluginManager.unregister_plugin(plugin_id)

      _ ->
        nil
    end

    {:ok, updated_bindings}
  end
end

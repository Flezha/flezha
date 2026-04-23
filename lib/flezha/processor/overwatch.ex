defmodule Flezha.Processor.Overwatch do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.ComputeEngine.StreamSup
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table

  require Logger

  use GenServer
  use ProcessHub.Strategy.Migration.HotSwap

  use Flezha.Plugin.Host,
    extensions: [
      {:on_processor_init, :fold, Flezha.Plugin.Extension.Ctx.Init},
      {:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO},
      {:on_output, :transform, Flezha.Plugin.Extension.Ctx.IO},
      {:on_interval, :timer, Flezha.Plugin.Extension.Ctx.Tick},
      {:on_message, :request, Flezha.Plugin.Extension.Ctx.Msg},
      {:on_snapshot, :fanout, Flezha.Plugin.Extension.Ctx.Snapshot},
      {:on_terminate, :fanout, Flezha.Plugin.Extension.Ctx.Terminate}
    ]

  @snapshop_interval 60_000

  defstruct [
    :id,
    :namespace,
    :compute_engine_id,
    :factory_id,
    :channel,
    :extensions,
    snapshot_interval: @snapshop_interval,
    snapshot: nil,
    init_values: %{},
    default_input_type: "2"
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init({namespace, compute_engine_id, processor}) do
    Process.flag(:trap_exit, true)

    snapshot_interval = Keyword.get(processor.opts, :snapshot_interval, @snapshop_interval)

    schedule_snapshot(snapshot_interval)

    state = %__MODULE__{
      id: processor.id,
      namespace: namespace,
      factory_id: processor.factory_id,
      init_values: processor.init_values,
      compute_engine_id: compute_engine_id,
      channel: attach_channel(processor.id, compute_engine_id),
      snapshot_interval: snapshot_interval,
      snapshot: nil,
      default_input_type: Keyword.get(processor.opts, :default_input_type, "2"),
      extensions: Table.new(__extensions_catalog__())
    }

    plugins = Keyword.get(processor.opts, :plugins, [])

    with {:ok, state} <- subscribe_initial_plugins(state, plugins),
         {:ok, state, processor} <- resolve_init_state(state, processor),
         {:ok, _} <- start_processor_sdk(compute_engine_id, processor) do
      {:ok, state}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp subscribe_initial_plugins(%__MODULE__{} = state, plugins) do
    entries = Enum.map(plugins, fn %Plugin{id: id} = plugin -> {id, plugin} end)

    case Table.bulk_subscribe(state.extensions, entries) do
      {:ok, table} -> {:ok, %{state | extensions: table}}
      {:error, {_plugin_id, reason}} -> {:error, reason}
    end
  end

  defp resolve_init_state(%__MODULE__{} = state, processor) do
    if processor.init_values == %{} do
      ctx = %Ctx.Init{
        processor_id: processor.id,
        namespace: state.namespace,
        factory_id: processor.factory_id,
        accumulated: %{}
      }

      case Extension.run(state.extensions, :on_processor_init, ctx) do
        {:ok, %Ctx.Init{accumulated: resolved}} ->
          {:ok, %{state | init_values: resolved}, %{processor | init_values: resolved}}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:ok, state, processor}
    end
  end

  @impl true
  def terminate(reason, state) do
    unless is_nil(state.compute_engine_id) do
      GenServer.cast(
        Identificator.control_unit(state.compute_engine_id),
        {:stop_processor_async, state.id}
      )

      fire_on_terminate(state, reason)
      state = %{state | extensions: Table.teardown_all(state.extensions)}
      _ = state
    end

    Logger.info(
      "Terminating processor #{state.id} in namespace #{state.namespace}: #{inspect(reason)}"
    )

    :ok
  end

  defp fire_on_terminate(%__MODULE__{} = state, reason) do
    ctx = %Ctx.Terminate{
      processor_id: state.id,
      namespace: state.namespace,
      compute_engine_id: state.compute_engine_id,
      reason: reason
    }

    Extension.run(state.extensions, :on_terminate, ctx)
    :ok
  end

  @impl true
  def handle_call(:registry_compute_engine_set, _from, state) do
    HubManager.processor_set_compute_engine(state.namespace, state.id, state.compute_engine_id)

    {:reply, {:ok, state.compute_engine_id}, state}
  end

  @impl true
  def handle_call(:stop_processor, from, state) do
    GenServer.call(
      Identificator.control_unit(state.compute_engine_id),
      {:stop_processor, state.id}
    )

    fire_on_terminate(state, :stopped)
    table = Table.teardown_all(state.extensions)

    GenServer.reply(from, :ok)

    {:stop, :normal, %{state | compute_engine_id: nil, extensions: table}}
  end

  @impl true
  def handle_call(
        {:plugin_subscribe, plugin_id, %Plugin{} = plugin},
        _from,
        %__MODULE__{} = state
      ) do
    case Table.subscribe(state.extensions, plugin_id, plugin) do
      {:ok, table} -> {:reply, :ok, %{state | extensions: table}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:plugin_unsubscribe, plugin_id}, _from, %__MODULE__{} = state) do
    table = Table.unsubscribe(state.extensions, plugin_id)
    {:reply, :ok, %{state | extensions: table}}
  end

  @impl true
  def handle_call(
        :get_compute_engine,
        _from,
        %__MODULE__{compute_engine_id: compute_engine} = state
      ) do
    {:reply, compute_engine, state}
  end

  @impl true
  def handle_call(:get_overwatch_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:get_processor_state, _from, state) do
    command = %ComputeEngineCommands.ProcessorState{processor_id: state.id}

    result = send_control(state.compute_engine_id, {:processor_state, command})

    {:reply, result, state}
  end

  @impl true
  def handle_call({:compute_engine_migrate, new_compute_engine_id}, _from, %__MODULE__{} = state) do
    state = %__MODULE__{
      state
      | compute_engine_id: new_compute_engine_id,
        channel: attach_channel(state.id, new_compute_engine_id)
    }

    init_values =
      cond do
        state.snapshot !== nil -> Payloader.json_to_map(state.snapshot)
        true -> state.init_values
      end

    processor = %Flezha.Processor.Builder{
      id: state.id,
      factory_id: state.factory_id,
      init_values: init_values,
      opts: []
    }

    start_processor_sdk(new_compute_engine_id, processor)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, :pong, state}
  end

  @impl true
  def handle_call({:io_in, message, metadata}, _from, state) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = run_io(state, :on_input, message, metadata)

    case payload do
      nil -> nil
      _ -> GenServer.call(state.channel, {:io_in, state.id, payload})
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:create_snapshot, _from, %__MODULE__{} = state) do
    snapshot = create_snapshot(state.id, state.compute_engine_id)

    {:reply, {:ok, snapshot}, %__MODULE__{state | snapshot: snapshot}}
  end

  @impl true
  def handle_cast({:io_in, message, metadata}, state) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = run_io(state, :on_input, message, metadata)

    case payload do
      nil -> nil
      _ -> GenServer.cast(state.channel, {:io_in, state.id, payload})
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, message, metadata}, state) do
    run_io(state, :on_output, message, metadata)
    {:noreply, state}
  end

  defp run_io(%__MODULE__{} = state, extension, message, metadata) do
    ctx = %Ctx.IO{
      processor_id: state.id,
      namespace: state.namespace,
      compute_engine_id: state.compute_engine_id,
      message: message,
      metadata: metadata
    }

    case Extension.run(state.extensions, extension, ctx) do
      {:ok, %Ctx.IO{message: msg, metadata: md}} -> {msg, md}
      {:skip, _reason} -> nil
      {:error, _reason} -> {message, metadata}
    end
  end

  @impl true
  def handle_info({:extension_timer, name, plugin_id}, %__MODULE__{} = state) do
    table = Table.fire_timer(state.extensions, name, plugin_id)
    {:noreply, %{state | extensions: table}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, %__MODULE__{} = state) do
    case Table.handle_down(state.extensions, ref, pid, reason) do
      {:ok, table} ->
        {:noreply, %{state | extensions: table}}

      :ignore ->
        Logger.error(
          "Binding not found for pid #{inspect(pid)} with reason: #{inspect(reason)}"
        )

        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    case reason do
      :normal -> Logger.info("Process #{inspect(pid)} exited normally.")
      _ -> Logger.error("Process #{inspect(pid)} exited with reason: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:schedule_snapshot, %__MODULE__{} = state) do
    snapshot = create_snapshot(state.id, state.compute_engine_id)

    ctx = %Ctx.Snapshot{
      processor_id: state.id,
      namespace: state.namespace,
      compute_engine_id: state.compute_engine_id,
      snapshot: snapshot
    }

    Extension.run(state.extensions, :on_snapshot, ctx)

    if state.snapshot_interval > 0 do
      schedule_snapshot(state.snapshot_interval)
    end

    {:noreply, %__MODULE__{state | snapshot: snapshot}}
  end

  @impl true
  def handle_info({:retry_plugin_restart, plugin_id}, state) do
    table = Table.retry(state.extensions, plugin_id)
    {:noreply, %{state | extensions: table}}
  end

  @impl true
  def handle_info(
        {:flezha_request, handler_key, payload, reply_to, ref},
        %__MODULE__{} = state
      ) do
    ctx = %Ctx.Msg{
      key: handler_key,
      payload: payload,
      reply_to: reply_to,
      ref: ref,
      processor_id: state.id,
      namespace: state.namespace,
      compute_engine_id: state.compute_engine_id
    }

    Extension.run(state.extensions, :on_message, ctx)

    {:noreply, state}
  end

  defp create_snapshot(processor_id, compute_engine_id) do
    command = %ComputeEngineCommands.ProcessorState{processor_id: processor_id}

    case send_control(compute_engine_id, {:processor_state, command}) do
      {:ok, snapshot} -> snapshot
      {:error, _reason} -> nil
    end
  end

  @impl true
  def prepare_handover_state(state) do
    serialize_data =
      case serialize_processor_sdk(state.compute_engine_id, state.id) do
        nil -> state.snapshot
        data -> data
      end

    handover = %{
      id: state.id,
      namespace: state.namespace,
      factory_id: state.factory_id,
      init_values: state.init_values,
      plugins: Table.serializable_plugins(state.extensions)
    }

    {handover, serialize_data}
  end

  @impl true
  def alter_handover_state(%__MODULE__{} = state, {handover_state, pipe_serialized}) do
    command = %ComputeEngineCommands.MigrationDeserialization{
      processor_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: pipe_serialized
    }

    result = send_control(state.compute_engine_id, {:migration_deserialization, command})

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize processor: #{inspect(reason)}")
      _else -> :ok
    end

    # Tear down existing, then rehydrate.
    table = Table.teardown_all(state.extensions)
    rehydrated = Table.rehydrate(table, Map.get(handover_state, :plugins, []))

    %__MODULE__{
      state
      | extensions: rehydrated,
        init_values: handover_state.init_values
    }
  end

  defp send_control(compute_engine_id, command) do
    control_unit = Identificator.control_unit(compute_engine_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {
          :error,
          "control unit not available for compute_engine #{compute_engine_id} with payload"
        }

      _ ->
        GenServer.call(
          Identificator.control_unit(compute_engine_id),
          {:send_control, command}
        )
    end
  end

  defp start_processor_sdk(compute_engine_id, processor) do
    GenServer.call(
      Identificator.control_unit(compute_engine_id),
      {:start_processor, processor},
      8_000
    )
  end

  defp schedule_snapshot(snapshot_interval) when is_integer(snapshot_interval) do
    Process.send_after(self(), :schedule_snapshot, snapshot_interval)
  end

  defp schedule_snapshot(_snapshot_interval), do: nil

  defp attach_channel(processor_id, compute_engine_id) do
    processor_int = String.to_charlist(processor_id) |> Enum.sum()
    count = ConfigManager.concurrent_channel_count()
    channel_index = rem(processor_int, count)

    StreamSup.stream_ids(compute_engine_id, :primary) |> Enum.at(channel_index)
  end

  defp serialize_processor_sdk(compute_engine_id, processor_id) do
    command = %ComputeEngineCommands.MigrationSerialization{processor_id: processor_id}

    result = send_control(compute_engine_id, {:migration_serialization, command})

    case result do
      {:ok, serialized_data} ->
        serialized_data

      {:error, reason} ->
        if Application.get_env(:flezha, :env) !== :test do
          Logger.error("Failed to serialize processor: #{inspect(reason)}")
        end

        nil
    end
  end
end

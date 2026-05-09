defmodule Flezha.Relay.Overwatch do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Executor.ControlChannel
  alias Flezha.Executor.StreamSup
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.State.{Restore, Wire}

  require Logger

  use GenServer
  use ProcessHub.Strategy.Migration.HotSwap

  use Flezha.Plugin.Host,
    extensions: [
      {:on_relay_init, :fold, Flezha.Plugin.Extension.Ctx.Init},
      {:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO},
      {:on_output, :transform, Flezha.Plugin.Extension.Ctx.IO},
      {:on_interval, :timer, Flezha.Plugin.Extension.Ctx.Tick},
      {:on_message, :request, Flezha.Plugin.Extension.Ctx.Msg},
      {:on_snapshot, :fanout, Flezha.Plugin.Extension.Ctx.Snapshot},
      {:on_terminate, :fanout, Flezha.Plugin.Extension.Ctx.Terminate}
    ]

  defstruct [
    :id,
    :namespace,
    :executor_id,
    :factory_id,
    :channel,
    :extensions,
    :builder,
    snapshot: nil,
    init_values: %{},
    default_input_type: "2",
    arrived_via_hotswap?: false
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init({namespace, executor_id, relay}) do
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      id: relay.id,
      namespace: namespace,
      factory_id: relay.factory_id,
      init_values: relay.init_values,
      executor_id: executor_id,
      channel: attach_channel(relay.id, executor_id),
      snapshot: nil,
      default_input_type: Keyword.get(relay.opts, :default_input_type, "2"),
      extensions: Table.new(__extensions_catalog__()),
      builder: relay
    }

    # `relay.opts[:plugins]` is the authoritative list of plugin
    # subscriptions for this relay. It is populated at `flz relay start`
    # time (via the CLI's `--plugins` flag) and by every subsequent
    # `flz plugin enable` / `flz plugin disable` call, which writes
    # through to the cspec via `ProcessHub.Service.ProcessRegistry.update/3`
    # (durably backed by ProcessHub's persistent registry under
    # `app_data/process_hub/<hub_id>/registry.dets`).
    plugins = Keyword.get(relay.opts, :plugins, [])

    with {:ok, state} <- subscribe_initial_plugins(state, plugins),
         {:ok, state, relay} <- resolve_init_state(state, relay),
         {:ok, _} <- start_relay_sdk(executor_id, relay) do
      _ = maybe_dispatch_restore_init_state(state, relay)
      {:ok, state}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Walk the on-disk Partition state for this relay (using `relay.id` as
  # the relay_class) and dispatch `RestoreInitState` to the SDK for each
  # Partition that has a non-empty payload. Skipped when the relay
  # arrived via HotSwap — handover already delivers the state in-band.
  # Skipped when persistence is disabled for this relay class.
  defp maybe_dispatch_restore_init_state(%__MODULE__{arrived_via_hotswap?: true}, _relay), do: :ok

  defp maybe_dispatch_restore_init_state(%__MODULE__{} = state, relay) do
    relay_class = Keyword.get(relay.opts, :relay_class, relay.id)

    if Flezha.State.PersistenceResolver.persistence_enabled?(relay_class) do
      partitions = enumerate_persisted_partitions(relay_class)

      Enum.each(partitions, fn {rc, sh, pk} ->
        case Flezha.State.Restore.has_persisted_state?({rc, sh, pk}) do
          true ->
            _ = dispatch_restore_init_state(state.executor_id, {rc, sh, pk})

          false ->
            :ok
        end
      end)
    end

    :ok
  end

  defp enumerate_persisted_partitions(relay_class) do
    Flezha.State.Registry.all_partitions()
    |> Enum.filter(fn {rc, _, _} -> rc == relay_class end)
  end

  defp subscribe_initial_plugins(%__MODULE__{} = state, plugins) do
    # `relay.opts[:plugins]` accepts two shapes: bare `%Plugin{}` (the
    # cold-start `flz relay start --plugins` path emits this) and
    # `{plugin_id, %Plugin{}}` tuples (the cspec-persistence path written
    # by `Flezha.Service.HubManager.update_relay_plugins/3` after a
    # successful `flz plugin enable`). Both normalise into the tuple
    # form `Table.bulk_subscribe/2` expects.
    entries =
      Enum.map(plugins, fn
        {id, %Plugin{} = plugin} -> {id, plugin}
        %Plugin{id: id} = plugin -> {id, plugin}
      end)

    case Table.bulk_subscribe(state.extensions, entries) do
      {:ok, table} -> {:ok, %{state | extensions: table}}
      {:error, {_plugin_id, reason}} -> {:error, reason}
    end
  end

  # Per the `relay-init-state` capability spec:
  #   - cold start with empty init_values → chain runs
  #   - cold start with non-empty init_values → chain MUST NOT run; inline values used as-is
  #
  # `init/1` is also the path for in-place restart plugin restoration: the
  # cspec's `relay.opts[:plugins]` list is read above and re-subscribed
  # by `subscribe_initial_plugins/2`. That path is independent of the
  # per-Partition state-restore path (`dispatch_restore_init_state/2`),
  # which rehydrates SDK-side WAL/snapshot state through the data plane.
  #
  # HotSwap migration is handled separately in `alter_handover_state/2`
  # and never reaches this function.
  defp resolve_init_state(%__MODULE__{} = state, %{init_values: init_values} = relay)
       when init_values == %{} do
    ctx = %Ctx.Init{
      relay_id: relay.id,
      namespace: state.namespace,
      factory_id: relay.factory_id,
      accumulated: relay.init_values
    }

    case Extension.run(state.extensions, :on_relay_init, ctx) do
      {:ok, %Ctx.Init{accumulated: resolved}} ->
        {:ok, %{state | init_values: resolved}, %{relay | init_values: resolved}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_init_state(%__MODULE__{} = state, relay) do
    # Non-empty inline init_values bypass the chain.
    {:ok, %{state | init_values: relay.init_values}, relay}
  end

  @impl true
  def terminate(reason, state) do
    unless is_nil(state.executor_id) do
      GenServer.cast(
        Identificator.control_unit(state.executor_id),
        {:stop_relay_async, state.id}
      )

      fire_on_terminate(state, reason)
      state = %{state | extensions: Table.teardown_all(state.extensions)}
      _ = state
    end

    Logger.info(
      "Terminating relay #{state.id} in namespace #{state.namespace}: #{inspect(reason)}"
    )

    :ok
  end

  defp fire_on_terminate(%__MODULE__{} = state, reason) do
    ctx = %Ctx.Terminate{
      relay_id: state.id,
      namespace: state.namespace,
      executor_id: state.executor_id,
      reason: reason
    }

    Extension.run(state.extensions, :on_terminate, ctx)
    :ok
  end

  @impl true
  def handle_call(:registry_executor_set, _from, state) do
    HubManager.relay_set_executor(state.namespace, state.id, state.executor_id)

    {:reply, {:ok, state.executor_id}, state}
  end

  @impl true
  def handle_call(:stop_relay, from, state) do
    GenServer.call(
      Identificator.control_unit(state.executor_id),
      {:stop_relay, state.id}
    )

    fire_on_terminate(state, :stopped)
    table = Table.teardown_all(state.extensions)

    GenServer.reply(from, :ok)

    {:stop, :normal, %{state | executor_id: nil, extensions: table}}
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
        :get_executor,
        _from,
        %__MODULE__{executor_id: executor} = state
      ) do
    {:reply, executor, state}
  end

  @impl true
  def handle_call(:get_overwatch_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:get_partition_state, _from, state) do
    command = %ExecutorCommands.PartitionState{relay_id: state.id}

    result = send_control(state.executor_id, {:partition_state, command})

    {:reply, result, state}
  end

  @impl true
  def handle_call({:executor_migrate, new_executor_id}, _from, %__MODULE__{} = state) do
    state = %__MODULE__{
      state
      | executor_id: new_executor_id,
        channel: attach_channel(state.id, new_executor_id)
    }

    init_values =
      cond do
        state.snapshot !== nil -> Payloader.json_to_map(state.snapshot)
        true -> state.init_values
      end

    relay = %Flezha.Relay.Builder{
      id: state.id,
      factory_id: state.factory_id,
      init_values: init_values,
      opts: []
    }

    start_relay_sdk(new_executor_id, relay)

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
    snapshot = create_snapshot(state.id, state.executor_id)

    {:reply, {:ok, snapshot}, %__MODULE__{state | snapshot: snapshot}}
  end

  @impl true
  def handle_call(:get_snapshot_data, _from, %__MODULE__{} = state) do
    data = %{
      namespace: state.namespace,
      builder: state.builder,
      plugins: Table.serializable_plugins(state.extensions),
      snapshot_binary: state.snapshot
    }

    {:reply, data, state}
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

  # Replaces `state.snapshot` with a binary supplied out-of-band by the SDK
  # on graceful shutdown. The snapshot is consumed by HotSwap's
  # `prepare_handover_state` so a subsequent migration ships the
  # executor's final state. Cold-boot restart is covered by the
  # per-Partition WAL files instead.
  @impl true
  def handle_cast({:apply_external_snapshot, binary}, %__MODULE__{} = state)
      when is_binary(binary) do
    {:noreply, %__MODULE__{state | snapshot: binary}}
  end

  defp run_io(%__MODULE__{} = state, extension, message, metadata) do
    ctx = %Ctx.IO{
      relay_id: state.id,
      namespace: state.namespace,
      executor_id: state.executor_id,
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
        Logger.error("Binding not found for pid #{inspect(pid)} with reason: #{inspect(reason)}")

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
      relay_id: state.id,
      namespace: state.namespace,
      executor_id: state.executor_id
    }

    Extension.run(state.extensions, :on_message, ctx)

    {:noreply, state}
  end

  defp create_snapshot(relay_id, executor_id) do
    command = %ExecutorCommands.PartitionState{relay_id: relay_id}

    case send_control(executor_id, {:partition_state, command}) do
      {:ok, snapshot} -> snapshot
      {:error, _reason} -> nil
    end
  end

  @impl true
  def prepare_handover_state(state) do
    serialize_data =
      case serialize_relay_sdk(state.executor_id, state.id) do
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
    command = %ExecutorCommands.MigrationDeserialization{
      relay_id: state.id,
      factory_id: Map.get(handover_state, :factory_id),
      serialized_state: pipe_serialized
    }

    result = send_control(state.executor_id, {:migration_deserialization, command})

    case result do
      {:error, reason} -> Logger.error("Failed to deserialize relay: #{inspect(reason)}")
      _else -> :ok
    end

    # Tear down existing, then rehydrate.
    table = Table.teardown_all(state.extensions)
    rehydrated = Table.rehydrate(table, Map.get(handover_state, :plugins, []))

    %__MODULE__{
      state
      | extensions: rehydrated,
        init_values: handover_state.init_values,
        arrived_via_hotswap?: true
    }
  end

  defp send_control(executor_id, command) do
    control_unit = Identificator.control_unit(executor_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {
          :error,
          "control unit not available for executor #{executor_id} with payload"
        }

      _ ->
        GenServer.call(
          Identificator.control_unit(executor_id),
          {:send_control, command}
        )
    end
  end

  defp start_relay_sdk(executor_id, relay) do
    GenServer.call(
      Identificator.control_unit(executor_id),
      {:start_relay, relay},
      # 8s wasn't enough when the SDK is on the Java side — Spring/JPA
      # warmup plus the AddRelay control-channel round-trip easily
      # exceeds that during a cold start. 30s gives Spring time to breathe
      # without masking genuine stalls.
      30_000
    )
  end

  defp attach_channel(relay_id, executor_id) do
    relay_int = String.to_charlist(relay_id) |> Enum.sum()
    count = ConfigManager.concurrent_channel_count()
    channel_index = rem(relay_int, count)

    StreamSup.stream_ids(executor_id, :primary) |> Enum.at(channel_index)
  end

  defp serialize_relay_sdk(executor_id, relay_id) do
    command = %ExecutorCommands.MigrationSerialization{relay_id: relay_id}

    result = send_control(executor_id, {:migration_serialization, command})

    case result do
      {:ok, serialized_data} ->
        serialized_data

      {:error, reason} ->
        if Application.get_env(:flezha, :env) !== :test do
          Logger.error("Failed to serialize relay: #{inspect(reason)}")
        end

        nil
    end
  end

  # ---------- Per-Partition persistence wiring ----------
  #
  # Helpers used by `Flezha.State.*` and tests to ship persistence
  # control commands to the SDK. Each takes the controller-side
  # `executor_id` and a structured `partition_id` triple
  # `{relay_class, shard_id, partition_key}`.

  @doc """
  Ship a `RestoreInitState` command to the SDK and return the controller's
  freshly assembled payload. The caller is expected to await the
  matching `RestoreInitStateResponse` over the control channel before
  unblocking input streaming for the Partition.
  """
  @spec dispatch_restore_init_state(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_restore_init_state(executor_id, {rc, sh, pk}) do
    case Restore.assemble_payload({rc, sh, pk}) do
      {:ok, payload} ->
        partition_id_bytes = Wire.encode_partition_id(rc, sh, pk)

        command =
          {:restore_init_state,
           %ExecutorCommands.RestoreInitState{
             partition_id: partition_id_bytes,
             snapshot_bytes: payload.snapshot_bytes,
             change_log_bytes: payload.change_log_bytes
           }}

        ControlChannel.send_control(executor_id, command)
    end
  end

  @doc """
  Ship a `CompactPartition` command to the SDK. The SDK responds with a
  `partition_state_snapshot` data frame (handled by
  `Flezha.Executor.StreamChannel`) followed by a
  `CompactPartitionResponse` control reply.
  """
  @spec dispatch_compact_partition(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_compact_partition(executor_id, {rc, sh, pk}) do
    partition_id_bytes = Wire.encode_partition_id(rc, sh, pk)

    command =
      {:compact_partition, %ExecutorCommands.CompactPartition{partition_id: partition_id_bytes}}

    ControlChannel.send_control(executor_id, command)
  end

  @doc """
  Ship a `PartitionMailboxStatus` notification to the SDK so the
  producing Shard's `OverflowPolicy` machinery can park or unpark
  emits to this Partition. `status` is `:full` or `:open`.
  """
  @spec notify_partition_backpressure({binary(), binary(), binary(), binary()}, :full | :open) ::
          :ok | {:error, term()}
  def notify_partition_backpressure({executor_id, rc, sh, pk}, status)
      when status in [:full, :open] do
    partition_id_bytes = Wire.encode_partition_id(rc, sh, pk)

    command =
      {:partition_mailbox_status,
       %ExecutorCommands.PartitionMailboxStatus{
         partition_id: partition_id_bytes,
         status: Atom.to_string(status)
       }}

    ControlChannel.send_control_async(executor_id, command)
  end
end

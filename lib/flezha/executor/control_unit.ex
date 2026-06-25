defmodule Flezha.Executor.ControlUnit do
  alias Flezha.Service.Identificator
  alias Flezha.Executor.ControlChannel
  alias Flezha.Executor.FleetManager
  alias Flezha.Service.ConfigManager

  use GenServer

  require Logger

  @spec start_link({binary(), binary()}) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link({namespace, executor_id}) do
    GenServer.start_link(
      __MODULE__,
      {namespace, executor_id},
      name: Identificator.control_unit(executor_id)
    )
  end

  @doc """
  Issues an end-to-end `Ping` round-trip to the executor's SDK. Returns
  `{:ok, :pong}` on a clean reply, `{:error, :timeout}` if the SDK does not
  reply within `timeout_ms` (default 2 s), or `{:error, reason}` for any
  transport or decode failure.

  Used by `Flezha.Relay.Overwatch.complete_init/3` before `:start_relay` to
  prove the control channel is end-to-end healthy. A timeout here signals a
  stale `stream_ref` that the bound `ControlChannel` cannot detect on its
  own (writes succeed against a closed transport with no failure signal).
  """
  @spec ping_executor(binary(), keyword()) ::
          {:ok, :pong} | {:error, term()}
  def ping_executor(executor_id, opts \\ []) when is_binary(executor_id) do
    timeout_ms = Keyword.get(opts, :timeout_ms, default_ping_timeout_ms())

    control_unit = Identificator.control_unit(executor_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {:error, :no_control_unit}

      _ ->
        try do
          GenServer.call(control_unit, {:ping_executor, timeout_ms}, timeout_ms + 500)
        catch
          :exit, {:timeout, _} -> {:error, :timeout}
          :exit, reason -> {:error, {:control_unit_exit, reason}}
        end
    end
  end

  defp default_ping_timeout_ms,
    do: Application.get_env(:flezha, :overwatch_ping_timeout_ms, 2_000)

  @doc """
  Sends a `RelayMessage` to the SDK-side relay via the control
  channel and returns the decoded `RelayMessageResponse`.

  Mirrors the `PartitionState` request/reply pattern: the compute engine's
  reply (a protobuf-encoded `RelayMessageResponse`) is delivered by
  `handle_payload/2` as the `raw_data` field of the outer `CommandResponse`.
  An empty `error` string is mapped to `{:ok, payload}`; a non-empty error
  becomes `{:error, error}`. Transport-layer or encoding failures surface as
  `{:error, reason}` unchanged.
  """
  @spec send_relay_message(binary(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def send_relay_message(executor_id, relay_id, payload)
      when is_binary(executor_id) and is_binary(relay_id) and is_binary(payload) do
    command = %ExecutorCommands.RelayMessage{
      relay_id: relay_id,
      payload: payload
    }

    case GenServer.call(
           Identificator.control_unit(executor_id),
           {:send_control, {:relay_message, command}}
         ) do
      {:ok, raw_data} -> decode_relay_message_response(raw_data)
      {:error, _} = err -> err
    end
  catch
    :exit, reason -> {:error, {:control_unit_exit, reason}}
  end

  defp decode_relay_message_response(raw_data) do
    case ExecutorCommands.RelayMessageResponse.decode(raw_data) do
      {:ok, %ExecutorCommands.RelayMessageResponse{error: "", payload: payload}} ->
        {:ok, payload}

      {:ok, %ExecutorCommands.RelayMessageResponse{error: err}} ->
        {:error, err}

      {:error, reason} ->
        {:error, {:decode_error, reason}}
    end
  end

  @impl true
  def init({namespace, executor_id}) do
    schedule_heartbeat()

    {
      :ok,
      %{
        namespace: namespace,
        executor_id: executor_id,
        response_refs: %{},
        env: Application.get_env(:flezha, :env)
      }
    }
  end

  @impl true
  def handle_call({:get_partition_state, relay_id}, from, state) do
    case ControlChannel.send_control(
           state.executor_id,
           {
             :get_partition_state,
             %ExecutorCommands.PartitionState{relay_id: relay_id}
           }
         ) do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:send_control, command}, from, state) do
    res = ControlChannel.send_control(state.executor_id, command)

    case res do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:command_assign_executor_id, from, state) do
    case ControlChannel.send_control(
           state.executor_id,
           {
             :assign_executor_id,
             %ExecutorCommands.AssignExecutorId{executor_id: state.executor_id}
           }
         ) do
      {:ok, ref} ->
        {:reply, :ok, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:command_sockets_connect, from, state) do
    sockets_count = ConfigManager.concurrent_channel_count()

    case ControlChannel.send_control(
           state.executor_id,
           {
             :connect_socket,
             %ExecutorCommands.ConnectSocket{sockets_count: sockets_count}
           }
         ) do
      {:ok, ref} ->
        {:reply, :ok, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:ping_executor, timeout_ms}, from, state) do
    nonce = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    command = {:ping, %ExecutorCommands.Ping{nonce: nonce}}

    case ControlChannel.send_control(state.executor_id, command) do
      {:ok, ref} ->
        timer =
          Process.send_after(self(), {:ping_timeout, ref}, timeout_ms)

        refs =
          Map.put(state.response_refs, ref, {:ping, from, timer, nonce})

        {:noreply, Map.put(state, :response_refs, refs)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:start_relay, relay}, from, state) do
    # TODO: Not sure if we should convert to map from the beginning.
    init_values =
      case relay.init_values !== %{} do
        true ->
          {:ok, json_iv} = Jason.encode(relay.init_values)
          json_iv

        false ->
          ""
      end

    # Empty kind ⇒ `:relay` on the SDK side, keeping the wire frame
    # byte-identical to pre-kind builds for the default kind.
    kind = if relay.kind in [nil, :relay], do: "", else: to_string(relay.kind)

    command = %ExecutorCommands.AddRelay{
      relay_id: relay.id,
      factory_id: relay.factory_id,
      init_values: init_values,
      kind: kind
    }

    case ControlChannel.send_control(state.executor_id, {:add_relay, command}) do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:stop_relay, relay_id}, from, state) do
    case ControlChannel.send_control(
           state.executor_id,
           {
             :rem_relay,
             %ExecutorCommands.RemRelay{relay_id: relay_id}
           }
         ) do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:disconnect_executor, from, state) do
    case ControlChannel.send_control(
           state.executor_id,
           {
             :executor_disconnect,
             %ExecutorCommands.ExecutorDisconnect{}
           }
         ) do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_cast({:stop_relay_async, relay_id}, state) do
    ControlChannel.send_control_async(
      state.executor_id,
      {
        :rem_relay,
        %ExecutorCommands.RemRelay{relay_id: relay_id}
      }
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:handle_payload, payload}, state) do
    {:noreply, handle_payload(state, payload)}
  end

  defp handle_payload(state, payload) do
    {:ok, response} = ExecutorCommands.CommandResponse.decode(payload)
    ref = Map.get(response, :reference_id, nil)

    reply =
      case response.status do
        "success" ->
          {:ok, response.raw_data}

        "error" ->
          reason = Enum.join(response.errors, " | ")

          if state[:env] !== :test do
            Logger.error(reason)
          end

          {:error, reason}
      end

    new_refs = handle_refs(state[:response_refs], ref, reply)

    Map.put(state, :response_refs, new_refs)
  end

  # Register the caller against the control-channel reference so the matching
  # `CommandResponse` (delivered later via `handle_payload/2`) is routed back.
  # The monotonic timestamp lets the heartbeat report the age of the oldest
  # outstanding reply — the backlog that grows silently on a dead transport.
  defp store_ref(state, ref, from) do
    entry = {:reply, from, System.monotonic_time(:millisecond)}
    Map.put(state, :response_refs, Map.put(state.response_refs, ref, entry))
  end

  defp handle_refs(refs, ref, reply) do
    case Map.get(refs, ref, nil) do
      nil ->
        refs

      {:ping, from, timer, _nonce} ->
        _ = Process.cancel_timer(timer)

        case reply do
          {:ok, _raw_data} -> GenServer.reply(from, {:ok, :pong})
          {:error, _} = err -> GenServer.reply(from, err)
        end

        Map.delete(refs, ref)

      {:reply, from, _inserted_at} ->
        GenServer.reply(from, reply)
        Map.delete(refs, ref)
    end
  end

  @impl true
  def handle_info({:ping_timeout, ref}, state) do
    case Map.get(state.response_refs, ref) do
      {:ping, from, _timer, _nonce} ->
        GenServer.reply(from, {:error, :timeout})
        {:noreply, Map.put(state, :response_refs, Map.delete(state.response_refs, ref))}

      _ ->
        {:noreply, state}
    end
  end

  # A control reply unacked past the evict threshold means the executor is dead:
  # fail the waiters and evict via FleetManager (not the wedged Overwatch).
  @impl true
  def handle_info(:heartbeat, state) do
    schedule_heartbeat()

    now = System.monotonic_time(:millisecond)
    evict_ms = pending_evict_ms()
    stuck = stuck_reply_refs(state.response_refs, now, evict_ms)

    if stuck == [] do
      {:noreply, state}
    else
      Logger.warning(
        "[ControlUnit] executor=#{state.executor_id} #{length(stuck)} control reply(s) " <>
          "unacked >= #{evict_ms}ms — evicting executor and rebinding its relays"
      )

      refs = fail_stuck_refs(state.response_refs, stuck)
      synthesize_disconnect(state.namespace, state.executor_id)
      {:noreply, %{state | response_refs: refs}}
    end
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  # Deferred replies outstanding past the threshold (ping refs self-clean).
  defp stuck_reply_refs(_refs, _now, evict_ms) when evict_ms <= 0, do: []

  defp stuck_reply_refs(refs, now, evict_ms) do
    for {ref, {:reply, _from, at}} <- refs, now - at >= evict_ms, do: ref
  end

  defp fail_stuck_refs(refs, stuck) do
    Enum.reduce(stuck, refs, fn ref, acc ->
      case Map.get(acc, ref) do
        {:reply, from, _at} ->
          GenServer.reply(from, {:error, :timeout})
          Map.delete(acc, ref)

        _ ->
          acc
      end
    end)
  end

  # Fire-and-forget so the heartbeat never blocks on FleetManager.
  defp synthesize_disconnect(namespace, executor_id) do
    spawn(fn ->
      try do
        GenServer.call(FleetManager, {:executor_disconnected, namespace, executor_id})
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  defp schedule_heartbeat() do
    interval = heartbeat_interval_ms()
    if interval > 0, do: Process.send_after(self(), :heartbeat, interval)
  end

  defp heartbeat_interval_ms,
    do: Application.get_env(:flezha, :control_unit_heartbeat_interval_ms, 15_000)

  defp pending_evict_ms,
    do: Application.get_env(:flezha, :control_unit_pending_evict_ms, 90_000)
end

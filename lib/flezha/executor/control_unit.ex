defmodule Flezha.Executor.ControlUnit do
  alias Flezha.Service.Identificator
  alias Flezha.Executor.ControlChannel
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

  Used by `Flezha.Operative.Overwatch.complete_init/3` before `:start_operative` to
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
  Sends a `OperationMessage` to the SDK-side operative via the control
  channel and returns the decoded `OperationMessageResponse`.

  Mirrors the `SegmentState` request/reply pattern: the compute engine's
  reply (a protobuf-encoded `OperationMessageResponse`) is delivered by
  `handle_payload/2` as the `raw_data` field of the outer `CommandResponse`.
  An empty `error` string is mapped to `{:ok, payload}`; a non-empty error
  becomes `{:error, error}`. Transport-layer or encoding failures surface as
  `{:error, reason}` unchanged.
  """
  @spec send_operation_message(binary(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def send_operation_message(executor_id, operative_id, payload)
      when is_binary(executor_id) and is_binary(operative_id) and is_binary(payload) do
    command = %ExecutorCommands.OperationMessage{
      operative_id: operative_id,
      payload: payload
    }

    case GenServer.call(
           Identificator.control_unit(executor_id),
           {:send_control, {:operation_message, command}}
         ) do
      {:ok, raw_data} -> decode_operation_message_response(raw_data)
      {:error, _} = err -> err
    end
  catch
    :exit, reason -> {:error, {:control_unit_exit, reason}}
  end

  defp decode_operation_message_response(raw_data) do
    case ExecutorCommands.OperationMessageResponse.decode(raw_data) do
      {:ok, %ExecutorCommands.OperationMessageResponse{error: "", payload: payload}} ->
        {:ok, payload}

      {:ok, %ExecutorCommands.OperationMessageResponse{error: err}} ->
        {:error, err}

      {:error, reason} ->
        {:error, {:decode_error, reason}}
    end
  end

  @doc """
  Cooperative external stop for a bounded operative (`flz operative stop`).

  Sends a `BoundedStop` command — distinct from the hard `RemOperative`: the SDK
  trips the bounded driver's cancellation token, lets in-flight work unwind, runs
  any saga compensation, then emits `BoundedCompleted` (status "cancelled") which
  drives the single teardown via `ControlChannel.handle_bounded_completed/2`.

  Fire-and-forget (mirrors `:stop_operative_async`) — the teardown is asynchronous,
  so the caller does not wait on the SDK ack. Returns `{:error, :no_control_unit}`
  when the executor's control unit is gone (caller falls back to a hard stop).
  """
  @spec bounded_stop(binary(), binary()) :: :ok | {:error, :no_control_unit}
  def bounded_stop(executor_id, operative_id)
      when is_binary(executor_id) and is_binary(operative_id) do
    control_unit = Identificator.control_unit(executor_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {:error, :no_control_unit}

      _ ->
        GenServer.cast(control_unit, {:bounded_stop, operative_id})
        :ok
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
  def handle_call({:get_segment_state, operative_id}, from, state) do
    case ControlChannel.send_control(
           state.executor_id,
           {
             :get_segment_state,
             %ExecutorCommands.SegmentState{operative_id: operative_id}
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
  def handle_call({:start_operative, operative}, from, state) do
    # TODO: Not sure if we should convert to map from the beginning.
    init_values =
      case operative.init_values !== %{} do
        true ->
          {:ok, json_iv} = Jason.encode(operative.init_values)
          json_iv

        false ->
          ""
      end

    # Empty kind ⇒ `:unbounded` on the SDK side, keeping the wire frame
    # byte-identical to pre-kind builds for the default kind.
    kind = if operative.kind in [nil, :unbounded], do: "", else: to_string(operative.kind)

    command = %ExecutorCommands.AddOperative{
      operative_id: operative.id,
      factory_id: operative.factory_id,
      init_values: init_values,
      kind: kind,
      # Non-zero only for a durable bounded (re)bind: the SDK driver holds its
      # first Operation until this many Segment restores land (crash-resume).
      # Stamped on opts by `Overwatch.with_resume_segment_count/3`.
      resume_segment_count: Keyword.get(operative.opts, :resume_segment_count, 0)
    }

    case ControlChannel.send_control(state.executor_id, {:add_operative, command}) do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:stop_operative, operative_id}, from, state) do
    case ControlChannel.send_control(
           state.executor_id,
           {
             :rem_operative,
             %ExecutorCommands.RemOperative{operative_id: operative_id}
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
  def handle_cast({:stop_operative_async, operative_id}, state) do
    ControlChannel.send_control_async(
      state.executor_id,
      {
        :rem_operative,
        %ExecutorCommands.RemOperative{operative_id: operative_id}
      }
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:bounded_stop, operative_id}, state) do
    ControlChannel.send_control_async(
      state.executor_id,
      {
        :bounded_stop,
        %ExecutorCommands.BoundedStop{operative_id: operative_id}
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

  # Unblock callers whose control reply never came (the executor wedged or the
  # transport dropped silently): fail the waiter with `:timeout` and drop the
  # ref so `response_refs` cannot grow unbounded. Caller-timeout only — executor
  # liveness/eviction is the `ControlChannel` watchdog's job, not this sweep.
  @impl true
  def handle_info(:heartbeat, state) do
    schedule_heartbeat()

    now = System.monotonic_time(:millisecond)
    timeout_ms = pending_reply_timeout_ms()
    stuck = stuck_reply_refs(state.response_refs, now, timeout_ms)

    if stuck == [] do
      {:noreply, state}
    else
      Logger.warning(
        "[ControlUnit] executor=#{state.executor_id} #{length(stuck)} control reply(s) " <>
          "unacked >= #{timeout_ms}ms — failing the stuck caller(s)"
      )

      refs = fail_stuck_refs(state.response_refs, stuck)
      {:noreply, %{state | response_refs: refs}}
    end
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  # Deferred replies outstanding past the threshold (ping refs self-clean).
  defp stuck_reply_refs(_refs, _now, timeout_ms) when timeout_ms <= 0, do: []

  defp stuck_reply_refs(refs, now, timeout_ms) do
    for {ref, {:reply, _from, at}} <- refs, now - at >= timeout_ms, do: ref
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

  defp schedule_heartbeat() do
    interval = heartbeat_interval_ms()
    if interval > 0, do: Process.send_after(self(), :heartbeat, interval)
  end

  defp heartbeat_interval_ms,
    do: Application.get_env(:flezha, :control_unit_heartbeat_interval_ms, 15_000)

  defp pending_reply_timeout_ms,
    do: Application.get_env(:flezha, :control_unit_pending_reply_timeout_ms, 90_000)
end

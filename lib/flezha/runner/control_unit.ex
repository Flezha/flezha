defmodule Flezha.Runner.ControlUnit do
  alias Flezha.Service.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.Service.ConfigManager

  use GenServer

  require Logger

  @default_reply_timeout_ms 5_000

  @spec start_link({binary(), binary()}) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link({namespace, runner_id}) do
    GenServer.start_link(
      __MODULE__,
      {namespace, runner_id},
      name: Identificator.control_unit(runner_id)
    )
  end

  @doc """
  Issues an end-to-end `Ping` round-trip to the runner's SDK. Returns
  `{:ok, :pong}` on a clean reply, `{:error, :timeout}` if the SDK does not
  reply within `timeout_ms` (default 2 s), or `{:error, reason}` for any
  transport or decode failure.

  Used by `Flezha.Operative.Lifecycle.complete_init/4` before `:start_operative` to
  prove the control channel is end-to-end healthy. A timeout here signals a
  stale `stream_ref` that the bound `ControlChannel` cannot detect on its
  own (writes succeed against a closed transport with no failure signal).
  """
  @spec ping_runner(binary(), keyword()) ::
          {:ok, :pong} | {:error, term()}
  def ping_runner(runner_id, opts \\ []) when is_binary(runner_id) do
    timeout_ms = Keyword.get(opts, :timeout_ms, default_ping_timeout_ms())

    control_unit = Identificator.control_unit(runner_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {:error, :no_control_unit}

      _ ->
        try do
          GenServer.call(control_unit, {:ping_runner, timeout_ms}, timeout_ms + 500)
        catch
          :exit, {:timeout, _} -> {:error, :timeout}
          :exit, reason -> {:error, {:control_unit_exit, reason}}
        end
    end
  end

  defp default_ping_timeout_ms,
    do: Application.get_env(:flezha, :overwatch_ping_timeout_ms, 2_000)

  @doc """
  Sends a `OperativeMessage` to the SDK-side operative via the control
  channel and returns the decoded `OperativeMessageResponse`.

  Mirrors the `EntityState` request/reply pattern: the compute engine's
  reply (a protobuf-encoded `OperativeMessageResponse`) is delivered by
  `handle_payload/2` as the `raw_data` field of the outer `CommandResponse`.
  An empty `error` string is mapped to `{:ok, payload}`; a non-empty error
  becomes `{:error, error}`. Transport-layer or encoding failures surface as
  `{:error, reason}` unchanged.
  """
  @spec send_operative_message(binary(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def send_operative_message(runner_id, operative_id, payload)
      when is_binary(runner_id) and is_binary(operative_id) and is_binary(payload) do
    command = %RunnerCommands.OperativeMessage{
      operative_id: operative_id,
      payload: payload
    }

    case send_control_await(runner_id, {:operative_message, command}) do
      {:ok, raw_data} -> decode_operative_message_response(raw_data)
      {:error, _} = err -> err
    end
  end

  @doc """
  Send a control `command` (a `{tag, %RunnerCommands.*{}}` pair) to the runner's
  ControlUnit and await the SDK's reply as raw bytes — the single request/reply
  seam every control round-trip decodes on top of. Returns `{:ok, raw_data}`
  (the `CommandResponse.raw_data` the caller decodes) or `{:error, reason}` on a
  timeout, transport failure, an unstarted ControlUnit, or a dead one.
  `send_operative_message/3`, the credit-window handshake, the operative
  handover round-trips, and the compaction response inspection all use it.

  Options: `:timeout` (default #{@default_reply_timeout_ms} ms) and
  `:metadata`. With `metadata: true` the reply is
  `{:ok, raw_data, frame_metadata}` — the reply frame's own metadata map, which
  is where an SDK answer carries anything that must not change the payload's
  shape (`EntityState` stamps `flz.change_seq` there for a single selected
  Entity).
  """
  @spec send_control_await(binary(), tuple(), keyword()) ::
          {:ok, binary()} | {:ok, binary(), map()} | {:error, term()}
  def send_control_await(runner_id, command, opts \\ []) when is_binary(runner_id) do
    # A control unit that was never started is a distinct failure from one that
    # is wedged, and the callers log or park on the difference. A wedged one
    # (waiting on a reply the dead transport will never deliver) is caught below,
    # so neither shape can crash the operative that asked.
    case GenServer.whereis(Identificator.control_unit(runner_id)) do
      nil ->
        {:error, "control unit not available for runner #{runner_id} with payload"}

      _pid ->
        GenServer.call(
          Identificator.control_unit(runner_id),
          {:send_control, command, Keyword.get(opts, :metadata, false)},
          Keyword.get(opts, :timeout, @default_reply_timeout_ms)
        )
    end
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, reason -> {:error, {:control_unit_exit, reason}}
  end

  defp decode_operative_message_response(raw_data) do
    case RunnerCommands.OperativeMessageResponse.decode(raw_data) do
      {:ok, %RunnerCommands.OperativeMessageResponse{error: "", payload: payload}} ->
        {:ok, payload}

      {:ok, %RunnerCommands.OperativeMessageResponse{error: err}} ->
        {:error, err}

      {:error, reason} ->
        {:error, {:decode_error, reason}}
    end
  end

  @doc """
  Cooperative external stop for a workflow operative (`flz operative stop`).

  Sends a `WorkflowStop` command — distinct from the hard `RemOperative`: the SDK
  trips the workflow driver's cancellation token, lets in-flight work unwind, runs
  any saga compensation, then emits `WorkflowCompleted` (status "cancelled") which
  drives the single teardown via `ControlChannel.handle_workflow_completed/2`.

  Fire-and-forget (mirrors `:stop_operative_async`) — the teardown is asynchronous,
  so the caller does not wait on the SDK ack. Returns `{:error, :no_control_unit}`
  when the runner's control unit is gone (caller falls back to a hard stop).
  """
  @spec workflow_stop(binary(), binary()) :: :ok | {:error, :no_control_unit}
  def workflow_stop(runner_id, operative_id)
      when is_binary(runner_id) and is_binary(operative_id) do
    cast_to_unit(runner_id, {:workflow_stop, operative_id})
  end

  @doc """
  Hard, fire-and-forget `RemOperative`: remove the operative from the runner
  without waiting for the SDK ack. The Overwatch's own teardown uses it, and so
  does the durable-suspension eviction — a suspended instance holds zero runner
  resources while its controller-side registry entry (and Overwatch) stay.
  """
  @spec stop_operative_async(binary(), binary()) :: :ok | {:error, :no_control_unit}
  def stop_operative_async(runner_id, operative_id)
      when is_binary(runner_id) and is_binary(operative_id) do
    cast_to_unit(runner_id, {:stop_operative_async, operative_id})
  end

  defp cast_to_unit(runner_id, message) do
    control_unit = Identificator.control_unit(runner_id)

    case GenServer.whereis(control_unit) do
      nil ->
        {:error, :no_control_unit}

      _ ->
        GenServer.cast(control_unit, message)
        :ok
    end
  end

  @impl true
  def init({namespace, runner_id}) do
    schedule_heartbeat()

    {
      :ok,
      %{
        namespace: namespace,
        runner_id: runner_id,
        response_refs: %{},
        env: Application.get_env(:flezha, :env)
      }
    }
  end

  @impl true
  def handle_call({:send_control, command, want_metadata}, from, state) do
    res = ControlChannel.send_control(state.runner_id, command)

    case res do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from, want_metadata)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:command_assign_runner_id, from, state) do
    case ControlChannel.send_control(
           state.runner_id,
           {
             :assign_runner_id,
             %RunnerCommands.AssignRunnerId{runner_id: state.runner_id}
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
           state.runner_id,
           {
             :connect_socket,
             %RunnerCommands.ConnectSocket{sockets_count: sockets_count}
           }
         ) do
      {:ok, ref} ->
        {:reply, :ok, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:ping_runner, timeout_ms}, from, state) do
    nonce = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    command = {:ping, %RunnerCommands.Ping{nonce: nonce}}

    case ControlChannel.send_control(state.runner_id, command) do
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

    # Empty kind ⇒ `:stream` on the SDK side, keeping the wire frame
    # byte-identical to pre-kind builds for the default kind.
    kind = if operative.kind in [nil, :stream], do: "", else: to_string(operative.kind)

    command = %RunnerCommands.AddOperative{
      operative_id: operative.id,
      factory_id: operative.factory_id,
      init_values: init_values,
      kind: kind,
      # Both stamped on opts by `Flezha.Operative.Lifecycle`. Non-zero only
      # for a durable workflow (re)bind: the SDK driver holds its first Procedure
      # until this many Entity restores land (crash-resume). `proc_journal` is
      # `nil` unless the instance has a journal to resume from.
      resume_entity_count: Keyword.get(operative.opts, :resume_entity_count, 0),
      proc_journal: Keyword.get(operative.opts, :proc_journal)
    }

    case ControlChannel.send_control(state.runner_id, {:add_operative, command}) do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:stop_operative, operative_id}, from, state) do
    case ControlChannel.send_control(
           state.runner_id,
           {
             :rem_operative,
             %RunnerCommands.RemOperative{operative_id: operative_id}
           }
         ) do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:disconnect_runner, from, state) do
    case ControlChannel.send_control(
           state.runner_id,
           {
             :runner_disconnect,
             %RunnerCommands.RunnerDisconnect{}
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
      state.runner_id,
      {
        :rem_operative,
        %RunnerCommands.RemOperative{operative_id: operative_id}
      }
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:workflow_stop, operative_id}, state) do
    ControlChannel.send_control_async(
      state.runner_id,
      {
        :workflow_stop,
        %RunnerCommands.WorkflowStop{operative_id: operative_id}
      }
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:handle_payload, payload, metadata}, state) do
    {:noreply, handle_payload(state, payload, metadata)}
  end

  defp handle_payload(state, payload, metadata) do
    {:ok, response} = RunnerCommands.CommandResponse.decode(payload)
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

    new_refs = handle_refs(state[:response_refs], ref, reply, metadata)

    Map.put(state, :response_refs, new_refs)
  end

  # Register the caller against the control-channel reference so the matching
  # `CommandResponse` (delivered later via `handle_payload/3`) is routed back.
  # The monotonic timestamp lets the heartbeat report the age of the oldest
  # outstanding reply — the backlog that grows silently on a dead transport.
  # `want_metadata` decides whether the reply also carries the reply frame's
  # metadata map.
  defp store_ref(state, ref, from, want_metadata \\ false) do
    entry = {:reply, from, System.monotonic_time(:millisecond), want_metadata}
    Map.put(state, :response_refs, Map.put(state.response_refs, ref, entry))
  end

  defp handle_refs(refs, ref, reply, metadata) do
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

      {:reply, from, _inserted_at, want_metadata} ->
        GenServer.reply(from, with_metadata(reply, metadata, want_metadata))
        Map.delete(refs, ref)
    end
  end

  defp with_metadata({:ok, raw_data}, metadata, true), do: {:ok, raw_data, metadata || %{}}
  defp with_metadata(reply, _metadata, _want), do: reply

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

  # Unblock callers whose control reply never came (the runner wedged or the
  # transport dropped silently): fail the waiter with `:timeout` and drop the
  # ref so `response_refs` cannot grow unbounded. Caller-timeout only — runner
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
        "[ControlUnit] runner=#{state.runner_id} #{length(stuck)} control reply(s) " <>
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
    for {ref, {:reply, _from, at, _want_metadata}} <- refs, now - at >= timeout_ms, do: ref
  end

  defp fail_stuck_refs(refs, stuck) do
    Enum.reduce(stuck, refs, fn ref, acc ->
      case Map.get(acc, ref) do
        {:reply, from, _at, _want_metadata} ->
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

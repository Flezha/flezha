defmodule Flezha.Runner.ControlUnit do
  @moduledoc """
  The controller's request side of one runner's control channel.

  Every controller-initiated command — assign runner id, connect sockets, add
  or remove a relay, ping, partition, compaction, steps stop — is built
  here, handed to `ControlChannel` to write, and its response matched back by
  reference id.

  Invariant: a command is in flight at most once per reference id, and a
  response that arrives for an id nothing is waiting on is dropped rather than
  guessed at.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.SDK.SafeCall

  use GenServer
  use Flezha.Process.Timer, keys: [:heartbeat]

  require Logger

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

  Used by `Flezha.Relay.Lifecycle.complete_init/5` before `:start_relay` to
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
        SafeCall.bounded(control_unit, {:ping_runner, timeout_ms}, timeout_ms + 500,
          on_error: refusal()
        )
    end
  end

  # One refusal for every call into this runner's control unit: a wedged unit
  # is a timeout the caller can retry, anything else names the exit it died of.
  # A function rather than an attribute, because the `other` arm quotes the raw
  # exit reason and a closure cannot be escaped into one.
  defp refusal(),
    do: %{timeout: {:error, :timeout}, other: &{:error, {:control_unit_exit, &1}}}

  defp default_ping_timeout_ms,
    do: Durations.get(:overwatch_ping_timeout_ms)

  @doc """
  Sends a `RelayMessage` to the SDK-side relay via the control
  channel and returns the decoded `RelayMessageResponse`.

  Mirrors the `CellState` request/reply pattern: the compute engine's
  reply (a protobuf-encoded `RelayMessageResponse`) is delivered by
  `handle_payload/2` as the `raw_data` field of the outer `CommandResponse`.
  An empty `error` string is mapped to `{:ok, payload}`; a non-empty error
  becomes `{:error, error}`. Transport-layer or encoding failures surface as
  `{:error, reason}` unchanged.
  """
  @spec send_relay_message(binary(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def send_relay_message(runner_id, relay_id, payload)
      when is_binary(runner_id) and is_binary(relay_id) and is_binary(payload) do
    command = %RunnerCommands.RelayMessage{
      relay_id: relay_id,
      payload: payload
    }

    case send_control_await(runner_id, {:relay_message, command}) do
      {:ok, raw_data} -> decode_message_response(raw_data)
      {:error, _} = err -> err
    end
  end

  @doc """
  Send a control `command` (a `{tag, %RunnerCommands.*{}}` pair) to the runner's
  ControlUnit and await the SDK's reply as raw bytes — the single request/reply
  seam every control round-trip decodes on top of. Returns `{:ok, raw_data}`
  (the `CommandResponse.raw_data` the caller decodes) or `{:error, reason}` on a
  timeout, transport failure, an unstarted ControlUnit, or a dead one.
  `send_relay_message/3`, the credit-window handshake, the relay
  handover round-trips, and the compaction response inspection all use it.

  Options: `:timeout` (default #{Durations.get(:control_unit_reply_timeout_ms)} ms) and
  `:metadata`. With `metadata: true` the reply is
  `{:ok, raw_data, frame_metadata}` — the reply frame's own metadata map, which
  is where an SDK answer carries anything that must not change the payload's
  shape (`CellState` stamps `flz.change_seq` there for a single selected
  Cell).
  """
  @spec send_control_await(binary() | nil, tuple(), keyword()) ::
          {:ok, binary()} | {:ok, binary(), map()} | {:error, term()}
  def send_control_await(runner_id, command, opts \\ [])

  # A relay with nothing bound has no control unit either, which is the
  # same answer as one whose unit was never started. Callers reach this on
  # every path that talks to "the relay's runner" while it has none — a
  # migration arriving or leaving during a group change, a snapshot of an
  # unbound relay — and none of them may be crashed by it.
  def send_control_await(runner_id, _command, _opts) when not is_binary(runner_id),
    do: {:error, :no_control_unit}

  def send_control_await(runner_id, command, opts) do
    # A control unit that was never started is a distinct failure from one that
    # is wedged, and the callers log or park on the difference. A wedged one
    # (waiting on a reply the dead transport will never deliver) is caught below,
    # so neither shape can crash the relay that asked.
    case GenServer.whereis(Identificator.control_unit(runner_id)) do
      nil ->
        # An atom, like the ping path's: a caller has to tell "the runner never
        # answered" from "the SDK answered with an error", and only the latter
        # is a binary joined from the reply's `errors`.
        {:error, :no_control_unit}

      _pid ->
        SafeCall.bounded(
          Identificator.control_unit(runner_id),
          {:send_control, command, Keyword.get(opts, :metadata, false)},
          Keyword.get(opts, :timeout, Durations.get(:control_unit_reply_timeout_ms)),
          on_error: refusal()
        )
    end
  end

  @doc """
  Disconnect `runner_id`, awaiting its acknowledgement.

  `terminal: true` makes the SDK run its graceful shutdown and then exit
  NON-ZERO, so the orchestrator that owns process lifecycle restarts it — the
  rung above `evict_runner`, which moves a wedged runner's relays but
  leaves the process connected and placeable. `reason` rides along for the
  runner's own log and is never interpreted by the SDK.

  Bounded and crash-proof through `send_control_await/3`: the target may be
  exactly the wedged process the caller is trying to clear.
  """
  @spec disconnect(binary(), keyword()) ::
          {:ok, binary()} | {:ok, binary(), map()} | {:error, term()}
  def disconnect(runner_id, opts \\ []) when is_binary(runner_id) do
    send_control_await(
      runner_id,
      {:runner_disconnect,
       %RunnerCommands.RunnerDisconnect{
         terminal: Keyword.get(opts, :terminal, false),
         reason: Keyword.get(opts, :reason, "")
       }},
      opts
    )
  end

  # The runner renders its refusal from the same catalogue; a reply that says
  # "error" and names none is still an error.
  defp runner_error([first | _rest]), do: Flezha.Error.from_wire(first)
  defp runner_error([]), do: Flezha.Error.new("runner.command_failed")

  defp decode_message_response(raw_data) do
    case RunnerCommands.RelayMessageResponse.decode(raw_data) do
      {:ok, %RunnerCommands.RelayMessageResponse{error: nil, payload: payload}} ->
        {:ok, payload}

      {:ok, %RunnerCommands.RelayMessageResponse{error: err}} ->
        {:error, Flezha.Error.from_wire(err)}

      {:error, reason} ->
        {:error, {:decode_error, reason}}
    end
  end

  @doc """
  Cooperative external stop for a steps relay (`flz relay stop`).

  Sends a `StepsStop` command — distinct from the hard `RemRelay`: the SDK
  trips the steps driver's cancellation token, lets in-flight work unwind, runs
  any saga compensation, then emits `RelayCompleted` (status "cancelled") which
  drives the single teardown via `ControlChannel.handle_relay_completed/2`.

  Fire-and-forget (mirrors `:stop_relay_async`) — the teardown is asynchronous,
  so the caller does not wait on the SDK ack. Returns `{:error, :no_control_unit}`
  when the runner's control unit is gone (caller falls back to a hard stop).
  """
  @spec steps_stop(binary(), binary()) :: :ok | {:error, :no_control_unit}
  def steps_stop(runner_id, relay_id)
      when is_binary(runner_id) and is_binary(relay_id) do
    cast_to_unit(runner_id, {:steps_stop, relay_id})
  end

  @doc """
  Hard, fire-and-forget `RemRelay`: remove the relay from the runner
  without waiting for the SDK ack. The Overwatch's own teardown uses it, and so
  does the durable-suspension eviction — a suspended instance holds zero runner
  resources while its controller-side registry entry (and Overwatch) stay.
  """
  @spec stop_relay_async(binary(), binary()) :: :ok | {:error, :no_control_unit}
  def stop_relay_async(runner_id, relay_id)
      when is_binary(runner_id) and is_binary(relay_id) do
    cast_to_unit(runner_id, {:stop_relay_async, relay_id})
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
    {
      :ok,
      arm_heartbeat(%{
        namespace: namespace,
        runner_id: runner_id,
        response_refs: %{},
        env: Config.get(:env)
      })
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

  # The two handshake commands are answered with what the runner answered, as
  # every other command is: a caller told `:ok` on the write alone is a caller
  # that records a refused connect as a working one.
  @impl true
  def handle_call(:command_assign_runner_id, from, state) do
    command = %RunnerCommands.AssignRunnerId{runner_id: state.runner_id}
    handle_call({:send_control, {:assign_runner_id, command}, false}, from, state)
  end

  @impl true
  def handle_call(:command_sockets_connect, from, state) do
    command = %RunnerCommands.ConnectSocket{sockets_count: Config.get(:concurrent_channel_count)}
    handle_call({:send_control, {:connect_socket, command}, false}, from, state)
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
  def handle_call({:start_relay, relay}, from, state) do
    init_values =
      case relay.init_values !== %{} do
        true ->
          {:ok, json_iv} = Jason.encode(relay.init_values)
          json_iv

        false ->
          ""
      end

    command = %RunnerCommands.AddRelay{
      relay_id: relay.id,
      factory_id: relay.factory_id,
      init_values: init_values,
      # The class says how it runs; the SDK does not read this.
      run: Atom.to_string(relay.run || :flow),
      # Both stamped on opts by `Flezha.Relay.Lifecycle`. Non-zero only
      # for a durable steps relay (re)bind: the SDK driver holds its first Tasklet
      # until this many Cell restores land (crash-resume). `task_journal` is
      # `nil` unless the instance has a journal to resume from.
      resume_cell_count: Keyword.get(relay.opts, :resume_cell_count, 0),
      inbox_tail_count: Keyword.get(relay.opts, :inbox_tail_count, 0),
      effect_result_count: length(Keyword.get(relay.opts, :effect_results, [])),
      task_journal: Keyword.get(relay.opts, :task_journal),
      # This controller parks a failed turn a steps driver sends on
      # `IterCommit.dead_letter`, whatever the bind.
      dead_letter_turns: true
    }

    command = with_partition_identity(command, relay.opts)

    case ControlChannel.send_control(state.runner_id, {:add_relay, command}) do
      {:ok, ref} ->
        {:noreply, store_ref(state, ref, from)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:stop_relay, relay_id}, from, state) do
    case ControlChannel.send_control(
           state.runner_id,
           {
             :rem_relay,
             %RunnerCommands.RemRelay{relay_id: relay_id}
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
      state.runner_id,
      {
        :rem_relay,
        %RunnerCommands.RemRelay{relay_id: relay_id}
      }
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:steps_stop, relay_id}, state) do
    ControlChannel.send_control_async(
      state.runner_id,
      {
        :steps_stop,
        %RunnerCommands.StepsStop{relay_id: relay_id}
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
          error = runner_error(response.errors)

          if state[:env] !== :test do
            Logger.error("runner refused a command: #{error.message} (#{error.code})")
          end

          {:error, error}
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
  # A partition member's owned key-group range, which arms the SDK's
  # mis-route guard. Left untouched for an ordinary relay, so its
  # `AddRelay` stays byte-identical: proto3 omits zero-valued scalars.
  defp with_partition_identity(command, opts) do
    case Flezha.Partition.identity(opts) do
      %{index: index, count: count, range: {from, to}} when count >= 2 ->
        %{
          command
          | partition_index: index,
            partition_count: count,
            key_group_from: from,
            key_group_to: to
        }

      _ ->
        command
    end
  end

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
  # ref so `response_refs` cannot grow flow. Caller-timeout only — runner
  # liveness/eviction is the `ControlChannel` watchdog's job, not this sweep.
  @impl true
  def handle_info(:heartbeat, state) do
    state = arm_heartbeat(state)

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

  # A non-positive interval turns the heartbeat off: nothing is armed and the
  # key simply holds no timer.
  defp arm_heartbeat(state) do
    interval = heartbeat_interval_ms()
    if interval > 0, do: arm_timer(state, :heartbeat, interval, :heartbeat), else: state
  end

  defp heartbeat_interval_ms,
    do: Durations.get(:control_unit_heartbeat_interval_ms)

  defp pending_reply_timeout_ms,
    do: Durations.get(:control_unit_pending_reply_timeout_ms)
end

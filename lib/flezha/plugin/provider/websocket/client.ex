defmodule Flezha.Plugin.Provider.Websocket.Client do
  @moduledoc """
  The Fresh socket process behind the websocket provider: the connection
  callbacks, frame ingest, and the idle/resubscribe watchdog.

  It owns the socket and its timers. What the socket ASSERTS upstream — the
  desired set, the reconcile cycle and the ack-gated commit — is
  `Websocket.Subscription`'s, and this module never reaches back into
  `Websocket`.
  """

  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.Websocket.Subscription

  require Logger

  use Fresh

  @doc "Default idle-watchdog deadline, in ms."
  @spec default_idle_timeout_ms() :: non_neg_integer()
  def default_idle_timeout_ms, do: Durations.get(:ws_idle_timeout_ms)

  @doc "Default data-liveness re-subscribe deadline, in ms. 0 disables it."
  @spec default_resubscribe_after_ms() :: non_neg_integer()
  def default_resubscribe_after_ms, do: Durations.get(:ws_resubscribe_after_ms)

  @doc """
  The socket process, started by the extension table at activation from the
  provider's declared worker spec. Every value it reads is a declared option
  already converted and defaulted, so this holds no default of its own.
  """
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts) do
    now = :erlang.monotonic_time(:millisecond)

    state = %{
      uri: opts.uri,
      plugin_id: opts.plugin_id,
      post_connect: opts.post_connect,
      overwatcher_pid: opts.overwatcher_pid,
      payload_metadata: opts.payload_metadata,
      idle_timeout_ms: opts.idle_timeout_ms,
      resubscribe_after_ms: opts.resubscribe_after_ms,
      ignore_pattern: compile_ignore_pattern(opts.ignore_pattern),
      # Effectively-once tier (from the activation env): ingest via a
      # blocking call so a zero-credit park stops the socket read.
      sync_ingest?: opts.sync_ingest?,
      websocket_established?: false,
      frames_seen: 0,
      last_frame_at: now,
      last_data_frame_at: now,
      last_resubscribe_at: now,
      reconnect_attempt: 0
    }

    Fresh.start_link(
      opts.uri,
      __MODULE__,
      with_subscription_state(state, opts),
      Keyword.put(reconnect_opts(opts.ws_opts), :name, opts.name)
    )
  end

  # The socket-level reconnect is the `:ws_reconnect` row: the library walks
  # the schedule, and the row is what says where it starts and where it stops.
  # An operator's own `ws_opts` still wins — the row is the default, not a
  # ceiling.
  defp reconnect_opts(ws_opts) do
    row = Backoff.ladder(:ws_reconnect)

    (ws_opts || [])
    |> Keyword.put_new(:backoff_initial, row.base)
    |> Keyword.put_new(:backoff_max, row.cap)
  end

  # The reconcile-loop fields exist ONLY on a plugin that configured a source.
  # That is what keeps the promise that an unconfigured plugin pays nothing:
  # `handle_in/2`'s ack clause matches on `:inflight`, so its head simply does
  # not match here, and no timer is ever scheduled.
  defp with_subscription_state(state, opts) do
    case opts.subscription do
      nil ->
        state

      subscription ->
        Map.merge(state, %{
          subscription: subscription,
          applied: MapSet.new(opts.applied_subscriptions),
          # A literal item list is the desired set from the start; a polled
          # one has none until a poll lands.
          desired: Subscription.static_desired(subscription),
          inflight: %{},
          next_req_id: 1,
          cycle: 0,
          poll: nil,
          poll_timer: nil,
          source_etag: nil,
          last_reconcile: nil,
          last_error: nil,
          source_plugin_id: opts.source_plugin_id,
          source_namespace: opts.source_namespace,
          source_operative_id: opts.source_operative_id
        })
    end
  end

  # Compile the substring matcher once at start so the per-frame path
  # runs `:binary.match/2` over a precompiled pattern rather than
  # recompiling on every frame. `nil` means "no pattern" — every data
  # frame counts as data with no scan at all.
  defp compile_ignore_pattern(pattern) when is_binary(pattern) and pattern != "" do
    :binary.compile_pattern(pattern)
  end

  defp compile_ignore_pattern(_), do: nil

  @impl Fresh
  def handle_terminate(reason, _state) do
    Logger.info("Websocket terminating: #{inspect(reason)}")
    :ok
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    Logger.info("Connected to websocket!")

    now = :erlang.monotonic_time(:millisecond)

    state =
      %{
        state
        | websocket_established?: true,
          last_frame_at: now,
          last_data_frame_at: now,
          last_resubscribe_at: now
      }
      |> Map.put(:reconnect_attempt, Backoff.reset(:ws_reconnect, state.plugin_id))

    schedule_idle_check(state)
    state = Subscription.schedule_subscription_poll(state)

    # Issue the subscribe via Fresh's reply form so the frame is sent
    # after the upgrade lands. A re-entrant `Fresh.send/2` here races the
    # upgrade and crashes with `{:badkey, :extensions, nil}`.
    #
    # Re-assert what is already applied, then converge on whatever the desired
    # set says beyond it — which is what makes a literal `subscription_items`
    # source subscribe with no poll to wait for. A polled source has no desired
    # set yet on a first connect, so the reconcile is a no-op there.
    {state, deltas} = Subscription.reconcile(state)

    case Subscription.subscription_frames(state) ++ deltas do
      [] -> {:ok, state}
      frames -> {:reply, frames, state}
    end
  end

  # Fresh owns the reconnect timer; the row owns the schedule it walks, and
  # this is where the wait becomes visible — the delay announced here is the
  # one the library is about to sleep.
  @impl Fresh
  def handle_disconnect(_code, _reason, state) do
    {:reconnect, announce_reconnect(state)}
  end

  @impl Fresh
  def handle_error(reason, state) do
    Logger.warning("Websocket error: #{inspect(reason)}")
    {:reconnect, announce_reconnect(state)}
  end

  defp announce_reconnect(state) do
    attempt = Map.get(state, :reconnect_attempt, 0) + 1
    _ = Backoff.delay(:ws_reconnect, attempt) |> announce_wait(attempt, state.plugin_id)
    state |> Map.put(:websocket_established?, false) |> Map.put(:reconnect_attempt, attempt)
  end

  defp announce_wait(delay, attempt, plugin_id) do
    :telemetry.execute([:flezha, :backoff, :wait], %{delay_ms: delay, attempt: attempt}, %{
      ladder: :ws_reconnect,
      target: plugin_id
    })

    delay
  end

  # Any control frame (ping/pong) proves the socket is alive, so it
  # refreshes the connection-liveness timer without touching data
  # liveness. Fresh auto-replies to pings before this callback runs.
  @impl Fresh
  def handle_control({:pong, _}, state) do
    {:ok, %{state | last_frame_at: :erlang.monotonic_time(:millisecond)}}
  end

  @impl Fresh
  def handle_control(control_frame, state) do
    Logger.warning("Received unexpected control frame: #{inspect(control_frame)}")
    {:ok, %{state | last_frame_at: :erlang.monotonic_time(:millisecond)}}
  end

  # A frame that acknowledges an outstanding subscription request commits its
  # delta. The clause head matches on `:inflight`, so a plugin with no
  # subscription source never reaches it — the key is not in its state — and a
  # configured plugin with nothing outstanding falls through on the empty-map
  # guard. The frame is then handled exactly as any other, so an acknowledgement
  # affects `last_data_frame_at` only through the existing `ignore_pattern`
  # classification.
  @impl Fresh
  def handle_in({:text, message} = frame, %{inflight: inflight} = state)
      when inflight != %{} do
    ingest(frame, Subscription.commit_acknowledged(state, message))
  end

  @impl Fresh
  def handle_in(frame, state), do: ingest(frame, state)

  defp ingest(
         {type, message},
         %{overwatcher_pid: overwatcher, payload_metadata: metadata} = state
       ) do
    # Effectively-once tier ingest is confirming: awaiting the journal ack means
    # a zero-credit park blocks this connection process, so the socket stops
    # being read (TCP back-pressure) instead of the input log growing unbounded.
    mode = if Map.get(state, :sync_ingest?, false), do: :confirming, else: :fire_and_forget
    _ = Host.inject(overwatcher, message, metadata, mode)

    # One clock read and one classification per frame; `last_frame_at`
    # is set unconditionally (connection liveness) and `last_data_frame_at`
    # only when the frame is real data (subscription liveness).
    now = :erlang.monotonic_time(:millisecond)
    counter = state.frames_seen + 1
    is_data? = data_frame?(type, message, state.ignore_pattern)

    if rem(counter, 1000) == 0 do
      sample = String.slice(message, 0, 120)

      Logger.debug(
        "[Plugin.Provider.Websocket] frames_seen=#{counter} plugin_id=#{inspect(state.plugin_id)} last_msg_size=#{byte_size(message)} sample=#{inspect(sample)}"
      )
    end

    state = %{state | frames_seen: counter, last_frame_at: now}
    state = if is_data?, do: %{state | last_data_frame_at: now}, else: state

    {:ok, state}
  end

  # Data-vs-heartbeat classification (drives `last_data_frame_at`):
  #   * No `ignore_pattern` configured → every frame counts as data
  #     (no scan at all).
  #   * Binary frames → always data (the pattern is a text substring;
  #     scanning opaque payloads would be a false positive).
  #   * Text frames containing the pattern → heartbeat-shaped, not data.
  defp data_frame?(_type, _message, nil), do: true

  defp data_frame?(:text, message, pattern) when is_binary(message) do
    :binary.match(message, pattern) == :nomatch
  end

  defp data_frame?(_type, _message, _pattern), do: true

  # Two-tier watchdog tick:
  #   1. connection dead (`now - last_frame_at >= idle_timeout_ms`) → reconnect
  #   2. live but data stale (`resubscribe_after_ms` window elapsed) → re-subscribe
  #   3. otherwise → reschedule
  @impl Fresh
  def handle_info(:idle_check, state) do
    now = :erlang.monotonic_time(:millisecond)

    cond do
      # While Fresh is reconnecting there is no socket to police and
      # last_frame_at stays stale until handle_connect resets it; gate the
      # idle tier on an established socket so it can't tear down a healthy
      # reconnect with :idle_timeout.
      state.websocket_established? and state.idle_timeout_ms > 0 and
          now - state.last_frame_at >= state.idle_timeout_ms ->
        reconnect_dead_connection(state, now - state.last_frame_at)

      state.resubscribe_after_ms > 0 and
        now - state.last_data_frame_at >= state.resubscribe_after_ms and
          now - state.last_resubscribe_at >= state.resubscribe_after_ms ->
        resubscribe_stale_data(state, now, now - state.last_data_frame_at)

      true ->
        schedule_idle_check(state)
        {:ok, state}
    end
  end

  # Liveness probe for the health contract (Phase 7.1): report the two-tier
  # watchdog's current silence ages without disturbing them. Off the data path.
  @impl Fresh
  def handle_info({:liveness_probe, from, ref}, state) do
    now = :erlang.monotonic_time(:millisecond)
    send(from, {ref, {now - state.last_frame_at, now - state.last_data_frame_at}})
    {:ok, state}
  end

  def handle_info({:subscription_probe, from, ref}, %{subscription: _} = state) do
    send(from, {ref, Subscription.subscription_report(state)})
    {:ok, state}
  end

  # A tick that lands on an outstanding request is dropped rather than queued:
  # the running fetch schedules the next one when it settles, so a slow
  # endpoint stretches the cadence instead of stacking concurrent requests.
  def handle_info(:subscription_poll, %{subscription: subscription, poll: nil} = state) do
    connection = self()

    request = Subscription.fetch_request(subscription, state.source_etag)

    # The fetch runs in a child so the connection process keeps reading the
    # socket while a slow or hanging source is outstanding.
    {pid, ref} =
      spawn_monitor(fn ->
        send(connection, {:subscription_fetched, self(), HttpFetch.fetch(request)})
      end)

    {:ok, %{state | poll: {pid, ref}}}
  end

  def handle_info(:subscription_poll, %{subscription: _} = state), do: {:ok, state}

  def handle_info({:subscription_fetched, pid, result}, %{poll: {pid, ref}} = state) do
    Process.demonitor(ref, [:flush])
    Subscription.settle_poll(%{state | poll: nil}, result)
  end

  def handle_info({:DOWN, ref, :process, _dead, reason}, %{poll: {_pid, ref}} = state) do
    Subscription.settle_poll(%{state | poll: nil}, {:error, {:fetch_crashed, reason}})
  end

  def handle_info(any, state) do
    Logger.warning("Received unexpected info at websocket process: #{inspect(any)}")
    {:ok, state}
  end

  defp reconnect_dead_connection(state, silence_ms) do
    Logger.warning(
      "[Plugin.Provider.Websocket] idle watchdog firing for plugin_id=" <>
        "#{inspect(state.plugin_id)} silence_ms=#{silence_ms} reason=idle_timeout"
    )

    :telemetry.execute(
      [:flezha, :ws, :idle_reconnect],
      %{silence_ms: silence_ms},
      %{plugin_id: state.plugin_id}
    )

    # The plugin manager monitors this pid; an abnormal-reason exit is
    # handled by `Flezha.Plugin.Extension.Table.handle_down/5`, which
    # calls `attempt_restart` and re-runs `init_provider/1` to spin up
    # a fresh Fresh.Connection with the subscription re-sent.
    Process.exit(self(), {:shutdown, :idle_timeout})
    {:ok, state}
  end

  defp resubscribe_stale_data(state, now, data_silence_ms) do
    # Rate-limit guard advances even when the send is skipped, so a
    # transiently-unestablished socket re-tries at most once per window.
    state = %{state | last_resubscribe_at: now}
    schedule_idle_check(state)

    with true <- Subscription.established(state, "re-subscribe"),
         [_ | _] = frames <- Subscription.subscription_frames(state) do
      :telemetry.execute(
        [:flezha, :ws, :resubscribe],
        %{data_silence_ms: data_silence_ms},
        %{plugin_id: state.plugin_id}
      )

      {:reply, frames, state}
    else
      _ -> {:ok, state}
    end
  end

  @doc """
  The watchdog tick interval for `state`, or `nil` when both tiers are off.

  Half the nearest enabled deadline so neither tier is observed late, floored
  so a small deadline cannot produce a busy tick.
  """
  @spec idle_tick_interval(map()) :: pos_integer() | nil
  def idle_tick_interval(state) do
    case Enum.filter([state.idle_timeout_ms, state.resubscribe_after_ms], &(&1 > 0)) do
      [] -> nil
      enabled -> max(Durations.get(:ws_min_tick_interval_ms), div(Enum.min(enabled), 2))
    end
  end

  # One meaning for every "milliseconds, non-negative, 0 disables" option:
  # clamp a negative to 0, and fall back to the default for anything that is
  # not a number at all. Reached from stored opts rather than a CLI string,
  # which is why it is silent where `convert_value/2` warns.

  @doc false
  def schedule_idle_check(state) do
    case idle_tick_interval(state) do
      nil ->
        :ok

      tick ->
        Process.send_after(self(), :idle_check, tick)
        :ok
    end
  end

  # Its own timer, independent of the watchdog tick: the two cadences answer
  # different questions and a source poll must not be paced by a liveness
  # deadline. No source configured means no timer at all.
  #
  # Idempotent by cancelling first, because `handle_connect/3` runs on EVERY
  # reconnect while a poll chain from the previous connection is still armed.
  # Without the cancel each reconnect would leave another chain running and the
  # source would be polled twice, then four times, then eight.
end

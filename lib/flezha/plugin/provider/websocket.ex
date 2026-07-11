defmodule Flezha.Plugin.Provider.Websocket do
  @moduledoc """
  First-party Websocket provider. `init_provider/1` starts a Fresh WebSocket
  connection process; incoming frames are cast as `{:io_in, msg, metadata}`
  to the overwatcher pid. The plugin subscribes to `:on_terminate` so the
  connection is closed cleanly when the plugin is disabled.

  The plugin runs a two-tier application-level watchdog that separates
  *connection liveness* from *data liveness*:

    * `last_frame_at` is refreshed by **every** inbound frame — data
      frames (`handle_in/2`), `ignore_pattern`-matching heartbeats, and
      control frames (`handle_control/2` ping/pong). When
      `now - last_frame_at >= idle_timeout_ms` the connection is treated
      as dead: the Fresh process exits with `{:shutdown, :idle_timeout}`,
      the plugin manager observes the abnormal exit and restarts the
      plugin via its retry path (`Flezha.Plugin.Extension.Table.handle_down`).
    * `last_data_frame_at` is refreshed only by non-`ignore_pattern` data
      frames. When `resubscribe_after_ms > 0`, the connection is alive,
      and `now - last_data_frame_at >= resubscribe_after_ms`, the plugin
      resends its `post_connect` payload on the existing connection
      (no teardown, no reconnect), rate-limited to once per window by
      `last_resubscribe_at`.

  A heartbeat — or any inbound frame — therefore holds the socket open
  even when no market data is flowing. `ignore_pattern` (string) still
  classifies a text data frame as heartbeat-shaped when its payload
  contains the pattern as a substring; such frames refresh only
  `last_frame_at`. Binary frames never match the pattern; when no pattern
  is configured every data frame counts as data.

  Per-frame liveness tracking is O(1) and allocation-free: each frame is
  classified exactly once against a `:binary.compile_pattern/1` matcher
  built at start, the monotonic clock is read once and reused for both
  timestamps, and every hot-path field is pre-initialised so updates use
  the known-key `%{state | …}` form.

  Each forced reconnect emits `[:flezha, :ws, :idle_reconnect]`
  (`%{silence_ms: integer()}`, `%{plugin_id: binary()}`); each in-place
  re-subscribe emits `[:flezha, :ws, :resubscribe]`
  (`%{data_silence_ms: integer()}`, `%{plugin_id: binary()}`).

  Required opts: `:uri`.
  Optional opts: `:post_connect`, `:ws_opts`, `:payload_metadata`,
  `:idle_timeout_ms` (default 30_000; set to 0 to disable the connection
  watchdog), `:resubscribe_after_ms` (default 0 = disabled),
  `:ignore_pattern` (string substring, default unset),
  `:plugin_id` (label for telemetry/logs; defaults to `:uri`).
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  use Fresh
  use ProviderBehaviour

  @default_idle_timeout_ms 30_000
  @default_resubscribe_after_ms 0
  @min_tick_interval_ms 1_000

  @enforce_keys [:uri]
  defstruct @enforce_keys ++
              [
                :conn,
                :plugin_id,
                :ignore_pattern,
                post_connect: %{},
                ws_opts: [],
                payload_metadata: %{},
                idle_timeout_ms: @default_idle_timeout_ms,
                resubscribe_after_ms: @default_resubscribe_after_ms
              ]

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:ws_opts, value) when is_binary(value) do
    Jason.decode(value)
  end

  defp convert_value(:payload_metadata, value) when is_binary(value) do
    Jason.decode(value)
  end

  # `handle_connect/3` indexes `state.post_connect["payload"]`, so the value
  # must be a map. CLI flags arrive as strings — decode them here.
  defp convert_value(:post_connect, value) when is_binary(value) and value != "" do
    Jason.decode(value)
  end

  defp convert_value(:post_connect, _), do: {:ok, %{}}

  defp convert_value(:idle_timeout_ms, value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} ->
        {:ok, max(n, 0)}

      _ ->
        Logger.warning(
          "[Plugin.Provider.Websocket] invalid idle_timeout_ms=#{inspect(value)} — " <>
            "falling back to default #{@default_idle_timeout_ms} ms"
        )

        {:ok, @default_idle_timeout_ms}
    end
  end

  defp convert_value(:idle_timeout_ms, value) when is_integer(value) do
    {:ok, max(value, 0)}
  end

  defp convert_value(:resubscribe_after_ms, value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} ->
        {:ok, max(n, 0)}

      _ ->
        Logger.warning(
          "[Plugin.Provider.Websocket] invalid resubscribe_after_ms=#{inspect(value)} — " <>
            "falling back to default #{@default_resubscribe_after_ms}"
        )

        {:ok, @default_resubscribe_after_ms}
    end
  end

  defp convert_value(:resubscribe_after_ms, value) when is_integer(value) do
    {:ok, max(value, 0)}
  end

  defp convert_value(:ignore_pattern, value) when is_binary(value) and value != "" do
    {:ok, value}
  end

  defp convert_value(:ignore_pattern, _), do: {:ok, nil}

  # init_provider receives `opts` from Flezha.Plugin, where the value may be
  # a raw JSON string (legacy CLI path), a map (installer/new path), or nil.
  # Coerce all of those to a map so handle_connect/3 can safely index into it.
  defp normalize_post_connect(value) when is_map(value), do: value

  defp normalize_post_connect(value) when is_binary(value) and value != "" do
    case Jason.decode(value) do
      {:ok, decoded} when is_map(decoded) -> decoded
      _ -> %{}
    end
  end

  defp normalize_post_connect(_), do: %{}

  # `init_provider/1` validates and normalises config; it does NOT open the
  # upstream WebSocket. The TLS handshake + subscribe round-trip moves to
  # `activate/1`, called by `Flezha.Plugin.Extension.Table.bulk_activate/1`
  # after the operative's SDK side has confirmed bound. See `operative-extensions`
  # "Two-phase plugin lifecycle gated on SDK readiness".
  def init_provider(opts) do
    uri = Map.fetch!(opts, :uri)
    idle_timeout_ms = normalize_idle_timeout(Map.get(opts, :idle_timeout_ms))
    resubscribe_after_ms = normalize_resubscribe_after(Map.get(opts, :resubscribe_after_ms))
    ignore_pattern = normalize_ignore_pattern(Map.get(opts, :ignore_pattern))
    plugin_id = Map.get(opts, :plugin_id) || uri

    Logger.info(
      "[Plugin.Provider.Websocket] init_provider prepared uri=#{inspect(uri)} plugin_id=#{inspect(plugin_id)}"
    )

    opts =
      opts
      |> Map.put(:overwatcher_pid, self())
      |> Map.put(:post_connect, normalize_post_connect(Map.get(opts, :post_connect)))
      |> Map.put(:payload_metadata, Map.get(opts, :payload_metadata, %{}))
      |> Map.put(:ws_opts, Map.get(opts, :ws_opts, []))
      |> Map.put(:idle_timeout_ms, idle_timeout_ms)
      |> Map.put(:resubscribe_after_ms, resubscribe_after_ms)
      |> Map.put(:ignore_pattern, ignore_pattern)
      |> Map.put(:plugin_id, plugin_id)

    {:ok, opts}
  end

  def activate(opts) when is_map(opts) do
    uri = Map.fetch!(opts, :uri)
    plugin_id = Map.fetch!(opts, :plugin_id)
    overwatcher_pid = Map.fetch!(opts, :overwatcher_pid)

    Logger.info(
      "[Plugin.Provider.Websocket] activate begin uri=#{inspect(uri)} plugin_id=#{inspect(plugin_id)}"
    )

    case start_link(uri,
           ws_opts: Map.get(opts, :ws_opts, []),
           post_connect: Map.get(opts, :post_connect, %{}),
           overwatcher_pid: overwatcher_pid,
           payload_metadata: Map.get(opts, :payload_metadata, %{}),
           idle_timeout_ms: Map.get(opts, :idle_timeout_ms, @default_idle_timeout_ms),
           resubscribe_after_ms:
             Map.get(opts, :resubscribe_after_ms, @default_resubscribe_after_ms),
           ignore_pattern: Map.get(opts, :ignore_pattern),
           plugin_id: plugin_id,
           sync_ingest?: Map.get(opts, :effectively_once, false)
         ) do
      {:ok, pid} ->
        Logger.info(
          "[Plugin.Provider.Websocket] activate success plugin_id=#{inspect(plugin_id)} pid=#{inspect(pid)}"
        )

        {:ok, pid}

      {:error, reason} = err ->
        Logger.error(
          "[Plugin.Provider.Websocket] activate failed plugin_id=#{inspect(plugin_id)} reason=#{inspect(reason)}"
        )

        err
    end
  end

  @doc """
  Health verdict reusing the two-tier watchdog's canonical timestamps — no new
  liveness state. Healthy means the connection process is alive and within its
  `idle_timeout_ms` (connection liveness) and, when `resubscribe_after_ms` is
  configured, within its data window (data liveness). The in-process watchdog
  (idle reconnect / resubscribe) remains the fast path; this contract only lets
  the reconciler escalate after it has repeatedly failed.
  """
  @impl ProviderBehaviour
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    pid = Map.get(opts, :owned_pid)
    idle = Map.get(opts, :idle_timeout_ms, @default_idle_timeout_ms)
    resub = Map.get(opts, :resubscribe_after_ms, @default_resubscribe_after_ms)

    cond do
      not is_pid(pid) -> {:unhealthy, :no_connection}
      not Process.alive?(pid) -> {:unhealthy, :not_alive}
      true -> health_from_liveness(pid, idle, resub)
    end
  end

  defp health_from_liveness(pid, idle, resub) do
    case query_liveness(pid) do
      {:ok, frame_age, data_age} ->
        cond do
          idle > 0 and frame_age >= idle -> {:unhealthy, :connection_idle}
          resub > 0 and data_age >= resub -> {:unhealthy, :data_idle}
          true -> :healthy
        end

      :timeout ->
        # Wedged probe → healthy so the reconciler does not escalate on
        # uncertainty (a genuinely dead pid is caught by Process.alive?/1).
        :healthy
    end
  end

  defp query_liveness(pid) do
    ref = make_ref()
    send(pid, {:liveness_probe, self(), ref})

    receive do
      {^ref, frame_age, data_age} -> {:ok, frame_age, data_age}
    after
      500 -> :timeout
    end
  end

  defp normalize_idle_timeout(value) when is_integer(value) and value >= 0, do: value
  defp normalize_idle_timeout(value) when is_integer(value), do: 0

  defp normalize_idle_timeout(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} -> max(n, 0)
      _ -> @default_idle_timeout_ms
    end
  end

  defp normalize_idle_timeout(_), do: @default_idle_timeout_ms

  defp normalize_resubscribe_after(value) when is_integer(value) and value >= 0, do: value
  defp normalize_resubscribe_after(value) when is_integer(value), do: 0

  defp normalize_resubscribe_after(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} -> max(n, 0)
      _ -> @default_resubscribe_after_ms
    end
  end

  defp normalize_resubscribe_after(_), do: @default_resubscribe_after_ms

  defp normalize_ignore_pattern(value) when is_binary(value) and value != "", do: value
  defp normalize_ignore_pattern(_), do: nil

  # :on_interval tick triggers nothing; connection is driven by the Fresh pid.
  def on_interval(%Ctx.Tick{}, _opts), do: :ok

  def on_terminate(%Ctx.Terminate{}, %{conn: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    :ok
  end

  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  def start_link(uri, opts) do
    idle_timeout_ms = Keyword.get(opts, :idle_timeout_ms, @default_idle_timeout_ms)
    resubscribe_after_ms = Keyword.get(opts, :resubscribe_after_ms, @default_resubscribe_after_ms)
    ignore_pattern = Keyword.get(opts, :ignore_pattern)
    plugin_id = Keyword.get(opts, :plugin_id) || uri

    now = :erlang.monotonic_time(:millisecond)

    Fresh.start_link(
      uri,
      __MODULE__,
      %{
        uri: uri,
        plugin_id: plugin_id,
        post_connect: opts[:post_connect],
        overwatcher_pid: opts[:overwatcher_pid],
        payload_metadata: Keyword.get(opts, :payload_metadata, %{}),
        idle_timeout_ms: idle_timeout_ms,
        resubscribe_after_ms: resubscribe_after_ms,
        ignore_pattern: compile_ignore_pattern(ignore_pattern),
        # Effectively-once tier (from the activation env): ingest via a
        # blocking call so a zero-credit park stops the socket read.
        sync_ingest?: Keyword.get(opts, :sync_ingest?, false),
        websocket_established?: false,
        frames_seen: 0,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now
      },
      opts[:ws_opts]
    )
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

    state = %{
      state
      | websocket_established?: true,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now
    }

    schedule_idle_check(state)

    # Issue the subscribe via Fresh's reply form so the frame is sent
    # after the upgrade lands. A re-entrant `Fresh.send/2` here races the
    # upgrade and crashes with `{:badkey, :extensions, nil}`.
    case subscribe_frame(state) do
      {:ok, frame} -> {:reply, [frame], state}
      :none -> {:ok, state}
    end
  end

  @impl Fresh
  def handle_disconnect(_code, _reason, state) do
    {:reconnect, %{state | websocket_established?: false}}
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

  @impl Fresh
  def handle_in(
        {type, message} = _p,
        %{overwatcher_pid: overwatcher, payload_metadata: metadata} = state
      ) do
    if Map.get(state, :sync_ingest?, false) do
      # Effectively-once tier ingest: await the journal ack so a
      # zero-credit park blocks this connection process — the socket stops
      # being read (TCP back-pressure) instead of the input-log growing
      # without bound.
      _ = GenServer.call(overwatcher, {:io_in, message, metadata}, :infinity)
    else
      GenServer.cast(overwatcher, {:io_in, message, metadata})
    end

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
    send(from, {ref, now - state.last_frame_at, now - state.last_data_frame_at})
    {:ok, state}
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
    # a fresh Fresh.Connection with the post_connect payload re-sent.
    Process.exit(self(), {:shutdown, :idle_timeout})
    {:ok, state}
  end

  defp resubscribe_stale_data(state, now, data_silence_ms) do
    # Rate-limit guard advances even when the send is skipped, so a
    # transiently-unestablished socket re-tries at most once per window.
    state = %{state | last_resubscribe_at: now}
    schedule_idle_check(state)

    with true <- state.websocket_established?,
         {:ok, frame} <- subscribe_frame(state) do
      :telemetry.execute(
        [:flezha, :ws, :resubscribe],
        %{data_silence_ms: data_silence_ms},
        %{plugin_id: state.plugin_id}
      )

      {:reply, [frame], state}
    else
      false ->
        Logger.warning(
          "[Plugin.Provider.Websocket] skipping re-subscribe; websocket not established " <>
            "plugin_id=#{inspect(state.plugin_id)}"
        )

        {:ok, state}

      :none ->
        {:ok, state}
    end
  end

  # Build the `post_connect` subscribe frame, or `:none` when no payload
  # is configured.
  defp subscribe_frame(state) do
    pcp = state.post_connect["payload"]

    if is_map(pcp) and Map.has_key?(pcp, "type") and Map.has_key?(pcp, "message") do
      type = if pcp["type"] == "binary", do: :binary, else: :text
      {:ok, {type, pcp["message"]}}
    else
      :none
    end
  end

  # Tick at half the nearest enabled deadline so neither tier is observed
  # late; no tick is scheduled when both tiers are disabled.
  defp schedule_idle_check(state) do
    case Enum.filter([state.idle_timeout_ms, state.resubscribe_after_ms], &(&1 > 0)) do
      [] ->
        :ok

      enabled ->
        tick = max(@min_tick_interval_ms, div(Enum.min(enabled), 2))
        Process.send_after(self(), :idle_check, tick)
        :ok
    end
  end
end

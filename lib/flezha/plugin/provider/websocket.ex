defmodule Flezha.Plugin.Provider.Websocket do
  @moduledoc """
  First-party Websocket provider. `init_provider/1` starts a Fresh WebSocket
  connection process; incoming frames are cast as `{:io_in, msg, metadata}`
  to the overwatcher pid. The plugin subscribes to `:on_terminate` so the
  connection is closed cleanly when the plugin is disabled.

  The plugin runs an application-level idle watchdog that forces a
  reconnect when no data frame has arrived within `idle_timeout_ms`
  (default 30 s). Control frames (ping/pong) do NOT count as activity —
  only `handle_in/2` data frames refresh the watchdog. When the threshold
  is exceeded, the Fresh process exits with `{:shutdown, :idle_timeout}`,
  the plugin manager observes the abnormal exit and restarts the plugin
  via its standard retry path (`Flezha.Plugin.Extension.Table.handle_down`).

  Some servers (notably Kraken) push a per-second JSON heartbeat frame
  alongside the actual market-data stream. Those frames count as
  protocol-level liveness but say nothing about whether the data feed is
  flowing, so the watchdog accepts an optional `:ignore_pattern` (string)
  that, when set, suppresses the `last_data_frame_at` refresh for any
  text data frame whose payload contains the pattern as a substring. The
  frame is still forwarded to the overwatcher and the `frames_seen`
  counter still increments, so the heartbeat-vs-data ratio remains
  visible in the periodic log line. Binary data frames never match the
  pattern; when no pattern is configured every data frame refreshes the
  timer (pre-change default).

  Each forced reconnect emits a Telemetry event
  `[:flezha, :ws, :idle_reconnect]` with measurements
  `%{silence_ms: integer()}` and metadata `%{plugin_id: binary()}`. Any
  downstream Telemetry → Prometheus bridge (e.g., the one
  `self-heal-asset-pipeline` will wire) automatically harvests these
  events with no further change to this module.

  Required opts: `:uri`.
  Optional opts: `:post_connect`, `:ws_opts`, `:payload_metadata`,
  `:idle_timeout_ms` (default 30_000; set to 0 to disable the watchdog),
  `:ignore_pattern` (string substring, default unset),
  `:plugin_id` (label for telemetry/logs; defaults to `:uri`).
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  use Fresh
  use ProviderBehaviour

  @default_idle_timeout_ms 30_000
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
                idle_timeout_ms: @default_idle_timeout_ms
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
  # after the relay's SDK side has confirmed bound. See `relay-extensions`
  # "Two-phase plugin lifecycle gated on SDK readiness".
  def init_provider(opts) do
    uri = Map.fetch!(opts, :uri)
    idle_timeout_ms = normalize_idle_timeout(Map.get(opts, :idle_timeout_ms))
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
           ignore_pattern: Map.get(opts, :ignore_pattern),
           plugin_id: plugin_id
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

  defp normalize_idle_timeout(value) when is_integer(value) and value >= 0, do: value
  defp normalize_idle_timeout(value) when is_integer(value), do: 0

  defp normalize_idle_timeout(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} -> max(n, 0)
      _ -> @default_idle_timeout_ms
    end
  end

  defp normalize_idle_timeout(_), do: @default_idle_timeout_ms

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
    ignore_pattern = Keyword.get(opts, :ignore_pattern)
    plugin_id = Keyword.get(opts, :plugin_id) || uri

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
        ignore_pattern: ignore_pattern,
        last_data_frame_at: System.monotonic_time(:millisecond)
      },
      opts[:ws_opts]
    )
  end

  @impl Fresh
  def handle_terminate(reason, _state) do
    Logger.info("Websocket terminating: #{inspect(reason)}")
    :ok
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    Logger.info("Connected to websocket!")

    pcp = state.post_connect["payload"]

    if is_map(pcp) && Map.has_key?(pcp, "type") && Map.has_key?(pcp, "message") do
      type =
        case pcp["type"] do
          "text" -> :text
          "binary" -> :binary
          _ -> :text
        end

      message = pcp["message"]

      send_payload(self(), {type, message})
    end

    state = %{state | last_data_frame_at: System.monotonic_time(:millisecond)}
    schedule_idle_check(state.idle_timeout_ms)
    {:ok, state}
  end

  @impl Fresh
  def handle_control({:pong, _}, state), do: {:ok, state}

  @impl Fresh
  def handle_control(control_frame, state) do
    Logger.warning("Received unexpected control frame: #{inspect(control_frame)}")
    {:ok, state}
  end

  @impl Fresh
  def handle_in(
        {type, message} = _p,
        %{overwatcher_pid: overwatcher, payload_metadata: metadata} = state
      ) do
    GenServer.cast(overwatcher, {:io_in, message, metadata})
    counter = Map.get(state, :frames_seen, 0) + 1

    if rem(counter, 1000) == 0 do
      sample = String.slice(message, 0, 120)

      Logger.info(
        "[Plugin.Provider.Websocket] frames_seen=#{counter} plugin_id=#{inspect(state[:plugin_id])} last_msg_size=#{byte_size(message)} sample=#{inspect(sample)}"
      )
    end

    state = Map.put(state, :frames_seen, counter)

    state =
      if data_frame_resets_watchdog?(type, message, Map.get(state, :ignore_pattern)) do
        Map.put(state, :last_data_frame_at, System.monotonic_time(:millisecond))
      else
        state
      end

    {:ok, state}
  end

  # Watchdog reset rules:
  #   * No `ignore_pattern` configured  → every data frame resets the timer
  #     (pre-change default).
  #   * Binary frames                   → always reset (the pattern is a text
  #     substring; treating binaries as a possible match would be a false
  #     positive on opaque payloads).
  #   * Text frames containing pattern  → suppressed (heartbeat-shaped data).
  defp data_frame_resets_watchdog?(_type, _message, nil), do: true
  defp data_frame_resets_watchdog?(_type, _message, ""), do: true

  defp data_frame_resets_watchdog?(:text, message, pattern)
       when is_binary(message) and is_binary(pattern) do
    not String.contains?(message, pattern)
  end

  defp data_frame_resets_watchdog?(_type, _message, _pattern), do: true

  @impl Fresh
  def handle_info(:idle_check, %{idle_timeout_ms: idle} = state) when idle <= 0 do
    {:ok, state}
  end

  def handle_info(:idle_check, state) do
    silence_ms = System.monotonic_time(:millisecond) - state.last_data_frame_at

    if silence_ms >= state.idle_timeout_ms do
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
    else
      schedule_idle_check(state.idle_timeout_ms)
      {:ok, state}
    end
  end

  def handle_info(any, state) do
    Logger.warning("Received unexpected info at websocket process: #{inspect(any)}")
    {:ok, state}
  end

  def send_payload(server, {type, msg}) do
    Fresh.send(server, {type, msg})
  end

  defp schedule_idle_check(idle_timeout_ms)
       when is_integer(idle_timeout_ms) and idle_timeout_ms > 0 do
    tick = max(@min_tick_interval_ms, div(idle_timeout_ms, 2))
    Process.send_after(self(), :idle_check, tick)
    :ok
  end

  defp schedule_idle_check(_), do: :ok
end

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

  Each forced reconnect emits a Telemetry event
  `[:flezha, :ws, :idle_reconnect]` with measurements
  `%{silence_ms: integer()}` and metadata `%{plugin_id: binary()}`. Any
  downstream Telemetry → Prometheus bridge (e.g., the one
  `self-heal-asset-pipeline` will wire) automatically harvests these
  events with no further change to this module.

  Required opts: `:uri`.
  Optional opts: `:post_connect`, `:ws_opts`, `:payload_metadata`,
  `:idle_timeout_ms` (default 30_000; set to 0 to disable the watchdog),
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

  def init_provider(opts) do
    uri = Map.fetch!(opts, :uri)
    idle_timeout_ms = normalize_idle_timeout(Map.get(opts, :idle_timeout_ms))
    plugin_id = Map.get(opts, :plugin_id) || uri

    case start_link(uri,
           ws_opts: Map.get(opts, :ws_opts, []),
           post_connect: normalize_post_connect(Map.get(opts, :post_connect)),
           overwatcher_pid: self(),
           payload_metadata: Map.get(opts, :payload_metadata, %{}),
           idle_timeout_ms: idle_timeout_ms,
           plugin_id: plugin_id
         ) do
      {:ok, pid} ->
        opts =
          opts
          |> Map.put(:conn, pid)
          |> Map.put(:idle_timeout_ms, idle_timeout_ms)
          |> Map.put(:plugin_id, plugin_id)

        {:ok, opts, pid}

      {:error, reason} ->
        {:error, reason}
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

  # :on_interval tick triggers nothing; connection is driven by the Fresh pid.
  def on_interval(%Ctx.Tick{}, _opts), do: :ok

  def on_terminate(%Ctx.Terminate{}, %{conn: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    :ok
  end

  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  def start_link(uri, opts) do
    idle_timeout_ms = Keyword.get(opts, :idle_timeout_ms, @default_idle_timeout_ms)
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
        {_type, message} = _p,
        %{overwatcher_pid: overwatcher, payload_metadata: metadata} = state
      ) do
    GenServer.cast(overwatcher, {:io_in, message, metadata})
    {:ok, %{state | last_data_frame_at: System.monotonic_time(:millisecond)}}
  end

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

  defp schedule_idle_check(idle_timeout_ms) when is_integer(idle_timeout_ms) and idle_timeout_ms > 0 do
    tick = max(@min_tick_interval_ms, div(idle_timeout_ms, 2))
    Process.send_after(self(), :idle_check, tick)
    :ok
  end

  defp schedule_idle_check(_), do: :ok
end

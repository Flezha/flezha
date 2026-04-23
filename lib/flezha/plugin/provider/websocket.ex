defmodule Flezha.Plugin.Provider.Websocket do
  @moduledoc """
  First-party Websocket provider. `init_provider/1` starts a Fresh WebSocket
  connection process; incoming frames are cast as `{:io_in, msg, metadata}`
  to the overwatcher pid. The plugin subscribes to `:on_terminate` so the
  connection is closed cleanly when the plugin is disabled.

  Required opts: `:uri`.
  Optional opts: `:post_connect`, `:ws_opts`, `:payload_metadata`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  use Fresh
  use ProviderBehaviour

  @enforce_keys [:uri]
  defstruct @enforce_keys ++
              [
                :conn,
                post_connect: "",
                ws_opts: [],
                payload_metadata: %{}
              ]

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:ws_opts, value) when is_binary(value) do
    Jason.decode(value)
  end

  defp convert_value(:payload_metadata, value) when is_binary(value) do
    Jason.decode(value)
  end

  def init_provider(opts) do
    uri = Map.fetch!(opts, :uri)

    case start_link(uri,
           ws_opts: Map.get(opts, :ws_opts, []),
           post_connect: Map.get(opts, :post_connect, ""),
           overwatcher_pid: self(),
           payload_metadata: Map.get(opts, :payload_metadata, %{})
         ) do
      {:ok, pid} -> {:ok, Map.put(opts, :conn, pid), pid}
      {:error, reason} -> {:error, reason}
    end
  end

  # :on_interval tick triggers nothing; connection is driven by the Fresh pid.
  def on_interval(%Ctx.Tick{}, _opts), do: :ok

  def on_terminate(%Ctx.Terminate{}, %{conn: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    :ok
  end

  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  def start_link(uri, opts) do
    Fresh.start_link(
      uri,
      __MODULE__,
      %{
        post_connect: opts[:post_connect],
        overwatcher_pid: opts[:overwatcher_pid],
        payload_metadata: Keyword.get(opts, :payload_metadata, %{})
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
    {:ok, state}
  end

  @impl Fresh
  def handle_info(any, state) do
    Logger.warning("Received unexpected info at websocket process: #{inspect(any)}")
    {:ok, state}
  end

  def send_payload(server, {type, msg}) do
    Fresh.send(server, {type, msg})
  end
end

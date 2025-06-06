defmodule Flezha.Plugin.Provider.Websocket do
  alias Flezha.Plugin.PipelineBinding
  alias Flezha.Plugin.PipelinePlugin
  alias Flezha.Plugin.PluginProtocol
  alias Flezha.Plugin.PluginBase

  require Logger

  use Fresh

  @behaviour PluginBase

  @enforce_keys PipelinePlugin.default_keys() ++ [:uri]
  defstruct @enforce_keys ++
              [
                :conn,
                post_connect: %{},
                ws_opts: []
              ]

  @impl true
  def plugin_id(), do: "pipeline_websocket"

  defimpl PluginProtocol, for: Flezha.Plugin.Provider.Websocket do
    use PipelinePlugin
  end

  defimpl PipelineBinding, for: Flezha.Plugin.Provider.Websocket do
    alias Flezha.Plugin.Provider.Websocket

    @impl true
    def bind(settings) do
      res =
        Websocket.start_link(settings.uri,
          ws_opts: settings.ws_opts,
          post_connect: settings.post_connect,
          overwatcher_pid: self()
        )

      case res do
        {:ok, pid} -> {:ok, %Websocket{settings | conn: pid}}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl true
    def exec(settings, payload) do
      Websocket.send_payload(settings.conn, payload)

      {:ok, payload}
    end
  end

  def start_link(uri, opts) do
    Fresh.start_link(
      uri,
      __MODULE__,
      %{
        post_connect: opts[:post_connect],
        overwatcher_pid: opts[:overwatcher_pid]
      },
      opts[:ws_opts]
    )
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    Logger.info("Connected to websocket!")

    pcp = state.post_connect[:payload]

    if is_map(pcp) && Map.has_key?(pcp, :type) && Map.has_key?(pcp, :message) do
      type =
        case pcp.type do
          "text" -> :text
          "binary" -> :binary
          _ -> :text
        end

      message = pcp.message

      send_payload(self(), {type, message})
    end

    {:ok, state}
  end

  @impl Fresh
  def handle_control({:pong, _}, state) do
    {:ok, state}
  end

  @impl Fresh
  def handle_control(control_frame, state) do
    Logger.warning("Received unexpected control frame: #{inspect(control_frame)}")

    {:ok, state}
  end

  @impl Fresh
  def handle_in({type, message} = _p, %{overwatcher_pid: overwatcher} = state) do
    GenServer.cast(overwatcher, {:io_in, {type, message}})

    {:ok, state}
  end

  @impl Fresh
  def handle_info(any, state) do
    Logger.warning("Received info: #{inspect(any)}")

    {:ok, state}
  end

  def send_payload(server, {type, msg}) do
    Fresh.send(server, {type, msg})
  end
end

defmodule Flezha.Plugin.Provider.Websocket do
  alias Flezha.Processor.IOBinding
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

  @impl true
  def help() do
    """
    WebSocket Plugin Help

    DESCRIPTION:
        Connects to a WebSocket server and forwards processor I/O through the WebSocket connection.
        Can send a post-connect message after establishing the connection.

    USAGE:
        flz plugin enable <plugin_id> processor_websocket [OPTIONS]

    REQUIRED PARAMETERS (X-prefixed):
        --Xuri <uri>                  WebSocket server URI (e.g., ws://localhost:4500)

    OPTIONAL PARAMETERS (X-prefixed):
        --Xpost_connect <json>        JSON message to send after connecting
        --Xws_opts <json>             WebSocket connection options as JSON
        --Xpayload_metadata <json>    Metadata to include with payloads

    COMMON OPTIONS:
        --namespace <namespace>       Namespace (default: default)
        --processor_id <id>          Target processor ID (required)
        --trigger <trigger>          Trigger mode: on_input|on_output|proactive|interval
        --position <num>             Plugin position in the chain

    TRIGGER MODES:
        on_input    - Triggered when processor receives input
        on_output   - Triggered when processor produces output  
        proactive   - Plugin actively polls/generates data
        interval    - Triggered at regular intervals

    EXAMPLES:
        # Basic WebSocket connection
        flz plugin enable ws_conn processor_websocket \\
            --Xuri=ws://localhost:4500 \\
            --processor_id=my_processor \\
            --trigger=proactive

        # With post-connect message
        flz plugin enable ws_conn processor_websocket \\
            --Xuri=ws://localhost:4500 \\
            --Xpost_connect='{"type":"subscribe","channel":"updates"}' \\
            --processor_id=my_processor \\
            --trigger=proactive \\
            --namespace=production
    """
  end

  defp convert_value(:ws_opts, value) when is_binary(value) do
    Jason.decode(value)
  end

  defp convert_value(:payload_metadata, value) when is_binary(value) do
    Jason.decode(value)
  end

  defimpl IOBinding, for: Flezha.Plugin.Provider.Websocket do
    alias Flezha.Plugin.Provider.Websocket

    @impl true
    def bind(binding) do
      res =
        Websocket.start_link(binding.uri,
          ws_opts: binding.ws_opts,
          post_connect: binding.post_connect,
          overwatcher_pid: self(),
          payload_metadata: binding.payload_metadata
        )

      case res do
        {:ok, pid} -> {:ok, {%Websocket{binding | conn: pid}, pid}}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl true
    def handle_io(binding, {message, metadata} = payload) do
      Websocket.send_payload(binding.conn, {metadata.type, message})

      {:ok, payload}
    end
  end

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
  def handle_control({:pong, _}, state) do
    {:ok, state}
  end

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

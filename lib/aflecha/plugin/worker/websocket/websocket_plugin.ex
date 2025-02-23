defmodule Aflecha.Plugin.Worker.Websocket.WebsocketPlugin do
  require Logger

  use Aflecha.Plugin.Worker.LinkedProcessContract
  use Fresh

  @type t() :: %__MODULE__{
          uri: String.t(),
          post_connect: %{
            payload: {:text | :binary, binary()}
          },
          overwatcher_pid: pid(),
          opts: keyword()
        }
  @enforce_keys [
    :uri,
    :overwatcher_pid
  ]
  defstruct @enforce_keys ++
              [
                post_connect: %{},
                opts: []
              ]

  @impl true
  def plugin_type() do
    :worker_linked_process
  end

  @impl true
  def load() do
    :ok
  end

  @impl true
  def start_link(%__MODULE__{uri: uri, overwatcher_pid: op, post_connect: pc, opts: ws_opts}) do
    Fresh.start_link(
      uri,
      __MODULE__,
      %{
        post_connect: pc,
        overwatcher_pid: op
      },
      ws_opts
    )
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    Logger.info("Connected to websocket!")

    post_conn_payload = state.post_connect[:payload]

    if is_tuple(post_conn_payload) && tuple_size(post_conn_payload) === 2 do
      send_payload(self(), post_conn_payload)
    end

    {:ok, state}
  end

  @impl Fresh
  def handle_control({_, message}, state) do
    IO.puts("Received ping with content: #{message}")

    {:ok, state}
  end

  @impl Fresh
  def handle_in({data_type, payload}, %{overwatcher_pid: owerwatcher} = state) do
    GenServer.cast(owerwatcher, {:io_in, payload, data_type})

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

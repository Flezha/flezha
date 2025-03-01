defmodule Aflecha.Plugin.Handler.Websocket do
  alias Aflecha.Plugin.WorkerBinding

  require Logger

  use Fresh

  @enforce_keys [:uri]
  defstruct @enforce_keys ++
              [
                :conn,
                post_connect: %{},
                ws_opts: []
              ]

  defimpl WorkerBinding, for: Aflecha.Plugin.Handler.Websocket do
    alias Aflecha.Plugin.Handler.Websocket

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

      {:ok, :frame_sent}
    end
  end

  @impl true
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

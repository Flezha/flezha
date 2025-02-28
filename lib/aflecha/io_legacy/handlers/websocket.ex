defmodule Aflecha.IO.Handler.Websocket do
  require Logger

  use Fresh
  use Aflecha.IO.Contract

  @type t() :: %__MODULE__{
          uri: String.t(),
          post_connect: %{
            payload: {:text | :binary, binary()}
          },
          opts: keyword()
        }
  @enforce_keys [
    :uri
  ]
  defstruct @enforce_keys ++
              [
                post_connect: %{},
                opts: []
              ]

  def start_link(pipeline_pid, %__MODULE__{uri: uri, post_connect: pc, opts: ws_opts}, opts) do
    Fresh.start_link(
      uri,
      __MODULE__,
      %{
        post_connect: pc,
        pipeline_pid: pipeline_pid,
        handling_type: Keyword.get(opts, :handling_type)
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

  # TODO: rename pipeline_pid to worker_manager or antyhing else
  @impl Fresh
  def handle_in({data_type, payload}, %{pipeline_pid: cp, handling_type: handling_type} = state) do
    GenServer.cast(cp, {:io_in, payload, data_type, handling_type})

    {:ok, state}
  end

  @impl Fresh
  def handle_info(any, state) do
    Logger.warning("Received info: #{inspect(any)}")

    {:ok, state}
  end

  @impl Aflecha.IO.Contract
  def send_payload(server, {type, msg}) do
    Fresh.send(server, {type, msg})
  end
end

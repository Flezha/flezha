defmodule Aflecha.IO.Websocket do
  require Logger

  use Fresh
  use Aflecha.IO.Contract

  @type t() :: %__MODULE__{
          uri: String.t(),
          opts: keyword()
        }
  @enforce_keys [
    :uri
  ]
  defstruct @enforce_keys ++
              [
                opts: []
              ]

  def start_link(%__MODULE__{uri: uri, opts: opts}) do
    Fresh.start_link(uri, __MODULE__, %{connected: false}, opts)
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    dbg("Connected to websocket!")

    Logger.info("Connected to websocket!")

    {:ok, Map.put(state, :connected, true)}
  end

  @impl Fresh
  def handle_control({_, message}, state) do
    IO.puts("Received ping with content: #{message}!")

    {:ok, state}
  end

  @impl Fresh
  def handle_in({data_type, payload}, state) do
    dbg()

    GenServer.cast(
      {:via, PartitionSupervisor, {Aflecha.ChannelSupervisor, self()}},
      {:io_in, data_type, payload}
    )

    {:ok, state}
  end

  @impl Fresh
  def handle_info(any, state) do
    IO.puts("Received info: #{inspect(any)}")

    {:ok, state}
  end

  @impl Aflecha.IO.Contract
  def send_payload(server, {type, msg}) do
    Fresh.send(server, {type, msg})
  end
end

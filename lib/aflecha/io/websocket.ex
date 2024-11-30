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
    case Fresh.start_link(uri, __MODULE__, %{subscriber: self()}, opts) do
      {:ok, pid} ->
        receive do
          :connected -> {:ok, pid}
        after
          5000 -> {:error, "Timeout while connecting to websocket"}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    dbg("Connected to websocket!")

    Logger.info("Connected to websocket!")

    send(state[:subscriber], :connected)

    {:ok, state}
  end

  @impl Fresh
  def handle_control({_, message}, state) do
    dbg()

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
    dbg()

    IO.puts("Received info: #{inspect(any)}")

    {:ok, state}
  end

  @impl Aflecha.IO.Contract
  def send_payload(server, {type, msg}) do
    Fresh.send(server, {type, msg})
  end
end

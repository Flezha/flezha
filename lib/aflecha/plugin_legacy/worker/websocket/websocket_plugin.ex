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
end

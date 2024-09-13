defmodule Flecha.Engine.EngineSupervisor do
  alias Flecha.Engine.ControlUnit
  alias Flecha.Engine.ControlChannel
  alias Flecha.Utility.Identifier
  alias Flecha.Socket.Listener

  use Supervisor

  def start_link({engine_id, sock}) do
    Supervisor.start_link(__MODULE__, {engine_id, sock}, name: Identifier.engine_sup(engine_id))
  end

  @impl true
  def init({engine_id, sock}) do
    engine_id_string = Atom.to_string(engine_id)
    # TODO: socket paths should be configurable.
    stream_socket_path = "app_data/sockets/stream_#{engine_id_string}.sock"

    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [engine_id]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{engine_id, sock}]}
      },
      %{
        id: Listener,
        start:
          {Listener, :start_link, [{Identifier.stream_listener(engine_id), stream_socket_path}]}
      },
      %{
        id: StreamSupervisor,
        start: {Flecha.Engine.StreamSupervisor, :start_link, [engine_id]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(engine_id) do
    Supervisor.which_children(Identifier.engine_sup(engine_id))
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end
end

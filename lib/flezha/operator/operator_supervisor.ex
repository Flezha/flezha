defmodule Flezha.Operator.OperatorSupervisor do
  alias Flezha.Operator.ControlUnit
  alias Flezha.Operator.ControlChannel
  alias Flezha.Service.Identificator
  alias Flezha.Socket.Listener
  alias Flezha.Socket.Support, as: SocketSupport

  use Supervisor

  def start_link({group_id, operator_id, sock}) do
    Supervisor.start_link(__MODULE__, {group_id, operator_id, sock},
      name: Identificator.unit_sup(operator_id)
    )
  end

  @impl true
  def init({group_id, operator_id, sock}) do
    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [{group_id, operator_id}]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{group_id, operator_id, sock}]}
      },
      %{
        id: :primary_stream_listener,
        start:
          {Listener, :start_link,
           [
             {
               Identificator.stream_listener(operator_id, :primary),
               fn -> SocketSupport.stream_socket_path(operator_id, :primary) end
             }
           ]}
      },
      %{
        id: :secondary_stream_listener,
        start:
          {Listener, :start_link,
           [
             {
               Identificator.stream_listener(operator_id, :secondary),
               fn -> SocketSupport.stream_socket_path(operator_id, :secondary) end
             }
           ]}
      },
      %{
        id: :primary_stream_sup,
        start: {Flezha.Operator.StreamSup, :start_link, [{operator_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Flezha.Operator.StreamSup, :start_link, [{operator_id, :secondary}]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(operator_id) do
    Identificator.unit_sup(operator_id)
    |> Supervisor.which_children()
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end
end

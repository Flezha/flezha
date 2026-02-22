defmodule Flezha.ComputeEngine.ComputeEngineSupervisor do
  alias Flezha.ComputeEngine.ControlUnit
  alias Flezha.ComputeEngine.ControlChannel
  alias Flezha.Service.Identificator
  alias Flezha.Service.ConfigManager
  alias Flezha.Socket.Listener
  alias Flezha.Socket.Support, as: SocketSupport
  alias Flezha.Transport.QuicStreamAcceptor

  use Supervisor

  def start_link({namespace, compute_engine_id, transport_info}) do
    Supervisor.start_link(__MODULE__, {namespace, compute_engine_id, transport_info},
      name: Identificator.unit_sup(compute_engine_id)
    )
  end

  @impl true
  def init({namespace, compute_engine_id, {:quic, conn, control_stream}}) do
    # QUIC mode: no per-stream socket listeners needed.
    # Streams are multiplexed over the QUIC connection.
    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [{namespace, compute_engine_id}]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{namespace, compute_engine_id, control_stream}]}
      },
      %{
        id: :primary_stream_sup,
        start: {Flezha.ComputeEngine.StreamSup, :start_link, [{compute_engine_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Flezha.ComputeEngine.StreamSup, :start_link, [{compute_engine_id, :secondary}]}
      },
      %{
        id: QuicStreamAcceptor,
        start:
          {QuicStreamAcceptor, :start_link,
           [{compute_engine_id, conn, ConfigManager.concurrent_channel_count()}]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @impl true
  def init({namespace, compute_engine_id, sock}) do
    # UDS mode: use socket listeners for stream connections.
    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [{namespace, compute_engine_id}]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{namespace, compute_engine_id, sock}]}
      },
      %{
        id: :primary_stream_listener,
        start:
          {Listener, :start_link,
           [
             {
               Identificator.stream_listener(compute_engine_id, :primary),
               fn -> SocketSupport.stream_socket_path(compute_engine_id, :primary) end
             }
           ]}
      },
      %{
        id: :secondary_stream_listener,
        start:
          {Listener, :start_link,
           [
             {
               Identificator.stream_listener(compute_engine_id, :secondary),
               fn -> SocketSupport.stream_socket_path(compute_engine_id, :secondary) end
             }
           ]}
      },
      %{
        id: :primary_stream_sup,
        start: {Flezha.ComputeEngine.StreamSup, :start_link, [{compute_engine_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Flezha.ComputeEngine.StreamSup, :start_link, [{compute_engine_id, :secondary}]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(compute_engine_id) do
    Identificator.unit_sup(compute_engine_id)
    |> Supervisor.which_children()
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end
end

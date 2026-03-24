defmodule Flezha.ComputeEngine.ComputeEngineSupervisor do
  alias Flezha.ComputeEngine.ControlUnit
  alias Flezha.ComputeEngine.ControlChannel
  alias Flezha.ComputeEngine.StreamAcceptor
  alias Flezha.Service.Identificator

  use Supervisor

  def start_link({namespace, compute_engine_id, stream_ref, transport}) do
    Supervisor.start_link(__MODULE__, {namespace, compute_engine_id, stream_ref, transport},
      name: Identificator.unit_sup(compute_engine_id)
    )
  end

  @impl true
  def init({namespace, compute_engine_id, stream_ref, transport}) do
    # Core children for all transport modes.
    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [{namespace, compute_engine_id}]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{namespace, compute_engine_id, stream_ref}]}
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

    # UDS mode: StreamAcceptors start per-stream NIF UDS servers.
    # QUIC mode: Data streams are routed by Transport.Server (no acceptors needed).
    children =
      case transport do
        :quic ->
          children

        _uds ->
          children ++
            [
              %{
                id: :primary_acceptor,
                start: {StreamAcceptor, :start_link, [{compute_engine_id, :primary}]}
              },
              %{
                id: :secondary_acceptor,
                start: {StreamAcceptor, :start_link, [{compute_engine_id, :secondary}]}
              }
            ]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(compute_engine_id) do
    Identificator.unit_sup(compute_engine_id)
    |> Supervisor.which_children()
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end
end

defmodule Flezha.ComputeEngine.ComputeEngineSupervisor do
  alias Flezha.ComputeEngine.ControlUnit
  alias Flezha.ComputeEngine.ControlChannel
  alias Flezha.ComputeEngine.StreamAcceptor
  alias Flezha.Service.Identificator

  use Supervisor

  def start_link({namespace, compute_engine_id, stream_ref}) do
    Supervisor.start_link(__MODULE__, {namespace, compute_engine_id, stream_ref},
      name: Identificator.unit_sup(compute_engine_id)
    )
  end

  @impl true
  def init({namespace, compute_engine_id, stream_ref}) do
    # Unified init — works for both UDS and QUIC via NIF.
    # stream_ref is a NIF StreamHandle resource.
    # Data streams are opened later by the SDK after ConnectSocket command.
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
      },
      %{
        id: :primary_acceptor,
        start: {StreamAcceptor, :start_link, [{compute_engine_id, :primary}]}
      },
      %{
        id: :secondary_acceptor,
        start: {StreamAcceptor, :start_link, [{compute_engine_id, :secondary}]}
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

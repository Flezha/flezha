defmodule Flezha.Executor.ExecutorSupervisor do
  alias Flezha.Executor.ControlUnit
  alias Flezha.Executor.ControlChannel
  alias Flezha.Executor.StreamAcceptor
  alias Flezha.Service.Identificator

  use Supervisor

  def start_link({namespace, executor_id, stream_ref, transport}) do
    Supervisor.start_link(__MODULE__, {namespace, executor_id, stream_ref, transport},
      name: Identificator.unit_sup(executor_id)
    )
  end

  @impl true
  def init({namespace, executor_id, stream_ref, transport}) do
    # Core children for all transport modes.
    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [{namespace, executor_id}]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{namespace, executor_id, stream_ref}]}
      },
      %{
        id: :primary_stream_sup,
        start: {Flezha.Executor.StreamSup, :start_link, [{executor_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Flezha.Executor.StreamSup, :start_link, [{executor_id, :secondary}]}
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
                start: {StreamAcceptor, :start_link, [{executor_id, :primary}]}
              },
              %{
                id: :secondary_acceptor,
                start: {StreamAcceptor, :start_link, [{executor_id, :secondary}]}
              }
            ]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(executor_id) do
    Identificator.unit_sup(executor_id)
    |> Supervisor.which_children()
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end
end

defmodule Flezha.Runner.RunnerSupervisor do
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.StreamAcceptor
  alias Flezha.Identificator

  use Supervisor

  def start_link({namespace, runner_id, stream_ref, transport}) do
    Supervisor.start_link(__MODULE__, {namespace, runner_id, stream_ref, transport},
      name: Identificator.unit_sup(runner_id)
    )
  end

  @impl true
  def init({namespace, runner_id, stream_ref, transport}) do
    # Core children for all transport modes.
    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [{namespace, runner_id}]}
      },
      %{
        id: ControlChannel.Commits,
        start: {ControlChannel.Commits, :start_link, [{namespace, runner_id}]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{namespace, runner_id, stream_ref}]}
      },
      %{
        id: :primary_stream_sup,
        start: {Flezha.Runner.StreamSup, :start_link, [{runner_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Flezha.Runner.StreamSup, :start_link, [{runner_id, :secondary}]}
      }
    ]

    # UDS starts per-stream NIF UDS servers (StreamAcceptors). QUIC and native
    # route/bind data streams through Transport.Server, so they need none. An
    # unknown atom is a boot bug — fail loudly rather than silently start
    # acceptors (the former `_uds` catch-all would).
    children =
      case transport do
        :uds ->
          children ++
            [
              %{
                id: :primary_acceptor,
                start: {StreamAcceptor, :start_link, [{runner_id, :primary}]}
              },
              %{
                id: :secondary_acceptor,
                start: {StreamAcceptor, :start_link, [{runner_id, :secondary}]}
              }
            ]

        t when t in [:quic, :native] ->
          children

        other ->
          raise ArgumentError,
                "RunnerSupervisor: unknown transport #{inspect(other)} " <>
                  "(expected :uds, :quic or :native)"
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  The runner's `ControlChannel` pid, or `{:error, reason}` when its supervisor
  is not there to ask.

  A runner's own supervision tree can fail to come up — an acceptor whose
  socket the kernel refuses, for one — and that is this runner's problem, not
  the fleet's: the caller runs inside `FleetManager`, so an exit here would
  take the manager down and every other runner with it.
  """
  def find_control_channel(runner_id) do
    Identificator.unit_sup(runner_id)
    |> Supervisor.which_children()
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> case do
      {_id, pid, _type, _mods} when is_pid(pid) -> {:ok, pid}
      nil -> {:error, :no_control_channel}
      {_id, other, _type, _mods} -> {:error, {:control_channel_not_running, other}}
    end
  catch
    :exit, reason -> {:error, {:runner_supervisor_down, reason}}
  end
end

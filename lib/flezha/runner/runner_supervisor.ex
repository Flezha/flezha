defmodule Flezha.Runner.RunnerSupervisor do
  @moduledoc """
  One runner's supervision tree: its control unit, its control channel and
  commit barrier, and the two pre-spawned data-stream pools.

  The transport decides only whether per-stream UDS acceptors are started —
  `:quic` and `:native` bind data streams through `Transport.Server` instead.

  Invariant: an unknown transport atom raises here rather than silently
  starting acceptors, because a runner brought up with no listener looks
  healthy and serves nothing.
  """

  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.StreamAcceptor

  use Supervisor

  def start_link({namespace, runner_id, stream_ref, transport}) do
    Supervisor.start_link(__MODULE__, {namespace, runner_id, stream_ref, transport},
      name: Identificator.unit_sup(runner_id)
    )
  end

  @impl true
  def init({namespace, runner_id, stream_ref, transport}) do
    core_children(namespace, runner_id, stream_ref)
    |> Kernel.++(acceptor_children(runner_id, transport))
    |> Supervisor.init(strategy: :one_for_one)
  end

  defp core_children(namespace, runner_id, stream_ref) do
    [
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
  end

  # UDS starts per-stream NIF UDS servers (StreamAcceptors). QUIC and native
  # route/bind data streams through Transport.Server, so they need none. An
  # unknown atom is a boot bug — fail loudly rather than silently start
  # acceptors.
  defp acceptor_children(runner_id, :uds) do
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
  end

  defp acceptor_children(_runner_id, transport) when transport in [:quic, :native], do: []

  defp acceptor_children(_runner_id, other) do
    raise ArgumentError,
          "RunnerSupervisor: unknown transport #{inspect(other)} " <>
            "(expected :uds, :quic or :native)"
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

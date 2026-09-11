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
  alias Flezha.Process.Cspec
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
      Cspec.build(
        ControlUnit,
        {ControlUnit, :start_link, [{namespace, runner_id}]},
        :worker_permanent
      ),
      Cspec.build(
        ControlChannel.Commits,
        {ControlChannel.Commits, :start_link, [{namespace, runner_id}]},
        :worker_permanent
      ),
      Cspec.build(
        ControlChannel,
        {ControlChannel, :start_link, [{namespace, runner_id, stream_ref}]},
        :worker_permanent
      ),
      Cspec.build(
        :primary_stream_sup,
        {Flezha.Runner.StreamSup, :start_link, [{runner_id, :primary}]},
        :worker_permanent
      ),
      Cspec.build(
        :secondary_stream_sup,
        {Flezha.Runner.StreamSup, :start_link, [{runner_id, :secondary}]},
        :worker_permanent
      )
    ]
  end

  # UDS starts per-stream NIF UDS servers (StreamAcceptors). QUIC and native
  # route/bind data streams through Transport.Server, so they need none. An
  # unknown atom is a boot bug — fail loudly rather than silently start
  # acceptors.
  defp acceptor_children(runner_id, :uds) do
    [
      Cspec.build(
        :primary_acceptor,
        {StreamAcceptor, :start_link, [{runner_id, :primary}]},
        :worker_permanent
      ),
      Cspec.build(
        :secondary_acceptor,
        {StreamAcceptor, :start_link, [{runner_id, :secondary}]},
        :worker_permanent
      )
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

  A tree that failed to start is refused before this is asked, with its start
  error. What the catch covers is a tree that started and died before
  `which_children` reached it, and that is this runner's problem, not the
  fleet's: the caller runs inside `FleetManager`, so an exit here would take
  the manager down and every other runner with it.
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

defmodule Flezha.Runner.StreamSup do
  @moduledoc """
  Supervises one runner's fixed pool of `StreamChannel` processes, primary or
  secondary.

  The channel names come from `Flezha.Runner.StreamPool`, so the supervisor
  that starts them, the acceptor that binds a socket into them and the fence
  that has to reach them all derive the same set.

  Invariant: the pool is fixed at start and `:one_for_one` — a channel that
  crashes comes back under the same name, unbound, and its session's
  `ControlChannel` has the runner bind it again.
  """

  alias Flezha.Identificator
  alias Flezha.Runner.StreamChannel
  alias Flezha.Runner.StreamPool

  use Supervisor

  def stream_specs(namespace, runner_id, type) do
    for id <- StreamPool.stream_ids(runner_id, type) do
      Supervisor.child_spec({StreamChannel, {namespace, runner_id, id, type}}, id: id)
    end
  end

  def start_link({namespace, runner_id, type}) do
    name =
      case type do
        :primary -> Identificator.stream_sup_primary(runner_id)
        :secondary -> Identificator.stream_sup_secondary(runner_id)
      end

    Supervisor.start_link(__MODULE__, {namespace, runner_id, type}, name: name)
  end

  @impl true
  def init({namespace, runner_id, type}) do
    children = stream_specs(namespace, runner_id, type)

    Supervisor.init(children, strategy: :one_for_one)
  end
end

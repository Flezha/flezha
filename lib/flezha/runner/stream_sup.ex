defmodule Flezha.Runner.StreamSup do
  alias Flezha.Config
  alias Flezha.Identificator
  alias Flezha.Runner.StreamChannel

  require Logger

  use Supervisor

  def stream_specs(runner_id, type) do
    for x <- 1..Config.concurrent_channel_count() do
      id =
        case type do
          :primary -> Identificator.stream_channel_primary(runner_id, x)
          :secondary -> Identificator.stream_channel_secondary(runner_id, x)
        end

      Supervisor.child_spec({StreamChannel, {runner_id, id, type}}, id: id)
    end
  end

  def stream_ids(runner_id, type) do
    stream_specs(runner_id, type) |> Enum.map(&Map.get(&1, :id))
  end

  @doc """
  Walk `candidate_names` and bind `stream_ref` to the first channel currently
  reporting a nil stream_ref via the atomic `{:bind, ref}` call.

  Returns `{:bound, pid, name}` on success. Returns `nil` when no candidate
  could accept the bind (every channel is already bound to a live stream, or
  the named processes are not alive). Channels that crash mid-call are
  skipped — the caller decides what to do when no slot is available.
  """
  def bind_stream_to_first_nil(candidate_names, stream_ref) do
    Enum.find_value(candidate_names, fn name ->
      case GenServer.whereis(name) do
        nil ->
          nil

        pid ->
          try do
            case GenServer.call(pid, {:bind, stream_ref}) do
              :ok -> {:bound, pid, name}
              :already_bound -> nil
            end
          catch
            :exit, reason ->
              Logger.debug(
                "[StreamSup] bind candidate #{inspect(name)} exited during call: " <>
                  inspect(reason)
              )

              nil
          end
      end
    end)
  end

  def start_link({runner_id, type}) do
    name =
      case type do
        :primary -> Identificator.stream_sup_primary(runner_id)
        :secondary -> Identificator.stream_sup_secondary(runner_id)
      end

    Supervisor.start_link(__MODULE__, {runner_id, type}, name: name)
  end

  @impl true
  def init({runner_id, type}) do
    children = stream_specs(runner_id, type)

    Supervisor.init(children, strategy: :one_for_one)
  end
end

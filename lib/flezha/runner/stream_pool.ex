defmodule Flezha.Runner.StreamPool do
  @moduledoc """
  The fixed channel-name set a runner session owns, and the bind into it.

  A session's data channels are pre-spawned under a name derived from the
  session token, so every party — the supervisor that starts them, the
  acceptor that binds an incoming socket, the fence that has to reach them —
  derives the same names without asking anyone.

  Invariant: a bind never overwrites a live stream. `bind_stream_to_first_nil/2`
  takes the first candidate that reports a nil stream_ref through the atomic
  `{:bind, ref}` call, and answers `nil` when there is none.
  """

  alias Flezha.Config
  alias Flezha.Identificator

  require Logger

  @doc "The channel names for `runner_id`'s `type` pool, in pool order."
  @spec stream_ids(binary(), :primary | :secondary) :: [atom()]
  def stream_ids(runner_id, type) do
    for x <- 1..Config.concurrent_channel_count() do
      case type do
        :primary -> Identificator.stream_channel_primary(runner_id, x)
        :secondary -> Identificator.stream_channel_secondary(runner_id, x)
      end
    end
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
                "[StreamPool] bind candidate #{inspect(name)} exited during call: " <>
                  inspect(reason)
              )

              nil
          end
      end
    end)
  end
end

defmodule Flezha.Runner.StreamPool do
  @moduledoc """
  The fixed channel-name set a runner session owns, and the bind into it.

  A session's data channels are pre-spawned under a name derived from the
  session token, so every party — the supervisor that starts them, the
  acceptor that binds an incoming socket, the fence that has to reach them —
  derives the same names without asking anyone. Reaching them is here too:
  `call_channels/3` is the one bounded fan-out over the set, and the route
  hooks, the takeover fence and the teardown barrier all go through it.

  Invariant: a bind never overwrites a live stream. `bind_stream_to_first_nil/2`
  takes the first candidate that reports a nil stream_ref through the atomic
  `{:bind, ref}` call, and answers `nil` when there is none.
  """

  alias Flezha.Config
  alias Flezha.Identificator
  alias Flezha.SafeCall

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
  Every live channel of `runner_id`'s session: the fixed name set above,
  resolved. A dead or unregistered channel forwards nothing and is skipped.
  """
  @spec channel_pids(binary()) :: [pid()]
  def channel_pids(runner_id) do
    for type <- [:primary, :secondary],
        name <- stream_ids(runner_id, type),
        pid = GenServer.whereis(name),
        is_pid(pid),
        do: pid
  end

  @doc """
  Issue `request` to every live channel of `runner_id` and answer once each has
  replied or the budget is spent.

  `budget_ms` bounds the WHOLE fan-out, not each call: the walk is sequential
  and the pool is `2 x System.schedulers_online()` wide by default, so a
  per-channel bound would multiply into a worst case not worth stating. A
  channel with nothing left to spend is skipped, which is what every caller
  wants — a wedged channel must not hold a registry write, a connect, or a
  teardown, and none of them may be crashed by one either.

  The default budget is what a control path spends reaching the channels: a
  skipped route seed simply resolves through `ClassRegistry`, and a skipped
  fence is caught by the superseded-session mark instead. `flush_channels/1`
  is the one caller whose budget comes from somewhere else.
  """
  @default_budget_ms 1_000

  @spec call_channels(binary(), term(), non_neg_integer()) :: :ok
  def call_channels(runner_id, request, budget_ms \\ @default_budget_ms) do
    deadline = System.monotonic_time(:millisecond) + budget_ms

    Enum.each(channel_pids(runner_id), fn pid ->
      _ = SafeCall.bounded(pid, request, max(deadline - System.monotonic_time(:millisecond), 0))
    end)
  end

  @doc """
  Flush `runner_id`'s channels: answer only once every frame already in each
  channel's mailbox has been routed and cast onward. What a completed
  workflow operative's teardown drives before its Overwatch is stopped.

  The barrier is the CALL, not what it asks — a `GenServer.call` is a message
  in the channel's FIFO mailbox, so any call it answers proves the frames
  ahead of it were handled. `:stream_ref_status` is the cheapest question a
  channel already answers.

  The budget is the window `Flezha.Convergence.StallClassifier` declares a
  driver loop frozen in: a channel silent that long is not about to answer,
  and the whole barrier stays well inside the 30 s window the same classifier
  calls a data-plane stall. A teardown must not outlive the wait it replaced.
  """
  @flush_budget_ms 5_000

  @spec flush_channels(binary()) :: :ok
  def flush_channels(runner_id),
    do: call_channels(runner_id, :stream_ref_status, @flush_budget_ms)

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

defmodule Flezha.Runner.FleetManager.Marks do
  @moduledoc """
  Two TTL-expiring ETS marks on a runner identity string.

  The fence mark decides correctness: `Flezha.State.WalWriter` refuses an
  append from a superseded session. The retiring mark decides only what a
  close is reported as. They are deliberately separate tables for that reason,
  and both are read from `{:flz_closed, …}` clauses — bare ETS reads, never a
  call, because a channel asking `FleetManager` whether its own close was
  expected would be asking the process that is tearing it down.

  Invariant: every mark expires. A runner id is a durable identity reused
  across reconnects, so a mark that outlived its disconnect would answer for a
  later, unrelated close of the same runner.
  """

  # Takeover fence marks (see `mark_session_superseded/1`). Entries outlive
  # eviction only long enough to cover in-flight mailbox residue; pruned on
  # the periodic cleanup tick.
  @superseded_table :flezha_superseded_sessions
  @superseded_ttl_ms 600_000
  # Runners the controller is itself closing, so the close their sockets report
  # is diagnosed as expected rather than as an incident. Same mechanism as the
  # fence above — a TTL-expiring mark on an identity string, pruned on the
  # cleanup tick — and deliberately a separate table: the fence decides
  # correctness, this decides only what gets reported.
  @retiring_table :flezha_retiring_runners

  @doc """
  Create the fence table, so it is owned by the caller.

  `FleetManager` calls this from `init/1`: the marks have to outlive the
  transient readers (WalWriters) that consult them.
  """
  @spec ensure_fence_table() :: :ok
  def ensure_fence_table, do: ensure_marks_table(@superseded_table)

  @doc """
  Mark a session token superseded by a newest-wins takeover. The mark is the
  WAL-side half of the write-path fence: `Flezha.State.WalWriter` refuses a
  realign (or any append) from a marked session, closing the in-flight window
  the channel-side flip cannot cover — a wedged `StreamChannel` that flushes
  its queued frames only after the binding swap.
  """
  @spec mark_session_superseded(binary()) :: :ok
  def mark_session_superseded(session_token) when is_binary(session_token) do
    ensure_marks_table(@superseded_table)
    :ets.insert(@superseded_table, {session_token, :erlang.monotonic_time(:millisecond)})
    :ok
  end

  @doc "Whether a takeover superseded `session_token` (see `mark_session_superseded/1`)."
  @spec superseded_session?(binary()) :: boolean()
  def superseded_session?(session_token) do
    :ets.whereis(@superseded_table) != :undefined and
      :ets.member(@superseded_table, session_token)
  end

  @doc """
  Record that the controller is itself closing `runner_id`, so the close its
  sockets report is diagnosed as expected rather than as an incident.

  Read from the `{:flz_closed, …}` clauses, which is why it is a bare ETS read
  and not a call: a channel asking `FleetManager` whether its own close was
  expected would be asking the process that is busy tearing it down.

  The mark expires. `runner_id` is a durable stable identity that survives
  reconnects, so a mark that outlived its disconnect would silence the
  diagnosis of a later, genuine close of the same runner.
  """
  @spec mark_runner_retiring(binary()) :: :ok
  def mark_runner_retiring(runner_id) when is_binary(runner_id) do
    ensure_marks_table(@retiring_table)
    :ets.insert(@retiring_table, {runner_id, :erlang.monotonic_time(:millisecond)})
    :ok
  end

  @doc "Whether the controller asked for `runner_id`'s close, within the mark's TTL."
  @spec runner_retiring?(binary()) :: boolean()
  def runner_retiring?(runner_id) when is_binary(runner_id) do
    with true <- :ets.whereis(@retiring_table) != :undefined,
         [{_, marked_at}] <- :ets.lookup(@retiring_table, runner_id) do
      :erlang.monotonic_time(:millisecond) - marked_at < retiring_ttl_ms()
    else
      _ -> false
    end
  end

  @doc "Drop `runner_id`'s retiring mark; the disconnect it covered is done."
  @spec clear_runner_retiring(binary()) :: :ok
  def clear_runner_retiring(runner_id) when is_binary(runner_id) do
    if :ets.whereis(@retiring_table) != :undefined,
      do: :ets.delete(@retiring_table, runner_id)

    :ok
  end

  @doc "Drop every mark past its TTL. Called from the runner-cleanup tick."
  @spec prune() :: :ok
  def prune do
    for {table, ttl} <- [
          {@superseded_table, @superseded_ttl_ms},
          {@retiring_table, retiring_ttl_ms()}
        ],
        :ets.whereis(table) != :undefined do
      cutoff = :erlang.monotonic_time(:millisecond) - ttl
      :ets.select_delete(table, [{{:_, :"$1"}, [{:<, :"$1", cutoff}], [true]}])
    end

    :ok
  end

  defp retiring_ttl_ms,
    do: Application.get_env(:flezha, :runner_retiring_mark_ttl_ms, 30_000)

  defp ensure_marks_table(table) do
    case :ets.whereis(table) do
      :undefined ->
        try do
          :ets.new(table, [:set, :public, :named_table, read_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end
end

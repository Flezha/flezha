defmodule Flezha.Runner.FleetManager.Marks do
  @moduledoc """
  The runner facts a channel reads with a bare ETS read: two TTL-expiring marks
  on a runner identity string, and the stable identity each live session
  presented.

  The fence mark decides correctness: `Flezha.State.WalWriter` refuses an
  append from a superseded session. The retiring mark decides only what a
  close is reported as. They are deliberately separate tables for that reason,
  and both are read from `{:flz_closed, …}` clauses — bare ETS reads, never a
  call, because a channel asking `FleetManager` whether its own close was
  expected would be asking the process that is tearing it down.

  A session's identity is what an operative's ownership record names, so a
  channel and an Overwatch read it here without calling the process that
  admitted the session. The same table remembers what a session has already
  been refused for, so a refusal is reported, and a copy told to stop, once.

  Invariant: every mark expires. A runner id is a durable identity reused
  across reconnects, so a mark that outlived its disconnect would answer for a
  later, unrelated close of the same runner. A session's identity rows do not
  expire: they are forgotten when the session ends, and a session token is
  never reused.
  """

  alias Flezha.Config.Durations

  # Takeover fence marks (see `mark_session_superseded/1`). Entries outlive
  # eviction only long enough to cover in-flight mailbox residue; pruned on
  # the periodic cleanup tick.
  @superseded_table :flezha_superseded_sessions
  # Runners the controller is itself closing, so the close their sockets report
  # is diagnosed as expected rather than as an incident. Same mechanism as the
  # fence above — a TTL-expiring mark on an identity string, pruned on the
  # cleanup tick — and deliberately a separate table: the fence decides
  # correctness, this decides only what gets reported.
  @retiring_table :flezha_retiring_runners
  # Each live session's stable identity, keyed by the session token, and one
  # `{session, subject}` row per operative or Cell the session was refused for.
  @identity_table :flezha_session_identities

  @doc """
  Create the fence and identity tables, so they are owned by the caller.

  `FleetManager` calls this from `init/1`: the tables have to outlive the
  transient readers (WalWriters, channels) that consult them.
  """
  @spec ensure_tables() :: :ok
  def ensure_tables do
    ensure_marks_table(@superseded_table)
    ensure_marks_table(@identity_table)
  end

  @doc """
  Record the stable identity `session` presented on connect. A degraded runner
  presents none and is its own session token (`identity_of/1`).
  """
  @spec bind_identity(binary(), binary() | nil) :: :ok
  def bind_identity(_session, nil), do: :ok

  def bind_identity(session, identity) when is_binary(session) and is_binary(identity) do
    ensure_marks_table(@identity_table)
    :ets.insert(@identity_table, {session, identity})
    :ok
  end

  @doc "The stable identity `session` presented, or the session token itself when none."
  @spec identity_of(binary()) :: binary()
  def identity_of(session) when is_binary(session) do
    with true <- :ets.whereis(@identity_table) != :undefined,
         [{_session, identity}] <- :ets.lookup(@identity_table, session) do
      identity
    else
      _ -> session
    end
  end

  @doc """
  Whether this is `session`'s first refusal for `subject`, an operative or a
  Cell: true once per session and subject, across every channel of the
  session, so a refusal is reported once however many frames repeat it.
  """
  @spec first_refusal?(binary(), term()) :: boolean()
  def first_refusal?(session, subject) do
    ensure_marks_table(@identity_table)
    :ets.insert_new(@identity_table, {{session, subject}, true})
  end

  @doc "Forget `session`'s identity and its refusals: the session has ended."
  @spec forget_identity(binary()) :: :ok
  def forget_identity(session) when is_binary(session) do
    if :ets.whereis(@identity_table) != :undefined do
      :ets.delete(@identity_table, session)
      :ets.match_delete(@identity_table, {{session, :_}, :_})
    end

    :ok
  end

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
          {@superseded_table, Durations.get(:superseded_mark_ttl_ms)},
          {@retiring_table, retiring_ttl_ms()}
        ],
        :ets.whereis(table) != :undefined do
      cutoff = :erlang.monotonic_time(:millisecond) - ttl
      :ets.select_delete(table, [{{:_, :"$1"}, [{:<, :"$1", cutoff}], [true]}])
    end

    :ok
  end

  defp retiring_ttl_ms,
    do: Durations.get(:runner_retiring_mark_ttl_ms)

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

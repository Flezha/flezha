defmodule Flezha.State.InputLog.Syncer do
  @moduledoc """
  An input log's sync companion: a second handle on the same file in its own
  process, since fdatasync syncs the inode, so the log's loop keeps appending.
  """

  @doc "Start the companion on `path`, linked to the caller, the log it syncs for."
  @spec start(Path.t()) :: pid()
  def start(path) do
    log = self()
    spawn_link(fn -> syncer_loop(open_raw(path), nil, log) end)
  end

  defp open_raw(path) do
    {:ok, fh} = :file.open(path, [:raw, :read, :write, :binary])
    fh
  end

  # `effects` is a second handle, on the effect log, once it exists; only an
  # effect append syncs it. `log` is the log's pid while syncs succeed, else
  # the answers held since the first that failed, until the log resumes.
  defp syncer_loop(fh, effects, log) do
    receive do
      {:sync, answers} ->
        # A backlog of ticks is one sync: datasync covers every write before it.
        answers = drain_syncs([answers])
        syncer_loop(fh, effects, sync(fh, answers, answers, log))

      {:sync, answers, from, ref} ->
        log = sync(fh, answers, answers, log)
        send(from, {ref, :ok})
        syncer_loop(fh, effects, log)

      # `seq` is the effect record the answers wait on, `nil` for none: a held
      # answer names it, so the log can cut the effect log back below it.
      {:sync_effects, answers, seq} ->
        held = for {from, _reply} <- answers, do: {from, {:effect_at, seq}}
        syncer_loop(fh, effects, sync(effects, answers, held, log))

      # The log takes back what was held, to cut its file below it and refuse it.
      {:resume, from, ref} ->
        send(from, {ref, if(is_list(log), do: log, else: [])})
        syncer_loop(fh, effects, from)

      {:reopen, path, from, ref} ->
        _ = :file.close(fh)
        fh = open_raw(path)
        send(from, {ref, :ok})
        syncer_loop(fh, effects, log)

      {:reopen_effects, path, from, ref} ->
        if effects, do: _ = :file.close(effects)
        send(from, {ref, :ok})
        syncer_loop(fh, open_raw(path), log)

      :stop ->
        if is_list(log), do: refuse(log, :stopped)
        _ = :file.close(fh)
        if effects, do: _ = :file.close(effects)
        :ok
    end
  end

  # A failed datasync leaves every write since the last good one in doubt, and a
  # later one may report them synced when they are not: from then on every
  # answer is held for the log, which is told, until it resumes this loop.
  defp sync(fh, answers, held, log) when is_pid(log) do
    case :file.datasync(fh) do
      :ok ->
        answer(answers)
        log

      {:error, reason} ->
        send(log, {:unsynced, reason})
        held
    end
  end

  defp sync(_fh, _answers, held, held_before), do: held ++ held_before

  defp drain_syncs(batches) do
    receive do
      {:sync, answers} -> drain_syncs([answers | batches])
    after
      0 -> batches |> Enum.reverse() |> Enum.concat()
    end
  end

  @doc "Reply to each `{from, reply}` in `answers`."
  @spec answer([{GenServer.from(), term()}]) :: :ok
  def answer(answers),
    do: Enum.each(answers, fn {from, reply} -> GenServer.reply(from, reply) end)

  @doc "Refuse each of `answers`, retryably: the disk did not take what it answers for."
  @spec refuse([{GenServer.from(), term()}], term()) :: :ok
  def refuse(answers, reason) do
    refusal = {:error, {:input_log_unavailable, reason}}
    answer(for {from, _reply} <- answers, do: {from, refusal})
  end

  @doc """
  A request only the companion can answer: the synchronous flush
  (`flush_now`, shutdown), the swap onto a rewritten file, or the resume after
  a failed sync, which answers the answers it held. The monitor is
  the bound: a caller's call timeout bounds the caller, never this loop, and
  an unbounded wait here parks every append the log owes for the rest of its
  life. `:error` is what the `FsyncBatcher` already reads as "still dirty,
  sync it on the next drain", so a lost sync is retried rather than reported
  durable.
  """
  @spec call(pid(), (pid(), reference() -> term())) :: :ok | [{GenServer.from(), term()}] | :error
  def call(syncer, request) do
    ref = Process.monitor(syncer)
    send(syncer, request.(self(), ref))

    receive do
      {^ref, reply} ->
        Process.demonitor(ref, [:flush])
        reply

      {:DOWN, ^ref, :process, _pid, _reason} ->
        :error
    end
  end
end

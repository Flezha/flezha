defmodule Flezha.State.InputLog.Syncer do
  @moduledoc """
  An input log's sync companion: a second handle on the same file in its own
  process, since fdatasync syncs the inode, so the log's loop keeps appending.
  """

  @doc "Start the companion on `path`, linked to the caller."
  @spec start(Path.t()) :: pid()
  def start(path) do
    spawn_link(fn -> syncer_loop(open_raw(path), nil) end)
  end

  defp open_raw(path) do
    {:ok, fh} = :file.open(path, [:raw, :read, :write, :binary])
    fh
  end

  # `effects` is a second handle, on the effect log, once it exists; only an
  # effect append syncs it.
  defp syncer_loop(fh, effects) do
    receive do
      {:sync, answers} ->
        # A backlog of ticks is one sync: datasync covers every write before it.
        answers = drain_syncs([answers])
        :ok = :file.datasync(fh)
        answer(answers)
        syncer_loop(fh, effects)

      {:sync, answers, from, ref} ->
        :ok = :file.datasync(fh)
        answer(answers)
        send(from, {ref, :ok})
        syncer_loop(fh, effects)

      {:sync_effects, answers} ->
        :ok = :file.datasync(effects)
        answer(answers)
        syncer_loop(fh, effects)

      {:reopen, path, from, ref} ->
        _ = :file.close(fh)
        fh = open_raw(path)
        send(from, {ref, :ok})
        syncer_loop(fh, effects)

      {:reopen_effects, path, from, ref} ->
        if effects, do: _ = :file.close(effects)
        send(from, {ref, :ok})
        syncer_loop(fh, open_raw(path))

      :stop ->
        _ = :file.close(fh)
        if effects, do: _ = :file.close(effects)
        :ok
    end
  end

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

  @doc """
  A request only the companion can answer: the synchronous flush
  (`flush_now`, shutdown) or the swap onto a rewritten file. The monitor is
  the bound: a caller's call timeout bounds the caller, never this loop, and
  an unbounded wait here parks every append the log owes for the rest of its
  life. `:error` is what the `FsyncBatcher` already reads as "still dirty,
  sync it on the next drain", so a lost sync is retried rather than reported
  durable.
  """
  @spec call(pid(), (pid(), reference() -> term())) :: :ok | :error
  def call(syncer, request) do
    ref = Process.monitor(syncer)
    send(syncer, request.(self(), ref))

    receive do
      {^ref, :ok} ->
        Process.demonitor(ref, [:flush])
        :ok

      {:DOWN, ^ref, :process, _pid, _reason} ->
        :error
    end
  end
end

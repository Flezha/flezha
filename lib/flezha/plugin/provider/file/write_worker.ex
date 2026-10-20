defmodule Flezha.Plugin.Provider.File.WriteWorker do
  @moduledoc """
  The writing half of `Flezha.Plugin.Provider.File.Sink`: one process per
  enabled plugin, holding its handle on the target and a bounded queue of
  records waiting for the next batch. The target is a folder file or S3
  objects alike, written through `Flezha.Plugin.Provider.File.Storage`.

  A batch is one append of every record's bytes and a newline. A failed batch
  is undone and retried under the backoff ladder, so nothing is dropped and the
  retry never appends after half a line. Callers park past `queue_max`.

  What a batch confirms is the layout's. Under `append` the target is the path,
  and the data sync confirms the holds its batch carries. Under `rename` the
  target is the open roll, a hidden temp file in the path's directory or, under
  an S3 root, memory, and its holds wait for the roll to be committed: its part
  written as `part-<N>`, then the sidecar `.progress` naming `N` and the
  per-Cell `(epoch, seq)` watermark, and only then confirmed. A record the
  outbox guarantees at or below the watermark is confirmed without being
  written again.
  """

  alias Flezha.Config.Backoff
  alias Flezha.Config.Durations
  alias Flezha.Dlq.Entry, as: DlqEntry
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Plugin.Provider.File.Storage
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.{Durable, Outbox}

  use GenServer
  use Flezha.Process.Timer, keys: [:linger, :retry, :roll]

  require Logger

  # The prefix is derived from the provider's id; nothing here names a root.
  @provider "file_sink"
  @progress ".progress"

  defstruct [
    :plugin_id,
    :path,
    :relay_id,
    :commit,
    :linger_ms,
    :max_batch,
    :queue_max,
    :retry_backoff_ms,
    :retry_max_backoff_ms,
    :roll_after_ms,
    :roll_bytes,
    # Where the bytes go, and its handle on the target.
    :storage,
    :io,
    # What a batch is appended to: `path` under `append`, the open roll's temp
    # file under `rename`.
    :target,
    :open_error,
    :last_error,
    :last_flush_ms,
    # Under `rename`, the committed watermark and the open roll; nil until the
    # directory has been reconciled.
    :roll,
    timers: %{},
    buffer: :queue.new(),
    buffer_len: 0,
    waiters: [],
    consecutive_failures: 0
  ]

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)

  @doc "Started by the extension table at activation, from the declared spec."
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Map.fetch!(opts, :name))

  @doc """
  Enqueue one record. Replies `:ok` at once while the queue is under
  `:queue_max`; past it the caller parks until a flush frees space.
  """
  @spec enqueue(GenServer.server(), binary(), map()) :: :ok
  def enqueue(server, msg, metadata) when is_binary(msg) and is_map(metadata),
    do: GenServer.call(server, {:enqueue, msg, metadata}, :infinity)

  @doc "Synchronous counters read (plugin health contract)."
  @spec stats(GenServer.server()) :: map()
  def stats(server), do: GenServer.call(server, :stats, Durations.get(:plugin_worker_call_ms))

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    fields =
      opts
      |> Map.take([
        :plugin_id,
        :path,
        :commit,
        :linger_ms,
        :max_batch,
        :queue_max,
        :retry_backoff_ms,
        :retry_max_backoff_ms,
        :roll_after_ms,
        :roll_bytes
      ])
      # Seeded at start: a sink that has never synced is measured from its own
      # birth, so a failing first write is judged by the same window.
      |> Map.merge(%{
        relay_id: opts[:source_relay_id],
        storage: Storage.Config.of(opts.path),
        target: if(opts.commit == "append", do: opts.path),
        last_flush_ms: now()
      })

    {:ok, struct(__MODULE__, fields), {:continue, :open}}
  end

  # Opened before any record arrives, so a path that cannot be written is a
  # verdict at once rather than at the first write. Under `rename` that is the
  # reconcile; the roll's temp file is created by the first batch.
  @impl true
  def handle_continue(:open, state) do
    started = if state.commit == "rename", do: reconciled(state), else: ensure_open(state)

    case started do
      {:ok, state} -> {:noreply, state}
      {:error, _reason, state} -> {:noreply, state}
    end
  end

  @impl true
  def handle_call({:enqueue, msg, metadata}, from, state) do
    if :binary.match(msg, "\n") == :nomatch do
      state |> buffer(msg, metadata) |> ProviderBehaviour.park_or_reply(from)
    else
      park(state, msg, metadata)
      {:reply, :ok, state}
    end
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       pending: state.buffer_len + held(state),
       consecutive_failures: state.consecutive_failures,
       last_error: state.last_error,
       last_flush_age_ms: now() - state.last_flush_ms,
       open_error: state.open_error
     }, state}
  end

  # A record in the open roll is synced but not yet delivered: its hold waits
  # for the roll's commit, and a commit that keeps failing leaves it pending.
  defp held(%{roll: %{holds: holds}}), do: length(holds)
  defp held(_no_roll), do: 0

  @impl true
  def handle_info(:linger, state), do: {:noreply, state |> timer_fired(:linger) |> flush()}
  def handle_info(:retry, state), do: {:noreply, state |> timer_fired(:retry) |> flush()}

  def handle_info(:roll, state),
    do: {:noreply, state |> timer_fired(:roll) |> roll_due() |> flush()}

  def handle_info(_msg, state), do: {:noreply, state}

  # A graceful stop writes what it holds and commits the open roll, so a disable
  # does not leave records the outbox handed over waiting for a re-drive. A
  # worker another has overtaken writes nothing more.
  @impl true
  def terminate({:progress_ahead, _stored}, state) do
    close(state)
    :ok
  end

  def terminate(_reason, state) do
    state |> flush() |> roll_due() |> flush() |> close()
    :ok
  end

  # ---------- the queue ----------

  defp buffer(state, msg, metadata) do
    state = %{
      state
      | buffer: :queue.in({msg, metadata}, state.buffer),
        buffer_len: state.buffer_len + 1
    }

    cond do
      state.buffer_len >= state.max_batch and not timer_armed?(state, :retry) ->
        state |> cancel_timer(:linger) |> flush()

      not timer_armed?(state, :linger) and not timer_armed?(state, :retry) ->
        arm_timer(state, :linger, state.linger_ms, :linger)

      true ->
        state
    end
  end

  # A record holding a newline cannot be one line, and the sink writes bytes
  # rather than editing them, so it is parked whole. A delivery-guaranteed
  # record's hold is answered once the park is durable, which is what moves its
  # cursor past it.
  defp park(state, msg, metadata) do
    DlqEntry.new!(%{
      relay_class: state.relay_id || @provider,
      tasklet_id: state.plugin_id,
      cell_key: Map.get(metadata, "flz.cell") || state.plugin_id,
      channel: Map.get(metadata, "channel", ""),
      error: "embedded_newline: a record holding a newline cannot be one line of #{state.path}",
      attempts: 1,
      payload: msg
    })
    |> DlqStore.append_async(fn _entry_ref -> Outbox.confirm(metadata) end)

    Logger.warning(
      "[Plugin.Provider.File.Sink] parked a record holding a newline path=#{state.path} " <>
        "bytes=#{byte_size(msg)}"
    )

    emit(:refused, %{bytes: byte_size(msg)}, Map.put(labels(state), :reason, :embedded_newline))
  end

  # ---------- flush ----------

  # A due roll is committed before another batch is written, so one roll is in
  # flight at a time: what lets the reconcile trust a part one ahead.
  defp flush(%{roll: %{due?: true}} = state) do
    case commit_roll(state) do
      {:ok, state} -> flush(state)
      {:error, reason, state} -> retry(state, reason)
    end
  end

  defp flush(%{buffer_len: 0} = state), do: state

  defp flush(state) do
    {taken, rest} = :queue.split(min(state.buffer_len, state.max_batch), state.buffer)
    records = :queue.to_list(taken)

    with {:ok, state} <- reconciled(state),
         {lines, confirmed, rolled} = admit(state, records),
         {:ok, state} <- write(state, lines) do
      # The sync is what these holds waited on: the batch answers them in one
      # report, and only now may their cursors advance.
      Outbox.confirm_many(confirmed)
      emit(:shipped, %{count: length(lines)}, labels(state))

      %{
        state
        | buffer: rest,
          buffer_len: state.buffer_len - length(records),
          consecutive_failures: Backoff.reset(:file_flush, state.plugin_id),
          last_error: nil,
          last_flush_ms: now()
      }
      |> into_roll(rolled)
      |> ProviderBehaviour.release_waiters()
      |> flush()
    else
      {:error, reason, state} -> retry(state, reason)
    end
  end

  # What a batch writes, which holds its sync confirms, and what it adds to the
  # open roll. Under `append` every record is written and confirmed.
  defp admit(%{commit: "append"}, records),
    do: {Enum.map(records, &line/1), Enum.map(records, &elem(&1, 1)), nil}

  # Under `rename` a guaranteed record at or below a committed part's watermark
  # is confirmed with this batch and not written; one at or below the open
  # roll's leaves its hold to the roll. Every other record is written, and its
  # hold waits for the roll.
  defp admit(%{roll: roll}, records) do
    {lines, confirmed, held, marks, bytes} =
      Enum.reduce(records, {[], [], [], roll.marks, 0}, &judge(&1, &2, roll.committed))

    {Enum.reverse(lines), confirmed, {held, marks, bytes}}
  end

  defp judge({msg, metadata} = record, {lines, confirmed, held, marks, bytes}, committed) do
    case verdict(metadata, committed, marks) do
      :committed ->
        {lines, [metadata | confirmed], held, marks, bytes}

      :in_roll ->
        {lines, confirmed, [metadata | held], marks, bytes}

      {:write, marks} ->
        {[line(record) | lines], confirmed, [metadata | held], marks, bytes + byte_size(msg) + 1}
    end
  end

  # Only a record the outbox guarantees is judged: its seq names one output of
  # one entry. Any other seq may repeat across invocations, and a watermark over
  # it would drop a record that was never written.
  defp verdict(metadata, committed, marks) do
    with true <- Outbox.guaranteed?(metadata),
         {:ok, identity} <- Outbox.output_identity(metadata) do
      cond do
        covers?(committed, identity) -> :committed
        covers?(marks, identity) -> :in_roll
        true -> {:write, Map.put(marks, identity.cell, identity)}
      end
    else
      _unguarded -> {:write, marks}
    end
  end

  defp covers?(marks, %{cell: cell, sort: sort}) do
    case marks do
      %{^cell => mark} -> sort <= mark.sort
      _no_mark -> false
    end
  end

  defp line({msg, _metadata}), do: [msg, ?\n]

  defp retry(state, reason) do
    failures = state.consecutive_failures + 1

    Logger.warning(
      "[Plugin.Provider.File.Sink] write failed path=#{state.path} attempt=#{failures} " <>
        "pending=#{state.buffer_len}: #{inspect(reason)}"
    )

    emit(:failed, %{consecutive_failures: failures}, Map.put(labels(state), :reason, reason))

    delay =
      Backoff.arm(:file_flush, failures, state.plugin_id,
        base: state.retry_backoff_ms,
        cap: state.retry_max_backoff_ms
      )

    arm_timer(
      %{state | consecutive_failures: failures, last_error: reason},
      :retry,
      delay,
      :retry
    )
  end

  defp write(state, []), do: {:ok, state}

  defp write(state, iodata) do
    with {:ok, state} <- ensure_open(state),
         do: through(state, state.storage.append(state.io, iodata))
  end

  # ---------- the rename layout ----------

  # The roll timer starts with a roll's first bytes, and `roll_bytes` commits
  # the roll early. Holds are kept newest first.
  defp into_roll(state, nil), do: state

  defp into_roll(%{roll: roll} = state, {held, marks, bytes}) do
    roll = %{roll | holds: held ++ roll.holds, marks: marks, bytes: roll.bytes + bytes}

    cond do
      roll.bytes >= state.roll_bytes ->
        %{cancel_timer(state, :roll) | roll: %{roll | due?: true}}

      roll.bytes > 0 and not timer_armed?(state, :roll) ->
        arm_timer(%{state | roll: roll}, :roll, state.roll_after_ms, :roll)

      true ->
        %{state | roll: roll}
    end
  end

  defp roll_due(%{roll: %{bytes: bytes} = roll} = state) when bytes > 0,
    do: %{state | roll: %{roll | due?: true}}

  defp roll_due(state), do: state

  # The part is put in place, then the sidecar naming it is written, and only
  # then are its holds confirmed. The open before the commit makes a cut a
  # failed batch still owes, so half a batch is never committed.
  defp commit_roll(%{roll: %{placed?: false} = roll} = state) do
    part = part_path(state.path, roll.part)

    with {:ok, state} <- ensure_open(state),
         {:ok, state} <- through(state, state.storage.commit_part(state.io, state.target, part)),
         do: commit_roll(%{state | roll: %{roll | placed?: true}})
  end

  defp commit_roll(%{roll: roll} = state) do
    case write_progress(state, roll.part, roll.marks) do
      :ok ->
        Outbox.confirm_many(Enum.reverse(roll.holds))

        {:ok,
         %{
           state
           | target: temp_path(state.path, roll.part + 1),
             roll: new_roll(roll.part, roll.marks)
         }}

      {:error, {:progress_ahead, stored}} ->
        overtaken!(state, stored)

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  # Another worker has committed a later part here, one a move started while
  # this one lingered. Confirming would answer holds for a part that worker has
  # overwritten, so this one stops, and the outbox re-drives its records to the
  # live one.
  defp overtaken!(state, stored) do
    Logger.warning(
      "[Plugin.Provider.File.Sink] progress names part #{stored} path=#{state.path}, " <>
        "ahead of part #{state.roll.part}; stopping without confirming"
    )

    exit({:progress_ahead, stored})
  end

  defp new_roll(last, marks),
    do: %{
      part: last + 1,
      committed: marks,
      marks: marks,
      holds: [],
      bytes: 0,
      due?: false,
      placed?: false
    }

  # Under `rename` the directory is reconciled before the first batch. A refused
  # reconcile is tried again by the next flush, so a directory a user
  # repairs is taken up without a restart.
  defp reconciled(%{commit: "rename", roll: nil} = state) do
    case reconcile(state.storage, state.path) do
      {:ok, roll} ->
        {:ok, %{state | roll: roll, target: temp_path(state.path, roll.part), open_error: nil}}

      {:error, reason} ->
        refuse_open(state, reason)
    end
  end

  defp reconciled(state), do: {:ok, state}

  # Every temp file, and a part one ahead of the sidecar, holds records whose
  # holds were never confirmed, so they are deleted and the re-drive writes those
  # records again. A part further ahead is nothing a crash leaves, and is refused
  # before anything is deleted.
  defp reconcile(storage, dir) do
    with {:ok, names} <- storage.list(dir),
         {:ok, last, marks} <- read_progress(storage, dir),
         :ok <- in_step(names, last),
         :ok <- remove_uncommitted(storage, dir, names, last) do
      {:ok, new_roll(last, marks)}
    end
  end

  defp in_step(names, last) do
    case names |> Enum.flat_map(&part_number/1) |> Enum.max(fn -> 0 end) do
      newest when newest > last + 1 -> {:error, {:part_ahead_of_progress, newest, last}}
      _in_step -> :ok
    end
  end

  defp remove_uncommitted(storage, dir, names, last) do
    names
    |> Enum.filter(&(Durable.in_progress?(&1) or part_number(&1) == [last + 1]))
    |> Enum.reduce_while(:ok, fn name, :ok ->
      case storage.delete(Path.join(dir, name)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:remove, name, reason}}}
      end
    end)
  end

  defp part_number("part-" <> digits) do
    if digits =~ ~r/^\d+$/, do: [String.to_integer(digits)], else: []
  end

  defp part_number(_name), do: []

  defp part_path(dir, part), do: Path.join(dir, part_name(part))

  # The open roll wears the sidecar's declared in-progress suffix under a hidden
  # name: a consumer listing `part-*` never sees it, and the reconcile's one
  # question, `Durable.in_progress?/1`, finds it beside the sidecar's own temp.
  defp temp_path(dir, part),
    do: Durable.in_progress_path(Path.join(dir, "." <> part_name(part)), :file_sink_progress)

  defp part_name(part), do: "part-" <> String.pad_leading(Integer.to_string(part), 8, "0")

  defp read_progress(storage, dir) do
    case storage.read(Path.join(dir, @progress)) do
      {:ok, body} -> decode_progress(body)
      {:error, :enoent} -> {:ok, 0, %{}}
      {:error, reason} -> {:error, {:progress_unreadable, reason}}
    end
  end

  # A sidecar that cannot be read back is refused rather than taken for none,
  # which would delete the part it names.
  defp decode_progress(body) do
    with {:ok, %{"part" => last, "watermarks" => marks}}
         when is_integer(last) and last >= 0 and is_map(marks) <- Jason.decode(body),
         {:ok, marks} <- decode_marks(marks) do
      {:ok, last, marks}
    else
      _corrupt -> {:error, :progress_corrupt}
    end
  end

  defp decode_marks(encoded) do
    Enum.reduce_while(encoded, {:ok, %{}}, fn {key, value}, {:ok, marks} ->
      with {:ok, cell} <- Base.url_decode64(key, padding: false),
           [epoch, seq] when is_integer(epoch) <- value,
           stamps = %{"flz.cell" => cell, "flz.seq" => seq, "flz.epoch" => epoch},
           {:ok, identity} <- Outbox.output_identity(stamps) do
        {:cont, {:ok, Map.put(marks, cell, identity)}}
      else
        _corrupt -> {:halt, :error}
      end
    end)
  end

  # A Cell is keyed in base64url: its key is bytes, and JSON carries text.
  defp write_progress(state, last, marks) do
    watermarks =
      Map.new(marks, fn {cell, identity} ->
        {Base.url_encode64(cell, padding: false), [identity.epoch, identity.seq]}
      end)

    body = Jason.encode!(%{part: last, watermarks: watermarks})
    state.storage.write_progress(Path.join(state.path, @progress), last, body)
  end

  # ---------- the target ----------

  defp ensure_open(state) do
    case state.storage.open_write(state.io, state.target, holds_bytes?(state)) do
      {:ok, io} -> {:ok, %{state | io: io, open_error: nil}}
      {:error, reason, io} -> refuse_open(%{state | io: io}, reason)
    end
  end

  # An open roll that already holds bytes must find them where they went.
  defp holds_bytes?(%{roll: %{bytes: bytes}}), do: bytes > 0
  defp holds_bytes?(_no_roll), do: false

  defp through(state, {:ok, io}), do: {:ok, %{state | io: io}}
  defp through(state, {:error, reason, io}), do: {:error, reason, %{state | io: io}}

  defp refuse_open(state, reason) do
    error = {reason, state.path}

    if state.open_error != error,
      do:
        Logger.warning(
          "[Plugin.Provider.File.Sink] cannot open path=#{state.path}: #{inspect(reason)}"
        )

    {:error, {:open, reason}, %{state | open_error: error}}
  end

  defp close(%{io: nil} = state), do: state
  defp close(state), do: %{state | io: state.storage.close(state.io)}

  defp labels(state), do: %{plugin_id: state.plugin_id, path: state.path}

  defp now(), do: :erlang.monotonic_time(:millisecond)
end

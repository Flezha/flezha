defmodule Flezha.Operator.Replay do
  @moduledoc """
  Restore-and-replay for one operator: crash resume, late-input correction and
  the user's `flz operator replay`.

  Restore and correction are one thing, which is why they are one module. A
  correction leaves the on-disk state at a rewound baseline with the input-log
  watermark lowered to match, and from there the ordinary resume replay IS the
  correction — they differ only in how the baseline was chosen.

  Everything here runs in the `Overwatch` process and reads its state without
  changing it. Invariant: the exceptions are `arm_bind_reforward/1` and
  `dispatch_restore/3`, because the un-park path drives
  `Lifecycle.complete_init/4` inside a task where `self()` is not the Overwatch.
  """

  alias Flezha.Config

  alias Flezha.Config.Durations
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Operator.Lookup
  alias Flezha.Operator.Overwatch
  alias Flezha.Process.Timer
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.State.Commands
  alias Flezha.State.DurabilityTier
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.TaskJournal
  alias Flezha.State.Restore
  alias Flezha.State.WalTransfer
  alias Flezha.State.WalWriter

  require Logger

  # ---------- process API ----------

  @doc """
  A Cell reported an input behind its event-time high-water but inside the
  correction window. Coalesce it; the debounce fires the replay.
  """
  @spec request_correction(pid(), tuple(), non_neg_integer()) :: :ok
  def request_correction(overwatch, cell_id, target_event_time),
    do: GenServer.cast(overwatch, {:correction_request, cell_id, target_event_time})

  @doc """
  Manual replay (`flz operator replay`): rewind a Cell to `baseline_seq`
  and re-run its retained inputs from there through the current code.

  The same transaction a late input drives, with the baseline chosen rather than
  derived. Synchronous, so the CLI reports the outcome rather than a hopeful
  "queued".
  """
  @spec replay_from(pid(), tuple(), non_neg_integer()) :: :ok | {:error, term()}
  def replay_from(overwatch, cell_id, baseline_seq),
    do: GenServer.call(overwatch, {:replay_from, cell_id, baseline_seq}, 60_000)

  @doc """
  External trigger for the bind-driven per-Cell replay. Called from
  `Flezha.State.Recovery.resync_for_overwatch/2` when a non-bind caller
  (the convergence Loop's rebind heal) needs to re-issue the
  `RestoreInitState` handshake for an already-bound Overwatch.

  Routes the work back to the Overwatch process via a cast so the caller never
  blocks on enumeration / dispatch — the Overwatch is the single owner of
  "enumerate my cells, dispatch RestoreInitState for each".
  """
  @spec kick_restore_for_bound_runner(pid(), binary()) :: :ok
  def kick_restore_for_bound_runner(overwatch_pid, runner_id)
      when is_pid(overwatch_pid) and is_binary(runner_id) do
    GenServer.cast(overwatch_pid, {:kick_restore, runner_id})
  end

  @doc """
  Fold a correction request into the pending set, arming the debounce on the
  first one.

  Concurrent late inputs for one Cell collapse into the one replay that
  reaches furthest back — a burst of five late trades is one restore-replay
  cycle, not five. The debounce is what gives the burst time to arrive; the
  `Map.has_key?` guard is what keeps it a single timer.
  """
  @spec note_correction(Overwatch.t(), tuple(), non_neg_integer()) :: Overwatch.t()
  def note_correction(%Overwatch{} = state, cell_id, target) do
    pending = state.corrections

    if Map.has_key?(pending, cell_id) do
      %{state | corrections: Map.update!(pending, cell_id, &min(&1, target))}
    else
      %Overwatch{
        state
        | corrections: Map.put(pending, cell_id, target),
          timers:
            Timer.arm(
              state.timers,
              {:correction, cell_id},
              correction_debounce_ms(),
              {:run_correction, cell_id}
            )
      }
    end
  end

  defp correction_debounce_ms(),
    do: Durations.get(:correction_debounce_ms)

  # ---------- bind-time restore ----------

  @doc """
  Walk the on-disk Cell state for this operator and dispatch `RestoreInitState`
  to the SDK for each Cell that has a non-empty payload.

  Skipped when the operator arrived via HotSwap — handover already delivers the
  state in-band — and when persistence is disabled for this operator class.
  """
  @spec restore_on_bind(Overwatch.t(), map()) :: :ok
  def restore_on_bind(%Overwatch{arrived_via_hotswap?: true}, _operator),
    do: :ok

  def restore_on_bind(%Overwatch{} = state, operator) do
    # An awaited restore already ran, before the tail was re-forwarded.
    unless awaited_restore?(state), do: dispatch_restore(state, operator.opts, state.runner_id)
    :ok
  end

  @doc """
  Whether the bind restores every persisted Cell, awaited, before it
  re-forwards the input tail onto them: an unbounded operator whose input is durable (the
  tier, or one that sleeps, whose tail is what arrived while it slept). A
  bounded operator's run holds its tail behind its own restore gate instead.
  """
  @spec awaited_restore?(Overwatch.t()) :: boolean()
  def awaited_restore?(%Overwatch{} = state),
    do: floor_reporter(state) in [:cells, :forward]

  @doc """
  Enumerate this operator's persisted Cells and ship a `RestoreInitState` for
  each, off-process — the replay does blocking disk reads and never touches state.

  The bind path reaches it through `restore_on_bind/2`; the
  convergence heal reaches it through the Overwatch's `{:kick_restore, _}` cast.
  """
  @spec dispatch_restore(Overwatch.t(), keyword(), binary()) :: :ok
  def dispatch_restore(%Overwatch{} = state, operator_opts, runner_id) do
    # The SDK keys per-Cell state by the operator INSTANCE id (== `state.id`),
    # so the WAL lives under `state/{operator_id}/...`. Enumerate by the same key.
    operator_class = Keyword.get(operator_opts, :operator_class, state.id)
    overwatch = owner_pid(state)

    spawn(fn ->
      :ok = adopt_foreign_state(operator_opts, operator_class)
      replay_persisted_cells(operator_class, runner_id, overwatch)
    end)

    :ok
  end

  @doc """
  Pull the instance's durable files from the node its state was last assembled
  on (`:state_source_node`) while this node holds none of them: a partition
  member placed away from the node that rescaled it, or a migration whose
  handover arrived empty. A node that holds any of them already runs the
  instance's state, and a copy from the source would overwrite that progress
  with what the source was left holding.
  """
  @spec adopt_foreign_state(keyword(), binary()) :: :ok
  def adopt_foreign_state(operator_opts, operator_class) do
    with source when source != nil <- foreign_state_source(operator_opts),
         [] <- Paths.durable_refs(operator_class) do
      _ = WalTransfer.pull_operator(source, operator_class)
    end

    :ok
  end

  # `:state_source_node` names the node an operator's durable state was last
  # assembled on. Only a foreign one means anything — a local value is already
  # where the ordinary restore looks.
  defp foreign_state_source(operator_opts) do
    case Keyword.get(operator_opts, :state_source_node) do
      source when is_atom(source) and not is_nil(source) and source != node() -> source
      _ -> nil
    end
  end

  # Bind-time restore follows the class's persistence switch, for either kind;
  # for a bounded operator it IS its crash-resume.
  defp replay_persisted_cells(operator_class, runner_id, overwatch) do
    # Awaited, not fire-and-forget: the reply carries whether the SDK had to
    # migrate records from an older schema version, and the controller answers
    # that with a compaction. This already runs off-process (the enumeration
    # does blocking disk reads) and Cells are independent, so the awaits run
    # concurrently rather than serialising a large rebind.
    refusals =
      restorable_cells(operator_class)
      |> Flezha.Parallel.map(&restore_cell(runner_id, &1),
        max_concurrency: restore_concurrency()
      )
      |> Enum.filter(&match?({:error, _}, &1))

    # Off-process: the bind already returned, so the verdict travels back as a
    # message — the Overwatch is the only thing that can still stop it.
    case refusals do
      [] -> :ok
      [{:error, reason} | _] -> send(overwatch, {:restore_refused, reason})
    end

    :ok
  end

  # Starting the writer replays its WAL scan: it arms the outbox re-drive of
  # what the Cell's records still owe the sinks, and repopulates the Cell's
  # durable consumed-offset watermark.
  defp restore_cell(runner_id, cell_id) do
    _ = Flezha.State.Supervisor.find_or_start(cell_id)
    await_restore(runner_id, cell_id)
  end

  @doc """
  Re-derive what a durable bounded operator's running Cell writers hold from its
  journal, before the bound runner writes anything newer. A writer still
  running from before this bind holds outputs of stages the journal may never
  name (an attempt the rebound runner interrupted, an iteration a rewind
  unwound), and drops them; a writer that is not running derives the same when
  it boots.
  """
  @spec seed_bounded_outboxes(Overwatch.t()) :: :ok
  def seed_bounded_outboxes(%Overwatch{durable_bounded: true} = state) do
    operator_class = Overwatch.operator_class(state)
    committed = TaskJournal.committed_records(operator_class)

    for writer <- Flezha.State.Supervisor.live_writers(operator_class),
        do: WalWriter.seed_commits(writer, committed)

    :ok
  end

  def seed_bounded_outboxes(%Overwatch{}), do: :ok

  @doc """
  Await one Cell's `RestoreInitState` and report a decisive verdict.

  The one place the awaited restore's ANSWER is read. `{:error, reason}` covers
  every way an SDK refuses a fold — language, schema, CRC, truncation, apply —
  because they mean one thing here: the durable state did not reach the runner,
  and folding new inputs onto empty state would lose it silently. A Cell
  with no snapshot and a zero-length WAL is a SUCCESS and binds normally.
  """
  @spec await_restore(binary(), Paths.cell_id()) :: :ok | {:error, term()}
  def await_restore(runner_id, cell_id) do
    case Commands.await_restore_init_state(runner_id, cell_id) do
      {:ok, _raw} ->
        :ok

      # The round trip never completed — a timeout, a dead control unit, a
      # runner shutting down. That is not a verdict on the state, and the
      # rebind path already owns a runner that stopped answering. Parking here
      # would turn ordinary teardown churn into stuck operators.
      {:error, reason} when not is_binary(reason) ->
        Logger.debug(
          "[Overwatch] restore unanswered cell=#{inspect(cell_id)} " <>
            "runner=#{runner_id}: #{inspect(reason)}"
        )

        :ok

      # The SDK answered and refused: the reason is its own error text, joined
      # from the reply's `errors`.
      {:error, reason} ->
        Logger.error(
          "[Overwatch] restore REFUSED cell=#{inspect(cell_id)} runner=#{runner_id}: " <>
            "#{inspect(reason)}; leaving the operator unbound rather than folding new " <>
            "inputs onto empty state"
        )

        :telemetry.execute(
          [:flezha, :state, :restore_refused],
          %{count: 1},
          %{cell: cell_id, runner: runner_id, reason: reason}
        )

        {:error, reason}
    end
  end

  defp restore_concurrency,
    do: Config.get(:bind_restore_concurrency)

  # Persisted, restorable Cells of this class, empty when its persistence is
  # off.
  defp restorable_cells(operator_class) do
    if PersistenceResolver.persistence_enabled?(operator_class) do
      # Remote hydration runs first, and in this one place: a controller
      # adopting an operator whose disk is gone then finds its Cells,
      # task-journal and input log on disk by the time this walks
      # them — and every caller (bind restore, the tier's awaited restore, the
      # resume-gate count) sees the same set. Inert unless a durability backend
      # is configured and the local state really is absent.
      DurabilityTier.hydrate_operator(operator_class)

      operator_class
      |> Paths.cells_of()
      |> Enum.filter(&Restore.has_persisted_state?/1)
    else
      []
    end
  end

  @doc """
  Number of persisted Cells the bind-time restore path will ship a
  `RestoreInitState` for on this (re)bind — the value threaded onto
  `AddOperator.resume_cell_count` so a durable bounded driver holds its
  first Tasklet until that many restores land (crash-resume,
  `bounded-operator-durability`).

  `0` for a fresh spawn and for a class whose persistence is off. Matches the
  frames `replay_persisted_cells/3` ships (same enumeration +
  `has_persisted_state?` filter); only a bounded operator's run waits on it.
  """
  @spec resume_cell_count(binary()) :: non_neg_integer()
  def resume_cell_count(operator_class),
    do: length(restorable_cells(operator_class))

  @doc """
  The `awaited_restore?/1` bind path: restore each persisted Cell through the
  awaited control round-trip, so the SDK has applied state + consumed floor
  before the bind re-forwards the input tail (`reforward_undelivered_tail/1`).

  Runs in the Overwatch process before plugin activation; Cells ahead of the
  min watermark skip re-forwarded offsets they already consumed (SDK
  `consumed_floor`).
  """
  @spec restore_cells_awaited(Overwatch.t()) :: {:ok, Overwatch.t()} | {:error, term()}
  def restore_cells_awaited(%Overwatch{} = state) do
    refused =
      state
      |> Overwatch.operator_class()
      |> restorable_cells()
      |> Enum.find_value(fn cell_id ->
        case restore_cell(state.runner_id, cell_id) do
          :ok -> nil
          {:error, reason} -> reason
        end
      end)

    # Stop before the re-forward: re-forwarding the input tail onto a Cell
    # whose baseline never landed is the silent loss this guards.
    if refused, do: {:error, refused}, else: {:ok, state}
  end

  # ---------- the instance's floor ----------

  @doc """
  Who reports the instance's floor in its input log, which is what makes its
  input durable: its Cells on the effectively-once tier, its journal when it
  journals, the controller's forwards for a plain unbounded operator that sleeps (the log
  then retains only what arrived while nobody could take it), or nobody, and
  its input is not journaled.
  """
  @spec floor_reporter(Overwatch.t()) :: :cells | :journal | :forward | nil
  def floor_reporter(%Overwatch{effectively_once: true}), do: :cells
  def floor_reporter(%Overwatch{durable_bounded: true}), do: :journal
  def floor_reporter(%Overwatch{sleeps: true}), do: :forward
  def floor_reporter(%Overwatch{}), do: nil

  defp forward_reporter(operator_class),
    do: {operator_class, operator_class, "_forwarded"}

  @doc """
  A payload reached the bound runner: for a plain sleeping unbounded operator its offset is
  now the runner's, and the instance's floor moves past it.
  """
  @spec forwarded(Overwatch.t(), {binary(), map()}) :: :ok
  def forwarded(%Overwatch{} = state, {_message, %{"flz.offset" => offset}}) do
    if floor_reporter(state) == :forward,
      do: release(state, String.to_integer(offset)),
      else: :ok
  end

  def forwarded(%Overwatch{}, _payload), do: :ok

  @doc """
  The park record `state` sleeps with. A plain sleeping unbounded operator's floor lives
  only in memory while it is awake, so its record carries it, and a boot that
  rediscovers the sleep reads it back through `release_parked/2`.
  """
  @spec floor_at_park(Overwatch.t(), ClassRegistry.park()) :: ClassRegistry.park()
  def floor_at_park(%Overwatch{} = state, park) do
    if floor_reporter(state) == :forward,
      do: %{park | consumed_offset: InputLog.watermark(Overwatch.operator_class(state))},
      else: park
  end

  @doc """
  Seed the instance's floor from its park record at boot: the offset a
  bounded operator parked unread, or a plain sleeping unbounded operator's forwarded floor.
  """
  @spec release_parked(Overwatch.t(), ClassRegistry.park() | nil) :: :ok
  def release_parked(%Overwatch{} = state, %{consumed_offset: offset}) when is_integer(offset),
    do: release(state, offset)

  def release_parked(%Overwatch{}, _park), do: :ok

  defp release(%Overwatch{} = state, offset), do: release(floor_reporter(state), state, offset)

  defp release(:journal, state, offset), do: TaskJournal.release_input(state.id, offset)

  defp release(:forward, state, offset) do
    class = Overwatch.operator_class(state)
    InputLog.record_consumed(class, forward_reporter(class), offset)
  end

  defp release(_cells_or_nobody, _state, _offset), do: :ok

  # ---------- re-forwarding a durable tail ----------

  @doc """
  Re-forward the retained input-log records above the instance's floor (the
  log's watermark), in offset order, each stamped with its original
  `flz.offset` and never re-journaled — the log already holds them,
  post-`:on_input`.

  The one re-forward, for either kind: every bind runs it once, before the
  instance's sources come up, which is what makes a fresh bind, a crash resume,
  a migration and a rehydrate from suspension one path, and a data-stream bind
  that could not carry an earlier input runs the same function. An instance
  without durable input has no log, and it does nothing.
  """
  @spec reforward_undelivered_tail(Overwatch.t()) :: Overwatch.t()
  def reforward_undelivered_tail(%Overwatch{} = state) do
    replay_slice(state, InputLog.undelivered(Overwatch.operator_class(state)))
    state
  end

  @doc """
  How many records `reforward_undelivered_tail/1` would re-forward now: the
  retained records above the instance's floor, `0` for none and for a log that
  refused. Read off the log's tail, so no record is read.
  """
  @spec owed_count(binary()) :: non_neg_integer()
  def owed_count(state_key) do
    case InputLog.tail(state_key) do
      {:ok, floor, last} when last > floor -> last - floor
      _owes_nothing_or_refused -> 0
    end
  end

  @doc """
  Arm `reforward_undelivered_tail/1` for a payload an unbound stream could not
  carry: ask the operator's channel to announce its next bind, which the
  Overwatch answers with the re-forward. The channel's waiter set IS the arming,
  so a second undelivered payload costs one call and schedules nothing extra.

  `:armed` means a re-forward will happen — including the race where the stream
  came up between the failed delivery and this call, settled by handing the
  Overwatch the notification it just missed. `:unarmed` means the operator has
  no channel that can ever bind, so nothing will re-forward and the caller must
  not report the payload accepted.
  """
  @spec arm_bind_reforward(Overwatch.t()) :: :armed | :unarmed
  def arm_bind_reforward(%Overwatch{channel: channel} = state) do
    owner = owner_pid(state)

    case Gate.notify_on_bind(channel, owner) do
      :unbound ->
        :armed

      :bound ->
        send(owner, {:stream_bound, channel})
        :armed

      :no_channel ->
        :unarmed
    end
  end

  # Who handles the notification — deliberately not `self()`. The un-park path
  # runs the whole bind, restore and journal re-forward included, inside a task,
  # and a task registered as the waiter dies before the bind can reach it. The
  # class registry already names the live Overwatch for exactly this reason (the
  # fsync-gated outbox flush resolves its `:on_output` target through it); the
  # fallback covers a state that was never registered, which is only a test's.
  defp owner_pid(%Overwatch{} = state) do
    case Lookup.overwatch(Overwatch.operator_class(state)) do
      {:ok, pid} -> pid
      {:error, _reason} -> self()
    end
  end

  # The one re-forward path, shared by crash recovery and deliberate correction
  # — which is the whole point. A correction leaves the on-disk state at a
  # rewound baseline with the input-log watermark lowered to match, and from
  # there the ordinary resume replay *is* the correction. The two differ only in
  # how the baseline was chosen.
  #
  # For a correction-declaring class every replayed slice is marked, and the
  # SDK holds it to the end marker so it can order it by event time before
  # dispatching any of it. Re-forwarding in arrival order would put the late
  # input back after the inputs that overtook it and recompute the same wrong
  # answer.
  defp replay_slice(%Overwatch{} = state, read) do
    case read do
      [] ->
        :ok

      # The log refused a record on the way: it serves no range, and the slice
      # is not re-forwarded from the prefix that happened to verify, which would
      # hand the instance part of its log as all of it. The refusal was reported
      # where it was read.
      {:error, _refusal} ->
        :ok

      payloads ->
        marks = if state.correction_window_ms, do: %{"flz.replay" => "1"}, else: %{}

        payloads
        |> Enum.map(fn {offset, message, metadata} ->
          {message,
           metadata
           |> Map.put("flz.offset", Integer.to_string(offset))
           |> Map.merge(marks)}
        end)
        |> then(&reforward(state, &1))

        # Close the slice. A broadcast rather than a flag on the last input:
        # an input reaches exactly one Cell (routed by key), so a marker
        # riding one would strand the slice every *other* Cell buffered.
        if state.correction_window_ms do
          reforward(state, [{"", %{"flz.replay_end" => "1"}}])
        end

        :ok
    end
  end

  # Re-forward already-durable payloads, stamped with the offset they were
  # journaled at. The delivery half of every resume, and deliberately NOT an
  # ingest: these payloads are already in their log and already ran
  # `:on_input`, so re-journaling them would duplicate them.
  defp reforward(%Overwatch{} = state, payloads) do
    dropped? =
      Enum.reduce(payloads, false, fn payload, any_dropped? ->
        Overwatch.deliver(state, payload) != :ok or any_dropped?
      end)

    # A re-forward the channel could not carry — no binding, or a send buffer
    # that never drained under the held sender — is dropped exactly as a fresh
    # ingest is, and the tail this pass could not deliver is still the tail the
    # next bind owes, so it arms the same re-forward the ingest path arms.
    if dropped?, do: arm_bind_reforward(state)

    :ok
  end

  # ---------- correction ----------

  @doc """
  The correction transaction. Runs synchronously in the Overwatch process,
  which is what makes it safe: the Overwatch is also the only thing that
  journals and forwards input for this operator, so while it is in here no
  new input can be journaled and the slice's upper bound cannot move.

  A crash anywhere in here needs no marker to recover. Before the rewind
  commits, nothing durable has changed: the late input is still in the log,
  still unconsumed, and the ordinary resume re-forwards it, the SDK
  re-classifies it, and we end up back here. After the rewind commits, the
  on-disk state *is* the baseline and the watermark was lowered to match — so
  the ordinary resume replay IS this replay. That is why crash recovery and
  deliberate correction share one path.
  """
  @spec run_correction(Overwatch.t(), tuple(), non_neg_integer()) :: Overwatch.t()
  def run_correction(%Overwatch{} = state, cell_id, target) do
    result =
      with {:ok, writer} <- Flezha.State.Supervisor.find_or_start(cell_id),
           {:ok, %{seq: baseline_seq, consumed_offset: consumed}} <-
             WalWriter.correction_baseline(writer, target) do
        rewind_and_replay(state, cell_id, writer, baseline_seq, consumed)
      end

    log_correction(state, cell_id, target, result)
    state
  end

  @doc """
  The user's half (`flz operator replay`): the baseline is given, so only
  the consumed offset has to be looked up. Everything downstream is the same
  transaction `run_correction/3` drives.
  """
  @spec run_replay(Overwatch.t(), tuple(), non_neg_integer()) :: :ok | {:error, term()}
  def run_replay(%Overwatch{} = state, cell_id, baseline_seq) do
    result =
      with {:ok, writer} <- Flezha.State.Supervisor.find_or_start(cell_id),
           consumed when is_integer(consumed) <-
             WalWriter.consumed_offset_at(writer, baseline_seq) do
        rewind_and_replay(state, cell_id, writer, baseline_seq, consumed)
      end

    log_correction(state, cell_id, baseline_seq, result)
    if match?({:ok, _}, result), do: :ok, else: result
  end

  defp rewind_and_replay(state, cell_id, writer, baseline_seq, consumed) do
    operator_class = Overwatch.operator_class(state)
    started = System.monotonic_time(:millisecond)

    # Explicit, not `current + 1` inside the writer: a re-run of an interrupted
    # correction must re-assert the epoch it already committed to, never bump a
    # second time — a second bump would leave the first attempt's rows behind at
    # an epoch nothing overwrites.
    epoch = WalWriter.outbox_epoch(writer) + 1

    # Blocking: the restore rides the control channel while the replay rides the
    # data channel, and without this fence they race — the runner could fold
    # slice inputs into the very state the rewind was about to replace. A
    # rewind the writer refused cut nothing, so there is no baseline to replay
    # onto and the correction is reported rather than attempted.
    with :ok <- WalWriter.rewind_to(writer, baseline_seq, epoch),
         :ok <- await_restore(state.runner_id, cell_id) do
      # The rewind already truncated the Cell back to its baseline, so a
      # refused restore leaves nothing to replay the slice ONTO. Report the
      # refusal instead: `log_correction/4` surfaces it and the correction is
      # retried, rather than the slice folding onto empty state.
      last = InputLog.last_offset(operator_class)

      if is_integer(last) and last > consumed,
        do: replay_slice(state, InputLog.read_range(operator_class, consumed + 1, last))

      {:ok,
       %{
         baseline_seq: baseline_seq,
         epoch: epoch,
         replayed: if(is_integer(last), do: max(last - consumed, 0), else: 0),
         duration_ms: System.monotonic_time(:millisecond) - started
       }}
    end
  end

  defp log_correction(state, cell_id, _target, {:ok, measurements}) do
    emit_correction(:completed, Overwatch.operator_class(state), cell_id, measurements)
    :ok
  end

  defp log_correction(state, cell_id, target, {:error, reason}) do
    Logger.warning(
      "[Overwatch] correction refused cell=#{inspect(cell_id)} " <>
        "target=#{target}: #{inspect(reason)}"
    )

    emit_correction(:refused, Overwatch.operator_class(state), cell_id, %{reason: reason})
  end

  defp emit_correction(event, operator_class, {_, tasklet, key}, measurements) do
    {reason, measurements} = Map.pop(measurements, :reason)

    :telemetry.execute(
      [:flezha, :correction, event],
      Map.put_new(measurements, :count, 1),
      %{operator: operator_class, tasklet: tasklet, key: key, reason: reason}
    )
  end
end

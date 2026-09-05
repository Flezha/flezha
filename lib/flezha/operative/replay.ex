defmodule Flezha.Operative.Replay do
  @moduledoc """
  Restore-and-replay for one operative: crash resume, late-input correction and
  the operator's `flz operative replay`.

  Restore and correction are one thing, which is why they are one module. A
  correction leaves the on-disk state at a rewound baseline with the input-log
  watermark lowered to match, and from there the ordinary resume replay IS the
  correction — they differ only in how the baseline was chosen.

  Everything here runs in the `Overwatch` process and reads its state without
  changing it. Invariant: the one exception is `arm_bind_reforward/1`, because
  the un-park path drives `Lifecycle.complete_init/4` inside a task where
  `self()` is not the Overwatch.
  """

  alias Flezha.Config.Durations
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Overwatch
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.State.Commands
  alias Flezha.State.DurabilityTier
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.ProcInbox
  alias Flezha.State.ProcJournal
  alias Flezha.State.Restore
  alias Flezha.State.WalTransfer
  alias Flezha.State.WalWriter

  require Logger

  # ---------- process API ----------

  @doc """
  An Entity reported an input behind its event-time high-water but inside the
  correction window. Coalesce it; the debounce fires the replay.
  """
  @spec request_correction(pid(), tuple(), non_neg_integer()) :: :ok
  def request_correction(overwatch, entity_id, target_event_time),
    do: GenServer.cast(overwatch, {:correction_request, entity_id, target_event_time})

  @doc """
  Operator replay (`flz operative replay`): rewind an Entity to `baseline_seq`
  and re-run its retained inputs from there through the current code.

  The same transaction a late input drives, with the baseline chosen rather than
  derived. Synchronous, so the CLI reports the outcome rather than a hopeful
  "queued".
  """
  @spec replay_from(pid(), tuple(), non_neg_integer()) :: :ok | {:error, term()}
  def replay_from(overwatch, entity_id, baseline_seq),
    do: GenServer.call(overwatch, {:replay_from, entity_id, baseline_seq}, 60_000)

  @doc """
  External trigger for the bind-driven per-Entity replay. Called from
  `Flezha.State.Recovery.resync_for_overwatch/2` when a non-bind caller
  (the convergence Loop's rebind heal) needs to re-issue the
  `RestoreInitState` handshake for an already-bound Overwatch.

  Routes the work back to the Overwatch process via a cast so the caller never
  blocks on enumeration / dispatch — the Overwatch is the single owner of
  "enumerate my entities, dispatch RestoreInitState for each".
  """
  @spec kick_restore_for_bound_runner(pid(), binary()) :: :ok
  def kick_restore_for_bound_runner(overwatch_pid, runner_id)
      when is_pid(overwatch_pid) and is_binary(runner_id) do
    GenServer.cast(overwatch_pid, {:kick_restore, runner_id})
  end

  @doc """
  Fold a correction request into the pending set, arming the debounce on the
  first one.

  Concurrent late inputs for one Entity collapse into the one replay that
  reaches furthest back — a burst of five late trades is one restore-replay
  cycle, not five. The debounce is what gives the burst time to arrive; the
  `Map.has_key?` guard is what keeps it a single timer.
  """
  @spec note_correction(Overwatch.t(), tuple(), non_neg_integer()) :: Overwatch.t()
  def note_correction(%Overwatch{} = state, entity_id, target) do
    pending = state.corrections

    if Map.has_key?(pending, entity_id) do
      %{state | corrections: Map.update!(pending, entity_id, &min(&1, target))}
    else
      Process.send_after(self(), {:run_correction, entity_id}, correction_debounce_ms())
      %{state | corrections: Map.put(pending, entity_id, target)}
    end
  end

  defp correction_debounce_ms(),
    do: Durations.get(:correction_debounce_ms)

  # ---------- bind-time restore ----------

  @doc """
  Walk the on-disk Entity state for this operative and dispatch `RestoreInitState`
  to the SDK for each Entity that has a non-empty payload.

  Skipped when the operative arrived via HotSwap — handover already delivers the
  state in-band — and when persistence is disabled for this operative class.
  """
  @spec restore_on_bind(Overwatch.t(), map()) :: :ok
  def restore_on_bind(%Overwatch{arrived_via_hotswap?: true}, _operative),
    do: :ok

  # Tier operatives restored synchronously in `restore_and_replay_journal/1`.
  def restore_on_bind(%Overwatch{effectively_once: true}, _operative),
    do: :ok

  def restore_on_bind(%Overwatch{} = state, operative) do
    dispatch_restore(state, operative.opts, state.runner_id)
    :ok
  end

  @doc """
  Enumerate this operative's persisted Entities and ship a `RestoreInitState` for
  each, off-process — the replay does blocking disk reads and never touches state.

  The bind path reaches it through `restore_on_bind/2`; the
  convergence heal reaches it through the Overwatch's `{:kick_restore, _}` cast.
  """
  @spec dispatch_restore(Overwatch.t(), keyword(), binary()) :: :ok
  def dispatch_restore(%Overwatch{} = state, operative_opts, runner_id) do
    # The SDK keys per-Entity state by the operative INSTANCE id (== `state.id`),
    # so the WAL lives under `state/{operative_id}/...`. Enumerate by the same key.
    operative_class = Keyword.get(operative_opts, :operative_class, state.id)
    overwatch = self()
    source_node = foreign_state_source(operative_opts)

    spawn(fn ->
      # A partition rescale gathers the group's Entities on the node that ran
      # it, so a member placed elsewhere pulls its class tree first. The
      # restore below is the same one every bind runs, and it is what covers
      # the case where the state is already local.
      if source_node, do: WalTransfer.pull_operative(source_node, operative_class)
      replay_persisted_entities(state.builder.kind, operative_class, runner_id, overwatch)
    end)

    :ok
  end

  # `:state_source_node` names the node an operative's durable state was last
  # assembled on. Only a foreign one means anything — a local value is already
  # where the ordinary restore looks.
  defp foreign_state_source(operative_opts) do
    case Keyword.get(operative_opts, :state_source_node) do
      source when is_atom(source) and not is_nil(source) and source != node() -> source
      _ -> nil
    end
  end

  # Bind-time restore is capability-gated on `persistent_state?` (kind+class):
  # `:stream` always; a durable `:workflow` class (this IS its crash-resume)
  # unless it opted out. A stateless kind has no WAL to replay.
  defp replay_persisted_entities(kind, operative_class, runner_id, overwatch) do
    # Transactional outbox re-drive (workflow-transactional-outbox): a procedure
    # journaled `proc_committed` (or `compensated`) whose outbox never reached the
    # plugins owes that output. Re-drive it — with the original keys — before the
    # resumed run advances, so a crash between commit and flush loses nothing.
    if kind == :workflow do
      _ = ProcJournal.flush_pending(operative_class, overwatch)
    end

    # Awaited, not fire-and-forget: the reply carries whether the SDK had to
    # migrate records from an older schema version, and the controller answers
    # that with a compaction. This already runs off-process (the enumeration
    # does blocking disk reads) and Entities are independent, so the awaits run
    # concurrently rather than serialising a large rebind.
    refusals =
      restorable_entities(kind, operative_class)
      |> Flezha.Parallel.map(&await_restore(runner_id, &1),
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

  @doc """
  Await one Entity's `RestoreInitState` and report a decisive verdict.

  The one place the awaited restore's ANSWER is read. `{:error, reason}` covers
  every way an SDK refuses a fold — language, schema, CRC, truncation, apply —
  because they mean one thing here: the durable state did not reach the runner,
  and folding new inputs onto empty state would lose it silently. An Entity
  with no snapshot and a zero-length WAL is a SUCCESS and binds normally.
  """
  @spec await_restore(binary(), Paths.entity_id()) :: :ok | {:error, term()}
  def await_restore(runner_id, entity_id) do
    case Commands.await_restore_init_state(runner_id, entity_id) do
      {:ok, _raw} ->
        :ok

      # The round trip never completed — a timeout, a dead control unit, a
      # runner shutting down. That is not a verdict on the state, and the
      # rebind path already owns a runner that stopped answering. Parking here
      # would turn ordinary teardown churn into stuck operatives.
      {:error, reason} when not is_binary(reason) ->
        Logger.debug(
          "[Overwatch] restore unanswered entity=#{inspect(entity_id)} " <>
            "runner=#{runner_id}: #{inspect(reason)}"
        )

        :ok

      # The SDK answered and refused: the reason is its own error text, joined
      # from the reply's `errors`.
      {:error, reason} ->
        Logger.error(
          "[Overwatch] restore REFUSED entity=#{inspect(entity_id)} runner=#{runner_id}: " <>
            "#{inspect(reason)}; leaving the operative unbound rather than folding new " <>
            "inputs onto empty state"
        )

        :telemetry.execute(
          [:flezha, :state, :restore_refused],
          %{count: 1},
          %{entity: entity_id, runner: runner_id, reason: reason}
        )

        {:error, reason}
    end
  end

  defp restore_concurrency,
    do: Application.get_env(:flezha, :bind_restore_concurrency, 16)

  # Persisted, restorable Entities of this class — empty when the kind/class
  # combination has no persistent state.
  defp restorable_entities(kind, operative_class) do
    if PersistenceResolver.persistent_state?(kind, operative_class) do
      # Remote hydration runs first, and in this one place: a controller
      # adopting an operative whose disk is gone then finds its Entities,
      # proc-journal, durable inbox and input log on disk by the time this walks
      # them — and every caller (bind restore, the tier's awaited restore, the
      # resume-gate count) sees the same set. Inert unless a durability backend
      # is configured and the local state really is absent.
      DurabilityTier.hydrate_operative(operative_class)

      operative_class
      |> Paths.entities_of()
      |> Enum.filter(&Restore.has_persisted_state?/1)
    else
      []
    end
  end

  @doc """
  Number of persisted Entities the bind-time restore path will ship a
  `RestoreInitState` for on this (re)bind — the value threaded onto
  `AddOperative.resume_entity_count` so a durable workflow driver holds its
  first Procedure until that many restores land (crash-resume,
  `workflow-operative-durability`).

  Non-zero only for a durable `:workflow` class with WAL state on disk; `0` for a
  fresh spawn, a stream operative, or an opted-out workflow one — those need
  no restore-before-run gate. Matches the frames `replay_persisted_entities/4`
  ships (same enumeration + `has_persisted_state?` filter).
  """
  @spec resume_entity_count(atom(), binary()) :: non_neg_integer()
  def resume_entity_count(:workflow, operative_class),
    do: length(restorable_entities(:workflow, operative_class))

  def resume_entity_count(_kind, _operative_class), do: 0

  @doc """
  Effectively-once bind path: restore each persisted Entity through the
  awaited control round-trip (the SDK has applied state + consumed floor
  when the reply lands), then re-forward the journaled input tail above
  the min durable consumed-offset.

  Runs in the Overwatch process before plugin activation; Entities ahead of the
  min watermark skip re-forwarded offsets they already consumed (SDK
  `consumed_floor`).
  """
  @spec restore_and_replay_journal(Overwatch.t()) :: {:ok, Overwatch.t()} | {:error, term()}
  def restore_and_replay_journal(%Overwatch{} = state) do
    refused =
      state.builder.kind
      |> restorable_entities(Overwatch.operative_class(state))
      |> Enum.find_value(fn entity_id ->
        # Starting the writer replays its WAL scan: arms the outbox re-drive
        # and repopulates the Entity's durable consumed-offset watermark.
        _ = Flezha.State.Supervisor.find_or_start(entity_id)

        case await_restore(state.runner_id, entity_id) do
          :ok -> nil
          {:error, reason} -> reason
        end
      end)

    # Stop before the journal replay: re-forwarding the input tail onto an
    # Entity whose baseline never landed is the silent loss this guards.
    if refused do
      {:error, refused}
    else
      replay_journaled_inputs(state)
      {:ok, state}
    end
  end

  # ---------- re-forwarding a durable tail ----------

  @doc """
  Re-forward the retained journaled inputs above the min durable
  consumed-offset, stamping each record's original `flz.offset` (never
  re-journaling — the log already holds them, post-`:on_input`).
  """
  @spec replay_journaled_inputs(Overwatch.t()) :: :ok
  def replay_journaled_inputs(%Overwatch{} = state) do
    operative_class = Overwatch.operative_class(state)

    if File.exists?(Paths.input_log_path(operative_class)) do
      floor = InputLog.watermark(operative_class) || -1

      # A refused log answers no tail. Re-forwarding from the prefix that
      # verified would hand the Entity part of its journal as all of it, so
      # nothing is re-forwarded; the refusal was already reported where it was
      # read, and the operative is left to the bind's own restore verdict.
      case InputLog.last_offset(operative_class) do
        last when is_integer(last) and last > floor ->
          replay_slice(state, operative_class, floor + 1, last)

        _no_slice_or_refused ->
          :ok
      end
    end

    :ok
  end

  @doc """
  Re-forward everything this operative's durable logs still owe: the
  effectively-once input tail and the durable workflow's inbox tail.

  The single entry point a data-stream bind takes. Both halves are the ones the
  restore path already calls, so a bind and a restore re-forward through one
  implementation and differ only in what triggered them. Both are self-guarding,
  so a class with neither log does nothing.
  """
  @spec reforward_undelivered_tail(Overwatch.t()) :: Overwatch.t()
  def reforward_undelivered_tail(%Overwatch{} = state) do
    _ = replay_journaled_inputs(state)
    deliver_inbox_tail(state)
  end

  @doc """
  Arm `reforward_undelivered_tail/1` for a payload an unbound stream could not
  carry: ask the operative's channel to announce its next bind, which the
  Overwatch answers with the re-forward. The channel's waiter set IS the arming,
  so a second undelivered payload costs one call and schedules nothing extra.

  `:armed` means a re-forward will happen — including the race where the stream
  came up between the failed delivery and this call, settled by handing the
  Overwatch the notification it just missed. `:unarmed` means the operative has
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
    case ClassRegistry.overwatch_pid(Overwatch.operative_class(state)) do
      {:ok, pid} -> pid
      :error -> self()
    end
  end

  @doc """
  Re-forward every inbox entry above the durable consumed offset, stamped with
  its original offset (never re-journaling — the inbox already holds them).

  Runs on every bind of a durable workflow instance, which is what makes crash
  resume, migration, and rehydrate-from-suspension one path. The durable-inbox
  half of what `replay_journaled_inputs/1` does for the effectively-once tier.
  """
  @spec deliver_inbox_tail(Overwatch.t()) :: Overwatch.t()
  def deliver_inbox_tail(%Overwatch{durable_workflow: false} = state), do: state

  def deliver_inbox_tail(%Overwatch{} = state) do
    state.id
    |> ProcInbox.undelivered()
    |> Enum.map(fn entry ->
      {entry.message,
       Map.put(entry.metadata, "flz.inbox_offset", Integer.to_string(entry.offset))}
    end)
    |> then(&reforward(state, &1))

    state
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
  defp replay_slice(%Overwatch{} = state, operative_class, from, to) do
    case InputLog.read_range(operative_class, from, to) do
      [] ->
        :ok

      # The log refused a record on the way: it serves no range, and the slice
      # is not re-forwarded from the prefix that happened to verify.
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
        # an input reaches exactly one Entity (routed by key), so a marker
        # riding one would strand the slice every *other* Entity buffered.
        if state.correction_window_ms do
          reforward(state, [{"", %{"flz.replay_end" => "1"}}])
        end

        :ok
    end
  end

  # Re-forward already-durable payloads, stamped with the offset they were
  # journaled at. The delivery half of both durability tiers' resume — the
  # effectively-once input log and the durable workflow inbox — and
  # deliberately NOT an ingest: these payloads are already in their log and
  # already ran `:on_input`, so re-journaling them would duplicate them.
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
  journals and forwards input for this operative, so while it is in here no
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
  def run_correction(%Overwatch{} = state, entity_id, target) do
    result =
      with {:ok, writer} <- Flezha.State.Supervisor.find_or_start(entity_id),
           {:ok, %{seq: baseline_seq, consumed_offset: consumed}} <-
             WalWriter.correction_baseline(writer, target) do
        rewind_and_replay(state, entity_id, writer, baseline_seq, consumed)
      end

    log_correction(state, entity_id, target, result)
    state
  end

  @doc """
  The operator's half (`flz operative replay`): the baseline is given, so only
  the consumed offset has to be looked up. Everything downstream is the same
  transaction `run_correction/3` drives.
  """
  @spec run_replay(Overwatch.t(), tuple(), non_neg_integer()) :: :ok | {:error, term()}
  def run_replay(%Overwatch{} = state, entity_id, baseline_seq) do
    result =
      with {:ok, writer} <- Flezha.State.Supervisor.find_or_start(entity_id),
           consumed when is_integer(consumed) <-
             WalWriter.consumed_offset_at(writer, baseline_seq) do
        rewind_and_replay(state, entity_id, writer, baseline_seq, consumed)
      end

    log_correction(state, entity_id, baseline_seq, result)
    if match?({:ok, _}, result), do: :ok, else: result
  end

  defp rewind_and_replay(state, entity_id, writer, baseline_seq, consumed) do
    operative_class = Overwatch.operative_class(state)
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
         :ok <- await_restore(state.runner_id, entity_id) do
      # The rewind already truncated the Entity back to its baseline, so a
      # refused restore leaves nothing to replay the slice ONTO. Report the
      # refusal instead: `log_correction/4` surfaces it and the correction is
      # retried, rather than the slice folding onto empty state.
      last = InputLog.last_offset(operative_class)

      if is_integer(last) and last > consumed,
        do: replay_slice(state, operative_class, consumed + 1, last)

      {:ok,
       %{
         baseline_seq: baseline_seq,
         epoch: epoch,
         replayed: if(is_integer(last), do: max(last - consumed, 0), else: 0),
         duration_ms: System.monotonic_time(:millisecond) - started
       }}
    end
  end

  defp log_correction(state, entity_id, _target, {:ok, measurements}) do
    emit_correction(:completed, Overwatch.operative_class(state), entity_id, measurements)
    :ok
  end

  defp log_correction(state, entity_id, target, {:error, reason}) do
    Logger.warning(
      "[Overwatch] correction refused entity=#{inspect(entity_id)} " <>
        "target=#{target}: #{inspect(reason)}"
    )

    emit_correction(:refused, Overwatch.operative_class(state), entity_id, %{reason: reason})
  end

  defp emit_correction(event, operative_class, {_, procedure, key}, measurements) do
    {reason, measurements} = Map.pop(measurements, :reason)

    :telemetry.execute(
      [:flezha, :correction, event],
      Map.put_new(measurements, :count, 1),
      %{operative: operative_class, procedure: procedure, key: key, reason: reason}
    )
  end
end

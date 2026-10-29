defmodule Flezha.Relay.Replay do
  @moduledoc """
  Restore-and-replay for one relay: crash resume, late-input correction and
  the user's `flz relay replay`.

  Restore and correction are one thing, which is why they are one module. A
  correction leaves the on-disk state at a rewound baseline with the input-log
  watermark lowered to match, and from there the ordinary resume replay IS the
  correction — they differ only in how the baseline was chosen.

  Everything here runs in the `Overwatch` process and reads its state without
  changing it. Invariant: the exceptions are `arm_bind_reforward/1` and
  `dispatch_restore/3`, because the un-park path drives
  `Lifecycle.complete_init/5` inside a task where `self()` is not the Overwatch.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Relay.Calls.Token
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Lookup
  alias Flezha.Relay.Overwatch
  alias Flezha.Process.Timer
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.State.Adoption
  alias Flezha.State.Commands
  alias Flezha.State.DurabilityTier
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal
  alias Flezha.State.Restore
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
  Manual replay (`flz relay replay`): rewind a Cell to `baseline_seq`
  and re-run its retained inputs from there through the current code.

  The same transaction a late input drives, with the baseline chosen rather than
  derived. Synchronous, so the CLI reports the outcome rather than a hopeful
  "queued".
  """
  @spec replay_from(pid(), tuple(), non_neg_integer()) :: :ok | {:error, term()}
  def replay_from(overwatch, cell_id, baseline_seq),
    do:
      GenServer.call(
        overwatch,
        {:replay_from, cell_id, baseline_seq},
        Durations.get(:replay_call_ms)
      )

  @doc """
  External trigger for the bind-driven per-Cell replay. Called from
  `Flezha.State.Recovery.resync_for_overwatch/2` when a non-bind caller
  (the convergence loop's `resync_runner` heal) needs to re-issue the
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
  The carriers that seed a bind, highest first: a handover that reached a runner excludes the
  durable restore of every Cell on disk, which overrides the seed, a graceful stop's final state
  before the init values.
  """
  @spec carrier(Overwatch.t()) :: [:handover | :durable | :shutdown_snapshot | :init]
  def carrier(%Overwatch{arrived_via_hotswap?: true} = state), do: [:handover | seed(state)]
  def carrier(%Overwatch{} = state), do: [:durable | seed(state)]

  defp seed(%Overwatch{snapshot: nil}), do: [:init]
  defp seed(_state), do: [:shutdown_snapshot]

  @doc """
  Walk the on-disk Cell state for this relay and send `RestoreInitState`
  to the SDK for each Cell that has a non-empty payload, awaiting every answer
  before the bind goes on, when the durable restore seeds it (`carrier/1`).
  """
  @spec restore_on_bind(Overwatch.t(), map()) :: :ok
  def restore_on_bind(%Overwatch{} = state, relay) do
    # An awaited restore already ran, before the tail was re-forwarded. This
    # one is awaited too, in the bind: run off-process, its `RestoreInitState`
    # could reach the runner after a hand-over the Overwatch absorbs next and
    # an input after that, resetting the runner's state to the WAL's.
    if :durable in carrier(state) and not awaited_restore?(state) do
      relay_class = Keyword.get(relay.opts, :relay_class, state.id)
      replay_persisted_cells(relay_class, state.runner_id, owner_pid(state))
    end

    :ok
  end

  @doc """
  Whether the bind restores every persisted Cell, awaited, before it
  re-forwards the input tail onto them: a flow relay whose input is
  durable. A steps relay's run holds its tail behind its own restore gate
  instead.
  """
  @spec awaited_restore?(Overwatch.t()) :: boolean()
  def awaited_restore?(%Overwatch{} = state), do: floor_reporter(state) == :cells

  @doc """
  Enumerate this relay's persisted Cells and ship a `RestoreInitState` for
  each, off-process — the replay does blocking disk reads and never touches state.

  The convergence heal reaches it through the Overwatch's `{:kick_restore, _}`
  cast; the bind restores in its own process (`restore_on_bind/2`).
  """
  @spec dispatch_restore(Overwatch.t(), keyword(), binary()) :: :ok
  def dispatch_restore(%Overwatch{} = state, relay_opts, runner_id) do
    # The SDK keys per-Cell state by the relay INSTANCE id (== `state.id`),
    # so the WAL lives under `state/{relay_id}/...`. Enumerate by the same key.
    relay_class = Keyword.get(relay_opts, :relay_class, state.id)
    overwatch = owner_pid(state)

    # Linked to its caller, the Overwatch or its bind task, so it stops with
    # the bind it belongs to.
    spawn_link(fn -> replay_persisted_cells(relay_class, runner_id, overwatch) end)

    :ok
  end

  @doc """
  The relay an arriving instance binds with, once its durable state is the
  newest copy there is (`Flezha.State.Adoption.adopt_newest/3`): the copy on
  the node its `:state_source_node` names, which the hub stamps on a moved
  relay and a partition rescale on a new member, else the backend's. Runs on
  arrival, before anything below reads or opens the state. A source whose copy
  did not land stays on the relay as `:state_source_node`; any other is
  dropped. `owner` is the Overwatch.
  """
  @spec adopt_moved_state(Overwatch.t(), Flezha.Relay.Builder.t(), pid()) ::
          Flezha.Relay.Builder.t()
  def adopt_moved_state(%Overwatch{} = state, relay, owner \\ self()) do
    source = foreign_state_source(relay.opts) || :backend
    relay = drop_source(relay)

    case {Adoption.adopt_newest(Overwatch.relay_class(state), source, owner), source} do
      {:not_landed, {:node, node}} -> put_in(relay.opts[:state_source_node], node)
      _landed_or_from_the_backend -> relay
    end
  end

  @doc """
  What a bind adopts before the runner starts: while this node holds nothing of a
  persistent class, `adopt_moved_state/3` again; else only what this controller
  shipped and lost since, so a lost Cell never ships a cut over its own.
  """
  @spec adopt_unlanded_state(Overwatch.t(), Flezha.Relay.Builder.t(), pid()) ::
          Flezha.Relay.Builder.t()
  def adopt_unlanded_state(%Overwatch{} = state, relay, owner) do
    class = Overwatch.relay_class(state)

    cond do
      not ClassRegistry.persistent?(class) -> relay
      Paths.durable_refs(class) == [] -> adopt_moved_state(state, relay, owner)
      true -> with :ok <- DurabilityTier.hydrate_relay(class, :own), do: drop_source(relay)
    end
  end

  defp drop_source(relay), do: %{relay | opts: Keyword.delete(relay.opts, :state_source_node)}

  # `:state_source_node` names the node a relay's durable state was last
  # assembled on. Only a foreign one means anything — a local value is already
  # where the ordinary restore looks.
  defp foreign_state_source(relay_opts) do
    case Keyword.get(relay_opts, :state_source_node) do
      source when is_atom(source) and not is_nil(source) and source != node() -> {:node, source}
      _ -> nil
    end
  end

  # Bind-time restore follows the class's persistence switch, for either kind;
  # for a steps relay it IS its crash-resume.
  defp replay_persisted_cells(relay_class, runner_id, overwatch) do
    # Awaited, not fire-and-forget: the reply carries whether the SDK had to
    # migrate records from an older schema version, and the controller answers
    # that with a compaction. Cells are independent, so the awaits run
    # concurrently rather than serialising a large rebind.
    refusals =
      restorable_cells(relay_class)
      |> Flezha.Parallel.map(&restore_cell(runner_id, &1),
        max_concurrency: restore_concurrency()
      )
      |> Enum.filter(&match?({:error, _}, &1))

    # The verdict travels to the Overwatch as a message, the one thing that can
    # stop the relay, whether this ran in the bind or in a heal's process.
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
  Re-derive what a durable steps relay's running Cell writers hold from its
  journal, before the bound runner writes anything newer. A writer still
  running from before this bind holds outputs of stages the journal may never
  name (an attempt the rebound runner interrupted, an iteration a rewind
  unwound), and drops them; a writer that is not running derives the same when
  it boots.
  """
  @spec seed_steps_outboxes(Overwatch.t()) :: :ok
  def seed_steps_outboxes(%Overwatch{durable_steps: true} = state) do
    relay_class = Overwatch.relay_class(state)
    committed = TaskJournal.committed_records(relay_class)

    for writer <- Flezha.State.Supervisor.live_writers(relay_class),
        do: WalWriter.seed_commits(writer, committed)

    :ok
  end

  def seed_steps_outboxes(%Overwatch{}), do: :ok

  @doc """
  Hand a new `owner`, before it is recorded, what the Cell writers sent an
  Overwatch now dead or left owed, so it lands ahead of anything newer.
  """
  @spec redrive_outboxes(Overwatch.t(), pid()) :: :ok
  def redrive_outboxes(%Overwatch{} = state, owner) do
    class = Overwatch.relay_class(state)

    if state.owes_outputs or match?({:error, _}, Lookup.overwatch(class)),
      do: Enum.each(Flezha.State.Supervisor.live_writers(class), &WalWriter.redrive(&1, owner))

    :ok
  end

  @doc """
  Ask this relay's running Cell writers to dispatch again, now, every call
  their outboxes hold from before this Overwatch started: how many entries hold
  call `id` still to reach it. An Overwatch restarted alone relearns its calls
  from those dispatches.
  """
  @spec redrive_calls(Overwatch.t(), binary()) :: non_neg_integer()
  def redrive_calls(%Overwatch{} = state, id) do
    state
    |> Overwatch.relay_class()
    |> Flezha.State.Supervisor.live_writers()
    |> Enum.map(&WalWriter.redrive_calls(&1, state.started_mono, id))
    |> Enum.filter(&is_integer/1)
    |> Enum.sum()
  end

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
    case runner_id |> Commands.await_restore_init_state(cell_id) |> sdk_verdict() do
      {:ok, _raw} ->
        :ok

      # Never written, with the runner live and draining nothing: it holds none
      # of this Cell, so the bind stops here as it does on a refusal.
      {:error, :send_buffer_full} = unsent ->
        unsent

      # The round trip never completed — a timeout, a dead control unit, a
      # runner shutting down. That is not a verdict on the state, and the
      # rebind path already owns a runner that stopped answering. Parking here
      # would turn ordinary teardown churn into stuck relays.
      {:error, reason} when not is_binary(reason) ->
        Logger.debug(
          "[Overwatch] restore unanswered cell=#{inspect(cell_id)} " <>
            "runner=#{runner_id}: #{inspect(reason)}"
        )

        :ok

      # Refused, by the SDK or by the controller's own read of the records:
      # the reason is the refusal's text.
      {:error, reason} ->
        :telemetry.execute(
          [:flezha, :state, :restore_refused],
          %{count: 1},
          %{cell: cell_id, runner: runner_id, reason: reason}
        )

        {:error, reason}
    end
  end

  # The SDK's refusal arrives as the runner's error; its text is the verdict.
  defp sdk_verdict({:error, %Flezha.Error{message: text}}), do: {:error, text}
  defp sdk_verdict(answer), do: answer

  defp restore_concurrency,
    do: Config.get(:bind_restore_concurrency)

  # Persisted, restorable Cells of this class, empty when its persistence is
  # off. Only listed: a copy comes in before the runner starts
  # (`adopt_unlanded_state/3`), never under its writers.
  defp restorable_cells(relay_class) do
    if ClassRegistry.persistent?(relay_class) do
      relay_class
      |> Paths.cells_of()
      |> Enum.filter(&Restore.has_persisted_state?/1)
    else
      []
    end
  end

  @doc """
  Number of persisted Cells the bind-time restore path will ship a
  `RestoreInitState` for on this (re)bind — the value threaded onto
  `AddRelay.resume_cell_count` so a durable steps driver holds its
  first Tasklet until that many restores land (crash-resume,
  `relay-durability`).

  `0` for a fresh spawn and for a class whose persistence is off. Matches the
  frames `replay_persisted_cells/3` ships (same enumeration +
  `has_persisted_state?` filter); only a steps relay's run waits on it.
  """
  @spec resume_cell_count(binary()) :: non_neg_integer()
  def resume_cell_count(relay_class),
    do: length(restorable_cells(relay_class))

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
      |> Overwatch.relay_class()
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

  # ---------- effect results ----------

  # Batches stay well under the 64 MiB frame cap; one result is at most the
  # runner's 4 MiB effect cap.
  @effect_batch_bytes 16 * 1024 * 1024

  @doc """
  Ship `results` (`Flezha.State.Commands.effect_results/1`, counted on the
  bind's `AddRelay`) to the bound runner in batches, awaiting each, before the
  bind re-forwards the input tail: a flow relay's re-run then finds them, and a
  steps relay's gate counts them.
  """
  @spec ship_effect_results(Overwatch.t(), [%RunnerCommands.EffectResult{}]) :: :ok
  def ship_effect_results(%Overwatch{} = state, results) do
    results
    |> Enum.chunk_while({[], 0}, &batch_result/2, &close_batch/1)
    |> Enum.each(&Commands.ship_effect_results(state.runner_id, state.id, &1))
  end

  defp batch_result(result, {batch, bytes}) do
    size = byte_size(result.payload)

    if batch != [] and bytes + size > @effect_batch_bytes,
      do: {:cont, Enum.reverse(batch), {[result], size}},
      else: {:cont, {[result | batch], bytes + size}}
  end

  defp close_batch({[], _bytes}), do: {:cont, {[], 0}}
  defp close_batch({batch, _bytes}), do: {:cont, Enum.reverse(batch), {[], 0}}

  # ---------- the instance's floor ----------

  @doc """
  Who reports the instance's floor in its input log, which is what makes its
  input durable: its journal when it is a durable steps relay, its Cells'
  own records when it is a flow relay on the effectively-once tier or
  one that sleeps, or nobody, and its input is not journaled.
  """
  @spec floor_reporter(Overwatch.t()) :: :cells | :journal | nil
  def floor_reporter(%Overwatch{durable_steps: true}), do: :journal

  def floor_reporter(%Overwatch{} = state) when state.effectively_once or state.sleeps,
    do: :cells

  def floor_reporter(%Overwatch{}), do: nil

  @doc """
  Seed the instance's floor in its input log from its own durable records,
  before the log is asked anything: a steps relay's journal, and a
  sleeping flow relay's Cells when no Cell row is held (a controller
  restart lost them, and a sleeper boots no Cell writer). Rows only move
  forward, so seeding again is harmless.
  """
  @spec seed_floor(Overwatch.t()) :: :ok
  def seed_floor(%Overwatch{} = state) do
    case floor_reporter(state) do
      :journal -> TaskJournal.release_input(state.id)
      :cells when state.sleeps -> seed_cell_floors(Overwatch.relay_class(state))
      _no_floor_to_seed -> :ok
    end
  end

  defp seed_cell_floors(relay_class) do
    if InputLog.cell_offsets(relay_class) == [] do
      for cell_id <- Paths.cells_of(relay_class),
          offset = WalWriter.consumed_offset_on_disk(cell_id),
          is_integer(offset) and offset >= 0,
          do: InputLog.record_consumed(relay_class, cell_id, offset)
    end

    :ok
  end

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

  A bind in flight, still on the channel it leaves, is marked to re-forward.
  """
  @spec reforward_undelivered_tail(Overwatch.t()) :: Overwatch.t()
  def reforward_undelivered_tail(%Overwatch{bind_task: %{} = task} = state),
    do: %Overwatch{state | bind_task: Map.put(task, :reforward, true)}

  def reforward_undelivered_tail(%Overwatch{} = state) do
    drop_unrecorded_turns(state)
    redeliver_edges(state)
    replay_slice(state, InputLog.undelivered(Overwatch.relay_class(state)))
    state
  end

  # A durable steps relay's turn that was parked but never reached its
  # `dead_lettered` record runs again over this tail, so its entry goes first.
  defp drop_unrecorded_turns(%Overwatch{durable_steps: true} = state),
    do: TaskJournal.Turns.drop_unrecorded_turns(Overwatch.relay_class(state))

  defp drop_unrecorded_turns(%Overwatch{}), do: :ok

  # A flow relay's inter-tasklet value is durable in its producer's
  # record wherever that record carries the offset: the values kept for inputs
  # above the floor go first, in origin order, and a consumer whose own records
  # acknowledge an origin drops its value. With nothing above the floor no value
  # can be owed, and no record is read. A steps relay's are its journal's.
  # Each value carries the call its origin input carried, so a downstream answer
  # still reaches a caller who waits.
  defp redeliver_edges(%Overwatch{durable_steps: true}), do: :ok

  defp redeliver_edges(%Overwatch{} = state) do
    relay_class = Overwatch.relay_class(state)

    with {:ok, floor, last} when last > floor <- InputLog.tail(relay_class) do
      edges =
        relay_class
        |> Paths.cells_of()
        |> Enum.flat_map(&WalWriter.edges_above(&1, floor))
        |> Enum.sort_by(& &1.origin)

      calls = origin_calls(relay_class, edges)

      edges
      |> Enum.map(&edge_payload(&1, calls))
      |> then(&reforward(state, &1))
    end

    :ok
  end

  # The token each origin's input-log record carries, read in one range.
  defp origin_calls(_relay_class, []), do: %{}

  defp origin_calls(relay_class, edges),
    do: InputLog.calls(relay_class, hd(edges).origin, List.last(edges).origin)

  defp edge_payload(edge, calls) do
    metadata = %{
      "flz.edge" => "#{edge.channel}:#{edge.consumer}",
      "flz.offset" => Integer.to_string(edge.origin)
    }

    case Map.fetch(calls, edge.origin) do
      {:ok, token} -> {edge.payload, Map.put(metadata, Token.reply_to_key(), token)}
      :error -> {edge.payload, metadata}
    end
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
  carry: ask the relay's channel to announce its next bind, which the
  Overwatch answers with the re-forward. The channel's waiter set IS the arming,
  so a second undelivered payload costs one call and schedules nothing extra.

  `:armed` means a re-forward will happen — including the race where the stream
  came up between the failed delivery and this call, settled by handing the
  Overwatch the notification it just missed. `:unarmed` means the relay has
  no channel to ask yet; the log still holds the payload, and the bind that
  gives it one re-forwards the tail, as a bind in flight does on adoption.
  """
  @spec arm_bind_reforward(Overwatch.t()) :: :armed | :unarmed
  def arm_bind_reforward(%Overwatch{bind_task: %{}}), do: :armed

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
    case Lookup.overwatch(Overwatch.relay_class(state)) do
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
  @doc false
  def replay_slice(%Overwatch{} = state, read) do
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
          reforward(state, [{"", %{"flz.control" => "replay_end"}}])
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
  journals and forwards input for this relay, so while it is in here no
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
  The user's half (`flz relay replay`): the baseline is given, so only
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
    relay_class = Overwatch.relay_class(state)
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
      last = InputLog.last_offset(relay_class)

      if is_integer(last) and last > consumed,
        do: replay_slice(state, InputLog.read_range(relay_class, consumed + 1, last))

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
    emit_correction(:completed, Overwatch.relay_class(state), cell_id, measurements)
    :ok
  end

  defp log_correction(state, cell_id, target, {:error, reason}) do
    emit_correction(:refused, Overwatch.relay_class(state), cell_id, %{
      reason: reason,
      target: target
    })
  end

  defp emit_correction(event, relay_class, {_, tasklet, key}, measurements) do
    {why, measurements} = Map.split(measurements, [:reason, :target])

    :telemetry.execute(
      [:flezha, :correction, event],
      Map.put_new(measurements, :count, 1),
      Map.merge(%{relay: relay_class, tasklet: tasklet, key: key, reason: nil}, why)
    )
  end
end
